const schedule = require("node-schedule");
const Airtable = require("airtable");
const { google } = require('googleapis');
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_WB
);
const { client } = require("../../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../../GoogleAuth");

async function fetchReportDataDaily(req, res) {
  const token = getStoredRefreshToken();

  if (!token.refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_WB,
      refresh_token: token.refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const startOfMonth = new Date();
    startOfMonth.setDate(1);

    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    const formattedStartOfMonth = startOfMonth.toISOString().split("T")[0].replace(/-/g, "");
    const formattedYesterday = yesterday.toISOString().split("T")[0].replace(/-/g, "");

    const metricsQuery = `
      SELECT
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        campaign.primary_status,
        segments.date
      FROM
        campaign
      WHERE
        segments.date BETWEEN '20241101' AND '20241231'
      ORDER BY
        segments.date DESC
    `;

    const formattedMetricsMap = {};
    let metricsPageToken = null;

    do {
      const metricsResponse = await customer.query(metricsQuery);
      metricsResponse.forEach((campaign) => {
        const key = `${campaign.campaign.id}-${campaign.segments.date}`;
        formattedMetricsMap[key] = {
          name: campaign.campaign.name,
          impressions: campaign.metrics.impressions,
          clicks: campaign.metrics.clicks,
          spend: campaign.metrics.cost_micros / 1_000_000,
          primary: campaign.campaign.primary_status,
          date: campaign.segments.date,
        };
      });
      // console.log(metricsResponse)
      metricsPageToken = metricsResponse.next_page_token;
    } while (metricsPageToken);

    const formattedMetrics = Object.values(formattedMetricsMap);

    // await sendToAirtableDaily(formattedMetrics);
    await writeToGoogleSheet(formattedMetrics);
    return formattedMetrics;

    // res.json(formattedMetrics);
  } catch (error) {
    console.error("Error fetching report data:", error);
    res.status(500).send("Error fetching report data");
  }
};

async function sendToAirtableDaily(data) {
  const recordsToUpdate = [];
  const recordsToCreate = [];

  const validFields = [
    "Impr.",
    "Clicks",
    "Spend",
    "Day",
    "Campaign",
    "Primary",
  ];

  const validateFields = (fields) => {
    const invalidFields = Object.keys(fields).filter(
      (key) => !validFields.includes(key)
    );
    if (invalidFields.length > 0) {
      console.warn(
        `Warning: Invalid field(s) detected and excluded: ${invalidFields.join(
          ", "
        )}`
      );
      for (const field of invalidFields) {
        delete fields[field];
      }
    }
  };

  for (const record of data) {
    const campaignName = record.name;
    const recordDate = record.date;

    try {
      const existingRecords = await base("Daily Report")
        .select({
          filterByFormula: `AND({Campaign} = '${campaignName}', 
          DATETIME_FORMAT({Day}, 'YYYY-MM-DD') = '${recordDate}')`,
        })
        .firstPage();

      if (existingRecords.length > 0) {
        const existingRecordId = existingRecords[0].id;
        const fieldsToUpdate = {
          "Impr.": record.impressions,
          Clicks: record.clicks,
          Spend: record.spend,
          Primary: record.primary,
        };
        validateFields(fieldsToUpdate);
        recordsToUpdate.push({
          id: existingRecordId,
          fields: fieldsToUpdate,
        });
      } else {
        const fieldsToCreate = {
          Day: recordDate,
          Campaign: campaignName,
          "Impr.": record.impressions,
          Clicks: record.clicks,
          Spend: record.spend,
          Primary: record.primary,
        };
        validateFields(fieldsToCreate);
        recordsToCreate.push({ fields: fieldsToCreate });
      }
    } catch (error) {
      console.error(
        `Error processing record for Campaign Name: ${campaignName} on Date: ${recordDate}`,
        error
      );
    }
  }

  const batchProcess = async (records, operation, label) => {
    for (let i = 0; i < records.length; i += 10) {
      const batch = records.slice(i, i + 10);
      try {
        if (operation === "update") {
          await base("Daily Report").update(batch);
        } else if (operation === "create") {
          await base("Daily Report").create(batch);
        }

        // console.log(`${label}: Successfully processed ${batch.length} records in a batch.`);
      } catch (error) {
        console.error(`${label}: Error processing batch`, error);
      }
    }
  };

  if (recordsToUpdate.length > 0) {
    await batchProcess(recordsToUpdate, "update", "Update");
  }

  if (recordsToCreate.length > 0) {
    await batchProcess(recordsToCreate, "create", "Create");
  }

  console.log("Daily Airtable sync process completed.");
};

async function writeToGoogleSheet(data) {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = '1ebtCKUsiBhOcK08gYKv8erd8MIXdIL7tkaFeLj6XE4A'; // Replace with your Google Sheets ID
  const range = 'Sheet1!A1:F'; // Assuming the data is in columns A to F, adjust as needed

  // Headers for the sheet
  const headers = ["Campaign", "Day", "Impr.", "Clicks", "Spend", "Primary"];

  // Function to validate field names (headers)
  const validateFields = (fields) => {
    const invalidFields = fields.filter((field) => !headers.includes(field));
    if (invalidFields.length > 0) {
      console.warn(`Warning: Invalid field(s) detected and excluded: ${invalidFields.join(", ")}`);
    }
  };

  // Clear existing data in the range before writing new data
  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range, // This clears the range where data will be written (A1:F for this case)
    });
    console.log("Existing data cleared successfully.");
  } catch (error) {
    console.error("Error clearing existing data:", error);
  }

  // Prepare the data
  const recordsToCreate = [];

  // Add headers to the sheet first
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: 'Sheet1!A1', // Set headers starting at A1
    valueInputOption: 'RAW',
    resource: {
      values: [headers], // The first row is the header row
    },
  });

  // Prepare data rows
  for (const record of data) {
    const newRow = [
      record.name,      // Campaign
      record.date,      // Day
      record.impressions, // Impressions
      record.clicks,      // Clicks
      record.spend,       // Spend
      record.primary,     // Primary
    ];

    // Validate row structure (fields) against header columns
    validateFields(newRow);

    // Add the row to recordsToCreate
    recordsToCreate.push(newRow);
  }

  // Create new rows without batching
  const batchCreate = async (records) => {
    const range = 'Sheet1!A2'; // Start appending at row 2 (after the headers)
    const values = records; // Just the new rows to be appended

    const resource = { values };

    try {
      const response = await sheets.spreadsheets.values.append({
        spreadsheetId,
        range,
        valueInputOption: 'RAW',
        resource,
      });
      console.log(`${response.data.updates.updatedCells} cells added in Google Sheets.`);
    } catch (error) {
      console.error("Error adding new rows to Google Sheets:", error);
    }
  };

  // Create new records (since we cleared the sheet and added headers)
  if (recordsToCreate.length > 0) {
    await batchCreate(recordsToCreate);
  }

  console.log("Google Sheets sync process completed.");
}

module.exports = {
  fetchReportDataDaily
};
