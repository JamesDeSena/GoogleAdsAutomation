const schedule = require("node-schedule");
const Airtable = require("airtable");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_HISKIN
);
const { client } = require("../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("./GoogleAuth");

async function fetchReportDataDaily(req, res) {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: refreshToken_Google,
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
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        segments.date
      FROM
        campaign
      WHERE
        segments.date BETWEEN '${formattedStartOfMonth}' AND '${formattedYesterday}'
      ORDER BY
        segments.date DESC
    `;

    const conversionQuery = `
      SELECT 
        campaign.id,
        metrics.all_conversions,
        segments.conversion_action_name,
        segments.date 
      FROM 
        campaign
      WHERE 
        segments.date BETWEEN '${formattedStartOfMonth}' AND '${formattedYesterday}'
        AND segments.conversion_action_name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation', 'BookingConfirmed') 
      ORDER BY 
        segments.date DESC`;

    const formattedMetricsMap = {};
    let metricsPageToken = null;

    do {
      const metricsResponse = await customer.query(metricsQuery);
      metricsResponse.forEach((campaign) => {
        const key = `${campaign.campaign.id}-${campaign.segments.date}`;
        formattedMetricsMap[key] = {
          id: campaign.campaign.id,
          name: campaign.campaign.name,
          impressions: campaign.metrics.impressions,
          clicks: campaign.metrics.clicks,
          spend: campaign.metrics.cost_micros / 1_000_000,
          date: campaign.segments.date,
          step1Value: 0,
          step5Value: 0,
          step6Value: 0,
          bookingConfirmed: 0,
        };
      });

      metricsPageToken = metricsResponse.next_page_token;
    } while (metricsPageToken);

    const conversionResponse = [];
    let conversionPageToken = null;

    do {
      const conversionBatchResponse = await customer.query(conversionQuery);
      conversionResponse.push(...conversionBatchResponse);

      conversionPageToken = conversionBatchResponse.next_page_token;
    } while (conversionPageToken);

    conversionResponse.forEach((conversion) => {
      const key = `${conversion.campaign.id}-${conversion.segments.date}`;
      const conversionValue = conversion.metrics.all_conversions;

      if (conversion.segments.conversion_action_name === "Book Now - Step 1: Locations") {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].step1Value += conversionValue;
        }
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 5:Confirm Booking (Initiate Checkout)") {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].step5Value += conversionValue;
        }
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 6: Booking Confirmation") {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].step6Value += conversionValue;
        }
      } else if (conversion.segments.conversion_action_name === "BookingConfirmed") {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].bookingConfirmed += conversionValue;
        }
      }
      
    });

    const formattedMetrics = Object.values(formattedMetricsMap);

    await sendToAirtableDaily(formattedMetrics);
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
    "Book Now - Step 1: Locations",
    "Book Now - Step 5: Confirm Booking",
    "Book Now - Step 6: Booking Confirmation",
    "Day",
    "Campaign",
    "Booking Confirmed",
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
          "Book Now - Step 1: Locations": record.step1Value,
          "Book Now - Step 5: Confirm Booking": record.step5Value,
          "Book Now - Step 6: Booking Confirmation": record.step6Value,
          "Booking Confirmed": record.bookingConfirmed,
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
          "Book Now - Step 1: Locations": record.step1Value,
          "Book Now - Step 5: Confirm Booking": record.step5Value,
          "Book Now - Step 6: Booking Confirmation": record.step6Value,
          "Booking Confirmed": record.bookingConfirmed,
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
        console.log(
          `${label}: Successfully processed ${batch.length} records in a batch.`
        );
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

module.exports = {
  fetchReportDataDaily
};
