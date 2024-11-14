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

    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1); // Move the date to yesterday
    const formattedYesterday = yesterday
      .toISOString()
      .split("T")[0]
      .replace(/-/g, "");

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
        segments.date = '${formattedYesterday}'
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
        segments.date = '${formattedYesterday}'
        AND segments.conversion_action_name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation') 
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
          cost: campaign.metrics.cost_micros / 1_000_000,
          date: campaign.segments.date,
          step1Value: 0,
          step5Value: 0,
          step6Value: 0,
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
}

async function sendToAirtableDaily(data) {
  const recordsToUpdate = [];
  const recordsToCreate = [];

  for (const record of data) {
    const campaignName = record.name;
    const recordDate = record.date;

    try {
      const existingRecords = await base("Daily Report").select({
        filterByFormula: `AND({Campaign} = '${campaignName}', 
        DATETIME_FORMAT({Day}, 'YYYY-MM-DD') = '${recordDate}')`,
      }).firstPage();

      if (existingRecords.length > 0) {
        const existingRecordId = existingRecords[0].id;
        recordsToUpdate.push({
          id: existingRecordId,
          fields: {
            "Impr.": record.impressions,
            Clicks: record.clicks,
            Cost: record.cost,
            "Book Now - Step 1: Locations": record.step1Value,
            "Book Now - Step 5: Confirm Booking": record.step5Value,
            "Book Now - Step 6: Booking Confirmation": record.step6Value,
          },
        });
        console.log(`Record prepared for update for Campaign Name: ${campaignName} on Date: ${recordDate}`);
      } else {
        recordsToCreate.push({
          fields: {
            Day: recordDate,
            Campaign: campaignName,
            "Impr.": record.impressions,
            Clicks: record.clicks,
            Cost: record.cost,
            "Book Now - Step 1: Locations": record.step1Value,
            "Book Now - Step 5: Confirm Booking": record.step5Value,
            "Book Now - Step 6: Booking Confirmation": record.step6Value,
          },
        });
        console.log(`Record prepared for creation for Campaign Name: ${campaignName} on Date: ${recordDate}`);
      }
    } catch (error) {
      console.error(`Error processing record for Campaign Name: ${campaignName} on Date: ${recordDate}`,error);
    }
  }

  const batchProcessUpdates = async (records) => {
    for (let i = 0; i < records.length; i += 10) {
      const batch = records.slice(i, i + 10);
      await base("Daily Report").update(batch);
      console.log(`Updated ${batch.length} records in batch.`);
    }
  };

  const batchProcessCreations = async (records) => {
    for (let i = 0; i < records.length; i += 10) {
      const batch = records.slice(i, i + 10);
      await base("Daily Report").create(batch);
      console.log(`Created ${batch.length} records in batch.`);
    }
  };

  if (recordsToUpdate.length > 0) {
    await batchProcessUpdates(recordsToUpdate);
    console.log("Daily Automation Update Done");
  }

  if (recordsToCreate.length > 0) {
    await batchProcessCreations(recordsToCreate);
    console.log("Daily Automation Create Done");
  }
}

async function testFetchDaily(req, res) {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const metricsQuery = `
    SELECT
      conversion_action.name,
      metrics.all_conversions,
      segments.date
    FROM
      conversion_action
    WHERE
      segments.date = '20240923'
    ORDER BY
      segments.date DESC
    LIMIT 100`;

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const metricsResponse = await customer.query(metricsQuery);

    res.json(metricsResponse);
    // console.log(metricsResponse);
  } catch (error) {
    console.error("Error fetching report data:", error);
    res.status(500).send("Error fetching report data");
  }
}

// const rule = new schedule.RecurrenceRule();
// rule.hour = 7;
// rule.minute = 0;
// rule.tz = "America/Los_Angeles";

// const dailyReportJob = schedule.scheduleJob(rule, () => {
//   fetchReportDataDaily();
//   console.log("Scheduled daily report sent at 7 AM PST California/Irvine.");
// });

module.exports = {
  fetchReportDataDaily,
  testFetchDaily,
};
