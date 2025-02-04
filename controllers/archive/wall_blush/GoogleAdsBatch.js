const schedule = require("node-schedule");
const Airtable = require("airtable");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_WB
);
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

async function fetchReportDataBatch(req, res) {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_WB,
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
        campaign.name,
        campaign.primary_status,
        campaign_budget.amount_micros,
        campaign.target_roas.target_roas,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.search_impression_share,
        metrics.cost_per_conversion,
        metrics.conversions,
        metrics.conversions_by_conversion_date,
        metrics.conversions_value_by_conversion_date,
        segments.date
      FROM
        campaign
      WHERE
        segments.date BETWEEN '20241201' AND '20241201'
      ORDER BY
        segments.date DESC
    `;

    const formattedMetricsMap = {};
    let metricsPageToken = null;

    do {
      const metricsResponse = await customer.query(metricsQuery);
      metricsResponse.forEach((campaign) => {
        const key = `${campaign.campaign.name}-${campaign.segments.date}`;
        formattedMetricsMap[key] = {
          name: campaign.campaign.name,
          budget: campaign.campaign_budget.amount_micros,
          search_impr: campaign.metrics.search_impression_share,
          cvalue_cost: campaign.metrics.conversions_value_by_conversion_date / campaign.metrics.cost_micros,
          target: campaign.campaign.target_roas,
          impressions: campaign.metrics.impressions,
          clicks: campaign.metrics.clicks,
          cost: campaign.metrics.cost_micros / 1_000_000,
          primary: campaign.campaign.primary_status,
        };
      });
      console.log(metricsResponse);
      metricsPageToken = metricsResponse.next_page_token || null;
    } while (metricsPageToken);

    const formattedMetrics = Object.values(formattedMetricsMap);

    // await sendToAirtableDaily(formattedMetrics);
    // return formattedMetrics;

    res.json(formattedMetrics);
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
    // const recordDate = record.date;

    try {
      const existingRecords = await base("Daily Report")
        .select({
          filterByFormula: `AND({Campaign} = '${campaignName}'`,
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
          await base("Batch Report").update(batch);
        } else if (operation === "create") {
          await base("Batch Report").create(batch);
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

module.exports = {
  fetchReportDataBatch
};
