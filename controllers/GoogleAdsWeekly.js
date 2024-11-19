const schedule = require("node-schedule");
const Airtable = require("airtable");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_HISKIN
);
const { client } = require("../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("./GoogleAuth");

const refreshToken_Google = getStoredRefreshToken();

if (!refreshToken_Google) {
  console.error("Access token is missing. Please authenticate.");
  return;
}

const getCustomer = () =>
  client.Customer({
    customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
    refresh_token: refreshToken_Google,
    login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
  });

let storedDateRanges = null;

const generateWeeklyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentStartDate = new Date(startDate);

  while (currentStartDate <= endDate) {
    let currentEndDate = new Date(currentStartDate);
    currentEndDate.setDate(currentStartDate.getDate() + 6);

    if (currentEndDate <= endDate) {
      dateRanges.push({
        start: currentStartDate.toISOString().split("T")[0],
        end: currentEndDate.toISOString().split("T")[0],
      });
    }

    currentStartDate.setDate(currentStartDate.getDate() + 7);
  }

  return dateRanges;
};

const getOrGenerateDateRanges = () => {
  const today = new Date();
  const dayOfWeek = today.getDay();
  const daysSinceFriday = (dayOfWeek + 1) % 7;

  const previousFriday = new Date(today);
  previousFriday.setDate(today.getDate() - daysSinceFriday);

  const currentThursday = new Date(previousFriday);
  currentThursday.setDate(previousFriday.getDate() + 6);

  const startDate = '2024-09-13'; //previousFriday 2024-09-13
  const fixedEndDate = '2024-11-07'; // currentThursday

  const endDate = currentThursday //new Date(fixedEndDate);

  if (
    !storedDateRanges ||
    new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate
  ) {
    storedDateRanges = generateWeeklyDateRanges(startDate, endDate);
  }

  return storedDateRanges;
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const sendToAirtable = async (data, tableName, field) => {
  for (const record of data) {
    const dateField = record.date;
    const recordDate = dateField.split(" - ")[0];
    try {
      const existingRecords = await base(tableName)
        .select({
          filterByFormula: `{${field}} = '${dateField}'`,
        })
        .firstPage();
      const recordFields = {
        [field]: dateField,
        "Impr.": record.impressions,
        Clicks: record.clicks,
        Cost: record.cost,
        "Book Now - Step 1: Locations": record.step1Value,
        "Book Now - Step 5: Confirm Booking": record.step5Value,
        "Book Now - Step 6: Booking Confirmation": record.step6Value,
      };
      if (existingRecords.length > 0) {
        await base(tableName).update(existingRecords[0].id, recordFields);
        console.log(
          `Record updated for Date: ${recordDate} Table: ${tableName}`
        );
      } else {
        await base(tableName).create(recordFields);
        console.log(
          `Record created for Date: ${recordDate} Table: ${tableName}`
        );
      }
    } catch (error) {
      console.error(`Error processing record for Date: ${recordDate}`, error);
    }
  }
};

const fetchReportDataWeekly = async (req, res) => {
  try {
    const customer = getCustomer();

    const dateRanges = getOrGenerateDateRanges();

    const aggregateDataForWeek = async (startDate, endDate) => {
      const aggregatedData = {
        date: `${startDate} - ${endDate}`,
        impressions: 0,
        clicks: 0,
        cost: 0,
        step1Value: 0,
        step5Value: 0,
        step6Value: 0,
      };

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
          segments.date BETWEEN '${startDate}' AND '${endDate}'
        ORDER BY
          segments.date DESC
      `;

      const conversionQuery = `
        SELECT 
          metrics.all_conversions,
          conversion_action.name,
          segments.date 
        FROM 
          conversion_action
        WHERE 
          segments.date BETWEEN '${startDate}' AND '${endDate}'
          AND conversion_action.name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation') 
        ORDER BY 
          segments.date DESC
      `;

      let metricsPageToken = null;
      do {
        const metricsResponse = await customer.query(metricsQuery);
        metricsResponse.forEach((campaign) => {
          aggregatedData.impressions += campaign.metrics.impressions || 0;
          aggregatedData.clicks += campaign.metrics.clicks || 0;
          aggregatedData.cost +=
            (campaign.metrics.cost_micros || 0) / 1_000_000;
        });
        metricsPageToken = metricsResponse.next_page_token;
      } while (metricsPageToken);

      let conversionPageToken = null;
      do {
        const conversionBatchResponse = await customer.query(conversionQuery);
        conversionBatchResponse.forEach((conversion) => {
          const conversionValue = conversion.metrics.all_conversions || 0;
          if (
            conversion.conversion_action.name === "Book Now - Step 1: Locations"
          ) {
            aggregatedData.step1Value += conversionValue;
          } else if (
            conversion.conversion_action.name ===
            "Book Now - Step 5:Confirm Booking (Initiate Checkout)"
          ) {
            aggregatedData.step5Value += conversionValue;
          } else if (
            conversion.conversion_action.name ===
            "Book Now - Step 6: Booking Confirmation"
          ) {
            aggregatedData.step6Value += conversionValue;
          }
        });
        conversionPageToken = conversionBatchResponse.next_page_token;
      } while (conversionPageToken);

      return aggregatedData;
    };

    const allWeeklyData = [];
    for (const { start, end } of dateRanges) {
      const weeklyData = await aggregateDataForWeek(start, end);
      allWeeklyData.push(weeklyData);
    }

    // await sendToAirtable(allWeeklyData, "All Weekly Report", "All Search");
    return allWeeklyData;

    // res.json(allWeeklyData);
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const aggregateDataForWeek = async (
  customer,
  startDate,
  endDate,
  campaignNameFilter
) => {
  const aggregatedData = {
    date: `${startDate} - ${endDate}`,
    impressions: 0,
    clicks: 0,
    cost: 0,
    step1Value: 0,
    step5Value: 0,
    step6Value: 0,
  };

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
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name LIKE '%${campaignNameFilter}%'
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
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name LIKE '%${campaignNameFilter}%'
      AND segments.conversion_action_name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation') 
    ORDER BY 
      segments.date DESC
  `;

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);
    metricsResponse.forEach((campaign) => {
      aggregatedData.impressions += campaign.metrics.impressions || 0;
      aggregatedData.clicks += campaign.metrics.clicks || 0;
      aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  let conversionPageToken = null;
  do {
    const conversionBatchResponse = await customer.query(conversionQuery);
    conversionBatchResponse.forEach((conversion) => {
      const conversionValue = conversion.metrics.all_conversions || 0;
      if (conversion.segments.conversion_action_name === "Book Now - Step 1: Locations") {
        aggregatedData.step1Value += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 5:Confirm Booking (Initiate Checkout)") {
        aggregatedData.step5Value += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 6: Booking Confirmation") {
        aggregatedData.step6Value += conversionValue;
      }
    });
    conversionPageToken = conversionBatchResponse.next_page_token;
  } while (conversionPageToken);

  return aggregatedData;
};

const fetchReportDataWeeklyFilter = async (req, res, campaignNameFilter, reportName) => {
  try {
    const customer = getCustomer();
    const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForWeek(customer, start, end, campaignNameFilter);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    // if (campaignNameFilter === "Brand" || campaignNameFilter === "NB") {
    //   await sendToAirtable(allWeeklyData, `${reportName} Weekly Report`, campaignNameFilter);
    // }

    return allWeeklyData;

  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const fetchReportDataWeeklyBrand = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "Brand", "Brand");
};

const fetchReportDataWeeklyNB = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "NB", "NB");
};

const fetchReportDataWeeklyGilbert = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "Gilbert", "Gilbert");
};

const fetchReportDataWeeklyMKT = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "MKT", "MKT");
};

const fetchReportDataWeeklyPhoenix = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "Phoenix", "Phoenix");
};

const fetchReportDataWeeklyScottsdale = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "Scottsdale", "Scottsdale");
};

const fetchReportDataWeeklyUptownPark = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "UptownPark", "UptownPark");
};

const fetchReportDataWeeklyMontrose = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "Montrose", "Montrose");
};

const fetchReportDataWeeklyRiceVillage = (req, res) => {
  return fetchReportDataWeeklyFilter(req, res, "RiceVillage", "RiceVillage");
};

const sendFinalReportToAirtable = async () => {
  try {
    const weeklyData = await fetchReportDataWeekly();
    const brandData = await fetchReportDataWeeklyBrand();
    const noBrandData = await fetchReportDataWeeklyNB();
    const gilbertData = await fetchReportDataWeeklyGilbert();
    const mktData = await fetchReportDataWeeklyMKT();
    const phoenixData = await fetchReportDataWeeklyPhoenix();
    const scottsdaleData = await fetchReportDataWeeklyScottsdale();
    const uptownParkData = await fetchReportDataWeeklyUptownPark();
    const montroseData = await fetchReportDataWeeklyMontrose();
    const riceVillageData = await fetchReportDataWeeklyRiceVillage();

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const addWoWVariance = (lastRecord, secondToLastRecord, filter) => {
      records.push({
        fields: {
          Week: "Wow Variance %",
          Filter: filter,
          "Impr. Raw": calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions),
          'Clicks Raw': calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks),
          'Cost Raw': calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost),
          "Book Now - Step 1: Locations Raw": calculateWoWVariance(lastRecord.step1Value, secondToLastRecord.step1Value),
          "Book Now - Step 5: Confirm Booking Raw": calculateWoWVariance(lastRecord.step5Value, secondToLastRecord.step5Value),
          "Book Now - Step 6: Booking Confirmation Raw": calculateWoWVariance(lastRecord.step6Value, secondToLastRecord.step6Value),
          "CPC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks),
          "CTR Raw": calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions),
          "Step 1 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step1Value, secondToLastRecord.cost / secondToLastRecord.step1Value),
          "Step 5 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step5Value, secondToLastRecord.cost / secondToLastRecord.step5Value),
          "Step 6 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step6Value, secondToLastRecord.cost / secondToLastRecord.step6Value),
          "Step 1 Conv Rate Raw": calculateWoWVariance(lastRecord.step1Value / lastRecord.clicks, secondToLastRecord.step1Value / secondToLastRecord.clicks),
          "Step 5 Conv Rate Raw": calculateWoWVariance(lastRecord.step5Value / lastRecord.clicks, secondToLastRecord.step5Value / secondToLastRecord.clicks),
          "Step 6 Conv Rate Raw": calculateWoWVariance(lastRecord.step6Value / lastRecord.clicks, secondToLastRecord.step6Value / secondToLastRecord.clicks),
        },
      });
    };

    const addDataToRecords = (data, filter) => {
      data.forEach((record) => {
        records.push({
          fields: {
            Week: record.date,
            Filter: filter,
            "Impr. Raw": record.impressions,
            'Clicks Raw': record.clicks,
            'Cost Raw': record.cost,
            "Book Now - Step 1: Locations Raw": record.step1Value,
            "Book Now - Step 5: Confirm Booking Raw": record.step5Value,
            "Book Now - Step 6: Booking Confirmation Raw": record.step6Value,
            "CPC Raw": record.cost / record.clicks,
            "CTR Raw": record.clicks / record.impressions,
            "Step 1 CAC Raw": record.cost / record.step1Value,
            "Step 5 CAC Raw": record.cost / record.step5Value,
            "Step 6 CAC Raw": record.cost / record.step6Value,
            "Step 1 Conv Rate Raw": (record.step1Value / record.clicks) * 100,
            "Step 5 Conv Rate Raw": (record.step5Value / record.clicks) * 100,
            "Step 6 Conv Rate Raw": (record.step6Value / record.clicks) * 100,
          },
        });
      });
    };

    addDataToRecords(weeklyData, "All Search");
    addDataToRecords(brandData, "Brand");
    addDataToRecords(noBrandData, "NB");
    addDataToRecords(gilbertData, "Gilbert");
    addDataToRecords(mktData, "MKT");
    addDataToRecords(phoenixData, "Phoenix");
    addDataToRecords(scottsdaleData, "Scottsdale");
    addDataToRecords(uptownParkData, "UptownPark");
    addDataToRecords(montroseData, "Montrose");
    addDataToRecords(riceVillageData, "RiceVillage");

    addWoWVariance(weeklyData.slice(-1)[0], weeklyData.slice(-2)[0], "All Search");
    addWoWVariance(brandData.slice(-1)[0], brandData.slice(-2)[0], "Brand");
    addWoWVariance(noBrandData.slice(-1)[0], noBrandData.slice(-2)[0], "NB");
    addWoWVariance(gilbertData.slice(-1)[0], gilbertData.slice(-2)[0], "Gilbert");
    addWoWVariance(mktData.slice(-1)[0], mktData.slice(-2)[0], "MKT");
    addWoWVariance(phoenixData.slice(-1)[0], phoenixData.slice(-2)[0], "Phoenix");
    addWoWVariance(scottsdaleData.slice(-1)[0], scottsdaleData.slice(-2)[0], "Scottsdale");
    addWoWVariance(uptownParkData.slice(-1)[0], uptownParkData.slice(-2)[0], "UptownPark");
    addWoWVariance(montroseData.slice(-1)[0], montroseData.slice(-2)[0], "Montrose");
    addWoWVariance(riceVillageData.slice(-1)[0], riceVillageData.slice(-2)[0], "RiceVillage");

    const table = base("Final Report");
    const existingRecords = await table.select().all();
    const deletePromises = existingRecords.map(record => table.destroy(record.id));
    await Promise.all(deletePromises);

    const batchSize = 10;
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      await table.create(batch);
      console.log(`Batch of ${batch.length} records sent to Airtable successfully!`);
    }

    console.log("Final weekly report sent to Airtable successfully!");
  } catch (error) {
    console.error("Error sending final report to Airtable:", error);
  }
};

const testFetchWeekly = async (req, res) => {
  res.json(
    await fetchWeeklyData(
      getCustomer(),
      req.query.metricsQuery,
      req.query.conversionQuery
    )
  );
};

// const rule = new schedule.RecurrenceRule();
// rule.dayOfWeek = 6;
// rule.hour = 7;
// rule.minute = 0;
// rule.tz = "America/Los_Angeles";

// const AM = schedule.scheduleJob(rule, () => {
//   sendFinalReportToAirtable();
//   console.log("Scheduled weekly report sent at 7 AM PST California/Irvine.");
// });

module.exports = {
  fetchReportDataWeekly,
  fetchReportDataWeeklyBrand,
  fetchReportDataWeeklyNB,
  sendFinalReportToAirtable,
  testFetchWeekly,
};
