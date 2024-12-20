const schedule = require("node-schedule");
const Airtable = require("airtable");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_HISKIN
);
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDateRanges = null;

const generateMonthlyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentMonthStart = new Date(startDate);

  while (currentMonthStart <= endDate) {
    const currentMonthEnd = new Date(currentMonthStart.getFullYear(), currentMonthStart.getMonth() + 1, 1);
    
    const adjustedEndDate = currentMonthEnd > endDate ? endDate : currentMonthEnd;

    dateRanges.push({
      start: currentMonthStart.toISOString().split('T')[0],
      end: adjustedEndDate.toISOString().split('T')[0],
    });

    currentMonthStart = new Date(currentMonthStart.getFullYear(), currentMonthStart.getMonth() + 1, 2);
  }

  return dateRanges;
};

const getOrGenerateDateRanges = () => {
  const today = new Date();
  const startDate = '2023-10';
  const endDate = today; 

  if (!storedDateRanges || new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate) {
    storedDateRanges = generateMonthlyDateRanges(startDate, endDate);
  }

  return storedDateRanges;
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const aggregateDataForMonth = async (customer, startDate, endDate, campaignNameFilter) => {
  const aggregatedData = {
    year: null,
    month: null,
    cost: 0,
  };

  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
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

  const monthNames = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);

    metricsResponse.forEach((campaign) => {
      const campaignDate = new Date(campaign.segments.date);
      const year = campaignDate.getFullYear();
      const month = monthNames[campaignDate.getMonth()];

      if (!aggregatedData.year) aggregatedData.year = year;
      if (!aggregatedData.month) aggregatedData.month = month;

      aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
    });

    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  return aggregatedData;
};

const fetchReportDataMonthlyFilter = async (req, res, campaignNameFilter, dateRanges) => {
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

    const allMonthlyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForMonth(customer, start, end, campaignNameFilter);
    });

    const allMonthlyData = await Promise.all(allMonthlyDataPromises);

    return allMonthlyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const fetchReportDataWeeklyGilbert = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Gilbert", dateRanges);
};

const fetchReportDataWeeklyMKT = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "MKT", dateRanges);
};

const fetchReportDataWeeklyPhoenix = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Phoenix", dateRanges);
};

const fetchReportDataWeeklyScottsdale = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Scottsdale", dateRanges);
};

const fetchReportDataWeeklyUptownPark = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Uptown", dateRanges);
};

const fetchReportDataWeeklyMontrose = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Montrose", dateRanges);
};

const fetchReportDataWeeklyRiceVillage = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "RiceVillage", dateRanges);
};

const fetchReportDataWeeklyDC = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "DC", dateRanges);
};

const fetchReportDataWeeklyMosaic = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "Mosaic", dateRanges);
};

const fetchReportDataWeeklyTotal = (req, res, dateRanges) => {
  return fetchReportDataMonthlyFilter(req, res, "", dateRanges);
};

const sendFinalMonthlyReportToAirtable = async (req, res) => {
  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const gilbertData = await fetchReportDataWeeklyGilbert(req, res, dateRanges);
    const phoenixData = await fetchReportDataWeeklyPhoenix(req, res, dateRanges);
    const scottsdaleData = await fetchReportDataWeeklyScottsdale(req, res, dateRanges);
    const mktData = await fetchReportDataWeeklyMKT(req, res, dateRanges);
    const uptownParkData = await fetchReportDataWeeklyUptownPark(req, res, dateRanges);
    const montroseData = await fetchReportDataWeeklyMontrose(req, res, dateRanges);
    const riceVillageData = await fetchReportDataWeeklyRiceVillage(req, res, dateRanges);
    const dcData = await fetchReportDataWeeklyDC(req, res, dateRanges);
    const mosaicData = await fetchReportDataWeeklyMosaic(req, res, dateRanges);
    const googleSpendData = await fetchReportDataWeeklyTotal(req, res, dateRanges);

    const records = [];

    const addDataToRecords = (data, fieldName) => {
      data.forEach((record) => {
        if (!record.year || !record.month || record.cost == null) {
          return;
        }

        const existingRecord = records.find(
          (r) =>
            r.fields["Year"] === record.year &&
            r.fields["Month"] === record.month
        );

        if (existingRecord) {
          existingRecord.fields[fieldName] = record.cost;
        } else {
          records.push({
            fields: {
              Year: record.year,
              Month: record.month,
              Gilbert: fieldName === "Gilbert" ? record.cost : 0,
              Phoenix: fieldName === "Phoenix" ? record.cost : 0,
              Scottsdale: fieldName === "Scottsdale" ? record.cost : 0,
              "MKT Heights": fieldName === "MKT Heights" ? record.cost : 0,
              "Uptown Park": fieldName === "Uptown Park" ? record.cost : 0,
              Montrose: fieldName === "Montrose" ? record.cost : 0,
              "Rice Village": fieldName === "Rice Village" ? record.cost : 0,
              DC: fieldName === "DC" ? record.cost : 0,
              Mosaic: fieldName === "Mosaic" ? record.cost : 0,
              "Google Spend": fieldName === "Google Spend" ? record.cost : 0,
            },
          });
        }
      });
    };

    addDataToRecords(gilbertData, "Gilbert");
    addDataToRecords(phoenixData, "Phoenix");
    addDataToRecords(scottsdaleData, "Scottsdale");
    addDataToRecords(mktData, "MKT Heights");
    addDataToRecords(uptownParkData, "Uptown Park");
    addDataToRecords(montroseData, "Montrose");
    addDataToRecords(riceVillageData, "Rice Village");
    addDataToRecords(dcData, "DC");
    addDataToRecords(mosaicData, "Mosaic");
    addDataToRecords(googleSpendData, "Google Spend");

    const table = base("Monthly Report");
    
    const createNewRecord = async (fields) => {
      await table.create([{ fields }]);
    };

    for (const record of records) {
      await createNewRecord(record.fields);
    }

    console.log("Final monthly report sent to Airtable successfully!");
  } catch (error) {
    console.error("Error sending final report to Airtable:", error);
  }
};

module.exports = {
  sendFinalMonthlyReportToAirtable,
};
