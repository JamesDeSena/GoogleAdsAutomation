const schedule = require("node-schedule");
const Airtable = require("airtable");
const { google } = require("googleapis");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_HISKIN
);
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDateRanges = null;

const generateMonthlyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentMonthStart = new Date(`${startDate}-01T00:00:00Z`); // Normalize to UTC

  while (currentMonthStart <= endDate) {
    const currentMonthEnd = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1, // Move to next month
      0 // Last day of the current month
    ));

    const adjustedEndDate = currentMonthEnd > endDate ? endDate : currentMonthEnd;

    dateRanges.push({
      start: currentMonthStart.toISOString().split('T')[0],
      end: adjustedEndDate.toISOString().split('T')[0],
    });

    // Move to the 1st of the next month
    currentMonthStart = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1,
      1
    ));
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
  const token = getStoredRefreshToken();

  if (!token.refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: token.refreshToken_Google,
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

const sendFinalMonthlyReportToGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.HI_SKIN_SPREADSHEET;
  const dataRange = 'Monthly!A2:M'; // Optional, just used for reference

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

    // Helper function to aggregate data by month
    const aggregateDataByMonth = (data, fieldName) => {
      data.forEach((record) => {
        if (!record.year || !record.month || record.cost == null) {
          return;
        }

        // Find the existing record for this month or create a new one
        let existingRecord = records.find(r => r.Year === record.year && r.Month === record.month);
        
        if (!existingRecord) {
          existingRecord = {
            Year: record.year,
            Month: record.month,
            Created: new Date().toLocaleString('en-US', { timeZone: 'Asia/Singapore' }),
            Gilbert: 0,
            Phoenix: 0,
            Scottsdale: 0,
            "MKT Heights": 0,
            "Uptown Park": 0,
            Montrose: 0,
            "Rice Village": 0,
            DC: 0,
            Mosaic: 0,
            "Google Spend": 0,
          };
          records.push(existingRecord);
        }

        // Aggregate the costs for the specific field
        existingRecord[fieldName] += record.cost;
      });
    };

    // Aggregate data for all fields
    aggregateDataByMonth(gilbertData, "Gilbert");
    aggregateDataByMonth(phoenixData, "Phoenix");
    aggregateDataByMonth(scottsdaleData, "Scottsdale");
    aggregateDataByMonth(mktData, "MKT Heights");
    aggregateDataByMonth(uptownParkData, "Uptown Park");
    aggregateDataByMonth(montroseData, "Montrose");
    aggregateDataByMonth(riceVillageData, "Rice Village");
    aggregateDataByMonth(dcData, "DC");
    aggregateDataByMonth(mosaicData, "Mosaic");
    aggregateDataByMonth(googleSpendData, "Google Spend");

    // Map the records to a format suitable for Google Sheets
    const sheetData = records.map(record => [
      record.Year,
      record.Month,
      record.Created,
      record.Gilbert,
      record.Phoenix,
      record.Scottsdale,
      record["MKT Heights"],
      record["Uptown Park"],
      record.Montrose,
      record["Rice Village"],
      record.DC,
      record.Mosaic,
      record["Google Spend"],
    ]);

    const resource = {
      values: sheetData,
    };

    // Append the data to the sheet (instead of clearing and updating a range)
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: 'Monthly!A2', // Starting from row 2 in the Monthly sheet
      valueInputOption: 'RAW',
      resource,
    });

    console.log("Final monthly report appended to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  sendFinalMonthlyReportToAirtable,
  sendFinalMonthlyReportToGoogleSheets
};
