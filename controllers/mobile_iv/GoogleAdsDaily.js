const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDate = null;

const generateDailyDate = () => {
  const now = new Date();
  const offset = -8 * 60;
  const laTime = new Date(now.getTime() + offset * 60000);
  return laTime.toISOString().split('T')[0];
};

const getOrGenerateDate = () => {
  const today = generateDailyDate();
  if (storedDate !== today) {
    storedDate = today;
  }

  return storedDate;
};

setInterval(getOrGenerateDate, 24 * 60 * 60 * 1000);

const aggregateDataForDaily = async (customer, startDate, campaignName = '') => {
  const startDateObj = new Date(startDate);
  const formattedDate = `${(startDateObj.getMonth() + 1).toString().padStart(2, '0')}/${startDateObj.getDate().toString().padStart(2, '0')}/${startDateObj.getFullYear()}`;

  const aggregatedData = {
    date: formattedDate,
    cost: 0,
  };

  const campaignFilter = campaignName ? `AND campaign.name LIKE '%${campaignName}%'` : '';

  const metricsQuery = `
    SELECT
      campaign.id,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date = '${startDate}'
      ${campaignFilter}
    ORDER BY
      segments.date DESC
  `;

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);
    metricsResponse.forEach((campaign) => {
      aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  return aggregatedData;
};

const fetchReportDataDailyFilter = async (req, res, campaignNameFilter, campaignNames) => {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: campaignNameFilter,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const date = getOrGenerateDate();

    const allDailyDataPromises = campaignNames.length
      ? campaignNames.map((name) => aggregateDataForDaily(customer, date, name))
      : [aggregateDataForDaily(customer, date)];

    const allDailyData = await Promise.all(allDailyDataPromises);

    return allDailyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const createFetchFunction = (campaignNameFilter, campaignNames = []) => {
  return (req, res) => fetchReportDataDailyFilter(req, res, campaignNameFilter, campaignNames);
};

const fetchFunctions = {
  fetchReportDataDailyAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ, ["Phoenix", "Tucson"]),
  fetchReportDataDailyLV: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV),
  fetchReportDataDailyNYC: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC),
};

const executeSpecificFetchFunctionMIV = async (req, res) => {
  const functionName = "fetchReportDataDailyLV";
  const dateRanges = getOrGenerateDate();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](req, res, dateRanges);
    console.log(data)
    // res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

const sendFinalDailyReportToGoogleSheetsMIV = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_MOBILE_DRIP;
  const dataRanges = {
    Phoenix: 'AZ Phoenix Bookings Data!A2:K',
    Tucson: 'AZ Tucson Bookings Data!A2:K',
    LV: 'LV Bookings Data!A2:K',
    NY: 'NY Bookings Data!A2:K',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDate(date);

    const azData = await fetchFunctions.fetchReportDataDailyAZ(req, res, dateRanges);
    const lvData = await fetchFunctions.fetchReportDataDailyLV(req, res, dateRanges);
    const nyData = await fetchFunctions.fetchReportDataDailyNYC(req, res, dateRanges);

    const sheetsData = {
      [dataRanges.Phoenix]: azData[0],
      [dataRanges.Tucson]: azData[1],
      [dataRanges.LV]: lvData[0],
      [dataRanges.NY]: nyData[0],
    };

    for (const [sheet, data] of Object.entries(sheetsData)) {
      if (!data) continue;

      const sheetDate = Array.isArray(data) ? data[0]?.date : data.date;
      if (!sheetDate) {
        console.error(`Missing date in data for sheet: ${sheet}`);
        continue;
      }

      const response = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: sheet,
      });

      const rows = response.data.values || [];
      let rowIndex = rows.findIndex(row => row[0] === sheetDate);

      if (rowIndex !== -1) {
        await sheets.spreadsheets.values.update({
          spreadsheetId,
          range: `${sheet.split("!")[0]}!K${rowIndex + 2}`,
          valueInputOption: 'RAW',
          resource: { values: [[data.cost]] },
        });
      } else {
        await sheets.spreadsheets.values.append({
          spreadsheetId,
          range: sheet,
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values: [[sheetDate, ...Array(9), data.cost]] },
        });
      }
    }

    console.log("Daily MIVD data updated successfully!");
  } catch (error) {
    console.error("Error updating daily report:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionMIV,
  sendFinalDailyReportToGoogleSheetsMIV,
};
