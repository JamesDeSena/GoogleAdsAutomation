const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDate = null;

const generateDailyDate = () => {
  const now = new Date();
  const offset = -8 * 60;
  let laTime = new Date(now.getTime() + offset * 60000);
  
  const startOfMonth = new Date(laTime.getFullYear(), laTime.getMonth(), 1);
  let today;
  
  if (now.getDate() === 1) {
    today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  } else {
    today = new Date(laTime.getFullYear(), laTime.getMonth(), laTime.getDate());
  }
  
  const dates = [];
  let currentDate = new Date(startOfMonth);
  
  while (currentDate <= today) {
    dates.push(currentDate.toISOString().split('T')[0]);
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  return dates;
};

const getOrGenerateDate = () => {
  const dates = generateDailyDate();
  if (!storedDate || storedDate.length !== dates.length) {
    storedDate = dates;
  }

  return storedDate;
};

setInterval(getOrGenerateDate, 24 * 60 * 60 * 1000);

const aggregateDataForDaily = async (customer, dates, campaignName = '') => {
  let aggregatedData = dates.map(date => {
    const dateObj = new Date(date);
    const formattedDate = `${(dateObj.getMonth() + 1).toString().padStart(2, '0')}/${dateObj.getDate().toString().padStart(2, '0')}/${dateObj.getFullYear()}`;
    return {
      date: formattedDate,
      cost: 0,
    };
  });

  const campaignFilter = campaignName ? `AND campaign.name LIKE '%${campaignName}%'` : '';

  for (const date of dates) {
    const metricsQuery = `
      SELECT
        campaign.id,
        metrics.cost_micros,
        segments.date
      FROM
        campaign
      WHERE
        segments.date = '${date}' 
        ${campaignFilter}
      ORDER BY
        segments.date DESC
    `;

    let metricsPageToken = null;
    do {
      const metricsResponse = await customer.query(metricsQuery);
      metricsResponse.forEach((campaign) => {
        const dateObj = new Date(date);
        const formattedDate = `${(dateObj.getMonth() + 1).toString().padStart(2, '0')}/${dateObj.getDate().toString().padStart(2, '0')}/${dateObj.getFullYear()}`;
        const dataEntry = aggregatedData.find(entry => entry.date === formattedDate);
        if (dataEntry) {
          dataEntry.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
        }
      });
      metricsPageToken = metricsResponse.next_page_token;
    } while (metricsPageToken);
  }

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

    const dates = getOrGenerateDate();

    const allDailyDataPromises = campaignNames.length
      ? campaignNames.map((name) => aggregateDataForDaily(customer, dates, name))
      : [aggregateDataForDaily(customer, dates)];

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
  fetchReportDataDailyAllAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ),
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
    AZ: 'AZ Bookings Data!A2:K',
    LV: 'LV Bookings Data!A2:K',
    NY: 'NY Bookings Data!A2:K',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDate(date);

    const azData = await fetchFunctions.fetchReportDataDailyAZ(req, res, dateRanges);
    const allAZData = await fetchFunctions.fetchReportDataDailyAllAZ(req, res, dateRanges);
    const lvData = await fetchFunctions.fetchReportDataDailyLV(req, res, dateRanges);
    const nyData = await fetchFunctions.fetchReportDataDailyNYC(req, res, dateRanges);

    const sheetsData = {
      [dataRanges.Phoenix]: azData[0],
      [dataRanges.Tucson]: azData[1],
      [dataRanges.AZ]: allAZData[0],
      [dataRanges.LV]: lvData[0],
      [dataRanges.NY]: nyData[0],
    };

    const batchGetRanges = Object.keys(sheetsData).map(sheet => sheet);
    const batchResponse = await sheets.spreadsheets.values.batchGet({
      spreadsheetId,
      ranges: batchGetRanges,
    });

    const existingData = {};
    batchResponse.data.valueRanges.forEach((response, index) => {
      existingData[batchGetRanges[index]] = response.values || [];
    });

    const batchUpdates = [];
    const batchAppends = [];

    for (const [sheet, dataArr] of Object.entries(sheetsData)) {
      if (!Array.isArray(dataArr) || dataArr.length === 0) {
        console.error(`No data for ${sheet}`);
        continue;
      }

      const rows = existingData[sheet] || [];

      for (const data of dataArr) {
        const sheetDate = data?.date;
        if (!sheetDate) {
          console.error(`Missing date in data for sheet: ${sheet}`);
          continue;
        }

        let rowIndex = rows.findIndex(row => row[0] === sheetDate);

        if (rowIndex !== -1) {
          batchUpdates.push({
            range: `${sheet.split("!")[0]}!K${rowIndex + 2}`,
            values: [[data.cost]],
          });
        } else {
          batchAppends.push([sheetDate, ...Array(9).fill(''), data.cost]);
        }
      }
    }

    if (batchUpdates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: 'RAW',
          data: batchUpdates,
        },
      });
    }

    if (batchAppends.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: batchGetRanges[0],
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: batchAppends },
      });
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
