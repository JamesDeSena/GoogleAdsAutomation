const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");

let storedDate = null;

const generateDailyDate = () => {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate()); // Get yesterday's date

  const startOfMonth = new Date();
  startOfMonth.setDate(1);

  const dates = [];

  while (startOfMonth <= yesterday) {
    dates.push(startOfMonth.toISOString().split("T")[0]); // YYYY-MM-DD format
    startOfMonth.setDate(startOfMonth.getDate() + 1);
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
  if (!dates?.length) return [];

  const sortedDates = [...dates].sort();
  const startDate = sortedDates[0];
  const endDate = sortedDates[sortedDates.length - 1];

  const dateMap = Object.fromEntries(
    sortedDates.map(date => {
      const dateObj = new Date(date);
      const formattedDate = `${(dateObj.getMonth() + 1).toString().padStart(2, '0')}/${dateObj.getDate().toString().padStart(2, '0')}/${dateObj.getFullYear()}`;
      return [formattedDate, 0];
    })
  );

  const campaignFilter = campaignName
    ? `AND campaign.name LIKE '%${campaignName.replace(/'/g, "\\'")}%'`
    : '';

  const metricsQuery = `
    SELECT
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      ${campaignFilter}
    ORDER BY
      segments.date ASC
  `;

  const metricsResponse = await customer.query(metricsQuery);

  metricsResponse.forEach((row) => {
    if (!row.segments?.date) return;

    const dateObj = new Date(row.segments.date);
    const formattedDate = `${(dateObj.getMonth() + 1).toString().padStart(2, '0')}/${dateObj.getDate().toString().padStart(2, '0')}/${dateObj.getFullYear()}`;

    if (Object.prototype.hasOwnProperty.call(dateMap, formattedDate)) {
      dateMap[formattedDate] += (row.metrics.cost_micros || 0) / 1_000_000;
    }
  });

  return Object.entries(dateMap).map(([date, cost]) => ({
    date,
    cost,
  }));
};

const fetchReportDataDailyFilter = async (req, res, campaignNameFilter, campaignNames, customDates = null) => {
  const refreshToken_Google = getStoredGoogleToken();
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

    const dates = customDates?.length ? customDates : getOrGenerateDate();

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
  return (req, res, customDates = null) =>
    fetchReportDataDailyFilter(req, res, campaignNameFilter, campaignNames, customDates);
};

const fetchFunctions = {
  fetchReportDataDailyAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ, ["Phoenix", "Tucson", "Florida"]),
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

  const spreadsheetId = process.env.SHEET_MIVD;
  const readOnlySpreadsheetId = process.env.SHEET_MIVD_BOOKING;

  const dataRanges = {
    Phoenix: 'AZ Phoenix Bookings Data!A2:K',
    Tucson: 'AZ Tucson Bookings Data!A2:K',
    LV: 'LV Bookings Data!A2:K',
    NY: 'NY Bookings Data!A2:K',
  };

  const readOnlyGids = {
    Phoenix: 0,
    Tucson: 385552116,
    LV: 163207021,
    NY: 1618558737,
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDate(date);

    const { data: meta } = await sheets.spreadsheets.get({
      spreadsheetId: readOnlySpreadsheetId
    });

    const gidToTitle = {};
    meta.sheets.forEach(sh => {
      gidToTitle[sh.properties.sheetId] = sh.properties.title;
    });

    const readOnlyRanges = Object.fromEntries(
      Object.entries(readOnlyGids).map(([key, gid]) => [key, `${gidToTitle[gid]}!A2:G`])
    );

    const [azData, allAZData] = await Promise.all([
      fetchFunctions.fetchReportDataDailyAZ(req, res, dateRanges),
      fetchFunctions.fetchReportDataDailyAllAZ(req, res, dateRanges)
    ]);

    await new Promise(resolve => setTimeout(resolve, 500));

    const [lvData, nyData] = await Promise.all([
      fetchFunctions.fetchReportDataDailyLV(req, res, dateRanges),
      fetchFunctions.fetchReportDataDailyNYC(req, res, dateRanges)
    ]);

    const sheetsData = {
      [dataRanges.Phoenix]: allAZData[0] || [],
      [dataRanges.Tucson]: azData[1] || [],
      [dataRanges.LV]: lvData[0] || [],
      [dataRanges.NY]: nyData[0] || [],
    };

    const batchGetRanges = Object.keys(sheetsData).filter(sheet => sheetsData[sheet].length > 0);
    if (batchGetRanges.length === 0) return console.log("No data to update.");

    const batchResponse = await sheets.spreadsheets.values.batchGet({
      spreadsheetId,
      ranges: batchGetRanges
    });

    const existingData = {};
    batchResponse.data.valueRanges.forEach((response, index) => {
      existingData[batchGetRanges[index]] = response.values || [];
    });

    const readOnlyResponse = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: readOnlySpreadsheetId,
      ranges: Object.values(readOnlyRanges),
    });

    const readOnlyDataBySheet = {};
    Object.keys(readOnlyRanges).forEach((key, i) => {
      readOnlyDataBySheet[key] = readOnlyResponse.data.valueRanges[i].values || [];
    });

    const batchUpdates = [];
    const batchAppends = {};
    for (const sheet of batchGetRanges) batchAppends[sheet] = [];

    for (const [sheet, dataArr] of Object.entries(sheetsData)) {
      if (!dataArr.length) continue;

      const rows = existingData[sheet] || [];
      const sheetKey = sheet.includes('Phoenix') ? 'Phoenix'
        : sheet.includes('Tucson') ? 'Tucson'
        : sheet.includes('LV') ? 'LV'
        : 'NY';

      const readOnlyRows = readOnlyDataBySheet[sheetKey] || [];

      for (const data of dataArr) {
        const sheetDate = data?.date;
        if (!sheetDate) continue;

        const rowIndex = rows.findIndex(row => row[0] === sheetDate);
        const matchingReadOnlyRow = readOnlyRows.find(row => row[0] === sheetDate);

        let bgValues = [];
        if (matchingReadOnlyRow) bgValues = matchingReadOnlyRow.slice(1, 7); // B–G

        if (rowIndex !== -1) {
          if (bgValues.length === 6) {
            batchUpdates.push({
              range: `${sheet.split("!")[0]}!B${rowIndex + 2}:G${rowIndex + 2}`,
              values: [bgValues]
            });
          }
          batchUpdates.push({
            range: `${sheet.split("!")[0]}!K${rowIndex + 2}`,
            values: [[data.cost]]
          });
        } else {
          const newRow = bgValues.length === 6
            ? [sheetDate, ...bgValues, "", "", null, data.cost]
            : [sheetDate, "", "", "", "", "", "", "", "", null, data.cost];

          batchAppends[sheet].push(newRow);
        }
      }
    }

    const updatePromises = [];
    if (batchUpdates.length > 0) {
      updatePromises.push(sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: { valueInputOption: 'RAW', data: batchUpdates },
      }));
    }

    for (const [sheet, values] of Object.entries(batchAppends)) {
      if (values.length > 0) {
        updatePromises.push(sheets.spreadsheets.values.append({
          spreadsheetId,
          range: sheet,
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values },
        }));
      }
    }

    await Promise.all(updatePromises);
    console.log("Daily MIVD bookings data updated successfully!");
  } catch (error) {
    console.error("Error updating daily report:", error);
  }
};

const sendFinalDailySpentToGoogleSheetsMIV = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_MIVD_BOOKING_2;

  const targetTabs = {
    Phoenix: 'Mobile IV Drip AZ',
    Tucson: 'Mobile IV Drip Tucson',
    NYC: 'Mobile IV Drip NYC',
    LV: 'Mobile IV Drip Las Vegas',
    Florida: 'Mobile IV Drip Florida',
  };

  try {
    const ranges = Object.values(targetTabs).map(tab => `${tab}!A2:I`);
    const batchResponse = await sheets.spreadsheets.values.batchGet({
      spreadsheetId,
      ranges,
    });

    const isDateString = value => /^\d{2}\/\d{2}\/\d{4}$/.test(String(value).trim());

    const today = new Date();
    today.setHours(23, 59, 59, 999);

    const allSheetDates = new Set();

    batchResponse.data.valueRanges.forEach((sheetResponse) => {
      const rows = sheetResponse.values || [];

      rows.forEach((row) => {
        const sheetDate = String(row[0] || '').trim();
        if (!isDateString(sheetDate)) return;

        const [month, day, year] = sheetDate.split('/').map(Number);
        const parsedDate = new Date(year, month - 1, day);

        if (parsedDate <= today) {
          const formattedForAds = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
          allSheetDates.add(formattedForAds);
        }
      });
    });

    const dateRanges = Array.from(allSheetDates).sort();

    if (!dateRanges.length) {
      console.log("No valid dates found in Column A.");
      return;
    }

    const [azData, allAZData] = await Promise.all([
      fetchFunctions.fetchReportDataDailyAZ(req, res, dateRanges),
      fetchFunctions.fetchReportDataDailyAllAZ(req, res, dateRanges),
    ]);

    await new Promise(resolve => setTimeout(resolve, 500));

    const [lvData, nyData] = await Promise.all([
      fetchFunctions.fetchReportDataDailyLV(req, res, dateRanges),
      fetchFunctions.fetchReportDataDailyNYC(req, res, dateRanges),
    ]);

    const spentByTab = {
      [targetTabs.Phoenix]: Object.fromEntries(
        (allAZData?.[0] || []).map(row => [row.date, `$${(row.cost || 0).toFixed(2)}`])
      ),
      [targetTabs.Tucson]: Object.fromEntries(
        (azData?.[1] || []).map(row => [row.date, `$${(row.cost || 0).toFixed(2)}`])
      ),
      [targetTabs.Florida]: Object.fromEntries(
        (azData?.[2] || []).map(row => [row.date, `$${(row.cost || 0).toFixed(2)}`])
      ),
      [targetTabs.NYC]: Object.fromEntries(
        (nyData?.[0] || []).map(row => [row.date, `$${(row.cost || 0).toFixed(2)}`])
      ),
      [targetTabs.LV]: Object.fromEntries(
        (lvData?.[0] || []).map(row => [row.date, `$${(row.cost || 0).toFixed(2)}`])
      ),
    };

    const batchUpdates = [];

    batchResponse.data.valueRanges.forEach((sheetResponse, index) => {
      const tabName = Object.values(targetTabs)[index];
      const rows = sheetResponse.values || [];
      const costMap = spentByTab[tabName] || {};

      rows.forEach((row, rowIndex) => {
        const sheetDate = String(row[0] || '').trim();
        if (!isDateString(sheetDate)) return;

        if (!Object.prototype.hasOwnProperty.call(costMap, sheetDate)) return;

        const currentValue = String(row[8] || '').trim();
        const newValue = costMap[sheetDate];

        if (currentValue === newValue) return;

        batchUpdates.push({
          range: `${tabName}!I${rowIndex + 2}`,
          values: [[newValue]],
        });
      });
    });

    if (!batchUpdates.length) {
      console.log("No Google Spent values to update.");
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId,
      resource: {
        valueInputOption: 'RAW',
        data: batchUpdates,
      },
    });

    console.log("Daily MIVD Google Spent updated successfully!");
  } catch (error) {
    console.error("Error updating MIVD Google Spent:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionMIV,
  sendFinalDailyReportToGoogleSheetsMIV,
  sendFinalDailySpentToGoogleSheetsMIV
};
