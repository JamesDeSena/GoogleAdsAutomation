const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");

let storedDateRanges = null;

const generateWeeklyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentStartDate = new Date(startDate);

  const adjustedEndDate = new Date(endDate);
  const daysToSunday = (7 - adjustedEndDate.getDay()) % 7;
  adjustedEndDate.setDate(adjustedEndDate.getDate() + daysToSunday);

  while (currentStartDate <= adjustedEndDate) {
    let currentEndDate = new Date(currentStartDate);
    currentEndDate.setDate(currentStartDate.getDate() + 6);

    dateRanges.push({
      start: currentStartDate.toISOString().split("T")[0],
      end: currentEndDate.toISOString().split("T")[0],
    });

    currentStartDate.setDate(currentStartDate.getDate() + 7);
  }

  return dateRanges;
};

const getOrGenerateDateRanges = (inputStartDate = null) => {
  const today = new Date();
  const dayOfWeek = today.getDay();
  const daysSinceLast = (dayOfWeek + 6) % 7; //Friday (dayOfWeek + 1) % 7; Monday (dayOfWeek + 6) % 7;

  const previousLast = new Date(today);
  previousLast.setDate(today.getDate() - daysSinceLast);

  const currentDay = new Date(previousLast);
  currentDay.setDate(previousLast.getDate() + 6);

  const startDate = '2025-08-10'; //previousFriday 2024-09-13 / 11-11
  // const fixedEndDate = '2024-11-07'; // currentDay

  const endDate = currentDay; //new Date(fixedEndDate); //currentDay;

  if (inputStartDate) {
    return generateWeeklyDateRanges(inputStartDate, new Date(new Date(inputStartDate).setDate(new Date(inputStartDate).getDate() + 6)));
  } else {
    if (
      !storedDateRanges ||
      new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate
    ) {
      storedDateRanges = generateWeeklyDateRanges(startDate, endDate);
    }
    return storedDateRanges;
  }
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const fetchWeeklyAdGroupReportWithKeywords = async (customer, startDate, endDate, exactCampaignName) => {
  const performanceQuery = `
    SELECT
      ad_group.id,
      ad_group.name,
      ad_group_criterion.criterion_id,
      ad_group_criterion.keyword.text,
      metrics.impressions,
      metrics.cost_micros
    FROM
      keyword_view
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name = '${exactCampaignName}'
      AND metrics.cost_micros > 0
  `;

  const qualityScoreQuery = `
    SELECT
      ad_group.id,
      ad_group_criterion.criterion_id,
      ad_group_criterion.quality_info.quality_score
    FROM
      ad_group_criterion
    WHERE
      campaign.name = '${exactCampaignName}'
      AND ad_group_criterion.type = 'KEYWORD'
      AND ad_group_criterion.status != 'REMOVED'
  `;

  const [performanceData, qualityScoreData] = await Promise.all([
    customer.query(performanceQuery),
    customer.query(qualityScoreQuery)
  ]);

  const qualityScoreMap = new Map();
  qualityScoreData.forEach(row => {
    if (row.ad_group_criterion.quality_info) {
      qualityScoreMap.set(
        row.ad_group_criterion.criterion_id,
        row.ad_group_criterion.quality_info.quality_score
      );
    }
  });

  const adGroupCalculations = new Map();
  performanceData.forEach(row => {
    const adGroupId = row.ad_group.id;
    const adGroupName = row.ad_group.name;
    const criterionId = row.ad_group_criterion.criterion_id;
    const qualityScore = qualityScoreMap.get(criterionId);

    if (qualityScore === undefined || qualityScore === null) {
      return;
    }

    if (!adGroupCalculations.has(adGroupId)) {
      adGroupCalculations.set(adGroupId, {
        name: adGroupName,
        qsSumProduct: 0,
        totalImpressions: 0,
      });
    }

    const adGroup = adGroupCalculations.get(adGroupId);
    const impressions = row.metrics.impressions;
    adGroup.qsSumProduct += qualityScore * impressions;
    adGroup.totalImpressions += impressions;
  });

  const finalReport = [];
  const weekString = `${startDate} - ${endDate}`;

  for (const [adGroupId, data] of adGroupCalculations.entries()) {
    let weightedQs = 0;
    if (data.totalImpressions > 0) {
      weightedQs = data.qsSumProduct / data.totalImpressions;
    }

    finalReport.push({
      week: weekString,
      adGroupName: data.name,
      weightedQs: weightedQs.toFixed(2),
    });
  }

  return finalReport;
};

const fetchReportDataWeeklyHSFilter = async (exactCampaignName, reportName, dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

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
    
    // const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return fetchWeeklyAdGroupReportWithKeywords(customer, start, end, exactCampaignName);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(300).send("Error fetching report data");
  }
};

const createFetchFunction = (exactCampaignName, reportName) => {
  return (dateRanges) => fetchReportDataWeeklyHSFilter(exactCampaignName, reportName, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklyHSSearch: createFetchFunction("Search_NB", "Search NB Report"),
};

const executeSpecificFetchFunctionHSAdG = async (req, res) => {
  const functionName = "fetchReportDataWeeklyHSSearch";
  const dateRanges = getOrGenerateDateRanges();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

let lastApiCallTime = 0;
const MIN_DELAY_BETWEEN_CALLS_MS = 3000;

const createThrottledFetch = (fetchFn) => async (...args) => {
  const now = Date.now();
  const timeSinceLastCall = now - lastApiCallTime;
  const delayNeeded = MIN_DELAY_BETWEEN_CALLS_MS - timeSinceLastCall;

  if (delayNeeded > 0) {
    console.log(`Throttling: Waiting for ${delayNeeded}ms before calling ${fetchFn.name || 'a function'}.`);
    await new Promise(resolve => setTimeout(resolve, delayNeeded));
  }

  const result = await fetchFn(...args);
  lastApiCallTime = Date.now();
  
  return result;
};

const throttledFetchFunctions = {
  searchNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSSearch),
};

const toColumnName = (num) => {
  let str = '';
  while (num >= 0) {
    str = String.fromCharCode(num % 26 + 65) + str;
    num = Math.floor(num / 26) - 1;
  }
  return str;
};

const sendFinalWeeklyReportToGoogleSheetsHSAdG = async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: "serviceToken.json",
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = process.env.SHEET_HS_QS;
    const sheetName = "Quality Score Automation";

    const adGroupColumnRange = `${sheetName}!A4:A`;
    const adGroupColumnResponse = await sheets.spreadsheets.values.get({ spreadsheetId, range: adGroupColumnRange });
    const adGroupNamesFromSheet = (adGroupColumnResponse.data.values || []).flat();

    const dynamicAdGroupRowMap = new Map();
    adGroupNamesFromSheet.forEach((name, index) => {
        if (name) {
            const rowNumber = index + 4;
            dynamicAdGroupRowMap.set(name, rowNumber);
        }
    });

    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const { searchNB: throttledBrandDataFetch } = throttledFetchFunctions;

    const nestedReportData = await throttledBrandDataFetch(dateRanges);
    const newApiReports = nestedReportData.flat();

    const reportsByWeek = newApiReports.reduce((acc, report) => {
      if (!acc[report.week]) {
        acc[report.week] = [];
      }
      acc[report.week].push(report);
      return acc;
    }, {});

    const maxRow = adGroupNamesFromSheet.length > 0 ? adGroupNamesFromSheet.length + 3 : 4;
    const readRange = `${sheetName}!A3:ZZ${maxRow}`;
    const sheetDataResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: readRange,
    });
    const existingSheetValues = sheetDataResponse.data.values || [];

    const combinedDataByWeek = {};
    const adGroupNamesByRow = {};
    dynamicAdGroupRowMap.forEach((rowNumber, name) => {
        adGroupNamesByRow[rowNumber] = name;
    });

    if (existingSheetValues.length > 0) {
      const existingHeaders = existingSheetValues[0] || [];
      existingHeaders.slice(1).forEach((weekHeader, colIndex) => {
        if (weekHeader && !combinedDataByWeek[weekHeader]) {
          combinedDataByWeek[weekHeader] = [];
        }
        if (weekHeader) {
          for (let rowIndex = 1; rowIndex < existingSheetValues.length; rowIndex++) {
            const row = existingSheetValues[rowIndex];
            const adGroupName = adGroupNamesByRow[rowIndex + 3];
            const qsValue = row ? row[colIndex + 1] : undefined;
            if (adGroupName && qsValue) {
              combinedDataByWeek[weekHeader].push({
                adGroupName,
                weightedQs: qsValue,
              });
            }
          }
        }
      });
    }

    for (const weekString in reportsByWeek) {
      combinedDataByWeek[weekString] = reportsByWeek[weekString];
    }

    const sortedWeeks = Object.keys(combinedDataByWeek).sort(
      (a, b) => new Date(a.split(" - ")[0]) - new Date(b.split(" - ")[0])
    );

    const clearRange = `${sheetName}!B3:ZZ`;
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: clearRange,
    });

    const dataForBatchUpdate = [];
    sortedWeeks.forEach((weekString, colIndex) => {
      const columnLetter = toColumnName(colIndex + 1);
      const weeklyReportData = combinedDataByWeek[weekString];

      dataForBatchUpdate.push({
        range: `${sheetName}!${columnLetter}3`,
        values: [[weekString]],
      });

      const columnData = Array(maxRow).fill(null).map(() => [null]);
      weeklyReportData.forEach((report) => {
        const rowNumber = dynamicAdGroupRowMap.get(report.adGroupName);
        if (rowNumber) {
          columnData[rowNumber - 1] = [report.weightedQs];
        }
      });

      dataForBatchUpdate.push({
        range: `${sheetName}!${columnLetter}4:${columnLetter}${maxRow}`,
        values: columnData.slice(3),
      });
    });

    if (dataForBatchUpdate.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: "USER_ENTERED",
          data: dataForBatchUpdate,
        },
      });
    }

    console.log(`Final Ads Group Keywords weekly report sent to Google Sheets successfully!`);
  } catch (error) {
    console.error("Error sending Ads Group Keywords Report to Google Sheets:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionHSAdG,
  sendFinalWeeklyReportToGoogleSheetsHSAdG,
};