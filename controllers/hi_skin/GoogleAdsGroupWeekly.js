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

  console.log(finalReport)
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
    
    const dateRanges = getOrGenerateDateRanges();

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
  brandData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSBrand),
};

const sendFinalWeeklyReportToGoogleSheetsHSAdG = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.SHEET_HI_SKIN;
  const dataRanges = {
    Live: 'Weekly Performance!A2:U',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const {
      brandData: throttledBrandDataFetch,
    } = throttledFetchFunctions;

    const brandData = await throttledBrandDataFetch(req, res, dateRanges);

    const records = [];

    const addDataToRecords = (data, filter) => {
      data.forEach((record) => {
        records.push({
          Week: record.date,
          Filter: filter,
        });
      });
    };

    addDataToRecords(brandData, "Brand Search", 1);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          finalRecords.push({
            Week: record.Filter,
            Filter: "Filter",
            isBold: true,
          });
          currentGroup = record.Filter;
        }
        finalRecords.push({ ...record, isBold: false });
      });
    }

    processGroup(records);

    const sheetData = finalRecords.map(record => [
      record.Week,
      record.Filter,
    ]);

    const dataToSend = {
      Live: sheetData.filter(row => ["Brand Search"].includes(row[0]) || ["Brand Search"].includes(row[1])),
    };    

    const formatSheets = async (sheetName, data) => {
      await sheets.spreadsheets.values.clear({ spreadsheetId, range: dataRanges[sheetName] });
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: dataRanges[sheetName],
        valueInputOption: "RAW",
        resource: { values: data },
      });

      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        resource: {
          requests: [
            {
              repeatCell: {
                range: {
                  sheetId: 0,
                  startRowIndex: 1,
                  endRowIndex: data.length + 1,
                  startColumnIndex: 0,
                  endColumnIndex: 6,
                },
                cell: {
                  userEnteredFormat: { horizontalAlignment: 'RIGHT' },
                },
                fields: 'userEnteredFormat.horizontalAlignment',
              },
            },
          ],
        },
      });
    };

    for (const [sheetName, data] of Object.entries(dataToSend)) {
      await formatSheets(sheetName, data);
    }

    console.log("Final Hi, Skin weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionHSAdG,
  sendFinalWeeklyReportToGoogleSheetsHSAdG,
};