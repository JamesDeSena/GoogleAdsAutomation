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

  const startDate = '2024-11-11'; //previousFriday 2024-09-13 / 11-11
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

const fetchReportDataWeeklyCampaignST = async (dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_ST,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    // const dateRanges = getOrGenerateDateRanges();

    const aggregateDataForWeek = async (startDate, endDate) => {
      const aggregatedData = {
        date: `${startDate} - ${endDate}`,
        impressions: 0,
        clicks: 0,
        cost: 0,
        ctr: 0,
        avgCpc: 0,
        searchImprShare: 0,
        conversions: 0,
        convValue: 0,
        convValuePerCost: 0,
        costPerConv: 0,
        convRate: 0,
        convByConvTime: 0,
        convValueByConvTime: 0,
        convValueByConvTimePerCost: 0,
      };

      const metricsQuery = `
        SELECT
          campaign.id,
          campaign.name,
          segments.date,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.search_impression_share,
          metrics.conversions,
          metrics.conversions_value,
          metrics.conversions_by_conversion_date,
          metrics.conversions_value_by_conversion_date
        FROM
          campaign
        WHERE
          segments.date BETWEEN '${startDate}' AND '${endDate}'
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
          aggregatedData.searchImprShare += campaign.metrics.search_impression_share || 0;
          aggregatedData.conversions += campaign.metrics.conversions || 0;
          aggregatedData.convValue += campaign.metrics.conversions_value || 0;
          aggregatedData.convByConvTime += campaign.metrics.conversions_by_conversion_date || 0;
          aggregatedData.convValueByConvTime += campaign.metrics.conversions_value_by_conversion_date || 0;
        });
        metricsPageToken = metricsResponse.next_page_token;
      } while (metricsPageToken);

      aggregatedData.ctr = aggregatedData.impressions > 0 ? (aggregatedData.clicks / aggregatedData.impressions) * 100 : 0;
      aggregatedData.avgCpc = aggregatedData.clicks > 0 ? aggregatedData.cost / aggregatedData.clicks : 0;
      aggregatedData.costPerConv = aggregatedData.conversions > 0 ? aggregatedData.cost / aggregatedData.conversions : 0;
      aggregatedData.convRate = aggregatedData.clicks > 0 ? (aggregatedData.conversions / aggregatedData.clicks) * 100 : 0;
      aggregatedData.convValuePerCost = aggregatedData.cost > 0 ? aggregatedData.convValue / aggregatedData.cost : 0;
      aggregatedData.convValueByConvTimePerCost = aggregatedData.cost > 0 ? aggregatedData.convValueByConvTime / aggregatedData.cost : 0;

      return aggregatedData;
    };

    const allWeeklyData = [];
    for (const { start, end } of dateRanges) {
      const weeklyData = await aggregateDataForWeek(start, end);
      allWeeklyData.push(weeklyData);
    }

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const aggregateDataForWeek = async (customer, startDate, endDate, campaignNameFilter, brandNBFilter) => {
  const aggregatedData = {
    date: `${startDate} - ${endDate}`,
    impressions: 0,
    clicks: 0,
    cost: 0,
    ctr: 0,
    avgCpc: 0,
    searchImprShare: 0,
    conversions: 0,
    convValue: 0,
    convValuePerCost: 0,
    costPerConv: 0,
    convRate: 0,
    convByConvTime: 0,
    convValueByConvTime: 0,
    convValueByConvTimePerCost: 0,
  };

  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
      segments.date,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.search_impression_share,
      metrics.conversions,
      metrics.conversions_value,
      metrics.conversions_by_conversion_date,
      metrics.conversions_value_by_conversion_date
    FROM
      campaign
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name LIKE '%${campaignNameFilter}%' AND campaign.name LIKE '%${brandNBFilter}%'
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
      aggregatedData.searchImprShare += campaign.metrics.search_impression_share || 0;
      aggregatedData.conversions += campaign.metrics.conversions || 0;
      aggregatedData.convValue += campaign.metrics.conversions_value || 0;
      aggregatedData.convByConvTime += campaign.metrics.conversions_by_conversion_date || 0;
      aggregatedData.convValueByConvTime += campaign.metrics.conversions_value_by_conversion_date || 0;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  aggregatedData.ctr = aggregatedData.impressions > 0 ? (aggregatedData.clicks / aggregatedData.impressions) * 100 : 0;
  aggregatedData.avgCpc = aggregatedData.clicks > 0 ? aggregatedData.cost / aggregatedData.clicks : 0;
  aggregatedData.costPerConv = aggregatedData.conversions > 0 ? aggregatedData.cost / aggregatedData.conversions : 0;
  aggregatedData.convRate = aggregatedData.clicks > 0 ? (aggregatedData.conversions / aggregatedData.clicks) * 100 : 0;
  aggregatedData.convValuePerCost = aggregatedData.cost > 0 ? aggregatedData.convValue / aggregatedData.cost : 0;
  aggregatedData.convValueByConvTimePerCost = aggregatedData.cost > 0 ? aggregatedData.convValueByConvTime / aggregatedData.cost : 0;

  return aggregatedData;
};

const fetchReportDataWeeklySTFilter = async (req, res, campaignNameFilter, brandNBFilter, dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_ST,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });
    
    // const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForWeek(customer, start, end, campaignNameFilter, brandNBFilter);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(300).send("Error fetching report data");
  }
};

const createFetchFunction = (campaignNameFilter, brandNBFilter = "") => {
  return (req, res, dateRanges) => fetchReportDataWeeklySTFilter(req, res, campaignNameFilter, brandNBFilter, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklySTShoppingNB: createFetchFunction("Shopping_Nonbrand", ""),
  fetchReportDataWeeklySTShoppingBrand: createFetchFunction("Shopping_Brand", ""),
  fetchReportDataWeeklySTSearchNB: createFetchFunction("Search_Nonbrand", ""),
  fetchReportDataWeeklySTPmax: createFetchFunction("Pmax", ""),
  fetchReportDataWeeklySTDemandGen: createFetchFunction("Demand Gen", ""),
};

const executeSpecificFetchFunctionST = async (req, res) => {
  const functionName = "fetchReportDataWeeklySTShoppingNB";
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
    weeklyCampaignData: createThrottledFetch(fetchReportDataWeeklyCampaignST),
    shoppingNBData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklySTShoppingNB),
    shoppingBrandData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklySTShoppingBrand),
    searchNBData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklySTSearchNB),
    pmaxData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklySTPmax),
    demandGenData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklySTDemandGen),
};

const sendFinalWeeklyReportToGoogleSheetsST = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.SHEET_ST;
  const dataRanges = {
    Live: 'Weekly Report!A2:O',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const {
      weeklyCampaignData: throttledWeeklyCampaignDataFetch,
      shoppingNBData: throttledShoppingNBDataFetch,
      shoppingBrandData: throttledShoppingBrandDataFetch,
      searchNBData: throttledSearchNBDataFetch,
      pmaxData: throttledPmaxDataFetch,
      demandGenData: throttledDemandGenDataFetch,
    } = throttledFetchFunctions;

    const weeklyCampaignData = await throttledWeeklyCampaignDataFetch(dateRanges);
    const shoppingNBData = await throttledShoppingNBDataFetch(req, res, dateRanges);
    const shoppingBrandData = await throttledShoppingBrandDataFetch(req, res, dateRanges);
    const searchNBData = await throttledSearchNBDataFetch(req, res, dateRanges);
    const pmaxData = await throttledPmaxDataFetch(req, res, dateRanges);
    const demandGenData = await throttledDemandGenDataFetch(req, res, dateRanges);

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

    const addWoWVariance = (lastRecord, secondToLastRecord, filter, filter2) => {
      records.push({
        Week: "WoW Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions)),
        "Clicks": formatPercentage(calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks)),
        "Cost": formatPercentage(calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.ctr, secondToLastRecord.ctr)),
        "Avg. CPC": formatPercentage(calculateWoWVariance(lastRecord.avgCpc, secondToLastRecord.avgCpc)),
        "Search Impr. Share": formatPercentage(calculateWoWVariance(lastRecord.searchImprShare, secondToLastRecord.searchImprShare)),
        "Conversions": formatPercentage(calculateWoWVariance(lastRecord.conversions, secondToLastRecord.conversions)),
        "Conv. Value/Cost": formatPercentage(calculateWoWVariance(lastRecord.convValuePerCost, secondToLastRecord.convValuePerCost)),
        "Cost/Conv.": formatPercentage(calculateWoWVariance(lastRecord.costPerConv, secondToLastRecord.costPerConv)),
        "Conv. Rate": formatPercentage(calculateWoWVariance(lastRecord.convRate, secondToLastRecord.convRate)),
        "Conv. (by conv. time)": formatPercentage(calculateWoWVariance(lastRecord.convByConvTime, secondToLastRecord.convByConvTime)),
        "Conv. Value (by conv. time)/Cost": formatPercentage(calculateWoWVariance(lastRecord.convValueByConvTimePerCost, secondToLastRecord.convValueByConvTimePerCost)),
      });
    };

    const addBiWeeklyVariance = (previousRecord, secondToPreviousRecord, lastRecord, secondToLastRecord, filter, filter2) => {
      records.push({
        Week: "Biweekly Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(
          previousRecord.impressions + secondToPreviousRecord.impressions, 
          lastRecord.impressions + secondToLastRecord.impressions
        )),
        "Clicks": formatPercentage(calculateWoWVariance(
          previousRecord.clicks + secondToPreviousRecord.clicks,
          lastRecord.clicks + secondToLastRecord.clicks
        )),
        "Cost": formatPercentage(calculateWoWVariance(
          previousRecord.cost + secondToPreviousRecord.cost, 
          lastRecord.cost + secondToLastRecord.cost
        )),
        "CTR": formatPercentage(calculateWoWVariance(
          (previousRecord.ctr + secondToPreviousRecord.ctr) / 2,
          (lastRecord.ctr + secondToLastRecord.ctr) / 2
        )),
        "Avg. CPC": formatPercentage(calculateWoWVariance(
          (previousRecord.avgCpc + secondToPreviousRecord.avgCpc) / 2,
          (lastRecord.avgCpc + secondToLastRecord.avgCpc) / 2
        )),
        "Search Impr. Share": formatPercentage(calculateWoWVariance(
          (previousRecord.searchImprShare + secondToPreviousRecord.searchImprShare) / 2,
          (lastRecord.searchImprShare + secondToLastRecord.searchImprShare) / 2
        )),
        "Conversions": formatPercentage(calculateWoWVariance(
          previousRecord.conversions + secondToPreviousRecord.conversions,
          lastRecord.conversions + secondToLastRecord.conversions
        )),
        "Conv. Value/Cost": formatPercentage(calculateWoWVariance(
          (previousRecord.convValuePerCost + secondToPreviousRecord.convValuePerCost) / 2,
          (lastRecord.convValuePerCost + secondToLastRecord.convValuePerCost) / 2
        )),
        "Cost/Conv.": formatPercentage(calculateWoWVariance(
          (previousRecord.costPerConv + secondToPreviousRecord.costPerConv) / 2,
          (lastRecord.costPerConv + secondToLastRecord.costPerConv) / 2
        )),
        "Conv. Rate": formatPercentage(calculateWoWVariance(
          (previousRecord.convRate + secondToPreviousRecord.convRate) / 2,
          (lastRecord.convRate + secondToLastRecord.convRate) / 2
        )),
        "Conv. (by conv. time)": formatPercentage(calculateWoWVariance(
          previousRecord.convByConvTime + secondToPreviousRecord.convByConvTime,
          lastRecord.convByConvTime + secondToLastRecord.convByConvTime
        )),
        "Conv. Value (by conv. time)/Cost": formatPercentage(calculateWoWVariance(
          (previousRecord.convValueByConvTimePerCost + secondToPreviousRecord.convValueByConvTimePerCost) / 2,
          (lastRecord.convValueByConvTimePerCost + secondToLastRecord.convValueByConvTimePerCost) / 2
        )),
      });
    };

    const addDataToRecords = (data, filter, filter2) => {
      data.forEach((record) => {
        records.push({
          Week: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          "Clicks": formatNumber(record.clicks),
          "Cost": formatCurrency(record.cost),
          "CTR": formatPercentage(record.ctr),
          "Avg. CPC": formatCurrency(record.avgCpc),
          "Search Impr. Share": formatPercentage(record.searchImprShare),
          "Conversions": formatNumber(record.conversions),
          "Conv. Value/Cost": formatNumber(record.convValuePerCost),
          "Cost/Conv.": formatCurrency(record.costPerConv),
          "Conv. Rate": formatPercentage(record.convRate),
          "Conv. (by conv. time)": formatNumber(record.convByConvTime),
          "Conv. Value (by conv. time)/Cost": formatNumber(record.convValueByConvTimePerCost),
        });
      });
    };

    addDataToRecords(weeklyCampaignData, "All Campaign", 1);
    addDataToRecords(shoppingNBData, "Shopping Nonbrand", 2);
    addDataToRecords(shoppingBrandData, "Shopping Brand", 3);
    addDataToRecords(searchNBData, "Search Nonbrand", 4);
    addDataToRecords(pmaxData, "Pmax", 5);
    addDataToRecords(demandGenData, "Demand Gen", 6);

    if (!date || date.trim() === '') {
      addWoWVariance(weeklyCampaignData.slice(-2)[0], weeklyCampaignData.slice(-3)[0], "All Campaign", 1);
      addWoWVariance(shoppingNBData.slice(-2)[0], shoppingNBData.slice(-3)[0], "Shopping Nonbrand", 2);
      addWoWVariance(shoppingBrandData.slice(-2)[0], shoppingBrandData.slice(-3)[0], "Shopping Brand", 3);
      addWoWVariance(searchNBData.slice(-2)[0], searchNBData.slice(-3)[0], "Search Nonbrand", 4);
      addWoWVariance(pmaxData.slice(-2)[0], pmaxData.slice(-3)[0], "Pmax", 5);
      addWoWVariance(demandGenData.slice(-2)[0], demandGenData.slice(-3)[0], "Demand Gen", 6);
    }
    records.sort((a, b) => a.Filter2 - b.Filter2);

    if (!date || date.trim() === '') {
      addBiWeeklyVariance(weeklyCampaignData.slice(-2)[0], weeklyCampaignData.slice(-3)[0], weeklyCampaignData.slice(-4)[0], weeklyCampaignData.slice(-5)[0], "All Campaign", 1);
      addBiWeeklyVariance(shoppingNBData.slice(-2)[0], shoppingNBData.slice(-3)[0], shoppingNBData.slice(-4)[0], shoppingNBData.slice(-5)[0], "Shopping Nonbrand", 2);
      addBiWeeklyVariance(shoppingBrandData.slice(-2)[0], shoppingBrandData.slice(-3)[0], shoppingBrandData.slice(-4)[0], shoppingBrandData.slice(-5)[0], "Shopping Brand", 3);
      addBiWeeklyVariance(searchNBData.slice(-2)[0], searchNBData.slice(-3)[0], searchNBData.slice(-4)[0], searchNBData.slice(-5)[0], "Search Nonbrand", 4);
      addBiWeeklyVariance(pmaxData.slice(-2)[0], pmaxData.slice(-3)[0], pmaxData.slice(-4)[0], pmaxData.slice(-5)[0], "Pmax", 5);
      addBiWeeklyVariance(demandGenData.slice(-2)[0], demandGenData.slice(-3)[0], demandGenData.slice(-4)[0], demandGenData.slice(-5)[0], "Demand Gen", 6);
    }
    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          finalRecords.push({
            Week: record.Filter,
            Filter: "Filter",
            Filter2: "Filter2",
            "Impr.": "Impr.",
            "Clicks": "Clicks",
            "Cost": "Cost",
            "CTR": "CTR",
            "Avg. CPC": "Avg. CPC",
            "Search Impr. Share": "Search Impr. Share",
            "Conversions": "Conversions",
            "Conv. Value/Cost": "Conv. Value/Cost",
            "Cost/Conv.": "Cost/Conv.",
            "Conv. Rate": "Conv. Rate",
            "Conv. (by conv. time)": "Conv. (by conv. time)",
            "Conv. Value (by conv. time)/Cost": "Conv. Value (by conv. time)/Cost",
            isBold: true,
          });
          currentGroup = record.Filter;
        }
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "Biweekly Variance %") {
          finalRecords.push({ Week: "", Filter: "", Filter2: "", isBold: false });
        }
      });
    }

    processGroup(records);

    const sheetData = finalRecords.map(record => [
      record.Week,
      record.Filter,
      record.Filter2,
      record["Impr."],
      record["Clicks"],
      record["Cost"],
      record["CTR"],
      record["Avg. CPC"],
      record["Search Impr. Share"],
      record["Conversions"],
      record["Conv. Value/Cost"],
      record["Cost/Conv."],
      record["Conv. Rate"],
      record["Conv. (by conv. time)"],
      record["Conv. Value (by conv. time)/Cost"]
    ]);

    const dataToSend = {
      Live: sheetData.filter(row => ["All Campaign", "Shopping Nonbrand", "Shopping Brand", "Search Nonbrand", "Pmax", "Demand Gen"].includes(row[0]) || ["All Campaign", "Shopping Nonbrand", "Shopping Brand", "Search Nonbrand", "Pmax", "Demand Gen"].includes(row[1])),
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
                  sheetId: 1684300321,
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

    console.log("Final Sleepy Tie weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending Sleepy Tie weekly report to Google Sheets:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionST,
  sendFinalWeeklyReportToGoogleSheetsST,
};