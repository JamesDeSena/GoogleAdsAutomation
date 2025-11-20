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

  // const startDate = '2025-04-14';
  const startDate = '2024-11-11';
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

const aggregateDataForWeek = async (customer, startDate, endDate ) => {
  const aggregatedData = {
    date: `${startDate} - ${endDate}`,
    impressions: 0,
    clicks: 0,
    cost: 0,
    conversions: 0,
    interactions: 0,
    // callsFromAds: 0,
    // formSubmissionProfile: 0,
    // formSubmitHiring: 0,
    // conv_date: 0,
  };

  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.conversions,
      metrics.interactions,
      metrics.conversions_value_by_conversion_date,
      segments.date
    FROM
      campaign
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
    ORDER BY
      segments.date DESC
  `;

  // const conversionQuery = `
  //   SELECT 
  //     campaign.id,
  //     metrics.all_conversions,
  //     segments.conversion_action_name,
  //     segments.date 
  //   FROM 
  //     campaign
  //   WHERE 
  //     segments.date BETWEEN '${startDate}' AND '${endDate}'
  //     AND segments.conversion_action_name IN ('MNR - Calls from ads', 'guardiancarers.co.uk - GA4 (web) Form Submission Profile', 'guardiancarers.co.uk - GA4 (web) Form Submit Hiring')
  //   ORDER BY 
  //     segments.date DESC
  // `;

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);
    metricsResponse.forEach((campaign) => {
      aggregatedData.impressions += campaign.metrics.impressions || 0;
      aggregatedData.clicks += campaign.metrics.clicks || 0;
      aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
      aggregatedData.conversions += campaign.metrics.conversions || 0;
      aggregatedData.interactions += campaign.metrics.interactions || 0;
      // aggregatedData.conv_date += campaign.metrics.conversions_value_by_conversion_date || 0;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  // let conversionPageToken = null;
  // do {
  //   const conversionBatchResponse = await customer.query(conversionQuery);
  //   conversionBatchResponse.forEach((conversion) => {
  //     const conversionValue = conversion.metrics.all_conversions || 0;
  //     if (conversion.segments.conversion_action_name === "MNR - Calls from ads") {
  //       aggregatedData.callsFromAds += conversionValue;
  //     } else if (conversion.segments.conversion_action_name === "guardiancarers.co.uk - GA4 (web) Form Submission Profile") {
  //       aggregatedData.formSubmissionProfile += conversionValue;
  //     } else if (conversion.segments.conversion_action_name === "guardiancarers.co.uk - GA4 (web) Form Submit Hiring") {
  //       aggregatedData.formSubmitHiring += conversionValue;
  //     }
  //   });
  //   conversionPageToken = conversionBatchResponse.next_page_token;
  // } while (conversionPageToken);

  return aggregatedData;
};

const fetchReportDataWeeklyFilter = async (req, res, campaignNameFilter, dateRanges) => {
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
    
    // const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForWeek(customer, start, end);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const createFetchFunction = (campaignNameFilter) => {
  return (req, res, dateRanges) => fetchReportDataWeeklyFilter(req, res, campaignNameFilter, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklyTotal: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_NPL, "Total"),
};

const executeSpecificFetchFunctionNPL = async (req, res) => {
  const functionName = "fetchReportDataWeeklyTotal";
  const dateRanges = getOrGenerateDateRanges();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](req, res, dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

const sendFinalWeeklyReportToGoogleSheetsNPL = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_NPL;
  const dataRanges = {
    NPLLive: 'Weekly Report!A2:K',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const total = await fetchFunctions.fetchReportDataWeeklyTotal(req, res, dateRanges);

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

    const addWoWVariance = (lastRecord, secondToLastRecord, filter, filter2) => {
      const baseRecord = {
        Week: "WoW Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions)),
        "Clicks": formatPercentage(calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks)),
        "Cost": formatPercentage(calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost)),
        "CPC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
        "Conversion": formatPercentage(calculateWoWVariance(lastRecord.conversions, secondToLastRecord.conversions)),
        "Cost Per Conv": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
        "Conv. Rate": formatPercentage(calculateWoWVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
      };
    
      records.push(baseRecord);
    };    

    const addBiWeeklyVariance = (previousRecord, secondToPreviousRecord, lastRecord, secondToLastRecord, filter, filter2) => {
      const baseRecord = {
        Week: "Biweekly Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance((previousRecord.impressions + secondToPreviousRecord.impressions), (lastRecord.impressions + secondToLastRecord.impressions))),
        "Clicks": formatPercentage(calculateWoWVariance((previousRecord.clicks + secondToPreviousRecord.clicks), (lastRecord.clicks + secondToLastRecord.clicks))),
        "Cost": formatPercentage(calculateWoWVariance((previousRecord.cost + secondToPreviousRecord.cost), (lastRecord.cost + secondToLastRecord.cost))),
        "CPC": formatPercentage(calculateWoWVariance(((previousRecord.cost / previousRecord.clicks) + (secondToPreviousRecord.cost / secondToPreviousRecord.clicks)), ((lastRecord.cost / lastRecord.clicks) + ( secondToLastRecord.cost / secondToLastRecord.clicks)))),
        "CTR": formatPercentage(calculateWoWVariance(((previousRecord.clicks / previousRecord.impressions) + (secondToPreviousRecord.clicks / secondToPreviousRecord.impressions)), ((lastRecord.clicks / lastRecord.impressions) + (secondToLastRecord.clicks / secondToLastRecord.impressions)))),
        "Conversion": formatPercentage(calculateWoWVariance((previousRecord.conversions + secondToPreviousRecord.conversions), (lastRecord.conversions + secondToLastRecord.conversions))),
        "Cost Per Conv": formatPercentage(calculateWoWVariance(((previousRecord.cost / previousRecord.conversions) + (secondToPreviousRecord.cost / secondToPreviousRecord.conversions)), ((lastRecord.cost / lastRecord.conversions) + (secondToLastRecord.cost / secondToLastRecord.conversions)))),
        "Conv. Rate": formatPercentage(calculateWoWVariance(((previousRecord.conversions / previousRecord.interactions) + (secondToPreviousRecord.conversions / secondToPreviousRecord.interactions)), ((lastRecord.conversions / lastRecord.interactions) + (secondToLastRecord.conversions / secondToLastRecord.interactions)))),
      };
    
      records.push(baseRecord);
    }; 

    const addDataToRecords = (data, filter, filter2) => { 
      data.forEach((record) => {
        const baseRecord = {
          Week: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          "Clicks": formatNumber(record.clicks),
          "Cost": formatCurrency(record.cost),
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          "Conversion": formatNumber(record.conversions),
          "Cost Per Conv": formatCurrency(record.cost / record.conversions),
          "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
        };

        records.push(baseRecord);
      });
    };    

    addDataToRecords(total, "All Campaign", 1);

    if (!date || date.trim() === '') {
      addWoWVariance(total.slice(-2)[0], total.slice(-3)[0], "All Campaign", 1);
    }

    if (!date || date.trim() === '') {
      addBiWeeklyVariance(total.slice(-2)[0], total.slice(-3)[0], total.slice(-4)[0], total.slice(-5)[0], "All Campaign", 1);
    }

    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
    
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          const baseHeader = {
            Week: record.Filter,
            Filter: "Filter",
            Filter2: "Filter2",
            "Impr.": "Impr.",
            "Clicks": "Clicks",
            "Cost": "Cost",
            "CPC": "CPC",
            "CTR": "CTR",
            "Conversion": "Conversion",
            "Cost Per Conv": "Cost Per Conv",
            "Conv. Rate": "Conv. Rate",
          };    
    
          finalRecords.push(baseHeader);
          currentGroup = record.Filter;
        }
    
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "Biweekly Variance %") {
          finalRecords.push({ Week: "", Filter: "", Filter2: "", isBold: false });
        }
      });
    }    

    processGroup(records);

    const sheetData = finalRecords.map(record => {
      const baseData = [
        record.Week,
        record.Filter,
        record.Filter2,
        record["Impr."],
        record["Clicks"],
        record["Cost"],
        record["CPC"],
        record["CTR"],
        record["Conversion"],
        record["Cost Per Conv"],
        record["Conv. Rate"],
      ];
      
      return baseData;
    });    

    const dataToSend = {
      NPLLive: sheetData.filter(row => ["All Campaign"].includes(row[1])),
    };    

    const formatSheets = async (sheetName, data) => {
      await sheets.spreadsheets.values.clear({
        spreadsheetId,
        range: dataRanges[sheetName],
      });
    
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
                  userEnteredFormat: {
                    horizontalAlignment: "LEFT",
                  },
                },
                fields: "userEnteredFormat.horizontalAlignment",
              },
            },
          ],
        },
      });
    };
    
    for (const [sheetName, data] of Object.entries(dataToSend)) {
      await formatSheets(sheetName, data);
    }    

    console.log("Final Nations Photo Lab weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  executeSpecificFetchFunctionNPL,
  sendFinalWeeklyReportToGoogleSheetsNPL,
};
