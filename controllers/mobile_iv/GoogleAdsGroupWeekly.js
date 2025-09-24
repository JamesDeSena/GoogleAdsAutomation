const { google } = require("googleapis");
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
  const daysSinceLast = (dayOfWeek + 6) % 7;

  const previousLast = new Date(today);
  previousLast.setDate(today.getDate() - daysSinceLast);

  const currentDay = new Date(previousLast);
  currentDay.setDate(previousLast.getDate() + 6);

  const startDate = "2025-08-10";
  const endDate = currentDay;

  if (inputStartDate) {
    return generateWeeklyDateRanges(
      inputStartDate,
      new Date(
        new Date(inputStartDate).setDate(new Date(inputStartDate).getDate() + 6)
      )
    );
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

const fetchWeeklyAdGroupReportWithKeywords = async (
  customer,
  startDate,
  endDate,
  exactCampaignName
) => {
  const reportQuery = `
    SELECT
      ad_group.id,
      ad_group.name,
      metrics.impressions,
      metrics.historical_quality_score,
      metrics.cost_micros
    FROM
      keyword_view
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name = '${exactCampaignName}'
      AND metrics.cost_micros > 0
  `;

  const performanceData = await customer.query(reportQuery);
  const adGroupCalculations = new Map();

  performanceData.forEach((row) => {
    const adGroupId = row.ad_group.id;
    const adGroupName = row.ad_group.name;
    const qualityScore = row.metrics.historical_quality_score;

    if (qualityScore === undefined || qualityScore === null) return;

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
    if (data.totalImpressions > 0)
      weightedQs = data.qsSumProduct / data.totalImpressions;

    finalReport.push({
      week: weekString,
      campaignName: exactCampaignName,
      adGroupName: data.name,
      weightedQs: weightedQs.toFixed(2),
    });
  }

  return finalReport;
};

const toColumnName = (num) => {
  let str = "";
  while (num >= 0) {
    str = String.fromCharCode((num % 26) + 65) + str;
    num = Math.floor(num / 26) - 1;
  }
  return str;
};

const normalizeName = (name) => {
  if (!name) return '';
  return name.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
};

const sendFinalWeeklyReportToGoogleSheetsMIVDAdG = async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: "serviceToken.json",
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = process.env.SHEET_MIVD_QS;
    const sheetName = "Quality Score Automation";

    const LOCATION_TO_CUSTOMER_ID_MAP = {
      AZ: process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ,
      LV: process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV,
      NYC: process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC,
    };

    const structureRange = `${sheetName}!A4:C`;
    const structureResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: structureRange,
    });
    const structureRows = structureResponse.data.values || [];

    const dynamicAdGroupRowMap = new Map();
    const campaignsByLocation = new Map();
    let currentLocation = "";
    let currentCampaign = "";

    structureRows.forEach((row, index) => {
      const location = row[0] || currentLocation;
      const campaignName = row[1] || currentCampaign;
      const adGroupName = row[2];
      currentLocation = location;
      currentCampaign = campaignName;

      if (adGroupName) {
        const rowNumber = index + 4;
        const uniqueKey = normalizeName(campaignName) + normalizeName(adGroupName);
        dynamicAdGroupRowMap.set(uniqueKey, rowNumber);

        if (!campaignsByLocation.has(location))
          campaignsByLocation.set(location, new Map());
        const campaignsInLocation = campaignsByLocation.get(location);
        if (!campaignsInLocation.has(campaignName))
          campaignsInLocation.set(campaignName, []);
        campaignsInLocation.get(campaignName).push(adGroupName);
      }
    });

    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);
    const allApiReports = [];
    const refreshToken = getStoredGoogleToken();

    for (const [location, campaignsToFetch] of campaignsByLocation.entries()) {
      const customerId = LOCATION_TO_CUSTOMER_ID_MAP[location];
      if (!customerId) continue;

      const customer = client.Customer({
        customer_id: customerId,
        refresh_token: refreshToken,
        login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
      });

      for (const [campaignName] of campaignsToFetch.entries()) {
        for (const range of dateRanges) {
          const weeklyData = await fetchWeeklyAdGroupReportWithKeywords(
            customer,
            range.start,
            range.end,
            campaignName
          );
          allApiReports.push(...weeklyData);
        }
      }
    }
    
    const reportsByWeek = allApiReports.reduce((acc, report) => {
      if (!acc[report.week]) acc[report.week] = [];
      acc[report.week].push(report);
      return acc;
    }, {});

    const maxRow = structureRows.length + 3;
    const readRange = `${sheetName}!A3:ZZ${maxRow}`;
    const sheetDataResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: readRange,
    });
    const existingSheetValues = sheetDataResponse.data.values || [];

    const combinedDataByWeek = {};
    const adGroupNamesByRow = {};
    dynamicAdGroupRowMap.forEach(
      (rowNumber, name) => (adGroupNamesByRow[rowNumber] = name)
    );

    if (existingSheetValues.length > 0) {
      const existingHeaders = existingSheetValues[0] || [];
      existingHeaders.slice(3).forEach((weekHeader, colIndex) => {
        if (weekHeader) {
          combinedDataByWeek[weekHeader] = combinedDataByWeek[weekHeader] || [];
          for (
            let rowIndex = 1;
            rowIndex < existingSheetValues.length;
            rowIndex++
          ) {
            const row = existingSheetValues[rowIndex];
            const adGroupName = adGroupNamesByRow[rowIndex + 3];
            const qsValue = row ? row[colIndex + 3] : undefined;
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

    for (const weekString in reportsByWeek)
      combinedDataByWeek[weekString] = reportsByWeek[weekString];

    const sortedWeeks = Object.keys(combinedDataByWeek).sort(
      (a, b) => new Date(a.split(" - ")[0]) - new Date(b.split(" - ")[0])
    );

    const clearRange = `${sheetName}!D3:ZZ`;
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: clearRange,
    });

    const dataForBatchUpdate = [];
    sortedWeeks.forEach((weekString, colIndex) => {
      const columnLetter = toColumnName(colIndex + 3);
      const weeklyReportData = combinedDataByWeek[weekString];

      dataForBatchUpdate.push({
        range: `${sheetName}!${columnLetter}3`,
        values: [[weekString]],
      });

      const columnData = Array(maxRow)
        .fill(null)
        .map(() => [null]);
      weeklyReportData.forEach((report) => {
        const uniqueKey = normalizeName(report.campaignName) + normalizeName(report.adGroupName);
        const rowNumber = dynamicAdGroupRowMap.get(uniqueKey);
        if (rowNumber) columnData[rowNumber - 1] = [report.weightedQs ?? ""];
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

    console.log(
      "Final MIVD Ads Group weekly report sent to Google Sheets successfully!"
    );
  } catch (error) {
    console.error(
      "Error sending MIVD Ads Group weekly report to Google Sheets:",
      error
    );
  }
};

module.exports = { 
  sendFinalWeeklyReportToGoogleSheetsMIVDAdG 
};
