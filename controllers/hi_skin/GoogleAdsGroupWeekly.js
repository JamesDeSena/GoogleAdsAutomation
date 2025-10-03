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
  let str = '';
  while (num >= 0) {
    str = String.fromCharCode(num % 26 + 65) + str;
    num = Math.floor(num / 26) - 1;
  }
  return str;
};

const normalizeName = (name) => {
  if (!name) return '';
  return name.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
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

    const structureRange = `${sheetName}!A4:B`;
    const structureResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: structureRange,
    });
    const structureRows = structureResponse.data.values || [];

    const dynamicAdGroupRowMap = new Map();
    const lastRowByCampaign = new Map();
    let currentCampaign = "";

    structureRows.forEach((row, index) => {
      const campaignName = row[0] || currentCampaign;
      const adGroupName = row[1];
      currentCampaign = campaignName;
      if (adGroupName) {
        const rowNumber = index + 4;
        const uniqueKey = normalizeName(campaignName) + normalizeName(adGroupName);
        dynamicAdGroupRowMap.set(uniqueKey, rowNumber);
        lastRowByCampaign.set(campaignName, rowNumber);
      }
    });

    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: getStoredGoogleToken(),
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const allApiReports = [];
    for (const [campaignName] of lastRowByCampaign.entries()) {
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

    const reportsByWeek = allApiReports.reduce((acc, report) => {
      if (!acc[report.week]) acc[report.week] = [];
      acc[report.week].push(report);
      return acc;
    }, {});

    let maxRow = structureRows.length + 3;

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: `${sheetName}!C3:ZZ`,
    });

    const sortedWeeks = Object.keys(reportsByWeek).sort(
      (a, b) => new Date(a.split(" - ")[0]) - new Date(b.split(" - ")[0])
    );

    const dataForBatchUpdate = [];

    for (let colIndex = 0; colIndex < sortedWeeks.length; colIndex++) {
      const weekString = sortedWeeks[colIndex];
      const columnLetter = toColumnName(colIndex + 2);
      const weeklyReportData = reportsByWeek[weekString];

      dataForBatchUpdate.push({
        range: `${sheetName}!${columnLetter}3`,
        values: [[weekString]],
      });

      const columnData = Array(maxRow).fill(null).map(() => [null]);

      for (const report of weeklyReportData) {
        const uniqueKey = normalizeName(report.campaignName) + normalizeName(report.adGroupName);
        let rowNumber = dynamicAdGroupRowMap.get(uniqueKey);

        if (!rowNumber) {
          // Insert a new row for the ad group
          const lastRow = lastRowByCampaign.get(report.campaignName) || maxRow;
          rowNumber = lastRow + 1;

          await sheets.spreadsheets.batchUpdate({
            spreadsheetId,
            requestBody: {
              requests: [
                {
                  insertDimension: {
                    range: {
                      sheetId: 459752368,
                      dimension: "ROWS",
                      startIndex: rowNumber - 1,
                      endIndex: rowNumber,
                    },
                    inheritFromBefore: true,
                  },
                },
              ],
            },
          });

          await sheets.spreadsheets.values.update({
            spreadsheetId,
            range: `${sheetName}!A${rowNumber}:B${rowNumber}`,
            valueInputOption: "USER_ENTERED",
            requestBody: {
              values: [[report.campaignName, report.adGroupName]],
            },
          });

          dynamicAdGroupRowMap.set(uniqueKey, rowNumber);
          lastRowByCampaign.set(report.campaignName, rowNumber);

          if (rowNumber > maxRow) maxRow = rowNumber;
        }

        columnData[rowNumber - 1] = [report.weightedQs];
      }

      dataForBatchUpdate.push({
        range: `${sheetName}!${columnLetter}4:${columnLetter}${maxRow}`,
        values: columnData.slice(3),
      });
    }

    if (dataForBatchUpdate.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: "USER_ENTERED",
          data: dataForBatchUpdate,
        },
      });
    }

    console.log(`Final Hi Skin Ads Group Keywords weekly report sent to Google Sheets successfully!`);
  } catch (error) {
    console.error("Error sending Hi Skin Ads Group Keywords weekly report to Google Sheets:", error);
  }
};

module.exports = {
  sendFinalWeeklyReportToGoogleSheetsHSAdG,
};