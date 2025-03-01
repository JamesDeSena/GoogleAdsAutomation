const axios = require('axios');
const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

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
  const daysSinceLast = dayOfWeek % 7; //Friday (dayOfWeek + 1) % 7; Monday (dayOfWeek + 6) % 7;

  const previousLast = new Date(today);
  previousLast.setDate(today.getDate() - daysSinceLast);

  const currentDay = new Date(previousLast);
  currentDay.setDate(previousLast.getDate() + 6);

  const startDate = '2024-12-29'; //previousFriday 2024-09-13 / 11-11
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

function formatDate(dateString) {
  const date = new Date(dateString).toLocaleString("en-US", { timeZone: "America/Los_Angeles" });
  const parsedDate = new Date(date);
  const month = String(parsedDate.getMonth() + 1).padStart(2, '0');
  const day = String(parsedDate.getDate()).padStart(2, '0');
  const year = parsedDate.getFullYear();
  const hours = String(parsedDate.getHours()).padStart(2, '0');
  const minutes = String(parsedDate.getMinutes()).padStart(2, '0');
  const seconds = String(parsedDate.getSeconds()).padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}:${seconds}`;
};

async function getCampaigns() {
  try {
    const initialResponse = await axios.get(
      "https://api.lawmatics.com/v1/prospects?page=1&fields=created_at,stage",
      { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
    );

    const totalPages = initialResponse.data.meta?.total_pages || 1;
    const requests = Array.from({ length: totalPages }, (_, i) =>
      axios.get(
        `https://api.lawmatics.com/v1/prospects?page=${i + 1}&fields=created_at,stage`,
        { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
      )
    );
    const responses = await Promise.all(requests);
    
    const allCampaigns = responses.flatMap(response =>
      response.data.stages || response.data.data || response.data.results || []
    );

    const stageMapping = {
      "37826": "Initial PCs",
      "80193": "Initial Call - Not something we handle",
      "113690": "At Capacity - Refer Out",
      "21589": "Initial Call - Not Moving Forward",
      "21574": "Strategy Session Scheduled",
      "60522": "Strategy Session - Time Remaining",
      "21575": "Strategy Session - Not moving forward",
    };

    const filteredCampaigns = allCampaigns
      .filter(({ attributes, relationships }) => {
        const createdAt = attributes?.created_at;
        const stageId = relationships?.stage?.data?.id;
        if (!createdAt || !stageMapping[stageId]) return false;
        return new Date(new Date(createdAt).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })) >=
          new Date("2025-01-01T00:00:00-08:00");
      })
      .map(({ attributes, relationships }) => ({
        created_at: formatDate(attributes.created_at),
        stage: stageMapping[relationships.stage.data.id],
      }));

    // console.log("All Campaigns:", JSON.stringify(filteredCampaigns, null, 2));
    return filteredCampaigns;
  } catch (error) {
    throw new Error(
      error.response ? error.response.data : error.message
    );
  }
};

const fetchAndAggregateLPCData = async () => {
  const refreshToken_Google = getStoredRefreshToken();
  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_LPC,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const dateRanges = getOrGenerateDateRanges();
    const lpcData = await Promise.all(
      dateRanges.map(async ({ start, end }) => {
        const aggregatedData = {
          date: `${start} - ${end}`,
          clicks: 0,
        };

        const metricsQuery = `
          SELECT
            campaign.id,
            metrics.clicks,
            segments.date
          FROM
            campaign
          WHERE
            segments.date BETWEEN '${start}' AND '${end}'
          ORDER BY
            segments.date DESC
        `;

        let metricsPageToken = null;
        do {
          const metricsResponse = await customer.query(metricsQuery);
          metricsResponse.forEach((campaign) => {
            aggregatedData.clicks += campaign.metrics.clicks || 0;
          });
          metricsPageToken = metricsResponse.next_page_token;
        } while (metricsPageToken);

        return aggregatedData;
      })
    );

    return lpcData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const dailyExport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC_BUDGET;
  const dataRange = 'Daily Export!A2:B';

  try {
    const filteredData = await getCampaigns(req, res);

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRange,
    });

    const transformedData = filteredData.map(({ created_at, stage }) => [created_at, stage]);

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRange,
      valueInputOption: "RAW",
      resource: { values: transformedData },
    });

    console.log("LPC Daily report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const dailyReport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC_BUDGET;
  const dataRanges = {
    Export: 'Daily Export!A2:C',
    Report: 'Daily Report!A2:E',
  };

  try {
    const reportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Report,
    });
    let reportData = reportResponse.data.values || [];

    const exportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Export,
    });
    let exportData = exportResponse.data.values || [];

    const validValuesForD = [
      "Initial Call - Not something we handle",
      "At Capacity - Refer Out",
      "Initial Call - Not Moving Forward"
    ];

    const validValuesForE = [
      "Strategy Session Scheduled",
      "Strategy Session - Not moving forward",
      "Strategy Session - Time Remaining"
    ];

    // Store previous "Yes" values mapped by Date + Time
    let previousYesMap = new Map();
    reportData.forEach(([prevDate, prevTime, prevC]) => {
      if (prevC === "Yes") {
        previousYesMap.set(`${prevDate} ${prevTime}`, true);
      }
    });

    const transformedData = exportData
      .map(([createdAt, columnB, columnC]) => {
        if (!createdAt) return null; // Ignore rows with no date

        const [date, time] = createdAt.split(" ");
        let columnCReport = previousYesMap.has(`${date} ${time}`) ? "Yes" : "";
        let columnDValue = validValuesForD.includes(columnB) ? columnB : "";
        let columnEValue = validValuesForE.includes(columnB) ? `${columnC ? columnC + ", " : ""}${columnB}` : "";

        if (columnB === "Initial PCs" && !columnCReport) {
          columnCReport = "Yes";
        }

        // Remove rows where C, D, and E are all empty
        if (!columnCReport && !columnDValue && !columnEValue) return null;

        return [date, time, columnCReport, columnDValue, columnEValue];
      })
      .filter(row => row !== null); // Remove null rows

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRanges.Report,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRanges.Report,
      valueInputOption: "RAW",
      resource: { values: transformedData },
    });

    console.log("LPC Daily report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const dailyToWeekly = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC_BUDGET;
  const dataRanges = {
    Report: 'Daily Report!A2:E',
    Weekly: 'Weekly Report!A2:F',
    Spend: 'Google & Bing Monthly Ad Spend!A2:B',
  };

  try {
    const lpcData = await fetchAndAggregateLPCData(req, res);
    const [reportRes, spendRes, weeklyRes] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId, range: dataRanges.Report }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: dataRanges.Spend }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: dataRanges.Weekly }),
    ]);

    const rows = reportRes.data.values;
    if (!rows || rows.length === 0) return [];

    const spendMap = Object.fromEntries(
      spendRes.data.values.map(([month, spend]) => [
        month.trim(),
        parseFloat(String(spend).replace(/[$,]/g, '')) || 0,
      ])
    );

    const startDate = new Date('2025-02-23');
    const weeks = {};

    rows.forEach(([date, , mqlVal, nopesVal, sqlVal]) => {
      if (!date) return;
      const [month, day, year] = date.split("/").map(Number);
      const currentDate = new Date(year, month - 1, day);
      if (currentDate < startDate && currentDate.toDateString() !== startDate.toDateString()) return;

      let weekStart = new Date(currentDate);
      weekStart.setDate(currentDate.getDate() - currentDate.getDay());
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);

      const formatDate = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
      const weekLabel = `${formatDate(weekStart)} - ${formatDate(weekEnd)}`;
      const monthKey = `${weekStart.toLocaleString('en-US', { month: 'short' })}-${weekStart.getFullYear().toString().slice(-2)}`;
      if (!spendMap[monthKey]) return;

      if (!weeks[weekLabel]) {
        weeks[weekLabel] = [weekLabel, 0, 0, 0, spendMap[monthKey], 0];
      }

      weeks[weekLabel][1] += mqlVal.trim().toLowerCase() === "yes" ? 1 : 0;
      weeks[weekLabel][2] += nopesVal && nopesVal.trim() !== "" ? 1 : 0;
      weeks[weekLabel][3] += sqlVal && sqlVal.trim() !== "" ? 1 : 0;
    });

    lpcData.forEach(({ date, clicks }) => {
      if (weeks[date]) weeks[date][5] = clicks;
    });

    const sortedWeeks = Object.values(weeks).sort(
      (a, b) => new Date(a[0].split(" - ")[0]) - new Date(b[0].split(" - ")[0])
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRanges.Weekly,
      valueInputOption: 'RAW',
      resource: { values: sortedWeeks },
    });
    
    console.log("LPC Daily to Weekly done successfully!");
  } catch (error) {
    console.error("Error aggregating weekly report:", error);
    return [];
  }
};

const runDailyExportAndReport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC_BUDGET;
  const dataRanges = {
    Export: 'Daily Export!A2:C',
    Report: 'Daily Report!A2:E',
  };

  try {
    const filteredData = await getCampaigns(req, res);
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: 'Daily Export!A2:B',
    });

    const transformedExportData = filteredData.map(({ created_at, stage }) => [created_at, stage]);
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: 'Daily Export!A2:B',
      valueInputOption: "RAW",
      resource: { values: transformedExportData },
    });

    const reportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Report,
    });
    let reportData = reportResponse.data.values || [];

    const exportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Export,
    });
    let exportData = exportResponse.data.values || [];

    const validValuesForD = [
      "Initial Call - Not something we handle",
      "At Capacity - Refer Out",
      "Initial Call - Not Moving Forward"
    ];

    const validValuesForE = [
      "Strategy Session Scheduled",
      "Strategy Session - Not moving forward",
      "Strategy Session - Time Remaining"
    ];

    let previousYesMap = new Map();
    reportData.forEach(([prevDate, prevTime, prevC]) => {
      if (prevC === "Yes") {
        previousYesMap.set(`${prevDate} ${prevTime}`, true);
      }
    });

    const transformedReportData = exportData
      .map(([createdAt, columnB, columnC]) => {
        if (!createdAt) return null;
        const [date, time] = createdAt.split(" ");
        let columnCReport = previousYesMap.has(`${date} ${time}`) ? "Yes" : "";
        let columnDValue = validValuesForD.includes(columnB) ? columnB : "";
        let columnEValue = validValuesForE.includes(columnB) ? `${columnC ? columnC + ", " : ""}${columnB}` : "";
        if (columnB === "Initial PCs" && !columnCReport) columnCReport = "Yes";
        if (!columnCReport && !columnDValue && !columnEValue) return null;
        return [date, time, columnCReport, columnDValue, columnEValue];
      })
      .filter(row => row !== null);

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRanges.Report,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRanges.Report,
      valueInputOption: "RAW",
      resource: { values: transformedReportData },
    });
    console.log("LPC Export to Report done successfully!");
  } catch (error) {
    console.error("Error processing daily export and report:", error);
  }
};

const runFullReportProcess = async (req, res) => {
  try {
    await runDailyExportAndReport(req, res);
    await dailyToWeekly(req, res);
    console.log("Full report process completed");
  } catch (error) {
    console.error("Error in full report process:", error);
  }
};

module.exports = {
  getCampaigns,
  dailyExport,
  dailyReport,
  dailyToWeekly,
  runDailyExportAndReport,
  runFullReportProcess
};
