const axios = require('axios');
const { google } = require('googleapis');

const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");
const { getStoredAccessToken } = require("../BingAuth");

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

  const startDate = '2021-10-03'; //previousFriday 2024-09-13 / 11-11
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

async function getAmountBing() {
  const token = getStoredAccessToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const formatMonthYear = (date) => {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const label = date.toLocaleString("en-US", { month: "short" }) + "-" + (year % 100);
    const value = `${year}-${month}`;
    return { label, value };
  };

  const fetchAmount = async (monthValue) => {
    const requestBody = `
      <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
        <s:Header xmlns="https://bingads.microsoft.com/Billing/v13">
          <Action mustUnderstand="1">GetAccountMonthlySpend</Action>
          <AuthenticationToken i:nil="false">${token.accessToken_Bing}</AuthenticationToken>
          <DeveloperToken i:nil="false">${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
        </s:Header>
        <s:Body>
          <GetAccountMonthlySpendRequest xmlns="https://bingads.microsoft.com/Billing/v13">
            <AccountId>${process.env.BING_ADS_ACCOUNT_ID_LPC}</AccountId>
            <MonthYear>${monthValue}</MonthYear>
          </GetAccountMonthlySpendRequest>
        </s:Body>
      </s:Envelope>
    `;

    const res = await axios.post(
      'https://clientcenter.api.bingads.microsoft.com/Api/Billing/v13/CustomerBillingService.svc?singleWsdl',
      requestBody,
      {
        headers: {
          Authorization: `Bearer ${token.accessToken_Bing}`,
          'Content-Type': 'text/xml;charset=utf-8',
          SOAPAction: 'GetAccountMonthlySpend',
        },
        timeout: 10000,
      }
    );

    const match = res.data.match(/<Amount>(.*?)<\/Amount>/);
    return match ? parseFloat(match[1]) : 0;
  };

  try {
    const now = new Date();
    const prevDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));

    const currentInfo  = formatMonthYear(now);
    const previousInfo = formatMonthYear(prevDate);

    const [prevAmt, currAmt] = await Promise.all([
      fetchAmount(previousInfo.value),
      fetchAmount(currentInfo.value),
    ]);

    const previous = { month: previousInfo.label, cost: parseFloat(prevAmt.toFixed(2)) };
    const current  = { month: currentInfo.label,  cost: parseFloat(currAmt.toFixed(2))  };

    console.log("bads", previous, current);
    return [previous, current];

  } catch (err) {
    console.error("Error fetching Bing data:", err.response?.data || err.message);
    throw err;
  }
}

async function getGoogleAdsCost() {
  const refreshToken_Google = getStoredRefreshToken();
  if (!refreshToken_Google) {
    console.error("Refresh token is missing. Please authenticate.");
    return;
  }

  const customer = client.Customer({
    customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_LPC,
    refresh_token: refreshToken_Google,
    login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
  });

  const formatDate = (date) => {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  const getCostForMonth = async (date) => {
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth();
    const label = date.toLocaleString("en-US", { month: "short" }) + "-" + (year % 100);

    const firstDay = new Date(Date.UTC(year, month, 1));
    const lastDay = new Date(Date.UTC(year, month + 1, 0));

    const startDate = formatDate(firstDay);
    const endDate = formatDate(lastDay);

    const query = `
      SELECT
        campaign.name,
        metrics.cost_micros,
        segments.date
      FROM
        campaign
      WHERE
        segments.date BETWEEN '${startDate}' AND '${endDate}'
      ORDER BY
        segments.date DESC
    `;

    const response = await customer.query(query);
    const total = response.reduce((sum, row) => sum + row.metrics.cost_micros / 1_000_000, 0);

    return { month: label, cost: parseFloat(total.toFixed(2)) };
  };

  try {
    const now = new Date();
    const prev = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));

    const [previous, current] = await Promise.all([
      getCostForMonth(prev),
      getCostForMonth(now),
    ]);
    console.log("gads", previous, current)
    return [previous, current];
  } catch (error) {
    console.error("Error fetching Google Ads data:", error);
    throw error;
  }
}

const sendLPCBudgettoGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const readRange = 'Monthly Ad Spend!A2:C';

  try {
    const [googleArr, bingArr] = await Promise.all([
      getGoogleAdsCost(),
      getAmountBing()
    ]);

    const googleMap = (googleArr || []).reduce((m, { month, cost }) => {
      m[month] = cost; return m;
    }, {});
    const bingMap = (bingArr || []).reduce((m, { month, cost }) => {
      m[month] = cost; return m;
    }, {});
    const allMonths = Array.from(new Set([
      ...Object.keys(googleMap),
      ...Object.keys(bingMap),
    ]));

    const sheetData = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: readRange,
    });
    const rows = sheetData.data.values || [];
    const labelToRow = {};
    rows.forEach((r, i) => {
      const label = r[0];
      if (label) labelToRow[label] = i + 2;
    });

    for (const label of allMonths) {
      const gCost = googleMap[label] || 0;
      const bCost = bingMap[label] || 0;

      if (labelToRow[label]) {
        const rowIndex = labelToRow[label];
        const updateRange = `Monthly Ad Spend!B${rowIndex}:C${rowIndex}`;
        await sheets.spreadsheets.values.update({
          spreadsheetId,
          range: updateRange,
          valueInputOption: 'RAW',
          resource: { values: [[ gCost, bCost ]] },
        });

      } else {
        await sheets.spreadsheets.values.append({
          spreadsheetId,
          range: 'Monthly Ad Spend!A:C',
          valueInputOption: 'RAW',
          insertDataOption: 'INSERT_ROWS',
          resource: { values: [[ label, gCost, bCost ]] },
        });
      }
    }

    console.log("LP+C budget updated in Google Sheets successfully!");
  } catch (err) {
    console.error("Error updating LP+C budget in Google Sheets:", err);
  }
};

async function getCampaigns() {
  try {
    const initialResponse = await axios.get(
      "https://api.lawmatics.com/v1/prospects?page=1&fields=created_at,stage,events",
      { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
    );

    const totalPages = initialResponse.data.meta?.total_pages || 1;
    const requests = Array.from({ length: totalPages }, (_, i) =>
      axios.get(
        `https://api.lawmatics.com/v1/prospects?page=${i + 1}&fields=created_at,stage,events`,
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
      "21578": "Pending Review with Firm",
      "21579": "Pending Engagement",
    };

    const formatDateToMMDDYYYY = (dateString) => {
      if (!dateString) return null;
      const date = new Date(dateString);
      const month = String(date.getMonth() + 1).padStart(2, "0");
      const day = String(date.getDate()).padStart(2, "0");
      const year = date.getFullYear();
      return `${month}/${day}/${year}`;
    };

    const filteredCampaigns = allCampaigns
      .filter(({ attributes, relationships }) => {
        const createdAt = attributes?.created_at;
        const stageId = relationships?.stage?.data?.id;
        if (!createdAt || !stageMapping[stageId]) return false;

        const createdDate = new Date(
          new Date(createdAt).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
        );

        return createdDate >= new Date("2021-10-03T00:00:00-08:00");
      })
      .map(({ attributes, relationships }) => ({
        created_at: formatDate(attributes.created_at),
        stage: stageMapping[relationships.stage.data.id],
        event: relationships?.events?.data?.length 
          ? relationships.events.data[0].id 
          : null,
      }));

    const eventRequests = filteredCampaigns
      .filter(campaign => campaign.event) 
      .map(campaign =>
        axios.get(`https://api.lawmatics.com/v1/events/${campaign.event}`, {
          headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }
        })
        .then(response => ({
          event: campaign.event,
          event_start: formatDateToMMDDYYYY(response.data.data.attributes.start_date)
        }))
        .catch(error => ({
          event: campaign.event,
          event_start: null
        }))
      );

    const eventResponses = await Promise.all(eventRequests);

    const finalCampaigns = filteredCampaigns.map(campaign => {
      const eventData = eventResponses.find(event => event.event === campaign.event);
      return {
        ...campaign,
        event_start: eventData ? eventData.event_start : null
      };
    });

    // console.log("Final Campaigns:", JSON.stringify(finalCampaigns, null, 2));
    return finalCampaigns;
  } catch (error) {
    throw new Error(
      error.response ? error.response.data : error.message
    );
  }
};

const dailyExport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
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
  const spreadsheetId = process.env.SHEET_LPC;
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
      "Strategy Session - Time Remaining",
      "Pending Review with Firm",
      "Pending Engagement",
    ];

    let previousYesMap = new Map();
    reportData.forEach(([prevDate, prevTime, prevC]) => {
      if (prevC === "Yes") {
        previousYesMap.set(`${prevDate} ${prevTime}`, true);
      }
    });

    const transformedData = exportData
      .map(([createdAt, columnB, columnC]) => {
        if (!createdAt) return null;

        const [date, time] = createdAt.split(" ");
        let columnCReport = previousYesMap.has(`${date} ${time}`) ? "Yes" : "";
        let columnDValue = validValuesForD.includes(columnB) ? columnB : "";
        let columnEValue = validValuesForE.includes(columnB) ? `${columnC ? columnC + ", " : ""}${columnB}` : "";

        if (columnB === "Initial PCs" && !columnCReport) {
          columnCReport = "Yes";
        }

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
      resource: { values: transformedData },
    });

    console.log("LPC Daily report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const runDailyExportAndReport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
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

module.exports = {
  sendLPCBudgettoGoogleSheets,
  getCampaigns,
  dailyExport,
  dailyReport,
  runDailyExportAndReport,
};
