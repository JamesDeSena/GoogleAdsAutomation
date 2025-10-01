const axios = require('axios');
const { google } = require('googleapis');

const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const { getStoredBingToken } = require("../BingAuth");

const fs = require("fs");
const path = require("path");
const https = require("https");
const { execSync } = require("child_process");
const AdmZip = require("adm-zip");

const tokenFilePath = path.join(__dirname, 'adCosts.json');
const csvFilePath = path.join(__dirname, 'report.csv');

let storedDateRanges = null;

const generateMonthlyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentMonthStart = new Date(`${startDate}-01T00:00:00Z`); // Normalize to UTC

  while (currentMonthStart <= endDate) {
    const currentMonthEnd = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1, // Move to next month
      0 // Last day of the current month
    ));

    const adjustedEndDate = currentMonthEnd > endDate ? endDate : currentMonthEnd;

    dateRanges.push({
      start: currentMonthStart.toISOString().split('T')[0],
      end: adjustedEndDate.toISOString().split('T')[0],
    });

    // Move to the 1st of the next month
    currentMonthStart = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1,
      1
    ));
  }

  return dateRanges;
};

const getOrGenerateDateRanges = () => {
  const today = new Date();
  const startDate = '2020-09';
  const endDate = today; 

  if (!storedDateRanges || new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate) {
    storedDateRanges = generateMonthlyDateRanges(startDate, endDate);
  }

  return storedDateRanges;
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

async function generateLPCBing() {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const today = new Date();
  const day = today.getDate();
  const month = today.getMonth() + 1;
  const year = today.getFullYear();

  const requestBody = `
    <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
        <Action mustUnderstand="1">SubmitGenerateReport</Action>
        <AuthenticationToken>${token.accessToken_Bing}</AuthenticationToken>
        <CustomerAccountId>${process.env.BING_ADS_ACCOUNT_ID_LPC}</CustomerAccountId>
        <CustomerId>${process.env.BING_ADS_CID}</CustomerId>
        <DeveloperToken>${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
      </s:Header>
      <s:Body>
        <SubmitGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
            <ReportRequest i:type="CampaignPerformanceReportRequest" xmlns:a="https://bingads.microsoft.com/Reporting/v13">
                <a:ExcludeColumnHeaders>false</a:ExcludeColumnHeaders>
                <a:ExcludeReportFooter>true</a:ExcludeReportFooter>
                <a:ExcludeReportHeader>true</a:ExcludeReportHeader>
                <a:Format>Csv</a:Format>
                <a:FormatVersion>2.0</a:FormatVersion>
                <a:ReportName>CampaignPerformanceReport</a:ReportName> <!-- Include Date -->
                <a:ReturnOnlyCompleteData>false</a:ReturnOnlyCompleteData>
                <a:Aggregation>Monthly</a:Aggregation>
                <a:Columns>
                  <a:CampaignPerformanceReportColumn>TimePeriod</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>CampaignName</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Spend</a:CampaignPerformanceReportColumn>
                </a:Columns>
                <a:Scope>
                  <a:AccountIds xmlns:b="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                      <b:long>${process.env.BING_ADS_ACCOUNT_ID_LPC}</b:long>
                  </a:AccountIds>
                </a:Scope>
                <a:Time>
                    <a:CustomDateRangeEnd>
                        <a:Day>${day}</a:Day>
                        <a:Month>${month}</a:Month>
                        <a:Year>${year}</a:Year>
                    </a:CustomDateRangeEnd>
                    <a:CustomDateRangeStart>
                        <a:Day>1</a:Day>
                        <a:Month>1</a:Month>
                        <a:Year>2020</a:Year>
                    </a:CustomDateRangeStart>
                    <a:ReportTimeZone>PacificTimeUSCanadaTijuana</a:ReportTimeZone>
                </a:Time>
            </ReportRequest>
        </SubmitGenerateReportRequest>
      </s:Body>
    </s:Envelope>
  `;

  try {
    const response = await axios.post(
      "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc?singleWsdl",
      requestBody,
      {
        headers: {
          "Content-Type": "text/xml;charset=utf-8",
          SOAPAction: "SubmitGenerateReport",
        },
        timeout: 10000,
      }
    );

    const match = response.data.match(/<ReportRequestId>(.*?)<\/ReportRequestId>/)?.[1];
    return match;
  } catch (error) {
    console.error("Error fetching Bing data:", error.response ? error.response.data : error.message);
    throw error;
  }
};

async function pollingLPCBing() {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  let retries = 5;
  let reportUrl = null;

  while (retries > 0 && !reportUrl) {
    let key;
    try {
      key = await generateLPCBing();
      if (!key) {
        console.error("Failed to generate LPCBing key, retrying...");
        retries -= 1;
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      const requestBody = `
        <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
          <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
            <Action mustUnderstand="1">SubmitGenerateReport</Action>
            <AuthenticationToken>${token.accessToken_Bing}</AuthenticationToken>
            <CustomerAccountId>${process.env.BING_ADS_ACCOUNT_ID_LPC}</CustomerAccountId>
            <CustomerId>${process.env.BING_ADS_CID}</CustomerId>
            <DeveloperToken>${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
          </s:Header>
          <s:Body>
            <PollGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
              <ReportRequestId>${key}</ReportRequestId>
            </PollGenerateReportRequest>
          </s:Body>
        </s:Envelope>
      `;

      const response = await axios.post(
        "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc?singleWsdl",
        requestBody,
        {
          headers: {
            "Content-Type": "text/xml;charset=utf-8",
            SOAPAction: "PollGenerateReport",
          },
          timeout: 10000,
        }
      );

      let match = response.data.match(/<ReportDownloadUrl>(.*?)<\/ReportDownloadUrl>/)?.[1];
      
      if (match) {
        reportUrl = match.replace(/&amp;/g, "&");
        return reportUrl;
      }

      console.error("ReportDownloadUrl not found, retrying...");
      retries -= 1;
      await new Promise(resolve => setTimeout(resolve, 5000));

    } catch (error) {
      console.error("Error fetching Bing data:", error.response ? error.response.data : error.message);
      retries -= 1;
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }

  throw new Error("Failed to retrieve report URL after multiple attempts.");
};

async function downloadAndExtractLPCBing() {
  const url = await pollingLPCBing();
  if (!url) return;

  const zip = path.join(__dirname, 'report.zip');
  const dir = path.join(__dirname, 'bing_report');
  const csv = csvFilePath

  fs.mkdirSync(dir, { recursive: true });

  await new Promise((res, rej) =>
    https.get(url, r => r.pipe(fs.createWriteStream(zip)).on("finish", res).on("error", rej))
  );

  if (process.platform === "win32") {
    const zipFile = new AdmZip(zip);
    zipFile.extractAllTo(dir, true);
  } else {
    execSync(`unzip ${zip} -d ${dir}`);
  }

  const file = fs.readdirSync(dir).find(f => f.endsWith(".csv"));
  if (!file) throw new Error("CSV not found.");

  fs.renameSync(path.join(dir, file), csv);
  [zip, dir].forEach(f => fs.rmSync(f, { recursive: true, force: true }));

  console.log("Saved CSV:", csv);
  return csv;
};

const calculateMonthlyTotals = async () => {
  const csv = csvFilePath

  if(!fs.existsSync(csv)){
    await downloadAndExtractLPCBing()
  }

  const fileContent = fs.readFileSync(csv, 'utf-8');
  const lines = fileContent.trim().split('\n');

  const monthlyTotals = {
    bingDataNoAZ: {},
    bingDataCA: {},
    bingDataAZ: {}
  };

  lines.slice(1).forEach(line => {
    const match = line.match(/"([^"]+)","([^"]+)","([^"]+)"/);
    if (!match) return;

    const [, timePeriod, campaignName, spend] = match;
    const date = new Date(timePeriod);
    const monthYear = `${date.toLocaleString('default', { month: 'short' })}-${date.getFullYear().toString().slice(-2)}`;
    const spendValue = parseFloat(spend);

    const isCA = campaignName.includes('CA');
    const isAZ = campaignName.includes('AZ');

    if (!monthlyTotals.bingDataNoAZ[monthYear]) monthlyTotals.bingDataNoAZ[monthYear] = 0;
    if (!monthlyTotals.bingDataCA[monthYear]) monthlyTotals.bingDataCA[monthYear] = 0;
    if (!monthlyTotals.bingDataAZ[monthYear]) monthlyTotals.bingDataAZ[monthYear] = 0;

    if (isCA) monthlyTotals.bingDataCA[monthYear] += spendValue;
    if (isAZ) monthlyTotals.bingDataAZ[monthYear] += spendValue;
    if (!isAZ) monthlyTotals.bingDataNoAZ[monthYear] += spendValue;
  });

  const toArray = (obj) =>
    Object.keys(obj)
      .sort((a, b) => new Date(`01-${a}`) - new Date(`01-${b}`))
      .map(date => ({ date, cost: Math.round(obj[date] * 100) / 100 }));

  const result = {
    bingDataNoAZ: toArray(monthlyTotals.bingDataNoAZ),
    bingDataCA: toArray(monthlyTotals.bingDataCA),
    bingDataAZ: toArray(monthlyTotals.bingDataAZ)
  };

  return result;
};

async function getAmountBing() {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const dateRanges = getOrGenerateDateRanges();

  const fetchAmountForMonth = async (monthYear) => {
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
            <MonthYear>${monthYear}</MonthYear>
          </GetAccountMonthlySpendRequest>
        </s:Body>
      </s:Envelope>
    `;

    try {
      const response = await axios.post(
        `https://clientcenter.api.bingads.microsoft.com/Api/Billing/v13/CustomerBillingService.svc?singleWsdl`,
        requestBody,
        {
          headers: {
            Authorization: `Bearer ${token.accessToken_Bing}`,
            "Content-Type": "text/xml;charset=utf-8",
            SOAPAction: "GetAccountMonthlySpend",
          },
          timeout: 10000,
        }
      );

      const amountMatch = response.data.match(/<Amount>(.*?)<\/Amount>/);
      const amount = amountMatch ? parseFloat(amountMatch[1]) : 0;

      const startDateObj = new Date(`${monthYear}-01`);
      const formattedDate = `${startDateObj.toLocaleString('en-US', { month: 'short' })}-${startDateObj.getFullYear().toString().slice(-2)}`;

      return { date: formattedDate, cost: amount };
    } catch (error) {
      const startDateObj = new Date(`${monthYear}-01`);
      const formattedDate = `${startDateObj.toLocaleString('en-US', { month: 'short' })}-${startDateObj.getFullYear().toString().slice(-2)}`;

      console.error(
        "Error fetching Bing data for month " + monthYear + ":",
        error.response ? error.response.data : error.message
      );
      return { date: formattedDate, cost: 0 };
    }
  };

  try {
    const allMonthlyAmounts = [];
    const BATCH_SIZE = 10;

    for (let i = 0; i < dateRanges.length; i += BATCH_SIZE) {
      const batch = dateRanges.slice(i, i + BATCH_SIZE);
      const promises = batch.map(({ start }) => {
        const startMonthYear = start.slice(0, 7);
        return fetchAmountForMonth(startMonthYear);
      });

      const batchResults = await Promise.all(promises);
      allMonthlyAmounts.push(...batchResults);
    }

    return allMonthlyAmounts;
  } catch (err) {
    console.error("Error fetching amounts:", err);
    throw err;
  }
}

const aggregateDataForMonth = async (customer, condition, campaignNameFilter, startDate, endDate ) => {
  const startDateObj = new Date(startDate);
  const formattedDate = `${startDateObj.toLocaleString('en-US', { month: 'short' })}-${startDateObj.getFullYear().toString().slice(-2)}`;

  const aggregatedData = {
    date: formattedDate,
    cost: 0,
    localServices: 0,
  };

  const metricsQuery = `
      SELECT
        campaign.name,
        metrics.cost_micros,
        segments.date
      FROM
        campaign
      WHERE
        segments.date BETWEEN '${startDate}' AND '${endDate}'
        AND campaign.name ${condition} '%${campaignNameFilter}%'
      ORDER BY
        segments.date DESC
    `;

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);
    metricsResponse.forEach((campaign) => {
      const cost = (campaign.metrics.cost_micros || 0) / 1_000_000;

      if (campaign.campaign.name.startsWith("LocalServicesCampaign:SystemGenerated")) {
        aggregatedData.localServices += cost;
      } else {
        aggregatedData.cost += cost;
      }

    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  return aggregatedData;
};

const fetchReportDataMonthlyFilter = async (req, res, condition, campaignNameFilter) => {
  const refreshToken_Google = getStoredGoogleToken();

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

    const allMonthlyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForMonth(customer, condition, campaignNameFilter, start, end);
    });

    const allMonthlyData = await Promise.all(allMonthlyDataPromises);

    return allMonthlyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const createFetchFunction = (condition, campaignNameFilter) => {
  return (req, res) => fetchReportDataMonthlyFilter(req, res, condition, campaignNameFilter);
};

const fetchFunctions = {
  fetchReportDataMonthly: createFetchFunction("LIKE", ""),
  fetchReportDataMonthlyNoAZ: createFetchFunction("NOT LIKE", "AZ_"),
  fetchReportDataMonthlyCA: createFetchFunction("LIKE", "CA_"),
  fetchReportDataMonthlyAZ: createFetchFunction("LIKE", "AZ_"),
};

const executeSpecificFetchFunctionLPC = async (req, res) => {
  const functionName = "fetchReportDataMonthly";
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName]();
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

async function fetchAndSaveAdCosts() {
  try {
    const [bingData, { bingDataNoAZ, bingDataCA, bingDataAZ }, googleData, googleDataNoAZ, googleDataCA, googleDataAZ] = await Promise.all([
      getAmountBing(),
      calculateMonthlyTotals(),
      fetchFunctions.fetchReportDataMonthly(),
      fetchFunctions.fetchReportDataMonthlyNoAZ(),
      fetchFunctions.fetchReportDataMonthlyCA(),
      fetchFunctions.fetchReportDataMonthlyAZ(),
    ]);

    const result = {
      bing: bingData,
      bingNoAZ: bingDataNoAZ,
      bingCA: bingDataCA,
      bingAZ: bingDataAZ,
      google: googleData,
      googleNoAZ: googleDataNoAZ,
      googleCA: googleDataCA,
      googleAZ: googleDataAZ,
    };

    saveMetricsToFile(result);
    return result;
  } catch (err) {
    console.error('Failed to fetch or save ad cost data:', err.message);
  }
}

function saveMetricsToFile(result) {
  try {
    let currentData = {};
    
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, 'utf8'));
    }

    if (JSON.stringify(currentData) !== JSON.stringify(result)) {
      fs.writeFileSync(tokenFilePath, JSON.stringify(result, null, 2));
    }
  } catch (error) {
    console.error('Error saving metrics data:', error);
  }
}

const getStoredMetrics = () => {
  try {
    const data = fs.readFileSync(tokenFilePath, "utf8");
    const metricsData = JSON.parse(data);
    return metricsData;
  } catch (err) {
    console.error("Error reading token:", err);
    return null;
  }
};

const sendLPCBudgettoGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const readRange = 'Monthly Ad Spend!A2:C';

  try {
    const { bing: bingArr, google: googleArr } = await fetchAndSaveAdCosts();

    const googleMap = (googleArr || []).reduce((m, { date, cost }) => {
      m[date] = cost; return m;
    }, {});
    const bingMap = (bingArr || []).reduce((m, { date, cost }) => {
      m[date] = cost; return m;
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

    const updateRequests = [];
    const rowsToAppend = [];

    for (const label of allMonths) {
      const gCost = googleMap[label] || 0;
      const bCost = bingMap[label] || 0;

      if (labelToRow[label]) {
        const rowIndex = labelToRow[label];
        updateRequests.push({
          range: `Monthly Ad Spend!B${rowIndex}:C${rowIndex}`,
          values: [[gCost, bCost]],
        });
      } else {
        rowsToAppend.push([label, gCost, bCost]);
      }
    }

    if (updateRequests.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: 'RAW',
          data: updateRequests,
        },
      });
    }

    if (rowsToAppend.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Monthly Ad Spend!A:C',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rowsToAppend },
      });
    }

    console.log("LP+C Monthly Budget updated in Google Sheets successfully!");
  } catch (err) {
    console.error("Error updating LP+C budget in Google Sheets:", err);
  }
};

const sendLPCDetailedBudgettoGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const readRange = 'Location Spend!A2:J';

  try {
    const record = getStoredMetrics();

    const mapGoogleData = (arr) =>
      (arr || []).reduce((map, { date, cost, localServices }) => {
        map[date] = { cost, localServices: localServices || 0 };
        return map;
      }, {});

    const mapBingData = (arr) =>
      (arr || []).reduce((map, { date, cost }) => {
        map[date] = cost;
        return map;
      }, {});

    const googleNoAzMap = mapGoogleData(record.googleNoAZ);
    const googleCAMap = mapGoogleData(record.googleCA);
    const googleAZMap = mapGoogleData(record.googleAZ);
    
    const bingNoAZMap = mapBingData(record.bingNoAZ);
    const bingCAMap = mapBingData(record.bingCA);
    const bingAZMap = mapBingData(record.bingAZ);

    const allMonths = Array.from(new Set([
      ...Object.keys(googleNoAzMap),
      ...Object.keys(googleAZMap),
      ...Object.keys(bingNoAZMap),
      ...Object.keys(bingAZMap),
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

    const updateRequests = [];
    const rowsToAppend = [];

    for (const date of allMonths) {
      const row = [];
      row[0] = date;
      row[2] = googleNoAzMap[date]?.cost || 0;
      row[3] = bingNoAZMap[date] || 0;
      row[6] = googleNoAzMap[date]?.localServices || 0;
      row[8] = googleAZMap[date]?.cost || 0;
      row[9] = bingAZMap[date] || 0;
      row[12] = googleAZMap[date]?.localServices || 0;

      if (labelToRow[date]) {
        const rowIndex = labelToRow[date];
        updateRequests.push({
          range: `Location Spend!C${rowIndex}:D${rowIndex}`,
          values: [[row[2], row[3]]],
        });
        updateRequests.push({
          range: `Location Spend!F${rowIndex}`,
          values: [[row[6]]],
        });
        updateRequests.push({
          range: `Location Spend!I${rowIndex}:J${rowIndex}`,
          values: [[row[8], row[9]]],
        });
        updateRequests.push({
          range: `Location Spend!L${rowIndex}`,
          values: [[row[12]]],
        });
      } else {
        rowsToAppend.push(row);
      }
    }

    if (updateRequests.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: 'RAW',
          data: updateRequests,
        },
      });
    }

    if (rowsToAppend.length > 0) {
      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Location Spend!A:M',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: { values: rowsToAppend },
      });
    }

    console.log("LP+C Monthly Detailed Budget updated in Google Sheets successfully!");
  } catch (err) {
    console.error("Error updating selected LP+C Detailed Budget:", err);
  }
};

const sendLPCMonthlyReport = async (req, res) => {
  try {
    const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    await sendLPCBudgettoGoogleSheets(req, res);
    await delay(500);
    await sendLPCDetailedBudgettoGoogleSheets(req, res);
    await delay(500);

    console.log("LP+C Monthly Budget & Monthly Detailed Budget successfully");
  } catch (error) {
    console.error("Error sending reports:", error);
  }
};

module.exports = {
  generateLPCBing,
  fetchAndSaveAdCosts,
  executeSpecificFetchFunctionLPC,
  sendLPCDetailedBudgettoGoogleSheets,
  sendLPCBudgettoGoogleSheets,
  sendLPCMonthlyReport,
};