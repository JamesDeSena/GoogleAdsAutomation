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
    bingDataCA: {},
    bingDataAZ: {},
    bingDataWA: {},
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
    const isWA = campaignName.includes('WA');

    if (!monthlyTotals.bingDataCA[monthYear]) monthlyTotals.bingDataCA[monthYear] = 0;
    if (!monthlyTotals.bingDataAZ[monthYear]) monthlyTotals.bingDataAZ[monthYear] = 0;
    if (!monthlyTotals.bingDataWA[monthYear]) monthlyTotals.bingDataWA[monthYear] = 0;

    if (isCA) monthlyTotals.bingDataCA[monthYear] += spendValue;
    if (isAZ) monthlyTotals.bingDataAZ[monthYear] += spendValue;
    if (isWA) monthlyTotals.bingDataWA[monthYear] += spendValue;
  });

  const toArray = (obj) =>
    Object.keys(obj)
      .sort((a, b) => new Date(`01-${a}`) - new Date(`01-${b}`))
      .map(date => ({ date, cost: Math.round(obj[date] * 100) / 100 }));

  const result = {
    bingDataCA: toArray(monthlyTotals.bingDataCA),
    bingDataAZ: toArray(monthlyTotals.bingDataAZ),
    bingDataWA: toArray(monthlyTotals.bingDataWA)
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

const aggregateDataForMonth = async (customer, campaignNameFilter, startDate, endDate) => {
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
      AND campaign.name REGEXP_MATCH '.*${campaignNameFilter}_.*'
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

const fetchReportDataMonthlyFilter = async (req, res, campaignNameFilter) => {
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
    const allMonthlyDataPromises = dateRanges.map(({ start, end }) =>
      aggregateDataForMonth(customer, campaignNameFilter, start, end)
    );

    const allMonthlyData = await Promise.all(allMonthlyDataPromises);
    return allMonthlyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const createFetchFunction = (campaignNameFilter) => {
  return (req, res) => fetchReportDataMonthlyFilter(req, res, campaignNameFilter);
};

const fetchFunctions = {
  fetchReportDataMonthly: createFetchFunction(""),
  fetchReportDataMonthlyCA: createFetchFunction("CA"),
  fetchReportDataMonthlyAZ: createFetchFunction("AZ"),
  fetchReportDataMonthlyWA: createFetchFunction("WA"),
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
    const [bingData, { bingDataCA, bingDataAZ, bingDataWA }, googleData, googleDataCA, googleDataAZ, googleDataWA] = await Promise.all([
      getAmountBing(),
      calculateMonthlyTotals(),
      fetchFunctions.fetchReportDataMonthly(),
      fetchFunctions.fetchReportDataMonthlyCA(),
      fetchFunctions.fetchReportDataMonthlyAZ(),
      fetchFunctions.fetchReportDataMonthlyWA(),
    ]);

    const result = {
      bing: bingData,
      bingCA: bingDataCA,
      bingAZ: bingDataAZ,
      bingWA: bingDataWA,
      google: googleData,
      googleCA: googleDataCA,
      googleAZ: googleDataAZ,
      googleWA: googleDataWA,
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

async function getRawCampaigns() {
  const fetchPaginatedData = async (baseUrl, token, batchSize) => {
    const allData = [];
    const headers = { Authorization: `Bearer ${token}` };

    const initialResponse = await axios.get(`${baseUrl}&page=1`, { headers, maxBodyLength: Infinity });
    const totalPages = initialResponse.data.meta?.total_pages || 1;

    for (let i = 0; i < totalPages; i += batchSize) {
      const batchPromises = [];
      const endOfBatch = Math.min(i + batchSize, totalPages);

      for (let j = i; j < endOfBatch; j++) {
        const pageNumber = j + 1;
        batchPromises.push(
          axios.get(`${baseUrl}&page=${pageNumber}`, { headers, maxBodyLength: Infinity })
        );
      }
      const batchResponses = await Promise.all(batchPromises);
      batchResponses.forEach(response => allData.push(...(response.data.data || [])));
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    return allData;
  };

  try {
    const LAWMATICS_TOKEN = process.env.LAWMATICS_TOKEN;
    const BATCH_SIZE = 20;

    const [allCampaignsData, allEventsData] = await Promise.all([
      fetchPaginatedData("https://api.lawmatics.com/v1/prospects?fields=created_at,stage,custom_field_values,utm_source", LAWMATICS_TOKEN, BATCH_SIZE),
      fetchPaginatedData("https://api.lawmatics.com/v1/events?fields=id,name,start_date,canceled_at,attendee_name", LAWMATICS_TOKEN, BATCH_SIZE)
    ]);
    
    const filteredCampaigns = allCampaignsData
      .filter(({ attributes }) => {
        if (!attributes?.created_at) return false;
        const createdDate = new Date(new Date(attributes.created_at).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
        if (createdDate < new Date("2024-01-01T00:00:00-08:00")) return false;
        // if (!/(google|bing)/i.test(attributes?.utm_source || "")) return false;
        // if ((attributes?.utm_source || "").toLowerCase() === "metaads") return false;
        return true;
      })
      .map(({ attributes, relationships }) => ({
        created_at: attributes.created_at,
        stage_id: relationships?.stage?.data?.id || null,
        jurisdiction: attributes?.custom_field_values?.["635624"]?.formatted_value || null,
        source: attributes?.utm_source || null,
      }));

    const strategySessions = allEventsData
      .filter(event => {
        const { name, start_date, canceled_at } = event.attributes || {};
        if (!name || !start_date) return false;
        if (canceled_at) return false;
        const eventDate = new Date(new Date(start_date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
        return eventDate >= new Date("2024-01-01T00:00:00-08:00");
      })
      .map(event => ({
        event_start: event.attributes?.start_date,
        event_id: event.id,
        jurisdiction: event.attributes?.name,
        name: event.attributes?.attendee_name
      }));

    return { campaigns: filteredCampaigns, events: strategySessions };
  } catch (error) {
    if (error.response) {
      console.error("API Error:", error.response.status, error.response.data);
      throw new Error(`API returned status ${error.response.status}`);
    } else if (error.request) {
      console.error("Network Error:", error.message);
      throw new Error("Network error or timeout connecting to API.");
    } else {
      console.error("Script Error:", error.message);
      throw new Error(`Script error: ${error.message}`);
    }
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

const sendLPCCACToGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: "serviceToken.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const sheetName = "Customer Acquisition Costs";
  const readRange = `${sheetName}!A2:R`;

  const blackFontFormat = {
    userEnteredFormat: {
      textFormat: {
        foregroundColor: { red: 0, green: 0, blue: 0 },
      },
    },
  };

  const getGridRange = (sheetId, rowIndex, startCol, endCol) => ({
    sheetId: sheetId,
    startRowIndex: rowIndex - 1,
    endRowIndex: rowIndex,
    startColumnIndex: startCol,
    endColumnIndex: endCol,
  });

  try {
    const { campaigns, events } = await getRawCampaigns();

    const startDate = new Date("2024-01-01");
    const today = new Date();
    const months = {};

    const nopeStages = {
      CA: new Set(["21589", "80193", "113690", "26783"]),
      AZ: new Set(["111596", "111597", "111599"]),
      WA: new Set(["110790", "110791", "110793", "110794"]),
    };

    const eventLikeStages = {
      CA: new Set(["21590", "37830", "21574", "135261", "81918", "60522", "21576", "21600", "36749", "58113", "21591", "21575"]),
      AZ: new Set(["111631", "126229", "111632", "111633", "111634", "129101", "111635", "111636"]),
      WA: new Set(["144176", "144177", "143884", "144178", "144179", "144180", "144181", "144182", "144183"]),
    };
    
    const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    
    const processDate = (date) => {
      if (!date) return null;
      const parsedDate = new Date(new Date(date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
      return parsedDate < startDate ? null : parsedDate;
    };
    
    const getMonthEntry = (date) => {
      const year = date.getFullYear();     // <-- Changed
      const monthIndex = date.getMonth();  // <-- Changed
      const key = `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
      if (!months[key]) {
        months[key] = {
          year: String(year), month: monthNames[monthIndex],
          leadsCA: 0, leadsAZ: 0, leadsWA: 0,
          nopesCA: 0, nopesAZ: 0, nopesWA: 0,
          sqlsCA: 0, sqlsAZ: 0, sqlsWA: 0,
          ssCA: 0, ssAZ: 0, ssWA: 0,
        };
      }
      return months[key];
    };

    campaigns.forEach(({ created_at, stage_id, jurisdiction }) => {
      const createdDate = processDate(created_at);
      if (!createdDate) return;
      if (createdDate > today && (createdDate.getMonth() !== currentMonth || createdDate.getFullYear() !== currentYear)) return;

      const monthData = getMonthEntry(createdDate);
      const region =
        (eventLikeStages.AZ.has(stage_id) || nopeStages.AZ.has(stage_id)) ? "AZ" :
        (eventLikeStages.CA.has(stage_id) || nopeStages.CA.has(stage_id)) ? "CA" :
        (eventLikeStages.WA.has(stage_id) || nopeStages.WA.has(stage_id)) ? "WA" :
        jurisdiction?.toLowerCase() === "arizona" ? "AZ" :
        jurisdiction?.toLowerCase() === "california" ? "CA" :
        jurisdiction?.toLowerCase() === "washington" ? "WA" : null;

      if (region === "CA") {
        monthData.leadsCA++;
        if (nopeStages.CA.has(stage_id)) monthData.nopesCA++;
        if (eventLikeStages.CA.has(stage_id)) monthData.sqlsCA++;
      } else if (region === "AZ") {
        monthData.leadsAZ++;
        if (nopeStages.AZ.has(stage_id)) monthData.nopesAZ++;
        if (eventLikeStages.AZ.has(stage_id)) monthData.sqlsAZ++;
      } else if (region === "WA") {
        monthData.leadsWA++;
        if (nopeStages.WA.has(stage_id)) monthData.nopesWA++;
        if (eventLikeStages.WA.has(stage_id)) monthData.sqlsWA++;
      }
    });
    
    events.forEach(({ event_start, jurisdiction }) => {
      const eventDate = processDate(event_start);
      if (!eventDate) return;

      if (eventDate > today && 
          (eventDate.getMonth() !== today.getMonth() || eventDate.getFullYear() !== today.getFullYear())
      ) return;

      const monthData = getMonthEntry(eventDate);

      let region = null;
      const j = jurisdiction.trim();

      if (j === "AZ - Strategy Session" || j.startsWith("AZ - Strategy Session -")) region = "AZ";
      else if (j === "CA - Strategy Session" || j.startsWith("CA - Strategy Session -")) region = "CA";
      else if (j === "WA - Strategy Session" || j.startsWith("WA - Strategy Session -")) region = "WA";

      if (!region && j === "Strategy Session" && eventDate.getFullYear() === 2025 && eventDate.getMonth() < 11) {
        region = "CA";
      }

      if (region === "CA") monthData.ssCA++;
      else if (region === "AZ") monthData.ssAZ++;
      else if (region === "WA") monthData.ssWA++;
    });

    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
    const sheet = spreadsheet.data.sheets.find(s => s.properties.title === sheetName);
    const sheetId = sheet?.properties.sheetId;

    if (typeof sheetId === 'undefined') {
      throw new Error(`Could not find sheet with name "${sheetName}"`);
    }

    const sheetData = await sheets.spreadsheets.values.get({ spreadsheetId, range: readRange });
    const rows = sheetData.data.values || [];
    const labelToRow = {};
    let lastDataRowIndex = 1;
    rows.forEach((r, i) => {
      const year = r[0]; const month = r[1];
      const currentRowIndex = i + 2;
      if (year && month) {
        labelToRow[year + month] = currentRowIndex;
        lastDataRowIndex = currentRowIndex;
      }
    });

    const valueUpdateRequests = [];
    const masterBatchRequests = [];
    const newMonthData = []; 

    const sortedMonthKeys = Object.keys(months).sort();

    for (const monthKey of sortedMonthKeys) {
      const data = months[monthKey];
      const key = data.year + data.month;

      if (labelToRow[key]) {
        const rowIndex = labelToRow[key];
  
        valueUpdateRequests.push({
          range: `${sheetName}!D${rowIndex}:F${rowIndex}`,
          values: [[data.leadsCA || null, data.leadsAZ || null, data.leadsWA || null]],
        });
        valueUpdateRequests.push({
          range: `${sheetName}!H${rowIndex}:J${rowIndex}`,
          values: [[data.nopesCA || null, data.nopesAZ || null, data.nopesWA || null]],
        });
        valueUpdateRequests.push({
          range: `${sheetName}!L${rowIndex}:N${rowIndex}`,
          values: [[data.sqlsCA || null, data.sqlsAZ || null, data.sqlsWA || null]],
        });
        valueUpdateRequests.push({
          range: `${sheetName}!P${rowIndex}:R${rowIndex}`,
          values: [[data.ssCA || null, data.ssAZ || null, data.ssWA || null]],
        });

        const fields = "userEnteredFormat.textFormat.foregroundColor";
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, rowIndex, 3, 6), cell: blackFontFormat, fields: fields } // D-F
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, rowIndex, 7, 10), cell: blackFontFormat, fields: fields } // H-J
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, rowIndex, 11, 14), cell: blackFontFormat, fields: fields } // L-N
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, rowIndex, 15, 18), cell: blackFontFormat, fields: fields } // P-R
        });

      } else {
        newMonthData.push(data);
      }
    }

    // --- Execute Google Sheets API Calls ---

    // 1. Update all existing row VALUES
    if (valueUpdateRequests.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: "USER_ENTERED",
          data: valueUpdateRequests,
        },
      });
    }

    // 2. Insert new rows (if any)
    if (newMonthData.length > 0) {
      const insertionStartIndex = lastDataRowIndex;

      // Add row insertion request to our master list
      masterBatchRequests.push({
        insertDimension: {
          range: {
            sheetId: sheetId,
            dimension: "ROWS",
            startIndex: insertionStartIndex,
            endIndex: insertionStartIndex + newMonthData.length,
          },
        },
      });

      // Prepare value and format requests for the *newly inserted* rows
      const newDataValueRequests = [];
      newMonthData.forEach((data, i) => {
        const newRowIndex = insertionStartIndex + 1 + i; // 1-based row number
        
        const newRow = new Array(18).fill(null);
        newRow[0] = data.year;    // Col A
        newRow[1] = data.month;   // Col B
        newRow[3] = data.leadsCA || null; // Col D
        newRow[4] = data.leadsAZ || null; // Col E
        newRow[5] = data.leadsWA || null; // Col F
        newRow[7] = data.nopesCA || null; // Col H
        newRow[8] = data.nopesAZ || null; // Col I
        newRow[9] = data.nopesWA || null; // Col J
        newRow[11] = data.sqlsCA || null; // Col L
        newRow[12] = data.sqlsAZ || null; // Col M
        newRow[13] = data.sqlsWA || null; // Col N
        newRow[15] = data.ssCA || null; // Col P
        newRow[16] = data.ssAZ || null; // Col Q
        newRow[17] = data.ssWA || null; // Col R

        newDataValueRequests.push({
          range: `${sheetName}!A${newRowIndex}:R${newRowIndex}`,
          values: [newRow],
        });

        const fields = "userEnteredFormat.textFormat.foregroundColor";
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, newRowIndex, 3, 6), cell: blackFontFormat, fields: fields } // D-F
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, newRowIndex, 7, 10), cell: blackFontFormat, fields: fields } // H-J
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, newRowIndex, 11, 14), cell: blackFontFormat, fields: fields } // L-N
        });
        masterBatchRequests.push({
          repeatCell: { range: getGridRange(sheetId, newRowIndex, 15, 18), cell: blackFontFormat, fields: fields } // P-R
        });
      });

      // 3. Populate the new rows with data
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource: {
          valueInputOption: "USER_ENTERED",
          data: newDataValueRequests,
        },
      });
    }

    // 4. Send all formatting and row-insertion requests at once
    if (masterBatchRequests.length > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        resource: { requests: masterBatchRequests },
      });
    }

    if (valueUpdateRequests.length === 0 && newMonthData.length === 0) {
      console.log("No new data to update or insert.");
    }
  } catch (error) {
    console.error("Error processing monthly CAC report:", error);
  }
};

const sendLPCDetailedBudgettoGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const readRange = 'Location Spend!A2:S';

  try {
    await fetchAndSaveAdCosts();
    await new Promise((r) => setTimeout(r, 1000));
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

    const googleCAMap = mapGoogleData(record.googleCA);
    const googleAZMap = mapGoogleData(record.googleAZ);
    const googleWAMap = mapGoogleData(record.googleWA);
    
    const bingCAMap = mapBingData(record.bingCA);
    const bingAZMap = mapBingData(record.bingAZ);
    const bingWAMap = mapBingData(record.bingWA);

    const allMonths = Array.from(new Set([
      ...Object.keys(googleCAMap),
      ...Object.keys(googleAZMap),
      ...Object.keys(googleWAMap),
      ...Object.keys(bingCAMap),
      ...Object.keys(bingAZMap),
      ...Object.keys(bingWAMap),
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
      row[2] = googleCAMap[date]?.cost || 0;
      row[3] = bingCAMap[date] || 0;
      row[6] = googleCAMap[date]?.localServices || 0;
      row[8] = googleAZMap[date]?.cost || 0;
      row[9] = bingAZMap[date] || 0;
      row[12] = googleAZMap[date]?.localServices || 0;
      row[14] = googleWAMap[date]?.cost || 0;
      row[15] = bingWAMap[date] || 0;
      row[18] = googleWAMap[date]?.localServices || 0;

      if (labelToRow[date]) {
        const rowIndex = labelToRow[date];
        updateRequests.push({
          range: `Location Spend!C${rowIndex}:D${rowIndex}`,
          values: [[row[2], row[3]]],
        });
        updateRequests.push({
          range: `Location Spend!G${rowIndex}`,
          values: [[row[6]]],
        });
        updateRequests.push({
          range: `Location Spend!I${rowIndex}:J${rowIndex}`,
          values: [[row[8], row[9]]],
        });
        updateRequests.push({
          range: `Location Spend!M${rowIndex}`,
          values: [[row[12]]],
        });
        updateRequests.push({
          range: `Location Spend!O${rowIndex}:P${rowIndex}`,
          values: [[row[14], row[15]]],
        });
        updateRequests.push({
          range: `Location Spend!S${rowIndex}`,
          values: [[row[18]]],
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
        range: 'Location Spend!A:S',
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

    await sendLPCCACToGoogleSheets(req, res);
    await delay(500);
    await sendLPCDetailedBudgettoGoogleSheets(req, res);

    console.log("LP+C CAC & Monthly Detailed Budget successfully");
  } catch (error) {
    console.error("Error sending reports:", error);
  }
};

async function testLawmaticsMonthly() {
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  
  const nopeStages = {
    CA: new Set(["21589", "80193", "113690", "26783"]),
    AZ: new Set(["111596", "111597", "111599"]),
    WA: new Set(["110790", "110791", "110793", "110794"]),
  };
  const eventLikeStages = {
    CA: new Set(["21590", "37830", "21574", "135261", "81918", "60522", "21576", "21600", "36749", "58113", "21591", "21575"]),
    AZ: new Set(["111631", "126229", "111632", "111633", "111634", "129101", "111635", "111636"]),
    WA: new Set(["144176", "144177", "143884", "144178", "144179", "144180", "144181", "144182", "144183"]),
  };

  const processDate = (date) => {
    if (!date) return null;
    return new Date(new Date(date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
  };

  const getMonthEntry = (months, date) => {
    const year = date.getFullYear();
    const monthIndex = date.getMonth();
    const key = `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
    if (!months[key]) {
      months[key] = {
        year: String(year),
        month: monthNames[monthIndex],
        leads: { CA: [], AZ: [], WA: [] },
        nopes: { CA: [], AZ: [], WA: [] },
        sqls: { CA: [], AZ: [], WA: [] },
        ss: { CA: [], AZ: [], WA: [] },
      };
    }
    return months[key];
  };

  try {
    const { campaigns, events } = await getRawCampaigns();
    const today = new Date();
    const currentMonth = today.getMonth();
    const currentYear = today.getFullYear();
    const months = {};

    // Process campaigns (leads, nopes, SQLs)
    campaigns.forEach(c => {
      const date = processDate(c.created_at);
      if (!date) return;
      if (date > today && (date.getMonth() !== currentMonth || date.getFullYear() !== currentYear)) return;

      const monthData = getMonthEntry(months, date);
      const region =
        (eventLikeStages.AZ.has(c.stage_id) || nopeStages.AZ.has(c.stage_id)) ? "AZ" :
        (eventLikeStages.CA.has(c.stage_id) || nopeStages.CA.has(c.stage_id)) ? "CA" :
        (eventLikeStages.WA.has(c.stage_id) || nopeStages.WA.has(c.stage_id)) ? "WA" :
        c.jurisdiction?.toLowerCase() === "arizona" ? "AZ" :
        c.jurisdiction?.toLowerCase() === "california" ? "CA" :
        c.jurisdiction?.toLowerCase() === "washington" ? "WA" : null;

      if (region === "CA") {
        monthData.leads.CA.push({ ...c });
        if (nopeStages.CA.has(c.stage_id)) monthData.nopes.CA.push({ ...c });
        if (eventLikeStages.CA.has(c.stage_id)) monthData.sqls.CA.push({ ...c });
      } else if (region === "AZ") {
        monthData.leads.AZ.push({ ...c });
        if (nopeStages.AZ.has(c.stage_id)) monthData.nopes.AZ.push({ ...c });
        if (eventLikeStages.AZ.has(c.stage_id)) monthData.sqls.AZ.push({ ...c });
      } else if (region === "WA") {
        monthData.leads.WA.push({ ...c });
        if (nopeStages.WA.has(c.stage_id)) monthData.nopes.WA.push({ ...c });
        if (eventLikeStages.WA.has(c.stage_id)) monthData.sqls.WA.push({ ...c });
      }
    });

    events.forEach(e => {
      const date = processDate(e.event_start);
      if (!date) return;

      if (date > today && (date.getMonth() !== currentMonth || date.getFullYear() !== currentYear)) return;

      const monthData = getMonthEntry(months, date);

      let region = null;
      const jurisdiction = e.jurisdiction.trim(); // remove trailing spaces

      // Strict match for standard "X - Strategy Session"
      if (jurisdiction === "AZ - Strategy Session" || jurisdiction.startsWith("AZ - Strategy Session -")) region = "AZ";
      else if (jurisdiction === "CA - Strategy Session" || jurisdiction.startsWith("CA - Strategy Session -")) region = "CA";
      else if (jurisdiction === "WA - Strategy Session" || jurisdiction.startsWith("WA - Strategy Session -")) region = "WA";

      // Special case: generic "Strategy Session" (historical, only CA before November)
      if (!region && jurisdiction === "Strategy Session") {
        if (date.getFullYear() === 2025 && date.getMonth() < 11) region = "CA"; // month < 10 means Jan-Oct
      }

      if (region) monthData.ss[region].push(e);
    });

    // Convert arrays to counts for reporting
    const monthlyReport = {};
    Object.keys(months).forEach(key => {
      monthlyReport[key] = {
        year: months[key].year,
        month: months[key].month,
        leads: { CA: months[key].leads.CA.length, AZ: months[key].leads.AZ.length, WA: months[key].leads.WA.length },
        nopes: { CA: months[key].nopes.CA.length, AZ: months[key].nopes.AZ.length, WA: months[key].nopes.WA.length },
        sqls: { CA: months[key].sqls.CA.length, AZ: months[key].sqls.AZ.length, WA: months[key].sqls.WA.length },
        ss: { CA: months[key].ss.CA.length, AZ: months[key].ss.AZ.length, WA: months[key].ss.WA.length },
        details: months[key]
      };
    });

    fs.writeFileSync("campaigns_monthly_report.json", JSON.stringify(monthlyReport, null, 2));
    console.log("Monthly report generated:", Object.keys(monthlyReport));

    return monthlyReport;

  } catch (error) {
    console.error("Error processing campaigns:", error);
    throw error;
  }
}

module.exports = {
  generateLPCBing,
  fetchAndSaveAdCosts,
  executeSpecificFetchFunctionLPC,
  sendLPCCACToGoogleSheets,
  sendLPCDetailedBudgettoGoogleSheets,
  sendLPCMonthlyReport,
  testLawmaticsMonthly,
};