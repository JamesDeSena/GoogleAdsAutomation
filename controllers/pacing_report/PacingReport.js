const axios = require("axios");

const { google } = require('googleapis');

const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const { getStoredBingToken } = require("../BingAuth");

const fs = require("fs");
const path = require("path");
const tokenFilePath = path.join(__dirname, "metrics.json");

function getCurrentMonth() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
};

function formatDateUTC(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
};

async function getAmountBing(accountId) {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const currentMonth = getCurrentMonth();
  const requestBody = `
    <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Header xmlns="https://bingads.microsoft.com/Billing/v13">
        <Action mustUnderstand="1">GetAccountMonthlySpend</Action>
        <AuthenticationToken i:nil="false">${token.accessToken_Bing}</AuthenticationToken>
        <DeveloperToken i:nil="false">${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
      </s:Header>
      <s:Body>
        <GetAccountMonthlySpendRequest xmlns="https://bingads.microsoft.com/Billing/v13">
          <AccountId>${accountId}</AccountId>
          <MonthYear>${currentMonth}</MonthYear>
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
    const amount = amountMatch ? parseFloat(amountMatch[1]) : null;
    return amount;
  } catch (error) {
    console.error(
      "Error fetching Bing data:",
      error.response ? error.response.data : error.message
    );
    throw error;
  }
};

async function getGoogleAdsCost(customerId, channelTypes = []) {
  const refreshToken_Google = getStoredGoogleToken();
  if (!refreshToken_Google) {
    console.error("Refresh token is missing. Please authenticate.");
    return;
  }

  const customer = client.Customer({
    customer_id: customerId,
    refresh_token: refreshToken_Google,
    login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
  });

  const now = new Date();
  const firstDayOfMonth = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)
  );
  const yesterday = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1)
  );

  const startDate = formatDateUTC(firstDayOfMonth);
  const endDate = formatDateUTC(yesterday);

  const typeFilterQuery =
    channelTypes.length > 0
      ? "AND campaign.advertising_channel_type IN (" +
      channelTypes.map(t => `'${t}'`).join(",") +
      ")"
      : "";

  const metricsQuery = `
    SELECT
      campaign.name,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      ${typeFilterQuery}
    ORDER BY
      segments.date DESC
  `;

  try {
    const metricsResponse = await customer.query(metricsQuery);

    const totalCost = metricsResponse.reduce((total, campaign) => {
      const costInDollars = campaign.metrics.cost_micros / 1_000_000;
      return total + costInDollars;
    }, 0);

    return parseFloat(totalCost.toFixed(2));
  } catch (error) {
    console.error("Error fetching Google Ads data:", error);
    throw error;
  }
};

async function getAmountGoogleLPC() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_LPC
    );
    return { GoogleLPC: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads LPC data");
  }
};

async function getAmountGoogleVault() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_VAULT
    );
    return { GoogleVault: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Vault data");
  }
};

async function getAmountGoogleHSCampaigns() {
  const refreshToken_Google = getStoredGoogleToken();

  if (!refreshToken_Google) {
    console.error("Refresh token is missing. Please authenticate.");
    return;
  }

  const customer = client.Customer({
    customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
    refresh_token: refreshToken_Google,
    login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
  });

  const now = new Date();
  const firstDayOfMonth = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)
  );
  const yesterday = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1)
  );

  const startDate = formatDateUTC(firstDayOfMonth);
  const endDate = formatDateUTC(yesterday);

  const campaigns = [
    "Brand",
    "NB",
    "PmaxBrand",
    "PmaxNB",
  ];

  try {
    let totalCosts = {};

    const campaignFilters = {
      PmaxBrand: ["%Pmax%", "%Brand%"],
      PmaxNB: ["%Pmax%", "%NB%"],
      Brand: ["%Brand%", "%Search%"],
      NB: ["%NB%"],
    };

    for (const campaignName of campaigns) {
      let whereClause = `segments.date BETWEEN '${startDate}' AND '${endDate}'`;

      const filters = campaignFilters[campaignName] || [`%${campaignName}%`];
      for (let filter of filters) {
        whereClause += ` AND campaign.name LIKE '${filter}'`;
      }

      const metricsQuery = `
        SELECT
          campaign.name,
          metrics.cost_micros,
          segments.date
        FROM
          campaign
        WHERE
          ${whereClause}
        ORDER BY
          segments.date DESC
      `;

      const metricsResponse = await customer.query(metricsQuery);

      let campaignTotalCost = 0;

      metricsResponse.forEach((campaign) => {
        const costInDollars = campaign.metrics.cost_micros / 1_000_000;
        campaignTotalCost += parseFloat(costInDollars);
      });

      totalCosts[campaignName] = parseFloat(campaignTotalCost.toFixed(2));
    }

    return totalCosts;
  } catch (error) {
    console.error("Error fetching Google Ads Hi Skin data:", error.message);
    throw new Error(`Error fetching Google Ads Hi Skin data: ${error.message}`);
  }
};

async function getAmountGoogleAZ() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ
    );
    return { AZ: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads MIVD AZ data");
  }
};

async function getAmountGoogleLV() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV
    );
    return { LV: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads MIVD LV data");
  }
};

async function getAmountGoogleNYC() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC
    );
    return { NYC: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads MIVD NYC data");
  }
};

async function getAmountGoogleTWCampaigns() {
  const refreshToken_Google = getStoredGoogleToken();

  if (!refreshToken_Google) {
    console.error("Refresh token is missing. Please authenticate.");
    return;
  }

  const customer = client.Customer({
    customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_TW,
    refresh_token: refreshToken_Google,
    login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
  });

  const now = new Date();
  const firstDayOfMonth = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)
  );
  const yesterday = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1)
  );

  const startDate = formatDateUTC(firstDayOfMonth);
  const endDate = formatDateUTC(yesterday);

  const campaigns = [
    "Search",
    "Youtube",
  ];

  try {
    let totalCosts = {};

    for (let campaignName of campaigns) {
      const metricsQuery = `
        SELECT
          campaign.name,
          metrics.cost_micros,
          segments.date
        FROM
          campaign
        WHERE
          segments.date BETWEEN '${startDate}' AND '${endDate}'
          AND campaign.name LIKE '%${campaignName}%'
        ORDER BY
          segments.date DESC
      `;

      const metricsResponse = await customer.query(metricsQuery);

      let campaignTotalCost = 0;

      metricsResponse.forEach((campaign) => {
        const costInDollars = campaign.metrics.cost_micros / 1_000_000;
        campaignTotalCost += parseFloat(costInDollars);
      });

      totalCosts[campaignName] = parseFloat(campaignTotalCost.toFixed(2));
    }

    return totalCosts;
  } catch (error) {
    throw new Error("Error fetching Google Ads Triple Whale data");
  }
};

async function getAmountGoogleGC() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_GC
    );
    return { GoogleGuardian: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Guardian Carers data");
  }
};

async function getAmountGoogleMNR() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_MNR
    );
    return { GoogleMenerals: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Menerals data");
  }
};

async function getAmountGoogleNB() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_NB
    );
    return { GoogleNB: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads NB data");
  }
};

async function getAmountGoogleST() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_ST
    );
    return { GoogleST: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Sleeping Tie data");
  }
};

async function getAmountGoogleFLX1() {
  const types = ["SHOPPING", "SEARCH", "PERFORMANCE_MAX"];
  const totalCost = await getGoogleAdsCost(
    process.env.GOOGLE_ADS_CUSTOMER_ID_FLX,
    types
  );
  return { GoogleFLX1: totalCost };
}

async function getAmountGoogleFLX2() {
  const types = ["DEMAND_GEN", "VIDEO"];
  const totalCost = await getGoogleAdsCost(
    process.env.GOOGLE_ADS_CUSTOMER_ID_FLX
  );
  return { GoogleFLX2: totalCost };
}

async function getAmountGoogleNPL() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_NPL
    );
    return { GoogleNPL: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Sleeping Tie data");
  }
};

async function getAmountBingTotal() {
  try {
    const BingLPC = await getAmountBing(
      process.env.BING_ADS_ACCOUNT_ID_LPC
    );
    const BingVault = await getAmountBing(
      process.env.BING_ADS_ACCOUNT_ID_VAULT
    );
    const BingHS = await getAmountBing(
      process.env.BING_ADS_ACCOUNT_ID_HS
    );
    return { BingLPC, BingVault, BingHS };
  } catch (error) {
    throw new Error(error.message);
  }
};

async function getAllMetrics() {
  try {
    const bingTotal = await getAmountBingTotal();
    const googleLPC = await getAmountGoogleLPC();
    const googleVault = await getAmountGoogleVault();
    const googleCampaigns = await getAmountGoogleHSCampaigns();
    const googleDripAZ = await getAmountGoogleAZ();
    const googleDripLV = await getAmountGoogleLV();
    const googleDripNYC = await getAmountGoogleNYC();
    const googleTW = await getAmountGoogleTWCampaigns();
    const googleGC = await getAmountGoogleGC();
    const googleMNR = await getAmountGoogleMNR();
    const googleNB = await getAmountGoogleNB();
    const googleST = await getAmountGoogleST();
    const googleFLX1 = await getAmountGoogleFLX1();
    const googleFLX2 = await getAmountGoogleFLX2();
    const googleNPL = await getAmountGoogleNPL();
    
    const metrics = {
      data: {
        ...bingTotal,
        ...googleLPC,
        ...googleVault,
        ...googleCampaigns,
        ...googleDripAZ,
        ...googleDripLV,
        ...googleDripNYC,
        ...googleTW,
        ...googleGC,
        ...googleMNR,
        ...googleNB,
        ...googleST,
        ...googleFLX1,
        ...googleFLX2,
        ...googleNPL,
      },
    };

    saveMetricsToFile(metrics);
    return metrics;
  } catch (error) {
    console.error("Error fetching all data:", error);
    throw new Error("Error fetching all data");
  }
};

const sendPacingReportToGoogleSheets = async () => {
  const auth = new google.auth.GoogleAuth({
    keyFile: "serviceToken.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_PACING;
  const dataRanges = "Raw Data!A2:E";

  const getFormattedDate = (timeZone) =>
    new Date().toLocaleString("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
    }).replace(",", "");

  const dateCST = getFormattedDate("Asia/Shanghai");
  const datePST = getFormattedDate("America/Los_Angeles");

  try {
    const record = await getAllMetrics();
    console.log(record)
    const newRows = [
      ["LP+C", "Google", dateCST, datePST, record.data.GoogleLPC],
      ["LP+C", "Bing", dateCST, datePST, record.data.BingLPC],
      ["The Vault", "Google", dateCST, datePST, record.data.GoogleVault],
      ["The Vault", "Bing", dateCST, datePST, record.data.BingVault],
      ["Hi, Skin", "Brand", dateCST, datePST, record.data.Brand],
      ["Hi, Skin", "NB", dateCST, datePST, record.data.NB],
      // ["Hi, Skin", "Pmax", dateCST, datePST, record.data.Pmax],
      ["Hi, Skin", "Pmax Brand", dateCST, datePST, record.data.PmaxBrand],
      ["Hi, Skin", "Pmax NB", dateCST, datePST, record.data.PmaxNB],
      ["Mobile IV Drip AZ", "Arizona", dateCST, datePST, record.data.AZ],
      ["Mobile IV Drip LV", "Las Vegas", dateCST, datePST, record.data.LV],
      ["Mobile IV Drip NYC", "New York", dateCST, datePST, record.data.NYC],
      // ["Triple Whale", "Google - Paid Search", dateCST, datePST, record.data.Search],
      // ["Triple Whale", "Google - Youtube", dateCST, datePST, record.data.Youtube],
      ["Guardian Carers", "Google", dateCST, datePST, record.data.GoogleGuardian],
      ["Menerals", "Google", dateCST, datePST, record.data.GoogleMenerals],
      // ["National Buyers", "Google", dateCST, datePST, record.data.GoogleNB],
      ["Sleepy Tie", "Google", dateCST, datePST, record.data.GoogleST],
      ["Flex", "Search, Shopping, Pmax", dateCST, datePST, record.data.GoogleFLX1],
      ["Flex", "DemandGen, Video", dateCST, datePST, record.data.GoogleFLX2],
      ["Nations Photo Lab", "Google", dateCST, datePST, record.data.GoogleNPL],
    ];

    const existingData = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges,
    });

    const existingRows = existingData.data.values || [];

    const updatedRows = [...newRows, ...existingRows];

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRanges,
      valueInputOption: "RAW",
      resource: {
        values: updatedRows,
      },
    });

    console.log("Pacing report prepended to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending pacing report to Google Sheets:", error);
  }
};

function saveMetricsToFile(metrics) {
  try {
    let currentData = {};

    console.log(tokenFilePath)
    
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, "utf8"));
    }
    
    if (JSON.stringify(currentData) !== JSON.stringify(metrics)) {
      fs.writeFileSync(tokenFilePath, JSON.stringify(metrics, null, 2));
    }
  } catch (error) {
    console.error("Error saving metrics data:", error);
  }
}

module.exports = {
  getAllMetrics,
  sendPacingReportToGoogleSheets,
};
