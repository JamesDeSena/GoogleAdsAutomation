const axios = require("axios");
const Airtable = require("airtable");
const schedule = require("node-schedule");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_PACING
);
const { client } = require("../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("./GoogleAuth");
const { getStoredAccessToken } = require("./BingAuth");

function getCurrentMonth() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function formatDateUTC(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

async function getAmountBing(accountId) {
  const token = getStoredAccessToken();
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
}

async function getGoogleAdsCost(customerId) {
  const refreshToken_Google = getStoredRefreshToken();
  console.log(refreshToken_Google)
  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
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

  const metricsQuery = `
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
}

async function getAmountGoogleLPC() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_LPC
    );
    return { GoogleLPC: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads LPC data");
  }
}

async function getAmountGoogleVault() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_VAULT
    );
    return { GoogleVault: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads Vault data");
  }
}

async function getAmountGoogleWB() {
  try {
    const totalCost = await getGoogleAdsCost(
      process.env.GOOGLE_ADS_CUSTOMER_ID_WB
    );
    return { WB: totalCost };
  } catch (error) {
    throw new Error("Error fetching Google Ads WB data");
  }
}

async function getAmountGoogleCampaigns() {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
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
    "MKTHeights",
    "Gilbert",
    "Scottsdale",
    "Phoenix",
    "Montrose",
    "Uptown",
    "RiceVillage",
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
    throw new Error("Error fetching Google Ads campaigns data");
  }
}

async function getAmountBingTotal() {
  try {
    const BingLPC = await getAmountBing(
      process.env.BING_ADS_ACCOUNT_ID_LPC
    );
    const BingVault = await getAmountBing(
      process.env.BING_ADS_ACCOUNT_ID_VAULT
    );
    return { BingLPC, BingVault };
  } catch (error) {
    throw new Error(error.message);
  }
}

async function getAllMetrics() {
  try {
    const bingTotal = await getAmountBingTotal();
    const googleLPC = await getAmountGoogleLPC();
    const googleVault = await getAmountGoogleVault();
    const googleWB = await getAmountGoogleWB();
    const googleCampaigns = await getAmountGoogleCampaigns();

    console.log({
      data: {
        ...bingTotal,
        ...googleLPC,
        ...googleVault,
        ...googleWB,
        ...googleCampaigns,
      },
    });

    return {
      data: {
        ...bingTotal,
        ...googleLPC,
        ...googleVault,
        ...googleWB,
        ...googleCampaigns,
      },
    };
  } catch (error) {
    console.error("Error fetching all data:", error.message);
    throw new Error("Error fetching all data");
  }
}

const sendFinalReportToAirtable = async () => {
  try {
    const record = await getAllMetrics();
    const isoDateString = new Date().toISOString();

    const records = [
      {
        fields: {
          Date: isoDateString,
          Brand: "LP+C",
          Campaign: "Total",
          "Monthly Budget": 31000.0,
          "MTD Spend": record.data.BingLPC + record.data.GoogleLPC,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "LP+C",
          Campaign: "Google",
          "Monthly Budget": 25000.0,
          "MTD Spend": record.data.GoogleLPC,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "LP+C",
          Campaign: "Bing",
          "Monthly Budget": 1000.0,
          "MTD Spend": record.data.BingLPC,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "The Vault",
          Campaign: "Total",
          "Monthly Budget": 10000.0,
          "MTD Spend": record.data.BingVault + record.data.GoogleVault,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "The Vault",
          Campaign: "Google",
          "Monthly Budget": 0,
          "MTD Spend": record.data.GoogleVault,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "The Vault",
          Campaign: "Bing",
          "Monthly Budget": 0,
          "MTD Spend": record.data.BingVault,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Wall Blush",
          Campaign: "Google",
          "Monthly Budget": 40000.0,
          "MTD Spend": record.data.WB,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Google Total",
          "Monthly Budget": 9000.0,
          "MTD Spend":
            record.data.MKTHeights +
            record.data.Gilbert +
            record.data.Scottsdale +
            record.data.Phoenix +
            record.data.Montrose +
            record.data.Uptown +
            record.data.RiceVillage,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Houston_MKTHeights",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.MKTHeights,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Gilbert",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.Gilbert,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Scottsdale",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.Scottsdale,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Phoenix",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.Phoenix,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Houston_Montrose",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.Montrose,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Houston_UptownPark",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.Uptown,
        },
      },
      {
        fields: {
          Date: isoDateString,
          Brand: "Hi, Skin",
          Campaign: "Rice Village",
          "Monthly Budget": 1500.0,
          "MTD Spend": record.data.RiceVillage,
        },
      },
    ];

    const batchSize = 10;
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      await base("Pacing Report").create(batch);
      console.log(
        `Batch of ${batch.length} records sent to Airtable successfully!`
      );
    }

    console.log("Pacing report sent to Airtable successfully!");
  } catch (error) {
    console.error("Error sending pacing report to Airtable:", error);
  }
};

const rule = new schedule.RecurrenceRule();
rule.hour = 7;
rule.minute = 0;
rule.tz = 'America/Los_Angeles';

const job = schedule.scheduleJob(rule, () => {
  sendFinalReportToAirtable();
  console.log("Scheduled report sent at 7 AM PST California/Irvine.");
});

// schedule.scheduleJob('* * * * *', () => {
//   console.log("Testing Node Schedule");
// });

module.exports = {
  getAllMetrics,
  sendFinalReportToAirtable,
};