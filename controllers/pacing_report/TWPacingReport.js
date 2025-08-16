const { google } = require('googleapis');
const { getStoredLinkedinToken } = require("../LinkedinAuth");
const axios = require('axios');

const fs = require("fs");
const path = require("path");
const tokenFilePath = path.join(__dirname, "metrics.json");

async function getLinkedinTotalSpend(startDate, endDate) {
  const accessToken = getStoredLinkedinToken();
  const adAccountId = process.env.LINKEDIN_AD_ACCOUNT_ID;

  if (!accessToken || !adAccountId) {
    throw new Error("LinkedIn Access Token or Ad Account ID is missing from environment variables.");
  }

  const start = new Date(startDate);
  const end = new Date(endDate);

  const q = `q=analytics`;
  const dateRange = `dateRange=(start:(year:${start.getFullYear()},month:${start.getMonth() + 1},day:${start.getDate()}),end:(year:${end.getFullYear()},month:${end.getMonth() + 1},day:${end.getDate()}))`;
  const timeGranularity = `timeGranularity=ALL`;
  const accounts = `accounts=List(urn%3Ali%3AsponsoredAccount%3A510102400)`;
  const fields = `fields=costInUsd,impressions,clicks`;

  const finalUrl = `https://api.linkedin.com/rest/adAnalytics?${q}&${dateRange}&${timeGranularity}&${accounts}&${fields}`;

  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'LinkedIn-Version': '202507',
    'X-Restli-Protocol-Version': '2.0.0',
  };

  try {
    const response = await axios.get(finalUrl, { headers });
    const result = response.data.elements[0];
    
    if (result && result.costInUsd) {
      const totalCost = parseFloat(result.costInUsd);
      console.log(`LinkedIn Spend from ${startDate} to ${endDate}: $${totalCost.toFixed(2)}`);
      return totalCost;
    } else {
      console.log(`No LinkedIn spend data found from ${startDate} to ${endDate}.`);
      return 0;
    }
  } catch (error) {
    console.error("Error fetching LinkedIn data:", error.response ? JSON.stringify(error.response.data, null, 2) : error.message);
    throw error;
  }
}

const runLinkedinReport = async () => {
  try {
    const today = new Date();
    const startDateObj = new Date(today.getFullYear(), today.getMonth(), 1);
    const endDateObj = new Date(today);
    endDateObj.setDate(today.getDate() - 1);
    
    const formatDate = (date) => {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0'); 
      return `${year}-${month}-${day}`;
    };

    const startDate = formatDate(startDateObj);
    const endDate = formatDate(endDateObj);

    const linkedinSpend = await getLinkedinTotalSpend(startDate, endDate);
    
    return linkedinSpend;
  } catch (error) {
    console.error("Failed to run the Linkedin report.", error);
  }
};

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

const sendTWtoGoogleSheets = async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: 'serviceToken.json',
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = process.env.SHEET_TW;
    const record = getStoredMetrics();
    const linkedinCost = await runLinkedinReport();

    if (!record?.data.Search || !record?.data.Youtube) {
      throw new Error("Missing data for Search or Youtube");
    }

    const values = [[record.data.Search], [record.data.Youtube], [linkedinCost]];
    const range = 'Pacing!E3:E5';

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: "RAW",
      resource: { values },
    });

    console.log("TW budget updated in Google Sheets successfully!");
  } catch (error) {
    console.error("Error updating TW budget in Google Sheets:", error);
  }
};

module.exports = {
  runLinkedinReport,
  sendTWtoGoogleSheets,
};