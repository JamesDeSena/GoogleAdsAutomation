const { google } = require("googleapis");
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const fs = require("fs");
const path = require("path");

const fetchDailyRegionCostReport = async (customer, dateRanges) => {
  const finalReportByDate = {};
  const allGeoTargetIds = new Set();
  const dailyRowsMap = {};

  for (const range of dateRanges) {
    const { start: date } = range;

    const reportQuery = `
      SELECT
        segments.date,
        segments.geo_target_region,
        metrics.cost_micros
      FROM
        geographic_view
      WHERE
        segments.date = '${date}'
        AND metrics.cost_micros > 0
    `;

    try {
      const rows = await customer.query(reportQuery);
      const rowsForThisDate = [];

      for (const row of rows) {
        const geoId = row.segments.geo_target_region; 
        const cost = parseFloat((row.metrics.cost_micros / 1_000_000).toFixed(2));
        if (!geoId) continue;

        allGeoTargetIds.add(geoId);
        rowsForThisDate.push({
          date: row.segments.date,
          geoId,
          cost,
        });
      }

      dailyRowsMap[date] = rowsForThisDate;
    } catch {}
  }

  const geoIdArray = Array.from(allGeoTargetIds);
  const geoInfoMap = {};

  if (geoIdArray.length > 0) {
    const chunkSize = 50;
    for (let i = 0; i < geoIdArray.length; i += chunkSize) {
      const chunk = geoIdArray.slice(i, i + chunkSize);
      const placeholders = chunk.map(id => `'${id}'`).join(",");

      const geoQuery = `
        SELECT
          geo_target_constant.resource_name,
          geo_target_constant.name
        FROM geo_target_constant
        WHERE geo_target_constant.resource_name IN (${placeholders})
      `;

      try {
        const geoRows = await customer.query(geoQuery);
        for (const g of geoRows) {
          geoInfoMap[g.geo_target_constant.resource_name] = g.geo_target_constant.name;
        }
      } catch {}
    }
  }

  for (const [date, rows] of Object.entries(dailyRowsMap)) {
    
    const regionTotals = {
      TX: { date: date, regionName: "TX", cost: 0 },
      AZ: { date: date, regionName: "AZ", cost: 0 },
      DMV: { date: date, regionName: "DMV", cost: 0 }
    };

    for (const row of rows) {
      const regionName = geoInfoMap[row.geoId] || "Unknown"; 
      
      if (regionName === 'Texas') {
        regionTotals.TX.cost += row.cost;
      } else if (regionName === 'Arizona') {
        regionTotals.AZ.cost += row.cost;
      } else if (regionName === 'Virginia' || regionName === 'Maryland' || regionName === 'District of Columbia') {
        regionTotals.DMV.cost += row.cost;
      }
    }

    const finalEntries = [
      regionTotals.TX,
      regionTotals.AZ,
      regionTotals.DMV
    ]
    .filter(region => region.cost > 0)
    .map(region => ({
      ...region,
      cost: parseFloat(region.cost.toFixed(2))
    }));

    finalReportByDate[date] = finalEntries;
  }

  const jsonFilePath = path.join(__dirname, "daily_region_cost_report.json");
  try {
    if (Object.keys(finalReportByDate).length === 0) {
      console.log("⚠️ No data found. JSON file not written.");
    } else {
      fs.writeFileSync(jsonFilePath, JSON.stringify(finalReportByDate, null, 2));
      console.log(`📁 Report saved to ${jsonFilePath}`);
    }
  } catch (err) {
    console.error(`❌ Failed to write JSON file:`, err?.message || err);
  }

  return finalReportByDate;
};

const runDailyLocationReport = async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: "serviceToken.json",
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = process.env.SHEET_HI_SKIN_TL;
    const sheetName = "MTD Market Spend";

    const dateColumnResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!B4:B`,
    });

    const rawDateRows = dateColumnResponse.data.values || [];
    const dateRows = rawDateRows
      .map(r => r[0])
      .filter(v => v && v.toLowerCase() !== "total");

    if (!dateRows.length) {
      console.log("⚠️ No dates found in sheet.");
      return;
    }

    const CURRENT_YEAR = new Date().getFullYear();

    const dateRanges = dateRows.map(dateText => {
      const [monthName, day] = dateText.split(" ");
      const month = new Date(`${monthName} 1`).getMonth() + 1;
      return {
        start: `${CURRENT_YEAR}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
      };
    });

    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: getStoredGoogleToken(),
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    await fetchDailyRegionCostReport(customer, dateRanges);

    console.log("✅ Final Hi Skin daily report generated using sheet dates!");
  } catch (error) {
    console.error("❌ Error running Hi Skin daily location report:", error?.message || error);
  }
};

const sendFinalDailyReportToGoogleSheetsHS = async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: "serviceToken.json",
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = process.env.SHEET_HI_SKIN_TL;
    const sheetName = "MTD Market Spend";

    // Read Column B (Dates) from Row 4 to Row 33
    const dateColumnResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!B4:B`,
    });
    const rawDateRows = dateColumnResponse.data.values || [];
    const dateRows = rawDateRows
      .map(r => r[0])
      .filter(v => v && v.toLowerCase() !== "total");

    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: getStoredGoogleToken(),
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    // Convert sheet dates to yyyy-mm-dd format for API
    const CURRENT_YEAR = new Date().getFullYear();

    const dateRanges = dateRows.map(dateText => {
      const [monthName, day] = dateText.split(" ");
      const month = new Date(`${monthName} 1`).getMonth() + 1;
      const dd = String(day).padStart(2, "0");
      const mm = String(month).padStart(2, "0");
      return { start: `${CURRENT_YEAR}-${mm}-${dd}` };
    });

    const dailyReport = await fetchDailyRegionCostReport(customer, dateRanges);

    // Prepare L, M, N columns only
    const sheetData = dateRanges.map(({ start }) => {
      const entries = dailyReport[start] || [];
      const regionTotals = {};
      for (const entry of entries) {
        regionTotals[entry.regionName] = entry.cost;
      }
      return [
        regionTotals.AZ || null, // Column L
        regionTotals.TX || null, // Column M
        regionTotals.DMV || null // Column N
      ];
    });

    // Update only Columns L, M, N (Rows 4-33)
    const startRow = 4;
    const endRow = startRow + dateRanges.length - 1;

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${sheetName}!I${startRow}:K${endRow}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: sheetData },
    });

    console.log(`Final Hi Skin daily report sent to Google Sheets successfully!`);
  } catch (error) {
    console.error("Error sending Hi Skin daily report to Google Sheets:", error);
  }
};

module.exports = {
  runDailyLocationReport,
  sendFinalDailyReportToGoogleSheetsHS,
};
