const { google } = require("googleapis");
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const fs = require("fs");
const path = require("path");

let storedDateRanges = null;

const generateDailyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentStartDate = new Date(startDate + 'T00:00:00Z');
  const adjustedEndDate = new Date(endDate + 'T00:00:00Z');

  while (currentStartDate <= adjustedEndDate) {
    const dateStr = currentStartDate.toISOString().split("T")[0];
    dateRanges.push({
      start: dateStr,
      end: dateStr,
    });
    currentStartDate.setUTCDate(currentStartDate.getUTCDate() + 1);
  }
  return dateRanges;
};

const getOrGenerateDateRanges = (inputStartDate = null) => {
  if (inputStartDate) {
    return generateDailyDateRanges(inputStartDate, inputStartDate);
  }

  const today = new Date(); 
  const dayOfWeek = today.getDay();
  const daysUntilSunday = (7 - dayOfWeek) % 7; 
  const endDate = new Date(today);
  endDate.setDate(today.getDate() + daysUntilSunday); 
  const endDateString = endDate.toISOString().split("T")[0];

  const startDate = "2025-11-01";

  if (
    !storedDateRanges ||
    new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate
  ) {
    storedDateRanges = generateDailyDateRanges(startDate, endDateString);
  }
  return storedDateRanges;
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const fetchDailyPostalCodeCostReport = async (customer, dateRanges) => {
  const finalReportByDate = {};
  const allGeoTargetIds = new Set();
  const dailyRowsMap = {};

  for (const range of dateRanges) {
    const { start: date } = range;

    const reportQuery = `
      SELECT
        segments.date,
        segments.geo_target_postal_code,
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
        const geoId = row.segments.geo_target_postal_code;
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
          geo_target_constant.name,
          geo_target_constant.canonical_name
        FROM geo_target_constant
        WHERE geo_target_constant.resource_name IN (${placeholders})
      `;

      try {
        const geoRows = await customer.query(geoQuery);
        for (const g of geoRows) {
          geoInfoMap[g.geo_target_constant.resource_name] = {
            name: g.geo_target_constant.name,
            canonical_name: g.geo_target_constant.canonical_name,
          };
        }
      } catch {}
    }
  }

  for (const [date, rows] of Object.entries(dailyRowsMap)) {
    const combinedMap = new Map();

    for (const row of rows) {
      const geoInfo = geoInfoMap[row.geoId] || {};
      const postalCode = geoInfo.name || row.geoId;
      const canonicalName = geoInfo.canonical_name || null;

      const key = `${postalCode}_${canonicalName}`;
      if (!combinedMap.has(key)) {
        combinedMap.set(key, { date: row.date, postalCode, canonicalName, cost: 0 });
      }
      combinedMap.get(key).cost += row.cost;
    }

    const entries = Array.from(combinedMap.values());

    const totals = { TX: 0, AZ: 0, DMV: 0 };
    for (const item of entries) {
      const postal = item.postalCode.toString();
      if (postal.startsWith("77")) totals.TX += item.cost;
      else if (postal.startsWith("85")) totals.AZ += item.cost;
      else if (postal.startsWith("20") || postal.startsWith("22")) totals.DMV += item.cost;
    }

    entries.push({
      regionTotals: {
        TX: parseFloat(totals.TX.toFixed(2)),
        AZ: parseFloat(totals.AZ.toFixed(2)),
        DMV: parseFloat(totals.DMV.toFixed(2)),
      },
    });

    finalReportByDate[date] = entries;
  }

  // const jsonFilePath = path.join(__dirname, "daily_postal_code_cost_report.json");
  // try {
  //   if (Object.keys(finalReportByDate).length > 0) {
  //     fs.writeFileSync(jsonFilePath, JSON.stringify(finalReportByDate, null, 2));
  //   }
  // } catch {}

  return finalReportByDate;
};

const runDailyLocationReport = async (req, res) => {
  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    if (!dateRanges?.length) {
      console.log("⚠️ No date ranges to process.");
      return;
    }

    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: getStoredGoogleToken(),
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    await fetchDailyPostalCodeCostReport(customer, dateRanges);
    console.log("✅ Final Hi Skin daily report sent successfully!");
  } catch (error) {
    console.error("❌ Error sending Hi Skin daily report:", error?.message || error);
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
      range: `${sheetName}!B4:B33`,
    });
    const dateRows = dateColumnResponse.data.values || [];

    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: getStoredGoogleToken(),
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    // Convert sheet dates to yyyy-mm-dd format for API
    const dateRanges = dateRows.map(([dateText]) => {
      const [monthName, day] = dateText.split(" ");
      const month = new Date(`${monthName} 1, 2025`).getMonth() + 1; // 1-indexed
      const dd = String(day).padStart(2, "0");
      const mm = String(month).padStart(2, "0");
      return { start: `2025-${mm}-${dd}` };
    });

    const dailyReport = await fetchDailyPostalCodeCostReport(customer, dateRanges);

    // Prepare L, M, N columns only
    const sheetData = dateRanges.map(({ start }) => {
      const entries = dailyReport[start] || [];
      const regionTotals = entries.find(e => e.regionTotals)?.regionTotals || {};
      return [
        regionTotals.AZ || 0, // Column L
        regionTotals.TX || 0, // Column M
        regionTotals.DMV || 0 // Column N
      ];
    });

    // Update only Columns L, M, N (Rows 4-33)
    if (sheetData.length > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: `${sheetName}!L4:N33`,
        valueInputOption: "USER_ENTERED",
        requestBody: { values: sheetData },
      });
    }

    console.log(`Final Hi Skin daily report sent to Google Sheets successfully!`);
  } catch (error) {
    console.error("Error sending Hi Skin daily report to Google Sheets:", error);
  }
};

module.exports = {
  runDailyLocationReport,
  sendFinalDailyReportToGoogleSheetsHS,
};
