const { google } = require("googleapis");
const { parse } = require("csv-parse/sync");
const { Readable } = require("stream");
require("dotenv").config();

const { sendValidationSummary } = require('./SlackNotifier');
const scraperConfig = require("./ScraperConfig");
const {
  ROOT_OUTPUT_FOLDER_ID,
  CONTROLLER_SHEET_ID,
  OUTPUT_SHEET_NAME,
} = scraperConfig;

const auth = new google.auth.GoogleAuth({
  keyFile: "serviceToken.json",
  scopes: [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
  ],
});

const drive = google.drive({ version: "v3", auth });
const sheets = google.sheets({ version: "v4", auth });

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function updateStatusCell(message) {
  try {
    const range = `${scraperConfig.STATUS_SHEET_NAME}!${scraperConfig.STATUS_CELL}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: scraperConfig.CONTROLLER_SHEET_ID,
      range,
      valueInputOption: "USER_ENTERED",
      resource: { values: [[message]] },
    });
  } catch (err) {
    console.warn(`Could not update status cell: ${err.message}`);
  }
}

function arrayToCsv(headers, data) {
  const headerRow = headers.join(",");
  const dataRows = data.map((row) => row.map((val) => `"${String(val).replace(/"/g, '""')}"`).join(","));
  return [headerRow, ...dataRows].join("\n");
}

async function uploadFileToDrive(folderId, fileName, mimeType, content) {
  const media = { mimeType, body: Readable.from(content) };
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await drive.files.create({ resource: { name: fileName, parents: [folderId] }, media, fields: "id, name", supportsAllDrives: true });
      return;
    } catch (err) {
      console.warn(`Upload attempt ${attempt} for ${fileName} failed. Retrying...`);
      if (attempt === 3) throw err;
      await sleep(1000);
    }
  }
}

async function updateReviewSheet(spreadsheetId, sheetName, headers, data) {
  const { data: { sheets: sheetList } } = await sheets.spreadsheets.get({ spreadsheetId });
  const existingSheet = sheetList.find((s) => s.properties.title === sheetName);
  if (!existingSheet) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] } });
    await sheets.spreadsheets.values.update({ spreadsheetId, range: `${sheetName}!A1`, valueInputOption: "RAW", resource: { values: [headers] } });
  } else {
    await sheets.spreadsheets.values.clear({ spreadsheetId, range: `${sheetName}!A2:Z` });
  }
  if (data.length > 0) {
    await sheets.spreadsheets.values.append({ spreadsheetId, range: `${sheetName}!A2`, valueInputOption: "USER_ENTERED", resource: { values: data } });
  }
}

function formatDateToYyyyMmDd(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getLastSaturdayString(today = new Date()) {
  const day = today.getDay();
  const diff = day >= 5 ? day - 5 : day + 2; // last Friday
  const lastFriday = new Date(today);
  lastFriday.setDate(today.getDate() - diff);
  return formatDateToYyyyMmDd(lastFriday);
}

function getCurrentTuesdayString(today = new Date()) {
  const day = today.getDay();
  const diff = day - 1; // current Monday
  const monday = new Date(today);
  monday.setDate(today.getDate() - diff);
  return formatDateToYyyyMmDd(monday);
}

async function downloadCsv(fileId) {
  const res = await drive.files.get({ fileId, alt: "media" }, { responseType: "arraybuffer" });
  const content = Buffer.from(res.data).toString("utf8");
  return parse(content, { columns: true, skip_empty_lines: true });
}

async function validateWeekendData() {
  console.log("🚀 Initializing Weekend Data Validation...");
  try {
    await updateStatusCell("Status: Starting weekend data validation...");

    const startDateString = getLastSaturdayString();
    const endDateString = getCurrentTuesdayString();

    const regionFoldersRes = await drive.files.list({
      q: `'${ROOT_OUTPUT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
      fields: "files(id, name)",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    if (regionFoldersRes.data.files.length === 0) {
      console.log("\nNo region folders found to process.");
    }

    for (const regionFolder of regionFoldersRes.data.files) {
      console.log(`\n➡️  Processing Region: ${regionFolder.name}`);
      
      let saturdayFileId = null, tuesdayFileId = null, tuesdayFolderId = null;

      const dateFoldersRes = await drive.files.list({ 
        q: `'${regionFolder.id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`, 
        fields: "files(id, name)",
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      for (const dateFolder of dateFoldersRes.data.files) {
        if (dateFolder.name === startDateString) {
          const filesRes = await drive.files.list({ q: `'${dateFolder.id}' in parents and name='review_data.csv' and trashed=false`, fields: "files(id)", supportsAllDrives: true, includeItemsFromAllDrives: true });
          if (filesRes.data.files.length > 0) saturdayFileId = filesRes.data.files[0].id;
        }
        if (dateFolder.name === endDateString) {
          tuesdayFolderId = dateFolder.id;
          const filesRes = await drive.files.list({ q: `'${dateFolder.id}' in parents and name='review_data.csv' and trashed=false`, fields: "files(id)", supportsAllDrives: true, includeItemsFromAllDrives: true });
          if (filesRes.data.files.length > 0) tuesdayFileId = filesRes.data.files[0].id;
        }
      }

      if (!saturdayFileId || !tuesdayFileId) {
        console.warn(`Skipping ${regionFolder.name}: Missing Saturday or Tuesday data file.`);
        continue;
      }
      
      const saturdayData = await downloadCsv(saturdayFileId);
      const tuesdayData = await downloadCsv(tuesdayFileId);
      const saturdayMap = new Map(saturdayData.map((row) => [`${row.id}_${row.link}`, row]));
      
      const sameItems = [], changedItems = [], newItems = [];

      for (const tueRow of tuesdayData) {
        const key = `${tueRow.id}_${tueRow.link}`;
        const satRow = saturdayMap.get(key);
        if (satRow) {
          if (satRow.current_status !== tueRow.current_status || satRow.detected_status !== tueRow.detected_status) {
            changedItems.push({ ...tueRow, change_status: "CHANGED" });
          } else {
            sameItems.push({ ...tueRow, change_status: "SAME" });
          }
          saturdayMap.delete(key);
        } else {
          newItems.push({ ...tueRow, change_status: "NEW" });
        }
      }

      const removedItems = Array.from(saturdayMap.values()).map(row => ({...row, change_status: "REMOVED"}));
      const totalItemsCompared = sameItems.length + changedItems.length + newItems.length + removedItems.length;
      const consolidatedData = [...sameItems, ...changedItems, ...newItems, ...removedItems];
      
      const finalReportData = consolidatedData.filter(row => row.current_status !== row.detected_status);
      console.log(`💾 Finalizing and uploading reports for ${regionFolder.name}...`);

      const mainReportHeaders = ["id", "link", "current_status", "detected_status", "gmc_availability_hint", "method", "evidence", "url_group", "checked_at_utc", "approval", "notes", "change_status"];
      const mainReportName = `${regionFolder.name} - ${OUTPUT_SHEET_NAME}`;

      const mainReportData = finalReportData.map((row) => mainReportHeaders.map((header) => row[header] || ""));
      await updateReviewSheet(CONTROLLER_SHEET_ID, mainReportName, mainReportHeaders, mainReportData);
      
      if (finalReportData.length > 0 && tuesdayFolderId) {
        const dataForCsv = finalReportData.map((row) => mainReportHeaders.map((header) => row[header] || ""));
        const csvContent = arrayToCsv(mainReportHeaders, dataForCsv);
        await uploadFileToDrive(tuesdayFolderId, "weekend_validation_report.csv", "text/csv", csvContent);
      }
      
      const unknownCount = finalReportData.filter(r => r.detected_status?.toUpperCase().includes("UNKNOWN")).length;
      
      await sendValidationSummary({
        region: regionFolder.name,
        date: new Date().toISOString().split('T')[0],
        totalCompared: totalItemsCompared,
        discrepancyCount: finalReportData.length,
        changedCount: changedItems.length,
        newCount: newItems.length,
        removedCount: removedItems.length,
        unknownCount: unknownCount,
        reviewSheetUrl: `https://docs.google.com/spreadsheets/d/${CONTROLLER_SHEET_ID}/`,
        driveFolderUrl: `https://drive.google.com/drive/folders/${tuesdayFolderId}`,
      });

      console.log(`✅ Reports for ${regionFolder.name} are complete.`);
    }

    console.log("\n🎉 All validation regions processed successfully.");
    await updateStatusCell("Status: Weekend data validation complete.");

  } catch (err) {
    console.error("❌ Validation error:", err.message);
    await updateStatusCell(`Status: ERROR! Validation failed. Check logs.`);
  } finally {
    console.log("✅ Validation process finished.");
  }
}

module.exports = {
  validateWeekendData,
};
