require("dotenv").config();
const { chromium } = require("patchright");
const { google } = require("googleapis");
const ScraperConfig = require("./ScraperConfig");
const { Readable } = require("stream");
const { sendSlackSummary } = require('./SlackNotifier');

const MIN_DELAY_MS = 1500;
const MAX_DELAY_MS = 4000;

const auth = new google.auth.GoogleAuth({
  keyFile: "serviceToken.json",
  scopes: [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
  ],
});
const sheets = google.sheets({ version: "v4", auth });
const drive = google.drive({ version: "v3", auth });

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const getRandomDelay = (min, max) =>
  Math.floor(Math.random() * (max - min + 1) + min);
const getTodayDateString = () => new Date().toISOString().split("T")[0];

function normalizeUrl(url) {
  try {
    const urlObj = new URL(url);
    const paramsToDelete = [
      "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
      "gclid", "msclkid", "fbclid", "ttclid", "yclid", "igshid", "_ga", "_gl",
    ];
    paramsToDelete.forEach((p) => urlObj.searchParams.delete(p));
    urlObj.hash = "";
    return urlObj.href;
  } catch {
    return url;
  }
}

async function updateStatusCell(message) {
  try {
    const range = `${ScraperConfig.STATUS_SHEET_NAME}!${ScraperConfig.STATUS_CELL}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: ScraperConfig.CONTROLLER_SHEET_ID,
      range,
      valueInputOption: "USER_ENTERED",
      resource: { values: [[message]] },
    });
  } catch (err) {
    console.warn(`Could not update status cell: ${err.message}`);
  }
}

async function getOrCreateFolder(parentFolderId, folderName) {
  const q = `'${parentFolderId}' in parents and name = '${folderName}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`;
  const res = await drive.files.list({
    q,
    fields: "files(id, name)",
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  if (res.data.files.length > 0) {
    return res.data.files[0].id;
  } else {
    const fileMetadata = {
      name: folderName,
      mimeType: "application/vnd.google-apps.folder",
      parents: [parentFolderId],
    };
    const folder = await drive.files.create({
      resource: fileMetadata,
      fields: "id",
      supportsAllDrives: true,
    });

    await sleep(1500);

    await drive.files.get({
      fileId: folder.data.id,
      fields: "id, name",
      supportsAllDrives: true,
    });

    return folder.data.id;
  }
}

function arrayToCsv(headers, data) {
  const headerRow = headers.join(",");
  const dataRows = data.map((row) =>
    row.map((val) => `"${String(val).replace(/"/g, '""')}"`).join(",")
  );
  return [headerRow, ...dataRows].join("\n");
}

async function uploadFileToDrive(folderId, fileName, mimeType, content) {
  const media = { mimeType, body: Readable.from(content) };

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const file = await drive.files.create({
        resource: { name: fileName, parents: [folderId] },
        media,
        fields: "id, name",
        supportsAllDrives: true,
      });
      return file.data.id;
    } catch (err) {
      if (attempt === 3) throw err;
      await sleep(1000);
    }
  }
}

async function updateReviewSheet(spreadsheetId, sheetName, headers, data) {
  const { data: { sheets: sheetList } } = await sheets.spreadsheets.get({ spreadsheetId });
  const existingSheet = sheetList.find((s) => s.properties.title === sheetName);

  if (!existingSheet) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{ addSheet: { properties: { title: sheetName } } }],
      },
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${sheetName}!A1`,
      valueInputOption: "RAW",
      resource: { values: [headers] },
    });
  } else {
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: `${sheetName}!A2:K`,
    });
  }

  if (data.length > 0) {
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${sheetName}!A2`,
      valueInputOption: "USER_ENTERED",
      resource: { values: data },
    });
  }
}

async function getProductsFromSheet(sheetId) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: `${ScraperConfig.SOURCE_SHEET_NAME}!A:Z`,
  });
  const rows = res.data.values;
  if (!rows || rows.length < 2) return [];

  const header = rows[0];
  const linkIndex = header.indexOf("link");
  const statusIndex = header.indexOf("availability");
  const idIndex = header.indexOf("id");

  if (linkIndex === -1 || idIndex === -1)
    throw new Error(`Sheet must contain 'id' and 'link' columns.`);

  return rows
    .slice(1)
    .map((r) => ({
      id: r[idIndex] || null,
      link: r[linkIndex] ? normalizeUrl(r[linkIndex]) : null,
      currentStatus: (statusIndex !== -1
        ? r[statusIndex]
        : "unknown"
      )?.toLowerCase(),
    }))
    .filter((p) => p.id && p.link);
}

async function scrapeProductStatus(browserContext, url) {
  let page;
  try {
    page = await browserContext.newPage();
    
    // Block unnecessary resources to speed up page loads
    await page.route('**/*.{png,jpg,jpeg,webp,gif,css,woff,woff2}', (route) => route.abort());

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

    const buttonLocator = page.locator("button.btn-primary.relative");
    await buttonLocator.waitFor({ state: "visible", timeout: 25000 });
    const buttonText = (await buttonLocator.innerText()).trim().toLowerCase();

    let status;
    if (buttonText.includes("out of stock") || buttonText.includes("sold out")) {
      status = "Out of Stock";
    } else if (buttonText.includes("add to cart")) {
      status = "In Stock";
    } else {
      status = `UNKNOWN (Button text: "${await buttonLocator.innerText()}")`;
    }
    return { status, error: null, evidence: buttonText };
  } catch (error) {
    const errorMessage = error.message.split("\n")[0];
    return {
      status: "UNKNOWN (Error during scrape)",
      error: errorMessage,
      evidence: errorMessage,
    };
  } finally {
    if (page) await page.close();
  }
}

async function finalizeRegionRun(regionConfig, results) {
  const { fullResults, changeResults, issueResults, evidenceResults } = results;
  const dateStamp = getTodayDateString();

  console.log(`💾 Finalizing and uploading reports for ${regionConfig.regionCode}...`);
  await updateStatusCell(`Status: Finalizing reports for ${regionConfig.regionCode}...`);

  const regionFolderId = await getOrCreateFolder(ScraperConfig.ROOT_OUTPUT_FOLDER_ID, regionConfig.outputFolderName);
  const dailyFolderId = await getOrCreateFolder(regionFolderId, dateStamp);

  const fullCsvHeaders = ["id", "link", "detected_status", "gmc_availability_hint", "method", "http_status", "redirected_to", "evidence", "url_group", "checked_at_utc", "note"];
  const issuesCsvHeaders = ["id", "link", "issue_type", "detail", "http_status", "url_group", "checked_at_utc"];

  const fullCsvContent = arrayToCsv(fullCsvHeaders, fullResults.map((r) => [r.id, r.link, r.detected_status, r.gmc_availability_hint, r.method, "", "", r.evidence, r.link, r.checked_at_utc, ""]));
  const issuesCsvContent = arrayToCsv(issuesCsvHeaders, issueResults.map((r) => [r.id, r.link, r.issue_type, r.detail, "", r.link, r.checked_at_utc]));
  const evidenceJsonlContent = evidenceResults.map((e) => JSON.stringify(e)).join("\n");

  await uploadFileToDrive(dailyFolderId, "stock_check_full.csv", "text/csv", fullCsvContent);
  await uploadFileToDrive(dailyFolderId, "issues_log.csv", "text/csv", issuesCsvContent);
  if (evidenceResults.length > 0) {
    await uploadFileToDrive(dailyFolderId, "evidence_log.jsonl", "application/json", evidenceJsonlContent);
  }

  const reviewSheetName = `${regionConfig.regionCode} - ${ScraperConfig.OUTPUT_SHEET_NAME}`;
  const reviewHeaders = ["id", "link", "current_status", "detected_status", "gmc_availability_hint", "method", "evidence", "url_group", "checked_at_utc", "approval", "notes"];
  const reviewData = changeResults.map((r) => [r.id, r.link, r.current_status, r.detected_status, r.gmc_availability_hint, r.method, r.evidence, r.link, r.checked_at_utc, "---", ""]);

  await updateReviewSheet(ScraperConfig.CONTROLLER_SHEET_ID, reviewSheetName, reviewHeaders, reviewData);

  await sendSlackSummary({
    region: regionConfig.regionCode,
    date: getTodayDateString(),
    totalProducts: fullResults.length,
    changesCount: changeResults.length,
    unknownCount: fullResults.filter(r => r.detected_status.includes("UNKNOWN")).length,
    reviewSheetUrl: `https://docs.google.com/spreadsheets/d/${ScraperConfig.CONTROLLER_SHEET_ID}/edit#gid=0`,
    driveFolderUrl: `https://drive.google.com/drive/folders/${dailyFolderId}`,
  });

  console.log(`✅ Reports for ${regionConfig.regionCode} are complete.`);
}

async function processSingleRegion(regionConfig, browserContext) {
  console.log(`➡️  Processing Region: ${regionConfig.regionCode}`);
  const productsToCheck = await getProductsFromSheet(regionConfig.sourceSheetId);

  if (productsToCheck.length === 0) {
    console.log(`No products found for ${regionConfig.regionCode}. Skipping.`);
    return;
  }

  await updateStatusCell(`Status (${regionConfig.regionCode}): Scraping ${productsToCheck.length} products...`);
  const fullResults = [], changeResults = [], issueResults = [], evidenceResults = [];
  let count = 0;

  for (const product of productsToCheck) {
    count++;
    const { status: liveStatus, error, evidence } = await scrapeProductStatus(browserContext, product.link);
    const checked_at_utc = new Date().toISOString();

    const resultRow = {
      id: product.id,
      link: product.link,
      current_status: product.currentStatus,
      detected_status: liveStatus,
      gmc_availability_hint: liveStatus === "In Stock" ? "in_stock" : "out_of_stock",
      method: "html",
      evidence,
      checked_at_utc,
    };

    fullResults.push(resultRow);
    if (liveStatus.toLowerCase() !== product.currentStatus) {
      changeResults.push(resultRow);
    }
    if (error) {
      issueResults.push({ id: product.id, link: product.link, issue_type: "scrape_error", detail: error, checked_at_utc });
    }
    if (liveStatus !== "In Stock") {
      evidenceResults.push({ id: product.id, link: product.link, signals: { html_button_text: evidence }, decision: liveStatus, checked_at_utc });
    }

    if (count < productsToCheck.length) {
      await sleep(getRandomDelay(MIN_DELAY_MS, MAX_DELAY_MS));
    }
  }

  await finalizeRegionRun(regionConfig, { fullResults, changeResults, issueResults, evidenceResults });
}

async function runStockVerification() {
  let browserContext;
  console.log("🚀 Initializing Full Stock Verification Process...");

  try {
    await updateStatusCell("Status: Initializing and launching browser...");
    browserContext = await chromium.launchPersistentContext("./user-data", {
      // channel: "chrome",
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    await updateStatusCell("Status: Starting verification for all regions...");

    for (const regionConfig of ScraperConfig.REGION_CONFIGS) {
      await processSingleRegion(regionConfig, browserContext);
    }

    const finalTimestamp = new Date().toLocaleString("en-US", { timeZone: "Asia/Manila" });
    await updateStatusCell(`Status: Complete. All regions processed at ${finalTimestamp}.`);
    console.log("\n🎉 All regions processed successfully.");
  } catch (error) {
    console.error("❌ A critical error occurred:", error);
    await updateStatusCell(`Status: ERROR! Process failed. Check logs for details.`);
  } finally {
    if (browserContext) {
      await browserContext.close();
      console.log("✅ Browser closed. Process finished.");
    }
  }
}

async function runSingleRegionVerification(regionCode) {
  let browserContext;
  console.log(`🚀 Initializing Single Region Verification for: ${regionCode}...`);

  const regionConfig = ScraperConfig.REGION_CONFIGS.find((rc) => rc.regionCode === regionCode);
  if (!regionConfig) {
    const errorMsg = `❌ Error: Region code "${regionCode}" not found in configuration.`;
    console.error(errorMsg);
    await updateStatusCell(`Status: ERROR! Invalid region code "${regionCode}".`);
    return;
  }

  try {
    await updateStatusCell(`Status: Initializing for ${regionCode}...`);
    browserContext = await chromium.launchPersistentContext("./user-data", {
      channel: "chrome",
      headless: true, // Render-ready settings
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    await processSingleRegion(regionConfig, browserContext);

    const finalTimestamp = new Date().toLocaleString("en-US", { timeZone: "Asia/Manila" });
    await updateStatusCell(`Status: Complete. ${regionCode} processed at ${finalTimestamp}.`);
    console.log(`\n🎉 ${regionCode} processed successfully.`);
  } catch (error) {
    console.error(`❌ A critical error occurred during ${regionCode} run:`, error);
    await updateStatusCell(`Status: ERROR! Process for ${regionCode} failed. Check logs.`);
  } finally {
    if (browserContext) {
      await browserContext.close();
      console.log(`✅ Browser for ${regionCode} run closed. Process finished.`);
    }
  }
}

module.exports = {
  runStockVerification,
  runSingleRegionVerification,
};