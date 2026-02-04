const axios = require("axios");
const { chromium } = require("patchright");
const { google } = require("googleapis");
const { Readable } = require("stream");
require("dotenv").config();

const fs = require("fs")
const path = require("path");

const { sendSlackSummary } = require('./SlackNotifier');
const scraperConfig = require("./ScraperConfig");
const {
  ROOT_OUTPUT_FOLDER_ID,
  CONTROLLER_SHEET_ID,
  OUTPUT_SHEET_NAME,
  SOURCE_SHEET_NAME,
  STATUS_SHEET_NAME,
  STATUS_CELL,
  USER_AGENTS,
  CONCURRENCY,
  STAGGER_DELAY_MS,
  BATCH_MIN_DELAY_MS,
  BATCH_MAX_DELAY_MS,
  RERUN_REGIONS,
  REGION_CONFIGS
} = scraperConfig;

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
    const range = `${STATUS_SHEET_NAME}!${STATUS_CELL}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: CONTROLLER_SHEET_ID,
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
      range: `${sheetName}!A2:Z`,
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

async function getProductsFromSheet(sheetId, sheetName) {
  const sheetNameToUse = sheetName || SOURCE_SHEET_NAME;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: `${sheetNameToUse}!A:Z`,
  });

  const rows = res.data.values;
  if (!rows || rows.length < 2) return [];

  const header = rows[0];
  const linkIndex = header.indexOf("link");
  const statusIndex = header.indexOf("availability") !== -1
    ? header.indexOf("availability")
    : header.indexOf("current_status");
  const idIndex = header.indexOf("id");

  if (linkIndex === -1 || idIndex === -1)
    throw new Error(`Sheet must contain 'id' and 'link' columns.`);

  return rows
    .slice(1)
    .map((r) => ({
      id: r[idIndex] || null,
      link: r[linkIndex] ? normalizeUrl(r[linkIndex]) : null,
      currentStatus: (statusIndex !== -1 ? r[statusIndex] : "unknown")?.toLowerCase(),
    }))
    .filter((p) => p.id && p.link);
    // .slice(0, 10);
}

async function handleCloseIfExists(page) {
  const closeButtonSelectors = [
    'button[aria-label="Close modal"]',
    'button[aria-label="Modal schließen"]',
    'div:has(> h2:has-text("Thanks for Visiting!")) button:has-text("OK")',
    'div:has(> h2:has-text("Vielen Dank für Ihren Besuch!")) button:has-text("OK")',
    'div.block-c15a8bf3-2bc2-478d-b60b-cda5874aa8bb button.button-c15a8bf3-2bc2-478d-b60b-cda5874aa8bb:has-text("I\'m Not Interested")',
    'div.block-29345a9f-1354-4e5b-87e2-599ede629d84 button.button-29345a9f-1354-4e5b-87e2-599ede629d84[aria-label="Dismiss popup"]',
    'button[aria-label="Dismiss popup"][data-block-type="CLOSE_BUTTON"]'
  ];

  for (let round = 0; round < 3; round++) {
    let clicked = false;

    for (const selector of closeButtonSelectors) {
      try {
        await page.waitForSelector(selector, { state: 'visible', timeout: 2000 });
        await page.locator(selector).click({ force: true });
        await page.waitForTimeout(500);
        clicked = true;
      } catch {}
    }

    if (!clicked) break;
    await page.waitForTimeout(1000);
  }
}

async function checkOneTimePurchase(page) {
  try {
    const oneTimePurchaseInput = page.locator('input[id^="onetime"]');
    const oneTimePurchaseLabel = page.locator('label[for^="onetime"]');

    await oneTimePurchaseLabel.waitFor({ state: 'attached', timeout: 7000 });

    const isAlreadyChecked = await oneTimePurchaseInput.getAttribute('aria-checked');

    if (isAlreadyChecked === 'true') {
      return true;
    }

    await oneTimePurchaseLabel.scrollIntoViewIfNeeded();
    await oneTimePurchaseLabel.click({ force: true });

    await page.waitForFunction(
      (selector) => {
        const input = document.querySelector(selector);
        return input && input.getAttribute('aria-checked') === 'true';
      },
      'input[id^="onetime"]',
      { timeout: 7000 }
    );

    return true;
  } catch (error) {
    console.warn(`⚠️ Could not select 'One Time Purchase'. Error: ${error.message.split('\n')[0]}`);
    return false;
  }
}

async function findVisibleButton(page, selectors) {
  for (const selector of selectors) {
    try {
      await page.waitForSelector(selector, { state: 'attached', timeout: 5000 });
      const locator = page.locator(selector);
      if ((await locator.count()) > 0 && await locator.first().isVisible()) {
        return { buttonLocator: locator.first(), usedSelector: selector };
      }
    } catch (e) {
      // Ignore timeout errors
    }
  }
  return null;
}

async function fetchWithRetries(url, options = {}, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const randomUserAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
      const requestOptions = {
        ...options,
        headers: { 'User-Agent': randomUserAgent },
        validateStatus: () => true
      };
      
      const res = await axios.get(url, requestOptions);

      if ([429, 403].includes(res.status)) {
        if (attempt < retries) {
          const delay = 7000 * attempt + getRandomDelay(500, 2000);
          console.log(`  -> Payload received HTTP ${res.status}. Retrying in ${delay / 1000}s (attempt ${attempt + 1})...`);
          await sleep(delay);
          continue;
        }
      }
      return res;
    } catch (e) {
      if (attempt === retries) throw e;
    }
  }
}

async function scrapeProductCrawler(browserContext, url) {
  let page;
  try {
    page = await browserContext.newPage();

    await page.route('**/*', route => {
      const urlStr = route.request().url().toLowerCase();
      if (
        urlStr.includes('klaviyo') ||
        urlStr.includes('privy') ||
        urlStr.includes('justuno') ||
        urlStr.includes('attentive') ||
        urlStr.includes('drift') ||
        urlStr.includes('omnisend') ||
        urlStr.includes('yotpo') ||
        urlStr.includes('marketing') ||
        urlStr.includes('popup') ||
        urlStr.includes('subscribe') ||
        urlStr.includes('newsletter')
      ) return route.abort();
      route.continue();
    });
    await page.goto(url, { timeout: 30000 });
    await page.route('**/*.{png,jpg,jpeg,webp,gif}', r => r.abort());
    await handleCloseIfExists(page);

    let status = "UNKNOWN", evidence;

    try {
      const buttonSelectors = [
        "button.btn-primary.relative",
        "div.w-full > button.grid.w-full"
      ];

      let buttonInfo = null;

      try {
        await page.waitForSelector('input[id^="onetime"]', { state: "attached", timeout: 10000 });
        if (await checkOneTimePurchase(page)) {
          buttonInfo = await findVisibleButton(page, buttonSelectors);
        }
      } catch {}

      if (!buttonInfo) {
        buttonInfo = await findVisibleButton(page, buttonSelectors);
      }

      if (buttonInfo) {
        await buttonInfo.buttonLocator.waitFor({ state: "visible", timeout: 25000 });
        const buttonText = (await buttonInfo.buttonLocator.innerText()).trim().toLowerCase();
        evidence = buttonText;

        if (buttonText.includes("out of stock") || buttonText.includes("sold out") || buttonText.includes("nicht vorrätig") || buttonText.includes("ausverkauft")) {
          status = "out of stock";
        } else if (buttonText.includes("add to cart") || buttonText.includes("add gift card") || buttonText.includes("update selection") || buttonText.includes("in den warenkorb") || buttonText.includes("auswahl aktualisieren") || buttonText.includes("zum warenkorb hinzufügen")) {
          status = "in stock";
        } else {
          status = "UNKNOWN";
          evidence = `Button text: "${buttonText}"`;
        }
      } else {
        const lostPageLocator = page.locator(
          'div.metafield-rich_text_field:has(h1:has-text("we think you might be lost")), div.metafield-rich_text_field:has(h1:has-text("glauben wir, dass sie verloren gehen könnten"))'
        );
        if (await lostPageLocator.count() > 0) {
          status = "404 Not Found";
          evidence = "404/Lost page detected (Crawler)";
        } else {
          status = "UNKNOWN";
          evidence = 'Purchase button not found';
          return { status, evidence, error: null, needsFallback: true };
        }
      }
    } catch (err) {
      status = "UNKNOWN";
      evidence = "Error during scrape";
      return { status, evidence, error: null, needsFallback: true };
    }

    return { status, evidence, error: null, needsFallback: false };
  } catch (outerErr) {
    const errorMsg = outerErr.message;
    const lowerMsg = errorMsg.toLowerCase();
    
    if (lowerMsg.includes('429') || lowerMsg.includes('403')) {
      return { 
        status: "UNKNOWN", 
        error: `HTTP ${lowerMsg.includes('429') ? '429' : '403'}`, 
        evidence: errorMsg.split("\n")[0], 
        needsFallback: true
      };
    }

    const isBlocked = lowerMsg.includes('timeout') || lowerMsg.includes('cloudflare') || lowerMsg.includes('net::');
    return { 
      status: "UNKNOWN", 
      error: errorMsg.split("\n")[0], 
      evidence: errorMsg.split("\n")[0], 
      needsFallback: isBlocked 
    };
  } finally {
    if (page) await page.close();
  }
}

async function scrapeProductPayload(url, regionCode) {
  let finalUrl = url;
  try {
    const htmlRes = await fetchWithRetries(finalUrl);
    if (htmlRes.status >= 400) {
      const evidence = htmlRes.status === 404 ? "404 - Product page not found (Payload)" : `HTTP ${htmlRes.status}`;
      return { status: "404 Not Found", error: "HTML page fetch failed", evidence };
    }

    const canonicalMatch = htmlRes.data.match(/<link\s+rel="canonical"\s+href="([^"]+)"/i);
    if (canonicalMatch && canonicalMatch[1]) finalUrl = canonicalMatch[1];
    
    try {
      const graphqlUrl = finalUrl.replace(/\/$/, "") + "/graphql.json";
      const payloadRes = await fetchWithRetries(graphqlUrl);

      if (payloadRes && payloadRes.status === 200) {
        const payloadData = payloadRes.data?.product;
        if (payloadData) {
          const regionTag = `oos-${regionCode.toLowerCase()}`;
          if ((payloadData.tags || []).map(t => t.toLowerCase()).includes(regionTag)) {
            return { status: "out of stock", error: null, evidence: `Found OOS tag '${regionTag}' in graphql.json` };
          } else if (typeof payloadData.availableForSale === "boolean") {
            const status = payloadData.availableForSale ? "in stock" : "out of stock";
            const evidence = `availableForSale=${payloadData.availableForSale} in graphql.json`;
            return { status, error: null, evidence };
          }
        }
      }
    } catch (e) {
      console.warn(`GraphQL fetch failed for ${url}, falling back to .js payload.`);
    }

    const jsUrl = finalUrl.replace(/\/$/, "") + ".js";
    const payloadRes = await fetchWithRetries(jsUrl);

    if (payloadRes && payloadRes.status === 200) {
      const payloadData = payloadRes.data;
      if (payloadData) {
        const regionTag = `oos-${regionCode.toLowerCase()}`;
        if ((payloadData.tags || []).map(t => t.toLowerCase()).includes(regionTag)) {
          return { status: "out of stock", error: null, evidence: `Found OOS tag '${regionTag}' in .js payload` };
        } else if (typeof payloadData.available === "boolean") {
          const status = payloadData.available ? "in stock" : "out of stock";
          const evidence = `available=${payloadData.available} in .js payload`;
          return { status, error: null, evidence };
        }
      }
    }

    return { status: "UNKNOWN", error: "Payload fetch failed", evidence: "Could not retrieve graphql.json or .js payload" };
  } catch (err) {
    const msg = err.message.split("\n")[0];
    return { status: "UNKNOWN", error: msg, evidence: msg };
  }
}

async function finalizeRegionRun(regionConfig, results) {
  const { fullResults, changeResults, issueResults, evidenceResults } = results;
  const dateStamp = getTodayDateString();

  console.log(`💾 Finalizing and uploading reports for ${regionConfig.regionCode}...`);
  await updateStatusCell(`Status: Finalizing reports for ${regionConfig.regionCode}...`);

  const regionFolderId = await getOrCreateFolder(ROOT_OUTPUT_FOLDER_ID, regionConfig.outputFolderName);
  const dailyFolderId = await getOrCreateFolder(regionFolderId, dateStamp);

  const fullCsvHeaders = ["id", "link", "detected_status", "gmc_availability_hint", "method", "http_status", "redirected_to", "evidence", "url_group", "checked_at_utc", "note"];
  const issuesCsvHeaders = ["id", "link", "issue_type", "detail", "http_status", "url_group", "checked_at_utc"];
  const reviewHeaders = ["id", "link", "current_status", "detected_status", "gmc_availability_hint", "method", "evidence", "url_group", "checked_at_utc", "approval", "notes"];

  const fullCsvContent = arrayToCsv(fullCsvHeaders, fullResults.map((r) => [r.id, r.link, r.detected_status, r.gmc_availability_hint, r.method, "", "", r.evidence, r.link, r.checked_at_utc, ""]));
  const issuesCsvContent = arrayToCsv(issuesCsvHeaders, issueResults.map((r) => [r.id, r.link, r.issue_type, r.detail, "", r.link, r.checked_at_utc]));
  const reviewCsvContent = arrayToCsv(reviewHeaders, fullResults.map((r) => [r.id, r.link, r.current_status || "", r.detected_status, r.gmc_availability_hint, r.method, r.evidence, r.link, r.checked_at_utc, "", ""]));
  const evidenceJsonlContent = evidenceResults.map((e) => JSON.stringify(e)).join("\n");

  await uploadFileToDrive(dailyFolderId, "stock_check_full.csv", "text/csv", fullCsvContent);
  await uploadFileToDrive(dailyFolderId, "issues_log.csv", "text/csv", issuesCsvContent);
  await uploadFileToDrive(dailyFolderId, "review_data.csv", "text/csv", reviewCsvContent);
  if (evidenceResults.length > 0) {
    await uploadFileToDrive(dailyFolderId, "evidence_log.jsonl", "application/json", evidenceJsonlContent);
  }

  const reviewSheetName = `${regionConfig.regionCode} - ${OUTPUT_SHEET_NAME}`;
  const reviewData = changeResults.map((r) => [r.id, r.link, r.current_status, r.detected_status, r.gmc_availability_hint, r.method, r.evidence, r.link, r.checked_at_utc, "", ""]);

  await updateReviewSheet(CONTROLLER_SHEET_ID, reviewSheetName, reviewHeaders, reviewData);

  await sendSlackSummary({
    region: regionConfig.regionCode,
    date: getTodayDateString(),
    totalProducts: fullResults.length,
    changesCount: changeResults.length,
    unknownCount: fullResults.filter(r => r.detected_status.includes("UNKNOWN")).length,
    reviewSheetUrl: `https://docs.google.com/spreadsheets/d/${CONTROLLER_SHEET_ID}/edit#gid=0`,
    driveFolderUrl: `https://drive.google.com/drive/folders/${dailyFolderId}`,
  });

  console.log(`✅ Reports for ${regionConfig.regionCode} are complete.`);
}

async function processSingleRegion(regionConfig, browserContext) {
  console.log(`➡️  Processing Region: ${regionConfig.regionCode}`);
  const productsToCheck = await getProductsFromSheet(regionConfig.sourceSheetId, regionConfig.sheetName);

  if (productsToCheck.length === 0) {
    console.log(`No products found for ${regionConfig.regionCode}. Skipping.`);
    return;
  }
  const totalProducts = productsToCheck.length;

  await updateStatusCell(
    `Status (${regionConfig.regionCode}): Starting Phase 1 (Crawlers) for ${totalProducts} products...`
  );

  // =================================================================
  // PHASE 1: Run all crawler tasks in parallel batches
  // =================================================================
  const crawlerResults = [];
  const batches = [];

  for (let i = 0; i < totalProducts; i += CONCURRENCY) {
    batches.push(productsToCheck.slice(i, i + CONCURRENCY));
  }

  for (const [index, batch] of batches.entries()) {
    console.log(`-- Starting Crawler Batch ${index + 1} of ${batches.length} --`);

    const batchPromises = batch.map(async (product, i) => {
      const staggerDelay = getRandomDelay(
        STAGGER_DELAY_MS * i,
        STAGGER_DELAY_MS * i + 500
      );
      await sleep(staggerDelay);

      const result = await scrapeProductCrawler(
        browserContext,
        product.link
      );

      return { ...product, ...result, method: "crawler" };
    });

    const batchResults = await Promise.all(batchPromises);
    crawlerResults.push(...batchResults.filter(r => r));

    await updateStatusCell(
      `Status (${regionConfig.regionCode}): Crawler phase ${Math.round(
        (crawlerResults.length / totalProducts) * 100
      )}% complete...`
    );

    if (index < batches.length - 1) {
      await sleep(getRandomDelay(BATCH_MIN_DELAY_MS, BATCH_MAX_DELAY_MS));
    }
  }

  const successfulCrawlerResults = [];
  const productsNeedingFallback = [];
  crawlerResults.forEach(result => {
    if (result.needsFallback) {
      productsNeedingFallback.push(result);
    } else {
      successfulCrawlerResults.push(result);
    }
  });

  // =================================================================
  // PHASE 2: Run payload tasks serially (one by one) for failures
  // =================================================================
  const payloadResults = [];
  if (productsNeedingFallback.length > 0) {
    console.log(
      `-- Starting Payload Fallback Phase for ${productsNeedingFallback.length} products --`
    );

    await updateStatusCell(
      `Status (${regionConfig.regionCode}): Phase 2 (Payload Fallback) for ${productsNeedingFallback.length} items...`
    );

    let payloadCount = 0;
    for (const failedProduct of productsNeedingFallback) {
      payloadCount++;
      console.log(
        `  -> Running payload check ${payloadCount} of ${productsNeedingFallback.length} for ${failedProduct.link}`
      );

      const payloadResult = await scrapeProductPayload(
        failedProduct.link,
        regionConfig.regionCode
      );

      payloadResults.push({
        ...failedProduct,
        ...payloadResult,
        method: "payload",
      });

      if (payloadCount < productsNeedingFallback.length) {
        await sleep(getRandomDelay(1000, 2000));
      }
    }
  }

  // =================================================================
  // PHASE 3: Combine all results and finalize the report
  // =================================================================
  const allResults = [...successfulCrawlerResults, ...payloadResults];

  const fullResults = [],
    changeResults = [],
    issueResults = [],
    evidenceResults = [];

  for (const result of allResults) {
    const resultRow = {
      id: result.id,
      link: result.link,
      current_status: result.currentStatus,
      detected_status: result.status,
      gmc_availability_hint:
        result.status === "in stock" ? "in_stock" : "out_of_stock",
      method: result.method,
      evidence: result.evidence,
      checked_at_utc: new Date().toISOString(),
    };

    fullResults.push(resultRow);

    const normalizeStatus = (status) => {
    if (!status) return "";
    const s = status.toLowerCase();
    if (s === "404" || s === "404 not found") return "404";
    return s;
  };

    const detectedStatus = normalizeStatus(result.status);
    const currentStatus = normalizeStatus(result.currentStatus);

    if (detectedStatus !== currentStatus) {
      changeResults.push(resultRow);
    }

    if (result.error) {
      issueResults.push({
        id: result.id,
        link: result.link,
        issue_type: "scrape_error",
        detail: result.error,
        checked_at_utc: new Date().toISOString(),
      });
    }

    if (result.status !== "in stock") {
      evidenceResults.push({
        id: result.id,
        link: result.link,
        signals: { [result.method]: result.evidence },
        decision: result.status,
        checked_at_utc: new Date().toISOString(),
      });
    }
  }

  await finalizeRegionRun(regionConfig, {
    fullResults,
    changeResults,
    issueResults,
    evidenceResults,
  });
}

async function rerunFailedScrapes() {
  let browserContext;
  console.log("🚀 Initializing Rerun of Failed Scrapes...");

  try {
    await updateStatusCell("Status: Initializing and launching browser for rerun...");
    browserContext = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    await updateStatusCell("Status: Starting verification for rerun regions...");

    for (const regionConfig of RERUN_REGIONS) {
      await processSingleRegion(regionConfig, browserContext);
    }

    const finalTimestamp = new Date().toLocaleString("en-US", { timeZone: "Asia/Manila" });
    await updateStatusCell(`Status: Rerun Complete. All regions processed at ${finalTimestamp}.`);
    console.log("\n🎉 All rerun regions processed successfully.");
  } catch (error) {
    console.error("❌ A critical error occurred during the rerun:", error);
    await updateStatusCell(`Status: RERUN ERROR! Process failed. Check logs.`);
  } finally {
    if (browserContext) {
      await browserContext.close();
      console.log("✅ Browser closed. Rerun process finished.");
    }
  }
}

async function runStockVerification() {
  let browserContext;
  console.log("🚀 Initializing Full Stock Verification Process...");

  try {
    await updateStatusCell("Status: Initializing and launching browser...");
    browserContext = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    await updateStatusCell("Status: Starting verification for all regions...");

    for (const regionConfig of REGION_CONFIGS) {
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

  const regionConfig = REGION_CONFIGS.find((rc) => rc.regionCode === regionCode);
  if (!regionConfig) {
    const errorMsg = `❌ Error: Region code "${regionCode}" not found in configuration.`;
    console.error(errorMsg);
    await updateStatusCell(`Status: ERROR! Invalid region code "${regionCode}".`);
    return;
  }

  try {
    await updateStatusCell(`Status: Initializing for ${regionCode}...`);
    browserContext = await chromium.launch({
      headless: true,
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

async function runTestStock() {
  const testUrl = "https://intl.drsquatch.com/products/harry-potter-4-pack";
  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    });

    const videoDir = path.join(__dirname, "test_video");
    if (!fs.existsSync(videoDir)) fs.mkdirSync(videoDir);

    const context = await browser.newContext({
      recordVideo: { dir: videoDir, size: { width: 1280, height: 720 } },
    });

    const page = await context.newPage();
    await page.route('**/*', route => {
      const url = route.request().url();
      if (
        url.includes('klaviyo') ||
        url.includes('privy') ||
        url.includes('justuno') ||
        url.includes('attentive') ||
        url.includes('drift') ||
        url.includes('omnisend') ||
        url.includes('yotpo') ||
        url.includes('marketing') ||
        url.includes('popup') ||
        url.includes('subscribe') ||
        url.includes('newsletter')
      ) {
        return route.abort();
      }

      route.continue();
    });
    await page.goto(testUrl, { timeout: 30000 });
    await handleCloseIfExists(page);

    let detectedStatus, evidence, error;

    try {
      const buttonSelectors = [
        "button.btn-primary.relative",
        "div.w-full > button.grid.w-full"
      ];

      let buttonInfo = null;

      try {
        await page.waitForSelector('input[id^="onetime"]', { state: "attached", timeout: 10000 });
        if (await checkOneTimePurchase(page)) {
          buttonInfo = await findVisibleButton(page, buttonSelectors);
        }
      } catch (radioError) {
        console.log("⚠️ Radio button fallback failed or was not applicable.");
      }

      if (!buttonInfo) {
        buttonInfo = await findVisibleButton(page, buttonSelectors);
      }

      if (buttonInfo) {
        console.log(`✅ Using selector: ${buttonInfo.usedSelector}`);
        await buttonInfo.buttonLocator.waitFor({ state: "visible", timeout: 25000 });
        const buttonText = (await buttonInfo.buttonLocator.innerText()).trim().toLowerCase();
        evidence = buttonText;

        if (buttonText.includes("out of stock") || buttonText.includes("sold out") || buttonText.includes("nicht vorrätig") || buttonText.includes("ausverkauft")) {
          detectedStatus = "out of stock";
        } else if (buttonText.includes("add to cart") || buttonText.includes("update selection") || buttonText.includes("in den warenkorb") || buttonText.includes("auswahl aktualisieren") || buttonText.includes("zum warenkorb hinzufügen")) {
          detectedStatus = "in stock";
        } else {
          detectedStatus = `UNKNOWN (Button text: "${buttonText}")`;
        }
      } else {
        console.log("⚠️ No purchase element found, scanning for lost/404 page...");
        const lostPageLocator = page.locator(
          'div.metafield-rich_text_field:has(h1:has-text("we think you might be lost")), div.metafield-rich_text_field:has(h1:has-text("glauben wir, dass sie verloren gehen könnten"))'
        );
        if (await lostPageLocator.count() > 0) {
          detectedStatus = "404 Not Found";
          evidence = "404/lost page detected (Crawler)";
        } else {
          detectedStatus = "UNKNOWN (Purchase button not found)";
        }
      }
    } catch (err) {
      detectedStatus = "UNKNOWN (Error during scrape)";
      error = err.message.split("\n")[0];
      evidence = error;
    }

    const screenshotPath = path.join(__dirname, "test_screenshot.png");
    await page.screenshot({ path: screenshotPath, fullPage: true });

    const rawHtml = await page.evaluate(() => document.documentElement.outerHTML);
    const htmlPath = path.join(__dirname, "test_page.html");
    fs.writeFileSync(htmlPath, rawHtml, "utf8");

    const videoPath = await page.video().path();
    await page.close();

    console.log("\n===== 🧪 TEST STATUS REPORT =====");
    console.log(`🔗 URL: ${testUrl}`);
    console.log(`📸 Screenshot: ${screenshotPath}`);
    console.log(`📼 Video: ${videoPath}`);
    console.log(`📄 HTML: ${htmlPath}`);
    console.log(`✅ Detected Status: ${detectedStatus}`);
    if (evidence) console.log(`🔍 Evidence: ${evidence}`);
    if (error) console.log(`⚠️ Error: ${error}`);
    console.log("=================================\n");

    return { screenshotPath, videoPath, htmlPath, detectedStatus, evidence, error };
  } catch (error) {
    console.error("runTestStock error:", error.message);
    return null;
  } finally {
    if (browser) await browser.close();
  }
}

async function runPayloadStockCheck() {
  const baseUrls = [
    "",
  ];

  const results = [];

  for (const baseUrl of baseUrls) {
    let finalUrl = baseUrl;
    let detectedStatus = "UNKNOWN";
    let evidence = "";
    let filePath = null;

    try {
      const htmlRes = await fetchWithRetries(baseUrl);
      if (htmlRes.status >= 400) {
        evidence = htmlRes.status === 404
          ? "404 - Product page not found"
          : `HTTP ${htmlRes.status} - Failed to reach product page`;
        results.push({ url: baseUrl, filePath, detectedStatus, evidence });
        continue;
      }

      const canonicalMatch = htmlRes.data.match(/<link\s+rel="canonical"\s+href="([^"]+)"/i);
      if (canonicalMatch && canonicalMatch[1]) finalUrl = canonicalMatch[1];

      const handleMatch = finalUrl.match(/products\/([a-zA-Z0-9\-]+)/);
      if (!handleMatch) {
        evidence = "Cannot extract product handle from canonical URL";
        results.push({ url: baseUrl, filePath, detectedStatus, evidence });
        continue;
      }
      const handle = handleMatch[1];

      const graphqlUrl = finalUrl.replace(/\/$/, "") + "/graphql.json";
      let payloadRes;
      try {
        payloadRes = await fetchWithRetries(graphqlUrl);
      } catch (err) {
        const jsUrl = finalUrl.replace(/\/$/, "") + ".js";
        payloadRes = await fetchWithRetries(jsUrl);
      }

      if (payloadRes && payloadRes.status === 200) {
        let payloadData = payloadRes.data;

        if (typeof payloadData === "string") {
          const match = payloadData.match(/var\s+\w+\s*=\s*(\{.*\});?/s);
          if (match) payloadData = JSON.parse(match[1]);
          else {
            try { payloadData = JSON.parse(payloadData); } catch(e) { payloadData = null; }
          }
        }

        if (payloadData) {
          filePath = path.join(__dirname, handle + ".json");
          fs.writeFileSync(filePath, JSON.stringify(payloadData, null, 2), "utf8");

          const regionTag = "oos-us";
          if ((payloadData.tags || []).map(t => t.toLowerCase()).includes(regionTag)) {
            detectedStatus = "out of stock";
            evidence = `Found OOS tag '${regionTag}' in payload`;
          } else {
            detectedStatus = payloadData.available ? "in stock" : "out of stock";
            evidence = payloadData.available ? "available=true in payload" : "available=false in payload";
          }

          results.push({ url: baseUrl, filePath, detectedStatus, evidence });
          continue;
        }
      }

      evidence = "Both graphql.json and .js payload fetch failed or product not found";
      results.push({ url: baseUrl, filePath, detectedStatus, evidence });

    } catch (err) {
      evidence = err.message.split("\n")[0];
      results.push({ url: baseUrl, filePath, detectedStatus, evidence });
    }
  }

  console.log("\n===== 🧪 PAYLOAD STOCK REPORT =====");
  results.forEach(r => {
    console.log(`🔗 URL: ${r.url}`);
    if (r.filePath) console.log(`📄 Saved Content: ${r.filePath}`);
    console.log(`✅ Detected Status: ${r.detectedStatus}`);
    if (r.evidence) console.log(`🔍 Evidence: ${r.evidence}`);
    console.log("------------------------------");
  });
  console.log("=================================\n");

  return results;
}

module.exports = {
  runPayloadStockCheck,
  runTestStock,
  rerunFailedScrapes,
  runStockVerification,
  runSingleRegionVerification,
};