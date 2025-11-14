const axios = require("axios");
const { chromium } = require("patchright");
const { google } = require("googleapis");
require("dotenv").config();

const fs = require("fs")
const path = require("path");

const scraperConfig = require("./ScraperConfig");
const {
  USER_AGENTS,
  CONCURRENCY,
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const getRandomDelay = (min, max) =>
  Math.floor(Math.random() * (max - min + 1) + min);

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

function toColumnName(colIndex) {
  let colName = '';
  let dividend = colIndex + 1;
  while (dividend > 0) {
    let modulo = (dividend - 1) % 26;
    colName = String.fromCharCode(65 + modulo) + colName;
    dividend = Math.floor((dividend - 1) / 26);
  }
  return colName;
}

async function getProductsToUpdate(sheetId, sheetName) {
  const targetSheetName = "Google Shopping Feed Test";
  const targetHeader = "custom_label_0";

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: `${targetSheetName}!A:Z`,
  });

  const rows = res.data.values;
  if (!rows || rows.length < 2) return [];

  const header = rows[0];
  const linkIndex = header.indexOf("link");
  const idIndex = header.indexOf("id");
  const labelIndex = header.indexOf(targetHeader);

  if (linkIndex === -1) throw new Error("Sheet must contain a 'link' column.");
  if (idIndex === -1) throw new Error("Sheet must contain an 'id' column.");
  if (labelIndex === -1) throw new Error(`Sheet must contain a '${targetHeader}' column.`);

  const labelColumnLetter = toColumnName(labelIndex);

  const productsToUpdate = [];
  //for (let i = 1; i < rows.length && i < 11; i++) {
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const labelValue = row[labelIndex] || null;

    if (!labelValue) {
      const link = row[linkIndex] ? normalizeUrl(row[linkIndex]) : null;
      const id = row[idIndex] || null;

      if (id && link) {
        productsToUpdate.push({
          id: id,
          link: link,
          rowNumber: i + 1,
          labelColumnLetter: labelColumnLetter,
          labelColumnIndex: labelIndex
        });
      }
    }
  }
  
  return productsToUpdate;
}

async function batchUpdateSheetValues(spreadsheetId, updates, sheetName) {
  const batchSize = 200; // A single batchUpdate can handle many updates. 200 is a safe number.

  for (let i = 0; i < updates.length; i += batchSize) {
    const chunk = updates.slice(i, i + batchSize);

    // Create the data payload for the batchUpdate API
    const dataToUpdate = chunk.map(result => ({
      range: `'${sheetName}'!${result.labelColumnLetter}${result.rowNumber}`,
      values: [[result.evidence]], // result.evidence is the title
    }));

    try {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: spreadsheetId,
        resource: {
          valueInputOption: "USER_ENTERED",
          data: dataToUpdate,
        },
      });
    } catch (err) {
      console.error(`Failed to batch update values: ${err.message}`);
    }

    // Add a small delay between batches to be safe with rate limits
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}

async function formatSheetCell(spreadsheetId, sheetId, cells) {
  const yellowColor = { red: 1, green: 1, blue: 0 };
  const batchSize = 20;

  for (let i = 0; i < cells.length; i += batchSize) {
    const chunk = cells.slice(i, i + batchSize);

    const requests = chunk.map(({ row, col }) => ({
      repeatCell: {
        range: {
          sheetId,
          startRowIndex: row,
          endRowIndex: row + 1,
          startColumnIndex: col,
          endColumnIndex: col + 1,
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: yellowColor,
          },
        },
        fields: "userEnteredFormat.backgroundColor",
      },
    }));

    try {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        resource: { requests },
      });
    } catch (err) {
      console.error(`Failed to format batch starting at cell ${chunk[0].row}: ${err.message}`);
    }

    await new Promise(resolve => setTimeout(resolve, 200));
  }
}

async function extractMetaTitle(page) {
  try {
    await page.waitForSelector('script[type="application/ld+json"]', { state: "attached", timeout: 10000 });

    const scripts = await page.$$eval('script[type="application/ld+json"]', els =>
      els.map(e => e.textContent)
    );

    const productScript = scripts.find(text => {
      try {
        const data = JSON.parse(text);
        return data["@type"] === "Product";
      } catch {
        return false;
      }
    });

    if (!productScript) return null;

    const productData = JSON.parse(productScript);
    return productData.name || null;

  } catch (error) {
    console.error("Error extracting Product name from ld+json:", error.message);
    return null;
  }
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

    let status = "UNKNOWN", evidence, error = null, needsFallback = false;

    try {
      const title = await extractMetaTitle(page);

      if (title) {
        status = "TITLE_FOUND";
        evidence = title;
        needsFallback = false;
      } else {
        status = "TITLE_NOT_FOUND";
        evidence = "og:title meta tag not found";
        needsFallback = true;
      }

    } catch (err) {
      status = "UNKNOWN";
      evidence = "Error during title extraction";
      error = err.message.split("\n")[0];
      needsFallback = true;
    }

    return { status, evidence, error, needsFallback };
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

async function scrapeProductPayload(url) {
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
        const payloadData = payloadRes.data;
        
        if (payloadData && payloadData.product && payloadData.product.title) {
          const title = payloadData.product.title;
          return { status: "TITLE_FOUND", error: null, evidence: title };
        } else if (payloadData) {
          return { status: "TITLE_NOT_FOUND", error: null, evidence: "Payload found but product.title missing (graphql.json)" };
        }
      }
    } catch (e) {
      console.warn(`GraphQL fetch failed for ${url}, falling back to .js payload.`);
    }

    const jsUrl = finalUrl.replace(/\/$/, "") + ".js";
    const payloadRes = await fetchWithRetries(jsUrl);

    if (payloadRes && payloadRes.status === 200) {
      let payloadData = payloadRes.data;

      if (typeof payloadData === "string") {
        const match = payloadData.match(/var\s+\w+\s*=\s*(\{.*\});?/s);
        if (match) {
          try { payloadData = JSON.parse(match[1]); } catch(e) { payloadData = null; }
        } else {
          try { payloadData = JSON.parse(payloadData); } catch(e) { payloadData = null; }
        }
      }

      if (payloadData && payloadData.product && payloadData.product.title) {
        const title = payloadData.product.title;
        return { status: "TITLE_FOUND", error: null, evidence: title };
      } else if (payloadData) {
        return { status: "TITLE_NOT_FOUND", error: null, evidence: "Payload found but product.title missing (.js)" };
      }
    }

    return { status: "UNKNOWN", error: "Payload fetch failed", evidence: "Could not retrieve graphql.json or .js payload" };
  
  } catch (err) {
    const msg = err.message.split("\n")[0];
    return { status: "UNKNOWN", error: msg, evidence: msg };
  }
}

async function processRegionTitles(regionConfig, browserContext) {
  console.log(`➡️  Processing Region: ${regionConfig.regionCode}`);
  
  const productsToUpdate = await getProductsToUpdate(regionConfig.sourceSheetId, regionConfig.sheetName);

  if (productsToUpdate.length === 0) {
    console.log(`No products with blank "custom_label_0" found for ${regionConfig.regionCode}. Skipping.`);
    return;
  }
  const totalProducts = productsToUpdate.length;
  console.log(`Found ${totalProducts} products to update for ${regionConfig.regionCode}.`);

  // =================================================================
  // PHASE 1: Run all crawler tasks in parallel batches
  // =================================================================
  const crawlerResults = [];
  const batches = [];
  for (let i = 0; i < totalProducts; i += CONCURRENCY) {
    batches.push(productsToUpdate.slice(i, i + CONCURRENCY));
  }

  for (const [index, batch] of batches.entries()) {
    console.log(`-- Starting Crawler Batch ${index + 1} of ${batches.length} --`);
    
    const batchPromises = batch.map(async (product) => {
      const result = await scrapeProductCrawler(browserContext, product.link);
      return { ...product, ...result, method: "crawler" };
    });

    const batchResults = await Promise.all(batchPromises);
    crawlerResults.push(...batchResults.filter(r => r));
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
  // PHASE 2: Run payload tasks serially for failures
  // =================================================================
  const payloadResults = [];
  if (productsNeedingFallback.length > 0) {
    console.log(`-- Starting Payload Fallback Phase for ${productsNeedingFallback.length} products --`);
    
    for (const failedProduct of productsNeedingFallback) {
      const payloadResult = await scrapeProductPayload(failedProduct.link);
      payloadResults.push({
        ...failedProduct,
        ...payloadResult,
        method: "payload",
      });
    }
  }

  // =================================================================
  // PHASE 3: Combine results and update the Google Sheet
  // =================================================================
  const allResults = [...successfulCrawlerResults, ...payloadResults];
  
  const successfulUpdates = allResults.filter(r => r.status === "TITLE_FOUND" && r.evidence);

  if (successfulUpdates.length === 0) {
    console.log(`No titles were successfully scraped for ${regionConfig.regionCode}.`);
    return;
  }

  console.log(`Updating ${successfulUpdates.length} cells in Google Sheets...`);
  
  const sheetMeta = await sheets.spreadsheets.get({ spreadsheetId: regionConfig.sourceSheetId });
  const sheet = sheetMeta.data.sheets.find(s => s.properties.title === "Google Shopping Feed Test");
  if (!sheet) {
    console.error("Could not find GID for 'Google Shopping Feed Test'. Aborting updates.");
    return;
  }
  const sheetGid = sheet.properties.sheetId;

  console.log(`Batch updating ${successfulUpdates.length} cell values...`);
  await batchUpdateSheetValues(regionConfig.sourceSheetId, successfulUpdates, "Google Shopping Feed Test");

  // 2. Then, format all the cells
  console.log(`Batch formatting ${successfulUpdates.length} cells...`);
  const cellsToFormat = successfulUpdates.map(r => ({
    row: r.rowNumber - 1,
    col: r.labelColumnIndex,
  }));
  await formatSheetCell(regionConfig.sourceSheetId, sheetGid, cellsToFormat);

  console.log(`✅ Finished updating ${successfulUpdates.length} product titles for ${regionConfig.regionCode}.`);
}

async function runSingleRegionLabel(regionCode) {
  let browserContext;
  console.log(`🚀 Initializing Single Region Title Fill for: ${regionCode}...`);

  const regionConfig = REGION_CONFIGS.find((rc) => rc.regionCode === regionCode);
  if (!regionConfig) {
    console.error(`❌ Error: Region code "${regionCode}" not found in configuration.`);
    return;
  }

  try {
    browserContext = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    await processRegionTitles(regionConfig, browserContext);

    console.log(`\n🎉 ${regionCode} title fill process finished successfully.`);
  } catch (error) {
    console.error(`❌ A critical error occurred during ${regionCode} run:`, error);
  } finally {
    if (browserContext) {
      await browserContext.close();
      console.log(`✅ Browser for ${regionCode} run closed.`);
    }
  }
}

async function runTestLabel() {
  const testUrl = "https://intl.drsquatch.com/products/adamantium-scrub-3-pack";
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

    let productTitle, error;

    try {
      const title = await extractMetaTitle(page);

      if (title) {
        productTitle = title;
        console.log(`✅ Successfully extracted title: ${productTitle}`);
      } else {
        productTitle = "UNKNOWN (og:title meta tag not found)";
      }

    } catch (err) {
      productTitle = "UNKNOWN (Error during title extraction)";
      error = err.message.split("\n")[0];
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
    console.log(`✅ Product Title: ${productTitle}`);
    if (error) console.log(`⚠️ Error: ${error}`);
    console.log("=================================\n");

    return { screenshotPath, videoPath, htmlPath, productTitle, error }; 

  } catch (error) {
    console.error("runTestLabel error:", error.message);
    return null;
  } finally {
    if (browser) await browser.close();
  }
}

async function runPayloadLabelCheck() {
  const baseUrls = [
     "https://www.drsquatch.com/products/hair-to-toe-coconut-castaway-1",
  ];

  const results = [];

  for (const baseUrl of baseUrls) {
    let finalUrl = baseUrl;
    let productTitle = "UNKNOWN";
    let evidence = "";
    let filePath = null;

    try {
      const htmlRes = await fetchWithRetries(baseUrl);
      if (htmlRes.status >= 400) {
        evidence = htmlRes.status === 404
          ? "404 - Product page not found"
          : `HTTP ${htmlRes.status} - Failed to reach product page`;
        results.push({ url: baseUrl, filePath, productTitle, evidence });
        continue;
      }

      const canonicalMatch = htmlRes.data.match(/<link\s+rel="canonical"\s+href="([^"]+)"/i);
      if (canonicalMatch && canonicalMatch[1]) finalUrl = canonicalMatch[1];

      const handleMatch = finalUrl.match(/products\/([a-zA-Z0-9\-]+)/);
      if (!handleMatch) {
        evidence = "Cannot extract product handle from canonical URL";
        results.push({ url: baseUrl, filePath, productTitle, evidence });
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

          if (payloadData.product && payloadData.product.title) {
            productTitle = payloadData.product.title;
            evidence = "Extracted title from payload (product.title)";
          } else {
            productTitle = "UNKNOWN (Title not found in payload)";
            evidence = "Payload found but product.title field was missing.";
          }

          results.push({ url: baseUrl, filePath, productTitle, evidence });
          continue;
        }
      }

      evidence = "Both graphql.json and .js payload fetch failed or product not found";
      results.push({ url: baseUrl, filePath, productTitle, evidence });

    } catch (err) {
      evidence = err.message.split("\n")[0];
      results.push({ url: baseUrl, filePath, productTitle, evidence });
    }
  }

  console.log("\n===== 🧪 PAYLOAD TITLE REPORT =====");
  results.forEach(r => {
    console.log(`🔗 URL: ${r.url}`);
    if (r.filePath) console.log(`📄 Saved Content: ${r.filePath}`);
    console.log(`✅ Product Title: ${r.productTitle}`);
    if (r.evidence) console.log(`🔍 Evidence: ${r.evidence}`);
    console.log("------------------------------");
  });
  console.log("=================================\n");

  return results;
}

module.exports = {
  runPayloadLabelCheck,
  runTestLabel,
  runSingleRegionLabel,
};