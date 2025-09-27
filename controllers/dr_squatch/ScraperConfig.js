require('dotenv').config();

const CONTROLLER_SHEET_ID = '1T-iQ0gU5E7Pu796LyTgS_88Smp-R2WS3Q4MlEeegydE';
const ROOT_OUTPUT_FOLDER_ID = '1Tk8dS3p_wYYvWDN9Bz-OX3WDtWQHhRso';
const RETENTION_DAYS = 30;

const CONCURRENCY = 2; // Number of parallel workers
const RETRY_ATTEMPTS = 3;
const BATCH_MIN_DELAY_MS = 1000; // 0.5 seconds
const BATCH_MAX_DELAY_MS = 2000; // 1.5 seconds
const STAGGER_DELAY_MS = 1000; // Base delay in ms to stagger requests within a batch
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/117.0",
];

const PRODUCT_LIST = [];
const SOURCE_SHEET_NAME = 'Google Shopping Feed';
const OUTPUT_SHEET_NAME = 'Stock Changes Review';
const STATUS_SHEET_NAME = 'Overview';
const STATUS_CELL = 'A1';

const REGION_CONFIGS = [
  { regionCode: 'US', sourceSheetId: '1Wbo6zQS-4yhnkyeNzM55e8hDOUOvSYnw2uYTl1pTJ9w', outputFolderName: 'US' },
  { regionCode: 'CA', sourceSheetId: '1NBADrmEjFtQ-G5lz7EX_9eI36ssP2l9gWtiCjh74jYE', outputFolderName: 'CA' },
  { regionCode: 'AU', sourceSheetId: '1M4QKxAXPyWo7lqvUx1Exf2-j80F1Xfu5wD4SihxCLhE', outputFolderName: 'AU' },
  { regionCode: 'EU', sourceSheetId: '1G8oyoVetNmAclWHlScmAZzBlpIS3mUUHlNXkef99XVY', outputFolderName: 'EU' },
  { regionCode: 'DE', sourceSheetId: '1gAOm4W_b8ZVY1HjTUK9GoHOiDXJOM0-DoPpL6d4XJWg', outputFolderName: 'DE' },
  { regionCode: 'UK', sourceSheetId: '1WnI1NzG7V3JzHo-EXt4ffGlwDMC_apgRPCjMXkgrDNY', outputFolderName: 'UK' },
];

const RERUN_REGIONS = [
  { regionCode: 'US', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'US - Stock Changes Review', outputFolderName: 'US' },
  { regionCode: 'CA', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'CA - Stock Changes Review', outputFolderName: 'CA' },
  { regionCode: 'AU', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'AU - Stock Changes Review', outputFolderName: 'AU' },
  { regionCode: 'EU', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'EU - Stock Changes Review', outputFolderName: 'EU' },
  { regionCode: 'DE', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'DE - Stock Changes Review', outputFolderName: 'DE' },
  { regionCode: 'UK', sourceSheetId: CONTROLLER_SHEET_ID, sheetName: 'UK - Stock Changes Review', outputFolderName: 'UK' },
];

const usCaCollectionSlugs = [
  'starter-bundles',
  'best-sellers',
  'last-chance-sale',
  'bundles',
  'all-soap-body-wash',
  'deodorant',
  'hair-care',
  'all-skincare-cologne',
  'shave',
  'limited-editions',
  'new',
  'accessories',
  'toothpaste'
];

const ukDeEuCollectionSlugs = [
  'starter-bundles-2022',
  'best-sellers',
  'last-chance-sale',
  'bundles',
  'all-soap-body-wash',
  'deodorant',
  'hair-care',
  'all-skincare-cologne',
  'shave',
  'limited-editions',
  'new',
  'accessories',
  'toothpaste'
];

const regionData = {
  'US': { baseUrl: 'https://www.drsquatch.com', collections: usCaCollectionSlugs },
  'CA': { baseUrl: 'https://ca.drsquatch.com', collections: usCaCollectionSlugs },
  'AU': { baseUrl: 'https://au.drsquatch.com', collections: usCaCollectionSlugs },
  'EU': { baseUrl: 'https://intl.drsquatch.com/en-eu', collections: ukDeEuCollectionSlugs },
  'DE': { baseUrl: 'https://intl.drsquatch.com/de-de', collections: ukDeEuCollectionSlugs },
  'UK': { baseUrl: 'https://intl.drsquatch.com/en-gb', collections: ukDeEuCollectionSlugs },
};

for (const regionCode in regionData) {
  const region = regionData[regionCode];
  for (const slug of region.collections) {
    PRODUCT_LIST.push({
      regionCode: regionCode,
      sourceLink: `${region.baseUrl}/collections/${slug}`,
    });
  }
}

module.exports = {
  CONTROLLER_SHEET_ID,
  ROOT_OUTPUT_FOLDER_ID,
  RETENTION_DAYS,
  REGION_CONFIGS,
  RERUN_REGIONS,
  SOURCE_SHEET_NAME,
  OUTPUT_SHEET_NAME,
  STATUS_SHEET_NAME,
  STATUS_CELL, 
  CONCURRENCY,
  RETRY_ATTEMPTS,
  BATCH_MIN_DELAY_MS,
  BATCH_MAX_DELAY_MS,
  STAGGER_DELAY_MS,
  USER_AGENTS
};