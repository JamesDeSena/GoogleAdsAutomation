require('dotenv').config();

const CONTROLLER_SHEET_ID = '1T-iQ0gU5E7Pu796LyTgS_88Smp-R2WS3Q4MlEeegydE';
const ROOT_OUTPUT_FOLDER_ID = '1Tk8dS3p_wYYvWDN9Bz-OX3WDtWQHhRso';
const RETENTION_DAYS = 30;

const REGION_CONFIGS = [
  { regionCode: 'US', sourceSheetId: '19WYR4z1TKSfX8ljYXywU5ue_hfkgVaCbcgbe1uwMp6U', outputFolderName: 'US' },
  { regionCode: 'DE', sourceSheetId: '1bacAA6S2scDqe7mi8jd0OFzQAMH3BZrp6x_OW4iE5PI', outputFolderName: 'DE' },
  { regionCode: 'EU', sourceSheetId: '1RIc_I0rpYL6_Wgg-jS2lZ2LSYemVUe7VN3wLlxBCRDA', outputFolderName: 'EU' },
  { regionCode: 'UK', sourceSheetId: '1j1K7v-BzQLonGUIQlodkqn61JZD7OBKI4d_kFHSbgpw', outputFolderName: 'UK' }
];

const SOURCE_SHEET_NAME = 'Google Shopping Feed';
const OUTPUT_SHEET_NAME = 'Stock Changes Review';
const STATUS_SHEET_NAME = 'Overview';
const STATUS_CELL = 'A1';

module.exports = {
  CONTROLLER_SHEET_ID,
  ROOT_OUTPUT_FOLDER_ID,
  RETENTION_DAYS,
  REGION_CONFIGS,
  SOURCE_SHEET_NAME,
  OUTPUT_SHEET_NAME,
  STATUS_SHEET_NAME,
  STATUS_CELL
};