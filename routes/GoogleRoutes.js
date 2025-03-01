const express = require('express');
const router = express.Router();

// const {
//   fetchReportDataDailyHS
// } = require('../controllers/hi_skin/GoogleAdsDaily');

const { 
  getCampaigns,
  dailyExport,
  dailyReport,
  dailyToWeekly,
  runDailyExportAndReport,
  runFullReportProcess
} = require("../controllers/lpc/DailyFetch");

const {
  downloadAndExtractHSBing,
  fetchReportDataWeeklyCampaignHS,
  fetchReportDataWeeklySearchHS,
  executeSpecificFetchFunctionHS,
  sendFinalWeeklyReportToGoogleSheetsHS,
  sendBlendedCACToGoogleSheetsHS
} = require('../controllers/hi_skin/GoogleAdsWeekly');

const {
  sendFinalMonthlyReportToGoogleSheetsHS
} = require('../controllers/hi_skin/GoogleAdsMonthly');

const {
  executeSpecificFetchFunctionMIV,
  sendFinalWeeklyReportToGoogleSheetsMIV,
  sendJaneToGoogleSheetsMIV
} = require('../controllers/mobile_iv/GoogleAdsWeekly');

// const {
//   fetchReportDataDaily
// } = require('../controllers/wall_blush/GoogleAdsDaily');

// const {
//   fetchReportDataBatch
// } = require('../controllers/wall_blush/GoogleAdsBatch');

// router.get('/report', fetchReportDataDaily);

// router.get('/report-week', fetchReportDataWeekly);
// router.get('/report-brand', fetchReportDataWeeklyBrand);
// router.get('/report-nb', fetchReportDataWeeklyNB);
// router.get('/report-final', sendFinalReportToAirtable);

router.get('/lpc/report', async (req, res) => {
  try {
    await runFullReportProcess(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/hi_skin/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsHS(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/hi_skin/report-month/:date?', async (req, res) => {
  try {
    await sendFinalMonthlyReportToGoogleSheetsHS(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/mobile_iv/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsMIV(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get("/test", async (req, res) => {
  try {
    await downloadAndExtractHSBing();
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error in /test route:", error);
    res.status(500).send("Error fetching all metrics");
  }
});

// router.get('/mobile_iv/report/:date?', executeSpecificFetchFunctionMIV);

module.exports = router;
