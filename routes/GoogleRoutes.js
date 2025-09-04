const express = require('express');
const router = express.Router();

const { 
  getCampaigns,
  runDailyExportAndReport,
} = require("../controllers/lpc/DailyFetch");

const {
  generateLPCBing,
  fetchAndSaveAdCosts,
  executeSpecificFetchFunctionLPC,
  sendLPCDetailedBudgettoGoogleSheets,
  sendLPCBudgettoGoogleSheets,
  sendLPCMonthlyReport
} = require("../controllers/lpc/GoogleAdsMonthly");

const {
  testLawmatics,
  getRawCampaigns,
  sendFinalWeeklyReportToGoogleSheetsLPC,
} = require("../controllers/lpc/GoogleAdsWeekly");

const {
  downloadAndExtractHSBing,
  aggregateWeeklyDataFromCSV,
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
  // executeSpecificFetchFunctionMIV,
  sendFinalDailyReportToGoogleSheetsMIV,
} = require('../controllers/mobile_iv/GoogleAdsDaily');

const {
  executeSpecificFetchFunctionMIV,
  sendFinalWeeklyReportToGoogleSheetsMIV,
  // sendBookings,
} = require('../controllers/mobile_iv/GoogleAdsWeekly');

const {
  // executeSpecificFetchFunctionMIV,
  sendFinalMonthlyReportToGoogleSheetsMIV,
  sendBookings,
} = require('../controllers/mobile_iv/GoogleAdsMonthly');

const {
  executeSpecificFetchFunctionGC,
  sendFinalWeeklyReportToGoogleSheetsGC,
} = require('../controllers/guardian_carers/GoogleAdsWeekly');

const {
  executeSpecificFetchFunctionMNR,
  sendFinalWeeklyReportToGoogleSheetsMNR,
} = require('../controllers/menerals/GoogleAdsWeekly');

router.get('/lpc/report-daily', async (req, res) => {
  try {
    await runDailyExportAndReport(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/lpc/report-month', async (req, res) => {
  try {
    await sendLPCMonthlyReport(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/lpc/report-final', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsLPC(req, res);
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

router.get('/mobile_iv/report-daily/:date?', async (req, res) => {
  try {
    await sendFinalDailyReportToGoogleSheetsMIV(req, res);
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

router.get('/mobile_iv/report-month/:date?', async (req, res) => {
  try {
    await sendFinalMonthlyReportToGoogleSheetsMIV(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/guardian_carers/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsGC(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/menerals/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsMNR(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

// router.get("/test", async (req, res) => {
//   try {
//     await generateLPCBing();
//     res.status(200).send("Process completed successfully.");
//   } catch (error) {
//     console.error("Error in /test route:", error);
//     res.status(500).send("Error fetching all metrics");
//   }
// });

// router.get('/singleTest', executeSpecificFetchFunctionHS);
// router.get('/mobile_iv/report/:date?', executeSpecificFetchFunctionMIV);
// router.get('/guardian_carers/report/:date?', executeSpecificFetchFunctionGC);

module.exports = router;
