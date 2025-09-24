const express = require('express');
const router = express.Router();

const { 
  getCampaigns,
  runDailyExportAndReport,
} = require("../controllers/lpc/DailyFetch");

const {
  testLawmatics,
  getRawCampaigns,
  sendFinalWeeklyReportToGoogleSheetsLPC,
} = require("../controllers/lpc/GoogleAdsWeekly");

const {
  generateLPCBing,
  fetchAndSaveAdCosts,
  executeSpecificFetchFunctionLPC,
  sendLPCDetailedBudgettoGoogleSheets,
  sendLPCBudgettoGoogleSheets,
  sendLPCMonthlyReport
} = require("../controllers/lpc/GoogleAdsMonthly");

const {
  sendFinalWeeklyReportToGoogleSheetsLPCAdG,
} = require('../controllers/lpc/GoogleAdsGroupWeekly');

const {
  downloadAndExtractHSBing,
  aggregateWeeklyDataFromCSV,
  executeSpecificFetchFunctionHS,
  sendFinalWeeklyReportToGoogleSheetsHS,
  sendBlendedCACToGoogleSheetsHS
} = require('../controllers/hi_skin/GoogleAdsWeekly');

const {
  sendFinalMonthlyReportToGoogleSheetsHS
} = require('../controllers/hi_skin/GoogleAdsMonthly');

const {
  executeSpecificFetchFunctionHSAdG,
  sendFinalWeeklyReportToGoogleSheetsHSAdG,
} = require('../controllers/hi_skin/GoogleAdsGroupWeekly');

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
  sendFinalWeeklyReportToGoogleSheetsMIVDAdG,
} = require('../controllers/mobile_iv/GoogleAdsGroupWeekly');

const {
  executeSpecificFetchFunctionGC,
  sendFinalWeeklyReportToGoogleSheetsGC,
} = require('../controllers/guardian_carers/GoogleAdsWeekly');

const {
  sendFinalWeeklyReportToGoogleSheetsGCAdG,
} = require('../controllers/guardian_carers/GoogleAdsGroupWeekly');

const {
  executeSpecificFetchFunctionMNR,
  sendFinalWeeklyReportToGoogleSheetsMNR,
} = require('../controllers/menerals/GoogleAdsWeekly');

const {
  executeSpecificFetchFunctionST,
  sendFinalWeeklyReportToGoogleSheetsST,
} = require('../controllers/sleepy_tie/GoogleAdsWeekly');

const {
  executeSpecificFetchFunctionNB,
  sendFinalWeeklyReportToGoogleSheetsNB,
} = require('../controllers/national_buyers/GoogleAdsWeekly');

router.get('/lpc/report-daily', async (req, res) => {
  try {
    await runDailyExportAndReport(req, res);
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

router.get('/lpc/report-month', async (req, res) => {
  try {
    await sendLPCMonthlyReport(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/lpc_adg/report-final', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsLPCAdG(req, res);
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

router.get('/hi_skin_adg/report-final', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsHSAdG(req, res);
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

router.get('/mobile_iv_adg/report-final', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsMIVDAdG(req, res);
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

router.get('/guardian_carers_adg/report-final', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsGCAdG(req, res);
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

router.get('/sleepy_tie/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsST(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/national_buyers/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheetsNB(req, res);
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

// router.get('/singleTest', executeSpecificFetchFunctionHSAdG);
// router.get('/mobile_iv/report/:date?', executeSpecificFetchFunctionMIV);
// router.get('/guardian_carers/report/:date?', executeSpecificFetchFunctionGC);

module.exports = router;
