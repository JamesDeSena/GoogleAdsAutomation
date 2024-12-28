const express = require('express');
const router = express.Router();

// const {
//   fetchReportDataDaily
// } = require('../controllers/hi_skin/GoogleAdsDaily');

const {
  fetchReportDataDaily
} = require('../controllers/wall_blush/GoogleAdsDaily');

const {
  fetchReportDataBatch
} = require('../controllers/wall_blush/GoogleAdsBatch');

const {
  // fetchReportDataWeekly,
  // fetchReportDataWeeklyBrand,
  // fetchReportDataWeeklyNB,
  sendFinalWeeklyReportToAirtable,
  sendFinalWeeklyReportToGoogleSheets
} = require('../controllers/hi_skin/GoogleAdsWeekly');

const {
  sendFinalMonthlyReportToAirtable,
  sendFinalMonthlyReportToGoogleSheets
} = require('../controllers/hi_skin/GoogleAdsMonthly');

router.get('/report', fetchReportDataDaily);

// router.get('/report-week', fetchReportDataWeekly);
// router.get('/report-brand', fetchReportDataWeeklyBrand);
// router.get('/report-nb', fetchReportDataWeeklyNB);
// router.get('/report-final', sendFinalReportToAirtable);

router.get('/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToGoogleSheets(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/report-month/:date?', async (req, res) => {
  try {
    await sendFinalMonthlyReportToGoogleSheets(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

module.exports = router;
