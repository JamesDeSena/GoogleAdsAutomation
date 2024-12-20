const express = require('express');
const router = express.Router();

// const {
//   fetchReportDataDaily
// } = require('../controllers/GoogleAdsDaily');

const {
  // fetchReportDataWeekly,
  // fetchReportDataWeeklyBrand,
  // fetchReportDataWeeklyNB,
  sendFinalWeeklyReportToAirtable
} = require('../controllers/hi_skin/GoogleAdsWeekly');

const {
  sendFinalMonthlyReportToAirtable
} = require('../controllers/hi_skin/GoogleAdsMonthly');

// router.get('/report', fetchReportDataDaily);

// router.get('/report-week', fetchReportDataWeekly);
// router.get('/report-brand', fetchReportDataWeeklyBrand);
// router.get('/report-nb', fetchReportDataWeeklyNB);
// router.get('/report-final', sendFinalReportToAirtable);

router.get('/report-final/:date?', async (req, res) => {
  try {
    await sendFinalWeeklyReportToAirtable(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get('/report-month/:date?', async (req, res) => {
  try {
    await sendFinalMonthlyReportToAirtable(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

module.exports = router;
