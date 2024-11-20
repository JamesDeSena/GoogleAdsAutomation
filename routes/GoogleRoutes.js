const express = require('express');
const router = express.Router();

// const {
//   fetchReportDataDaily
// } = require('../controllers/GoogleAdsDaily');

const {
  // fetchReportDataWeekly,
  // fetchReportDataWeeklyBrand,
  // fetchReportDataWeeklyNB,
  sendFinalReportToAirtable
} = require('../controllers/GoogleAdsWeekly');

// router.get('/report', fetchReportDataDaily);

// router.get('/report-week', fetchReportDataWeekly);
// router.get('/report-brand', fetchReportDataWeeklyBrand);
// router.get('/report-nb', fetchReportDataWeeklyNB);
// router.get('/report-final', sendFinalReportToAirtable);
router.get('/report-final/:date?', sendFinalReportToAirtable);

module.exports = router;
