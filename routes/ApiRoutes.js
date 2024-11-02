const express = require('express');
const router = express.Router();

const {
  fetchReportDataDaily,
  testFetchDaily
} = require('../controllers/GoogleAdsDaily');

const {
  fetchReportDataWeekly,
  fetchReportDataWeeklyBrand,
  fetchReportDataWeeklyNB,
  sendFinalReportToAirtable,
  testFetchWeekly
} = require('../controllers/GoogleAdsWeekly');

router.get('/report', fetchReportDataDaily);

router.get('/report-week', fetchReportDataWeekly);
router.get('/report-brand', fetchReportDataWeeklyBrand);
router.get('/report-nb', fetchReportDataWeeklyNB);
router.get('/report-final', sendFinalReportToAirtable);

router.get('/test', testFetchWeekly);

module.exports = router;
