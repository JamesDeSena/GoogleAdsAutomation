const express = require('express');
const router = express.Router();
const {
  fetchReportData,
  redirectToGoogle,
  handleOAuthCallback,
  fetchTest,
  testFilterByFormula
} = require('../controllers/GoogleAdsController');

router.get('/report', fetchReportData);
router.get('/test', fetchTest);
router.get('/google', redirectToGoogle);
router.get('/callback', handleOAuthCallback);

module.exports = router;
