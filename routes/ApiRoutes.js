const express = require('express');
const router = express.Router();
const {
  fetchReportData,
  redirectToGoogle,
  handleOAuthCallback
} = require('../controllers/GoogleAdsController');

router.get('/report', fetchReportData);
router.get('/google', redirectToGoogle);
router.get('/callback', handleOAuthCallback);

module.exports = router;
