const express = require('express');
const router = express.Router();

const {
  redirectToGoogle,
  handleOAuthCallbackGoogle
} = require('../controllers/GoogleAuth.js');

const {
  redirectToBing,
  handleOAuthCallbackBing
} = require('../controllers/BingAuth.js');

const {
  redirectToLinkedin,
  handleOAuthCallbackLinkedin
} = require('../controllers/LinkedinAuth.js');

router.get('/google', redirectToGoogle);
router.get('/google/callback', handleOAuthCallbackGoogle);

router.get('/bing', redirectToBing);
router.get('/bing/callback', handleOAuthCallbackBing);

router.get('/linkedin', redirectToLinkedin);
router.get('/linkedin/callback', handleOAuthCallbackLinkedin);

module.exports = router;
