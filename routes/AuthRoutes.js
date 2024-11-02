const express = require('express');
const router = express.Router();

const {
  redirectToGoogle,
  handleOAuthCallback,
} = require('../controllers/Auth.js');

router.get('/google', redirectToGoogle);
router.get('/callback', handleOAuthCallback);

module.exports = router;
