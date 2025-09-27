const express = require('express');
const router = express.Router();

const {
  runPayloadStockCheck,
  runStockVerification,
  rerunFailedScrapes,
  runSingleRegionVerification,
  runTestStock,
} = require('../controllers/dr_squatch/StockScraper');

router.get('/stock', (req, res) => {
  try {
    runStockVerification(); 
    res.status(202).send("✅ Accepted: Verification process for ALL regions has been started. Monitor the 'Overview' sheet for progress.");
  } catch (error) {
    console.error("Error starting verification for all regions:", error);
    res.status(500).send("❌ Error starting verification for all regions.");
  }
});

router.get('/rerun', (req, res) => {
  try {
    rerunFailedScrapes(); 
    res.status(202).send("✅ Accepted: Verification process for ALL regions has been started. Monitor the 'Overview' sheet for progress.");
  } catch (error) {
    console.error("Error starting verification for all regions:", error);
    res.status(500).send("❌ Error starting verification for all regions.");
  }
});

router.get('/test', (req, res) => {
  try {
    runTestStock(); 
    res.status(202).send("Test Link Started");
  } catch (error) {
    console.error("Error starting verification for all regions:", error);
    res.status(500).send("❌ Error starting verification for all regions.");
  }
});

router.get('/stock/:regionCode', (req, res) => {
  const { regionCode } = req.params;

  try {
    runSingleRegionVerification(regionCode);
    res.status(202).send(`✅ Accepted: Verification for region ${regionCode.toUpperCase()} has been started. Monitor the 'Overview' sheet for progress.`);
  } catch (error) {
    console.error(`Error starting verification for region ${regionCode.toUpperCase()}:`, error);
    res.status(500).send(`❌ Error starting verification for region ${regionCode.toUpperCase()}.`);
  }
});


module.exports = router;