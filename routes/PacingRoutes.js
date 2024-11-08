const express = require("express");
const router = express.Router();

const { 
  getAllMetrics,
  sendFinalReportToAirtable
} = require("../controllers/PacingReport");

router.get("/test", getAllMetrics);
router.get("/send", sendFinalReportToAirtable);

module.exports = router;
