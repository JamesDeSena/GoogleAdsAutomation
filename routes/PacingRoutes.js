const express = require("express");
const router = express.Router();

const { 
  getAllMetrics,
  sendFinalReportToAirtable
} = require("../controllers/PacingReport");

router.get("/test", async (req, res) => {
  try {
    const metrics = await getAllMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).send("Error fetching all metrics");
  }
});
router.get("/send", sendFinalReportToAirtable);

module.exports = router;
