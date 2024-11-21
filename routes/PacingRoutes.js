const express = require("express");
const router = express.Router();

const { 
  getAllMetrics,
  sendFinalPacingReportToAirtable,
  fetchAndFormatTimeCreatedCST
} = require("../controllers/PacingReport");

router.get("/test", async (req, res) => {
  try {
    const metrics = await getAllMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).send("Error fetching all metrics");
  }
});
router.get("/send", sendFinalPacingReportToAirtable);
router.get("/test2", fetchAndFormatTimeCreatedCST);

module.exports = router;
