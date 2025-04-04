const express = require("express");
const router = express.Router();

// const { 
//   getAllMetrics,
//   sendPacingReportToGoogleSheets,
//   sendSubPacingReport,
// } = require("../controllers/pacing_report/PacingReport");

const { 
  getAllMetrics,
  sendPacingReportToGoogleSheets,
  sendSubPacingReport,
} = require("../controllers/pacing_report/PacingReport");

router.get("/test", async (req, res) => {
  try {
    const metrics = await getAllMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).send("Error fetching all metrics");
  }
});
router.get("/send", sendPacingReportToGoogleSheets);
router.get("/sheets", sendSubPacingReport);

module.exports = router;
