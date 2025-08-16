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
} = require("../controllers/pacing_report/PacingReport");

const { 
  runLinkedinReport,
  sendTWtoGoogleSheets,
} = require("../controllers/pacing_report/TWPacingReport");

router.get("/test", async (req, res) => {
  try {
    const metrics = await getAllMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).send("Error fetching all metrics");
  }
});

router.get('/sheets', async (req, res) => {
  try {
    await sendTWtoGoogleSheets(req, res);
    res.status(200).send("Process completed successfully.");
  } catch (error) {
    console.error("Error processing final report:", error);
    res.status(500).send("Error processing final report.");
  }
});

router.get("/send", sendPacingReportToGoogleSheets);
// router.get("/sheets", sendSubPacingReport);

module.exports = router;
