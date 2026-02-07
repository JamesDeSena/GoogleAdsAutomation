require('dotenv').config();
const express = require('express');
const cors = require('cors');
const schedule = require("node-schedule");
// const ConnectDB = require("./config/Database");

const app = express();

// ConnectDB();

require("./utils/highlightIO");
require("./utils/slackNotifier")

const googleRoutes = require('./routes/GoogleRoutes');
const bingRoutes = require('./routes/PacingRoutes');
const apiRoutes = require('./routes/AuthRoutes');
const scrapeRoutes = require('./routes/ScraperRoutes');

const { pingRenderApp } = require('./utils/renderPing');
const { sendPacingReportToGoogleSheets } = require('./controllers/pacing_report/PacingReport');
const { sendFinalDailyReportToGoogleSheetsHS } = require('./controllers/hi_skin/GoogleAdsDaily');
const { sendFinalWeeklyReportToGoogleSheetsHS, sendBlendedCACToGoogleSheetsHS } = require('./controllers/hi_skin/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsHSAdG } = require('./controllers/hi_skin/GoogleAdsGroupWeekly');
const { sendFinalMonthlyReportToGoogleSheetsHS } = require('./controllers/hi_skin/GoogleAdsMonthly');
const { runDailyExportAndReport } = require('./controllers/lpc/DailyFetch');
const { sendFinalWeeklyReportToGoogleSheetsLPCAdG } = require('./controllers/lpc/GoogleAdsGroupWeekly');
const { sendFinalWeeklyReportToGoogleSheetsLPC } = require('./controllers/lpc/GoogleAdsWeekly');
const { sendLPCMonthlyReport } = require('./controllers/lpc/GoogleAdsMonthly');
const { sendFinalDailyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsDaily');
const { sendFinalWeeklyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsMIVDAdG } = require('./controllers/mobile_iv/GoogleAdsGroupWeekly');
const { sendFinalMonthlyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsMonthly');
const { sendFinalWeeklyReportToGoogleSheetsGCAdG } = require('./controllers/guardian_carers/GoogleAdsGroupWeekly');
const { sendFinalWeeklyReportToGoogleSheetsGC } = require('./controllers/guardian_carers/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsMNR } = require('./controllers/menerals/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsST } = require('./controllers/sleepy_tie/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsFLX } = require('./controllers/flexfits/GoogleAdsWeekly');
const { sendFinalWeeklyReportToGoogleSheetsNPL } = require('./controllers/nations_photolab/GoogleAdsWeekly');

const { highlightErrorHandler, highlightJobErrorHandler } = require('./utils/highlightIO');

app.use(express.json());

app.use(
  cors({
    origin: "*",
    credentials: true,
    optionsSuccessStatus: 200,
  })
);

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS,POST,PUT,PATCH");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept, x-client-key, x-client-token, x-client-secret, Authorization"
  );
  console.log(req.path, req.method);
  next();
});

app.use('/api/google', googleRoutes);
app.use('/api/pacing', bingRoutes)
app.use('/api/auth', apiRoutes)
app.use('/api/scrape', scrapeRoutes)
app.use("/test", (req, res) => {
  res.send("Hello Akorn Media!");
});
app.use("/", (req, res) => {
  res.sendStatus(200);
});

app.use(highlightErrorHandler);

const executeSequentialJobs = async (jobs, jobName) => {
  console.log(`Executing ${jobName} jobs sequentially.`);

  for (const job of jobs) {
    try {
      await job();
    } catch (error) {
      console.error(`Error in individual ${jobName} job:`, error);
      highlightJobErrorHandler(error, { jobName: jobName, jobFunction: job.name || 'anonymous' });
      console.log(`Halting execution of ${jobName} jobs due to an error.`);
      return; 
    }
  }
  
  console.log(`All ${jobName} jobs completed successfully.`);
};

pingRenderApp();

const isDSTInLA = () => {
  const laDate = new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" });
  const now = new Date(laDate);
  const jan = new Date(now.getFullYear(), 0, 1);
  const jul = new Date(now.getFullYear(), 6, 1);
  const stdOffset = Math.max(jan.getTimezoneOffset(), jul.getTimezoneOffset());
  return now.getTimezoneOffset() < stdOffset; // true if DST (PDT)
};

const hourBase = isDSTInLA() ? 7 : 6; // PDT → 7, PST → 6

const rule1 = new schedule.RecurrenceRule();
rule1.hour = hourBase;
rule1.minute = 0;
rule1.tz = 'America/Los_Angeles';

const rule2 = new schedule.RecurrenceRule();
rule2.hour = hourBase + 12;
rule2.minute = 0;
rule2.tz = 'America/Los_Angeles';

const rule3 = new schedule.RecurrenceRule();
rule3.hour = hourBase;
rule3.minute = 15;
rule3.tz = 'America/Los_Angeles';

const rule4 = new schedule.RecurrenceRule();
rule4.hour = hourBase + 12;
rule4.minute = 15;
rule4.tz = 'America/Los_Angeles';

const rule5 = new schedule.RecurrenceRule();
rule5.hour = hourBase;
rule5.minute = 30;
rule5.tz = 'America/Los_Angeles';

const morningJobs = [
  sendPacingReportToGoogleSheets,
  sendFinalDailyReportToGoogleSheetsHS,
  sendFinalDailyReportToGoogleSheetsMIV,
  sendFinalWeeklyReportToGoogleSheetsHSAdG,
  sendFinalWeeklyReportToGoogleSheetsMIVDAdG,
  sendFinalWeeklyReportToGoogleSheetsLPCAdG,
  sendFinalWeeklyReportToGoogleSheetsGCAdG,
];

const eveningJobs = [
  sendPacingReportToGoogleSheets,
  sendFinalDailyReportToGoogleSheetsMIV,
];

const morningJobs2 = [
  sendLPCMonthlyReport,
  runDailyExportAndReport,
  sendFinalMonthlyReportToGoogleSheetsMIV,
  sendFinalMonthlyReportToGoogleSheetsHS,
  sendBlendedCACToGoogleSheetsHS,
  // sendTWtoGoogleSheets,
];

const eveningJobs2 = [
  sendLPCMonthlyReport,
  sendFinalMonthlyReportToGoogleSheetsHS,
];

const morningJobs3 = [
  sendFinalWeeklyReportToGoogleSheetsGC,
  sendFinalWeeklyReportToGoogleSheetsMNR,
  sendFinalWeeklyReportToGoogleSheetsFLX,
  sendFinalWeeklyReportToGoogleSheetsNPL,
  sendFinalWeeklyReportToGoogleSheetsST,
  sendFinalWeeklyReportToGoogleSheetsMIV,
  sendFinalWeeklyReportToGoogleSheetsLPC,
  sendFinalWeeklyReportToGoogleSheetsHS,
];

schedule.scheduleJob(rule1, () => {
  executeSequentialJobs(morningJobs, "Morning");
});

schedule.scheduleJob(rule2, () => {
  executeSequentialJobs(eveningJobs, "Evening");
});

schedule.scheduleJob(rule3, () => {
  executeSequentialJobs(morningJobs2, "Morning2");
});

schedule.scheduleJob(rule4, () => {
  executeSequentialJobs(eveningJobs2, "Evening2");
});

schedule.scheduleJob(rule5, () => {
  executeSequentialJobs(morningJobs3, "Morning3");
});

// schedule.scheduleJob('* * * * *', () => {
//   console.log("Testing Node Schedule");
// });

app.listen(process.env.PORT, () =>
  console.log(`Server started on port ${process.env.PORT}`)
);

