require('dotenv').config();
const express = require('express');
const cors = require('cors');
const schedule = require("node-schedule");
// const ConnectDB = require("./config/Database");

const app = express();

// ConnectDB();

require("./utils/slackNotifier")

const googleRoutes = require('./routes/GoogleRoutes');
const bingRoutes = require('./routes/PacingRoutes');
const apiRoutes = require('./routes/AuthRoutes');
const { pingRenderApp } = require('./utils/renderPing');
const { sendPacingReportToGoogleSheets, sendSubPacingReport } = require('./controllers/pacing_report/PacingReport');
const { sendFinalWeeklyReportToGoogleSheetsHS, sendBlendedCACToGoogleSheetsHS } = require('./controllers/hi_skin/GoogleAdsWeekly');
const { sendFinalMonthlyReportToGoogleSheetsHS } = require('./controllers/hi_skin/GoogleAdsMonthly');
const { runDailyExportAndReport } = require('./controllers/lpc/DailyFetch');
const { sendFinalWeeklyReportToGoogleSheetsLPC } = require('./controllers/lpc/GoogleAdsWeekly');
const { sendLPCMonthlyReport } = require('./controllers/lpc/GoogleAdsMonthly');
const { sendFinalDailyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsDaily');
const { sendFinalWeeklyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsWeekly');
const { sendFinalMonthlyReportToGoogleSheetsMIV } = require('./controllers/mobile_iv/GoogleAdsMonthly');

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
app.use("/test", (req, res) => {
  res.send("Hello Akorn Media!");
});
app.use("/", (req, res) => {
  res.sendStatus(200);
});

pingRenderApp();

const executeConcurrentJobs = async (jobs, jobName) => {
  console.log(`Executing ${jobName} jobs concurrently.`);
  try {
    await Promise.all(jobs.map(job => job()));
    console.log(`${jobName} jobs completed successfully.`);
  } catch (error) {
    console.error(`Error in ${jobName} jobs:`, error);
  }
};

const rule1 = new schedule.RecurrenceRule();
rule1.hour = 7;
rule1.minute = 0;
rule1.tz = 'America/Los_Angeles';

const rule2 = new schedule.RecurrenceRule();
rule2.hour = 19;
rule2.minute = 0;
rule2.tz = 'America/Los_Angeles';

const rule3 = new schedule.RecurrenceRule();
rule3.hour = 7;
rule3.minute = 15;
rule3.tz = 'America/Los_Angeles';

const rule4 = new schedule.RecurrenceRule();
rule4.hour = 19;
rule4.minute = 15;
rule4.tz = 'America/Los_Angeles';

const rule5 = new schedule.RecurrenceRule();
rule5.hour = 7;
rule5.minute = 30;
rule5.tz = 'America/Los_Angeles';

const morningJobs = [
  sendPacingReportToGoogleSheets,
  sendFinalDailyReportToGoogleSheetsMIV,
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
  sendSubPacingReport,
];

const eveningJobs2 = [
  sendLPCMonthlyReport,
  sendFinalMonthlyReportToGoogleSheetsHS,
];

const morningJobs3 = [
  sendFinalWeeklyReportToGoogleSheetsMIV,
  sendFinalWeeklyReportToGoogleSheetsLPC,
  sendFinalWeeklyReportToGoogleSheetsHS,
];

schedule.scheduleJob(rule1, () => {
  executeConcurrentJobs(morningJobs, "Morning");
});

schedule.scheduleJob(rule2, () => {
  executeConcurrentJobs(eveningJobs, "Evening");
});

schedule.scheduleJob(rule3, () => {
  executeConcurrentJobs(morningJobs2, "Morning2");
});

schedule.scheduleJob(rule4, () => {
  executeConcurrentJobs(eveningJobs2, "Evening2");
});

schedule.scheduleJob(rule5, () => {
  executeConcurrentJobs(morningJobs3, "Morning3");
});

// schedule.scheduleJob('* * * * *', () => {
//   console.log("Testing Node Schedule");
// });

app.listen(process.env.PORT, () =>
  console.log(`Server started on port ${process.env.PORT}`)
);

