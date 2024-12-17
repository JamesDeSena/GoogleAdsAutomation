require('dotenv').config();
const express = require('express');
const cors = require('cors');
const schedule = require("node-schedule");
// const ConnectDB = require("./config/Database");

const app = express();

// ConnectDB();

const googleRoutes = require('./routes/GoogleRoutes');
const bingRoutes = require('./routes/PacingRoutes');
const apiRoutes = require('./routes/AuthRoutes');
const { pingRenderApp } = require('./controllers/RenderPing');
const { sendFinalPacingReportToAirtable } = require('./controllers/PacingReport');
const { fetchReportDataDaily } = require('./controllers/GoogleAdsDaily');
const { sendFinalWeeklyReportToAirtable } = require('./controllers/GoogleAdsWeekly');
const { sendFinalMonthlyReportToAirtable } = require('./controllers/GoogleAdsMonthly');

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

const rule1 = new schedule.RecurrenceRule();
rule1.hour = 7;
rule1.minute = 0;
rule1.tz = 'America/Los_Angeles';

const AM = schedule.scheduleJob(rule1, () => {
  sendFinalPacingReportToAirtable();
  console.log("Scheduled pacing report sent at 7 AM PST California/Irvine.");
});

const dailyReportJob = schedule.scheduleJob(rule1, () => {
  fetchReportDataDaily();
  console.log("Scheduled daily report sent at 7 AM PST California/Irvine.");
});

const weeklyReportJob = schedule.scheduleJob(rule1, () => {
  sendFinalWeeklyReportToAirtable();
  console.log("Scheduled weekly report sent at 7 AM PST California/Irvine.");
});

const AMmonthlyReportJob = schedule.scheduleJob(rule1, () => {
  sendFinalMonthlyReportToAirtable();
  console.log("Scheduled monthly report sent at 7 AM PST California/Irvine.");
});

const rule2 = new schedule.RecurrenceRule();
rule2.hour = 19;
rule2.minute = 0;
rule2.tz = 'America/Los_Angeles';

const PM = schedule.scheduleJob(rule2, () => {
  sendFinalPacingReportToAirtable();
  console.log("Scheduled pacing report sent at 7 PM PST California/Irvine.");
});

const PMmonthlyReportJob = schedule.scheduleJob(rule2, () => {
  sendFinalMonthlyReportToAirtable();
  console.log("Scheduled monthly report sent at 7 AM PST California/Irvine.");
});

// schedule.scheduleJob('* * * * *', () => {
//   console.log("Testing Node Schedule");
// });

app.listen(process.env.PORT, () =>
  console.log(`Server started on port ${process.env.PORT}`)
);

