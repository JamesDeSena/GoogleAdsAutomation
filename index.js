require('dotenv').config();
const express = require('express');
const cors = require('cors');
// const ConnectDB = require("./config/Database");

const app = express();

// ConnectDB();

// const googleRoutes = require('./routes/GoogleRoutes');
const bingRoutes = require('./routes/PacingRoutes');
const apiRoutes = require('./routes/AuthRoutes');
const { pingRenderApp } = require('./controllers/RenderPing');

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

// app.use('/api/google', googleRoutes);
app.use('/api/pacing', bingRoutes)
app.use('/api/auth', apiRoutes)
app.use("/test", (req, res) => {
  res.send("Hello Akorn Media!");
});
app.use("/", (req, res) => {
  res.sendStatus(200);
});

pingRenderApp();

app.listen(process.env.PORT, () =>
  console.log(`Server started on port ${process.env.PORT}`)
);

