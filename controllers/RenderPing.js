const axios = require("axios");
const schedule = require("node-schedule");

const pingRenderApp = () => {
  const renderAppUrl = "https://googleadsautomation.onrender.com";

  axios
    .get(renderAppUrl)
    .then((response) => {
      console.log("Ping Sent!");
    })
    .catch((error) => {
      console.error("Failed to wake up the app:", error);
    });
};

setInterval(() => {
  pingRenderApp();
}, 5 * 60 * 1000);

module.exports = {
  pingRenderApp,
};
