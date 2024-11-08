const { ConfidentialClientApplication } = require("@azure/msal-node");

const msalConfig = {
  auth: {
    clientId: process.env.BING_ADS_CLIENT_ID,
    authority: `https://login.microsoftonline.com/${process.env.BING_ADS_TENANT_ID}`,
    clientSecret: process.env.BING_ADS_CLIENT_SECRET,
  },
};

const msalClient = new ConfidentialClientApplication(msalConfig);

module.exports = {
  msalClient,
};
