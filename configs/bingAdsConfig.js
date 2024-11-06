const { ConfidentialClientApplication } = require("@azure/msal-node");

const msalConfig = {
  auth: {
    clientId: process.env.BING_ADS_CLIENT_ID,
    authority: `https://login.microsoftonline.com/${process.env.BING_ADS_TENANT_ID}`,
    clientSecret: process.env.BING_ADS_CLIENT_SECRET,
  },
};

const msalClient = new ConfidentialClientApplication(msalConfig);

// async function getAccessToken() {
//   const tokenRequest = {
//     scopes: ["https://ads.microsoft.com/.default"],
//   };

//   try {
//     const response = await msalClient.acquireTokenByClientCredential(tokenRequest);
//     return response.accessToken;
//   } catch (error) {
//     console.error("Error acquiring access token:", error);
//     throw new Error("Failed to acquire access token");
//   }
// }

module.exports = {
  msalClient,
  // getAccessToken
};
