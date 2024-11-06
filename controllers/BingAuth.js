const { msalClient } = require("../configs/bingAdsConfig");
const fs = require('fs');
const path = require('path');

const tokenFilePath = path.join(__dirname, 'token.json');

const saveAccessToken = (accessToken_Bing) => {
  try {
    let currentData = {};
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, 'utf8'));
    }

    if (currentData.accessToken_Bing !== accessToken_Bing) {
      currentData.accessToken_Bing = accessToken_Bing;
      fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
    }
  } catch (error) {
    console.error("Error saving access token:", error);
  }
};

const getStoredAccessToken = () => {
  try {
    const data = fs.readFileSync(tokenFilePath, 'utf8');
    const parsedData = JSON.parse(data);
    return parsedData.accessToken_Bing;
  } catch (err) {
    return null;
  }
};

const redirectToBing = async (req, res) => {
  try {
    const authUrl = await msalClient.getAuthCodeUrl({
      scopes: ["https://ads.microsoft.com/.default"],
      redirectUri: process.env.BING_ADS_REDIRECT_URI,
    });

    res.redirect(authUrl);
  } catch (error) {
    console.error("Error generating auth URL:", error);
    res.status(500).send("Error generating auth URL.");
  }
};

const handleOAuthCallbackBing = async (req, res) => {
  const { code } = req.query;

  try {
    const tokenResponse = await msalClient.acquireTokenByCode({
      code,
      redirectUri: process.env.BING_ADS_REDIRECT_URI,
      scopes: ["https://ads.microsoft.com/.default"],
    });

    saveAccessToken(tokenResponse.accessToken);

    res.send("OAuth2 authentication successful.");
  } catch (error) {
    console.error("Error getting OAuth tokens:", error);
    res.status(500).send("Error during OAuth2 callback.");
  }
};

module.exports = {
  redirectToBing,
  handleOAuthCallbackBing,
  getStoredAccessToken
};
