const { msalClient } = require("../configs/bingAdsConfig");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const tokenFilePath = path.join(__dirname, "token.json");

const saveBingToken = (accessToken_Bing, refreshToken_Bing,) => {
  try {
    let currentData = {};

    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, "utf8"));
    }

    currentData.accessToken_Bing = accessToken_Bing;
    currentData.refreshToken_Bing = refreshToken_Bing;

    fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
  } catch (error) {
    console.error("Error saving Bing token:", error);
  }
};

const getStoredBingToken = () => {
  try {
    if (fs.existsSync(tokenFilePath)) {
      const data = fs.readFileSync(tokenFilePath, "utf8");
      const parsedData = JSON.parse(data);

      return parsedData;
    } else {
      return null;
    }
  } catch (err) {
    console.error("Error reading Bing token:", err);
    return null;
  }
};

const redirectToBing = async (req, res) => {
  try {
    const authUrl = await msalClient.getAuthCodeUrl({
      scopes: ["https://ads.microsoft.com/.default"],
      // redirectUri: process.env.BING_ADS_REDIRECT_URI_DEV,
      redirectUri: process.env.BING_ADS_REDIRECT_URI_PROD,
    });

    res.redirect(authUrl);
  } catch (error) {
    console.error("Error generating Bing auth URL:", error);
    res.status(500).send("Error generating Bing auth URL.");
  }
};

const handleOAuthCallbackBing = async (req, res) => {
  const { code } = req.query;

  try {
    const tokenRequestData = new URLSearchParams({
      client_id: process.env.BING_ADS_CLIENT_ID,
      scope: "https://ads.microsoft.com/.default",
      code: code,
      // redirect_uri: process.env.BING_ADS_REDIRECT_URI_DEV,
      redirect_uri: process.env.BING_ADS_REDIRECT_URI_PROD,
      grant_type: "authorization_code",
      client_secret: process.env.BING_ADS_CLIENT_SECRET,
    });

    const response = await axios.post(
      `https://login.microsoftonline.com/${process.env.BING_ADS_TENANT_ID}/oauth2/v2.0/token`,
      tokenRequestData.toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    const tokenResponse = response.data;
    saveBingToken(
      tokenResponse.access_token,
      tokenResponse.refresh_token,
    );

    res.send("Bing Ads OAuth2 authentication successful. You can close this window.");
  } catch (error) {
    console.error("Error getting Bing OAuth2 tokens:", error);
    res.status(500).send("Error during Bing OAuth2 callback.");
  }
};

const refreshAccessToken = async () => {
  try {
    const storedData = getStoredBingToken();

    if (storedData) {
      const { refreshToken_Bing } = storedData;

      const tokenRequestData = new URLSearchParams({
        client_id: process.env.BING_ADS_CLIENT_ID,
        scope: "https://ads.microsoft.com/.default",
        refresh_token: refreshToken_Bing,
        grant_type: "refresh_token",
        client_secret: process.env.BING_ADS_CLIENT_SECRET,
      });

      const response = await axios.post(
        `https://login.microsoftonline.com/${process.env.BING_ADS_TENANT_ID}/oauth2/v2.0/token`,
        tokenRequestData.toString(),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
      );

      const tokenResponse = response.data;

      saveBingToken(
        tokenResponse.access_token,
        tokenResponse.refresh_token,
      );

      console.log("Bing access token has been refreshed.");
    } else {
      console.log("Bing access token is not valid, no stored token found.");
    }
  } catch (error) {
    console.error("Error refreshing Bing token:", error);
  }
};

setInterval(async () => {
  console.log("Attempting to refresh Bing access token...");
  await refreshAccessToken();
}, 40 * 60 * 1000);

module.exports = {
  redirectToBing,
  handleOAuthCallbackBing,
  getStoredBingToken,
};
