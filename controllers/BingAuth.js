const { msalClient } = require("../configs/bingAdsConfig");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const tokenFilePath = path.join(__dirname, "token.json");

const saveAccessToken = (accessToken_Bing, refreshToken_Bing, expiresIn_Bing) => {
  try {
    let currentData = {};

    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, "utf8"));
    }

    currentData.accessToken_Bing = accessToken_Bing;
    currentData.refreshToken_Bing = refreshToken_Bing;
    currentData.expiresIn_Bing = expiresIn_Bing;

    fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
  } catch (error) {
    console.error("Error saving access token:", error);
  }
};

const getStoredAccessToken = () => {
  try {
    if (fs.existsSync(tokenFilePath)) {
      const data = fs.readFileSync(tokenFilePath, "utf8");
      const parsedData = JSON.parse(data);

      return parsedData;
    } else {
      return null;
    }
  } catch (err) {
    console.error("Error reading token:", err);
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
    console.error("Error generating auth URL:", error);
    res.status(500).send("Error generating auth URL.");
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
    saveAccessToken(
      tokenResponse.access_token,
      tokenResponse.refresh_token,
      tokenResponse.expires_in
    );

    res.send("OAuth2 authentication successful.");
  } catch (error) {
    console.error("Error getting OAuth tokens:", error);
    res.status(500).send("Error during OAuth2 callback.");
  }
};

const refreshAccessToken = async () => {
  try {
    const storedData = getStoredAccessToken();

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

      saveAccessToken(
        tokenResponse.access_token,
        tokenResponse.refresh_token,
        tokenResponse.expires_in
      );

      console.log("Bing access token has been refreshed.");
    } else {
      console.log("Bing access token is not valid, no stored token found.");
    }
  } catch (error) {
    console.error("Error refreshing access token:", error);
  }
};

setInterval(async () => {
  console.log("Attempting to refresh Bing access token...");
  await refreshAccessToken();
}, 40 * 60 * 1000);

module.exports = {
  redirectToBing,
  handleOAuthCallbackBing,
  getStoredAccessToken,
};
