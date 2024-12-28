const { oauth2Client } = require("../configs/googleAdsConfig");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const tokenFilePath = path.join(__dirname, "token.json");

const saveRefreshToken = (accessToken_Google, refreshToken_Google, expiresIn_Google) => {
  try {
    let currentData = {};
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, "utf8"));
    }

    currentData.accessToken_Google = accessToken_Google;
    currentData.refreshToken_Google = refreshToken_Google;
    currentData.expiresIn_Google = expiresIn_Google;

    fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
    
  } catch (error) {
    console.error("Error saving refresh token:", error);
  }
};

const getStoredRefreshToken = () => {
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

const redirectToGoogle = (req, res) => {
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: "offline",
    scope: ["https://www.googleapis.com/auth/adwords"],
    prompt: "consent",
  });
  res.redirect(authUrl);
};

const handleOAuthCallbackGoogle = async (req, res) => {
  const { code } = req.query;

  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    saveRefreshToken(
      tokens.access_token,
      tokens.refresh_token,
    );

    res.send("OAuth2 authentication successful.");
  } catch (error) {
    console.error("Error getting OAuth tokens:", error);
    res.status(500).send("Error during OAuth2 callback.");
  }
};

const refreshAccessToken = async () => {
  try {
    const storedData = getStoredRefreshToken();

    if (storedData) {
      const { refreshToken_Google } = storedData;

      const tokenRequestData = new URLSearchParams({
        client_id: process.env.GOOGLE_ADS_CLIENT_ID,
        client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
        refresh_token: refreshToken_Google,
        grant_type: "refresh_token",
      });

      const response = await axios.post(
        `https://oauth2.googleapis.com/token`,
        tokenRequestData.toString(),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
      );

      const tokenResponse = response.data;
      saveRefreshToken(
        tokenResponse.access_token,
        tokenResponse.refresh_token,
        tokenResponse.expiry_in
      );

      console.log("Google access token has been refreshed.");
    } else {
      console.log("Google access token is not valid, no stored token found.");
    }
  } catch (error) {
    console.error("Error refreshing access token:", error);
  }
};

setInterval(async () => {
  console.log("Attempting to refresh google access token...");
  await refreshAccessToken();
}, 40 * 60 * 1000);

module.exports = {
  redirectToGoogle,
  handleOAuthCallbackGoogle,
  getStoredRefreshToken,
};
