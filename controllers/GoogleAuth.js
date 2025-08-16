const { oauth2Client } = require("../configs/googleAdsConfig");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const tokenFilePath = path.join(__dirname, "token.json");

const saveGoogleToken = (refreshToken_Google) => {
  try {
    let currentData = {};
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, "utf8"));
    }

    if (currentData.refreshToken_Google !== refreshToken_Google) {
      currentData.refreshToken_Google = refreshToken_Google;
      fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
    }
    
  } catch (error) {
    console.error("Error saving Google token:", error);
  }
};

const getStoredGoogleToken = () => {
  try {
    const data = fs.readFileSync(tokenFilePath, "utf8");
    const parsedData = JSON.parse(data);
    return parsedData.refreshToken_Google;
  } catch (err) {
    console.error("Error reading Google token:", err);
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

    saveGoogleToken(tokens.refresh_token);
    
    res.send("Google Ads OAuth2 authentication successful. You can close this window.");
  } catch (error) {
    console.error("Error getting Google OAuth tokens:", error);
    res.status(500).send("Error during Google OAuth2 callback.");
  }
};

// const refreshAccessToken = async () => {
//   try {
//     const storedData = getStoredGoogleToken();

//     if (storedData) {
//       const { refreshToken_Google } = storedData;

//       const tokenRequestData = new URLSearchParams({
//         client_id: process.env.GOOGLE_ADS_CLIENT_ID,
//         client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
//         refresh_token: refreshToken_Google,
//         grant_type: "refresh_token",
//       });

//       const response = await axios.post(
//         `https://oauth2.googleapis.com/token`,
//         tokenRequestData.toString(),
//         { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
//       );

//       const tokenResponse = response.data;
//       console.log(tokenResponse)
//       saveGoogleToken(
//         tokenResponse.access_token
//       );

//       console.log("Google access token has been refreshed.");
//     } else {
//       console.log("Google access token is not valid, no stored token found.");
//     }
//   } catch (error) {
//     console.error("Error refreshing access token:", error);
//   }
// };

// setInterval(async () => {
//   console.log("Attempting to refresh google access token...");
//   await refreshAccessToken();
// }, 1 * 60 * 1000);

module.exports = {
  redirectToGoogle,
  handleOAuthCallbackGoogle,
  getStoredGoogleToken,
};
