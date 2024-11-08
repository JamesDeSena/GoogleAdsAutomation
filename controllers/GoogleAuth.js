const { oauth2Client } = require("../configs/googleAdsConfig");
const fs = require("fs");
const path = require("path");

const tokenFilePath = path.join(__dirname, "token.json");

const saveRefreshToken = (refreshToken_Google) => {
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
    console.error("Error saving refresh token:", error);
  }
};

const getStoredRefreshToken = () => {
  try {
    const data = fs.readFileSync(tokenFilePath, "utf8");
    const parsedData = JSON.parse(data);
    return parsedData.refreshToken_Google;
  } catch (err) {
    return null;
  }
};

const redirectToGoogle = (req, res) => {
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: "offline",
    scope: ["https://www.googleapis.com/auth/adwords"],
  });
  res.redirect(authUrl);
};

const handleOAuthCallbackGoogle = async (req, res) => {
  const { code } = req.query;

  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    saveRefreshToken(tokens.refresh_token);

    res.send("OAuth2 authentication successful.");
  } catch (error) {
    console.error("Error getting OAuth tokens:", error);
    res.status(500).send("Error during OAuth2 callback.");
  }
};

module.exports = {
  redirectToGoogle,
  handleOAuthCallbackGoogle,
  getStoredRefreshToken,
};
