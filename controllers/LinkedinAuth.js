const config = require('../configs/linkedinAdsConfig');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const tokenFilePath = path.join(__dirname, 'token.json');

const saveLinkedinToken = (accessToken_Linkedin) => {
  try {
    let currentData = {};
    if (fs.existsSync(tokenFilePath)) {
      currentData = JSON.parse(fs.readFileSync(tokenFilePath, 'utf8'));
    }

    if (currentData.accessToken_Linkedin !== accessToken_Linkedin) {
      currentData.accessToken_Linkedin = accessToken_Linkedin;
      fs.writeFileSync(tokenFilePath, JSON.stringify(currentData, null, 2));
    }

  } catch (error) {
    console.error("Error saving Linkedin tokens:", error);
  }
};

const getStoredLinkedinToken = () => {
  try {
    const data = fs.readFileSync(tokenFilePath, "utf8");
    const parsedData = JSON.parse(data);
    return parsedData.accessToken_Linkedin;
  } catch (err) {
    console.error("Error reading Linkedin tokens:", err);
    return null;
  }
};

const redirectToLinkedin = (req, res) => {
  const authUrl = `https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=${config.clientId}&redirect_uri=${config.redirectUri}&scope=${encodeURIComponent(config.scope)}`;
  res.redirect(authUrl);
};

const handleOAuthCallbackLinkedin = async (req, res) => {
  const { code } = req.query;
  if (!code) {
    return res.status(400).send("Error: Authorization code is missing.");
  }

  try {
    const tokenRequestData = new URLSearchParams({
      grant_type: 'authorization_code',
      code: code,
      redirect_uri: config.redirectUri,
      client_id: config.clientId,
      client_secret: config.clientSecret,
    });

    const response = await axios.post(
      'https://www.linkedin.com/oauth/v2/accessToken',
      tokenRequestData.toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    const { access_token, refresh_token, expires_in } = response.data;
    saveLinkedinToken(access_token, refresh_token, expires_in);
    res.send("Linkedin OAuth2 authentication successful. You can close this window.");
  } catch (error) {
    console.error("Error getting Linkedin OAuth tokens:", error.response ? error.response.data : error.message);
    res.status(500).send("Error during Linkedin OAuth2 callback.");
  }
};

module.exports = {
  redirectToLinkedin,
  handleOAuthCallbackLinkedin,
  getStoredLinkedinToken,
};