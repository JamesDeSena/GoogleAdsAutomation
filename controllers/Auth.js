const { oauth2Client } = require('../configs/googleAdsConfig');

const redirectToGoogle = (req, res) => {
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline', 
    scope: ['https://www.googleapis.com/auth/adwords']
  });
  res.redirect(authUrl);
};

const handleOAuthCallback = async (req, res) => {
  const { code } = req.query;

  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    console.log(tokens);
    res.send('OAuth2 authentication successful.', tokens);
  } catch (error) {
    console.error('Error getting OAuth tokens:', error);
    res.status(500).send('Error during OAuth2 callback.');
  }
};

module.exports = {
  redirectToGoogle,
  handleOAuthCallback
};