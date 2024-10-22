const { GoogleAdsApi } = require('google-ads-api');
const { OAuth2Client } = require('google-auth-library'); // Add this for OAuth2

// Initialize Google Ads API client with environment variables
const client = new GoogleAdsApi({
    client_id: process.env.GOOGLE_ADS_CLIENT_ID,
    client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    developer_token: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN
});

const oauth2Client = new OAuth2Client(
  process.env.GOOGLE_ADS_CLIENT_ID,
  process.env.GOOGLE_ADS_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

module.exports = {
  client,
  oauth2Client
};
