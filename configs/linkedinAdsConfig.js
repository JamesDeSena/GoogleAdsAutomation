module.exports = {
  clientId: process.env.LINKEDIN_CLIENT_ID,
  clientSecret: process.env.LINKEDIN_CLIENT_SECRET,
  // redirectUri: process.env.LINKEDIN_REDIRECT_URI_DEV,
  redirectUri: process.env.LINKEDIN_REDIRECT_URI_PROD,
  scope: 'r_ads_reporting r_ads',
};