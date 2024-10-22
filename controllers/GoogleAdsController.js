const {client, oauth2Client} = require('../configs/googleAdsConfig');

// Controller function to fetch ad performance
async function fetchReportData(req, res) {
  const query = `
    SELECT
      campaign.id,
      campaign.name,
      ad_group.id,
      ad_group.name,
      ad_group_criterion.criterion_id,
      ad_group_criterion.keyword.text,
      ad_group_criterion.keyword.match_type,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros
    FROM
      keyword_view
    WHERE
      segments.date DURING LAST_30_DAYS
    ORDER BY
      metrics.impressions DESC
    LIMIT 10`;

  try {
    // Define the customer by passing the required parameters
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID, // Make sure you pass customer_id
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
    });

    // Fetch the report data using the customer instance
    const response = await customer.query(query);
    console.log('Report Data:', response);

    // Send the response back to the client
    res.json(response);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
}

// Redirect to Google's OAuth2 consent screen
const redirectToGoogle = (req, res) => {
  const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline', // Can be 'online' or 'offline'
      scope: [
          'https://www.googleapis.com/auth/adwords', // Add required scopes
      ],
  });
  res.redirect(authUrl); // Redirect to the generated URL
};

// Handle the OAuth2 callback
const handleOAuthCallback = async (req, res) => {
  const { code } = req.query; // Get the authorization code from the query parameters

  try {
      const { tokens } = await oauth2Client.getToken(code); // Exchange the code for tokens
      oauth2Client.setCredentials(tokens); // Set the tokens for the client
      console.log(tokens)
      // Optionally, store tokens in the database or session here
      res.send('OAuth2 authentication successful.'); // Respond to the user
  } catch (error) {
      console.error('Error getting OAuth tokens:', error);
      res.status(500).send('Error during OAuth2 callback.');
  }
};

module.exports = {
  fetchReportData,
  redirectToGoogle,
  handleOAuthCallback,
};
