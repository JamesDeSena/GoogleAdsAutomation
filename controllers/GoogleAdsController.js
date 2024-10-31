const Airtable = require('airtable');
const { client, oauth2Client } = require('../configs/googleAdsConfig');

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function fetchReportData(req, res) {
  const query = `
    SELECT
      campaign.id,
      campaign.name,
      campaign.status,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date = '20241029'
    ORDER BY
      segments.date DESC
    LIMIT 10`;

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID,
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID
    });
    
    const response = await customer.query(query);

    const formattedResponse = response.map(campaign => ({
      id: campaign.campaign.id,
      name: campaign.campaign.name,
      status: campaign.campaign.status,
      impressions: campaign.metrics.impressions,
      clicks: campaign.metrics.clicks,
      cost: campaign.metrics.cost_micros / 1_000_000,
      date: campaign.segments.date
    }));

    await sendToAirtable(formattedResponse);

    res.json(formattedResponse);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
}

async function fetchTest(req, res) {
  const query = `
    SELECT
      campaign.id,
      campaign.name,
      campaign.status,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date = '20241029'
    ORDER BY
      segments.date DESC
    LIMIT 10`;

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID,
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID
    });
    
    const response = await customer.query(query);

    const formattedResponse = response.map(campaign => ({
      id: campaign.campaign.id,
      name: campaign.campaign.name,
      status: campaign.campaign.status,
      impressions: campaign.metrics.impressions,
      clicks: campaign.metrics.clicks,
      cost: campaign.metrics.cost_micros / 1_000_000,
      date: campaign.segments.date
    }));

    await sendToAirtable(formattedResponse);

    res.json(formattedResponse);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
}

async function sendToAirtable(data) {
  for (const record of data) {
    try {
      const existingRecords = await base('Table 1').select({
        filterByFormula: `{Campaign} = '${record.name}'`
      }).firstPage();

      if (existingRecords.length > 0) {
        const existingRecordId = existingRecords[0].id;
        await base('Table 1').update(existingRecordId, {
          'Day': record.date,
          'Impr.': record.impressions,
          'Clicks': record.clicks,
          'Cost': record.cost
        });
        console.log(`Record updated for Campaign Name: ${record.name}`);
      } else {
        await base('Table 1').create({
          'Day': record.date,
          'Campaign': record.name,
          'Impr.': record.impressions,
          'Clicks': record.clicks,
          'Cost': record.cost
        });
        console.log(`Record added for Campaign Name: ${record.name}`);
      }
    } catch (error) {
      console.error(`Error processing record for Campaign Name: ${record.name}`, error);
    }
  }
}

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
    res.send('OAuth2 authentication successful.');
  } catch (error) {
    console.error('Error getting OAuth tokens:', error);
    res.status(500).send('Error during OAuth2 callback.');
  }
};

module.exports = {
  fetchReportData,
  fetchTest,
  redirectToGoogle,
  handleOAuthCallback
};
