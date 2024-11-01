const Airtable = require('airtable');
const { client, oauth2Client } = require('../configs/googleAdsConfig');

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function fetchReportData(req, res) {
  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date >= '20241001' AND segments.date <= '20241031'
    ORDER BY
      segments.date DESC
    `;

  const conversionQuery = `
    SELECT
      campaign.id,
      segments.conversion_action_name,
      metrics.conversions,
      segments.date
    FROM
      campaign
    WHERE
      segments.date >= '20241001' AND segments.date <= '20241031'
      AND segments.conversion_action_name IN ('Book Now - Step 1: Email Signup', 'Book Now - Step 6: Booking Confirmation')
    ORDER BY
      segments.date DESC
    `;

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID,
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID
    });

    // Fetching metrics data in batches
    const formattedMetricsMap = {};
    let metricsPageToken = null; // Initialize the page token

    do {
      const metricsResponse = await customer.query(metricsQuery);
      metricsResponse.forEach(campaign => {
        const key = `${campaign.campaign.id}-${campaign.segments.date}`;
        formattedMetricsMap[key] = {
          id: campaign.campaign.id,
          name: campaign.campaign.name,
          impressions: campaign.metrics.impressions,
          clicks: campaign.metrics.clicks,
          cost: campaign.metrics.cost_micros / 1_000_000,
          date: campaign.segments.date,
          step1Value: 0,
          step6Value: 0,
        };
      });

      metricsPageToken = metricsResponse.next_page_token; // Update the page token
    } while (metricsPageToken); // Continue until there are no more pages

    // Fetching conversion data in batches
    const conversionResponse = [];
    let conversionPageToken = null; // Initialize the page token

    do {
      const conversionBatchResponse = await customer.query(conversionQuery);
      conversionResponse.push(...conversionBatchResponse);

      conversionPageToken = conversionBatchResponse.next_page_token; // Update the page token
    } while (conversionPageToken); // Continue until there are no more pages

    // Update the step values based on conversion data
    conversionResponse.forEach(conversion => {
      const key = `${conversion.campaign.id}-${conversion.segments.date}`;
      const conversionValue = conversion.metrics.conversions;

      if (conversion.segments.conversion_action_name === 'Book Now - Step 1: Email Signup') {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].step1Value += conversionValue; // Aggregate if necessary
        }
      } else if (conversion.segments.conversion_action_name === 'Book Now - Step 6: Booking Confirmation') {
        if (formattedMetricsMap[key]) {
          formattedMetricsMap[key].step6Value += conversionValue; // Aggregate if necessary
        }
      }
    });
    
    const formattedMetrics = Object.values(formattedMetricsMap);

    await sendToAirtable(formattedMetrics);

    res.json(formattedMetrics);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
}

async function fetchTest(req, res) {
  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM
      campaign
    WHERE
      segments.date = '20241031'
    ORDER BY
      segments.date DESC
    LIMIT 100`;

  const conversionQuery = `
    SELECT
      campaign.id,
      segments.conversion_action_name,
      metrics.conversions,
      segments.date
    FROM
      campaign
    WHERE
      segments.date = '20241031'
      AND segments.conversion_action_name IN ('Book Now - Step 1: Email Signup', 'Book Now - Step 6: Booking Confirmation')
    ORDER BY
      segments.date DESC
    LIMIT 100`;

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID,
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID
    });

    const metricsResponse = await customer.query(metricsQuery);
    const conversionResponse = await customer.query(conversionQuery);

    const formattedMetrics = metricsResponse.map(campaign => ({
      id: campaign.campaign.id,
      name: campaign.campaign.name,
      impressions: campaign.metrics.impressions,
      clicks: campaign.metrics.clicks,
      cost: campaign.metrics.cost_micros / 1_000_000,
      date: campaign.segments.date,
      step1Value: 0,
      step6Value: 0,
    }));

    conversionResponse.forEach(conversion => {
      const campaignId = conversion.campaign.id;
      const conversionValue = conversion.metrics.conversions;

      if (conversion.segments.conversion_action_name === 'Book Now - Step 1: Email Signup') {
        const campaign = formattedMetrics.find(c => c.id === campaignId);
        if (campaign) {
          campaign.step1Value = conversionValue;
        }
      } else if (conversion.segments.conversion_action_name === 'Book Now - Step 6: Booking Confirmation') {
        const campaign = formattedMetrics.find(c => c.id === campaignId);
        if (campaign) {
          campaign.step6Value = conversionValue;
        }
      }
    });

    const filteredResponse = formattedMetrics.filter(item => item.step1Value > 0 || item.step6Value > 0);

    res.json(filteredResponse);
    console.log(filteredResponse);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
}

async function sendToAirtable(data) {
  const recordsToUpdate = [];
  const recordsToCreate = [];

  for (const record of data) {
    // Use record values directly without trimming
    const campaignName = record.name; // No trim
    const recordDate = record.date; // No trim

    try {
      // Find existing records matching the campaign and date
      const existingRecords = await base('Table 1').select({
        filterByFormula: `AND(
          {Campaign} = '${campaignName}', 
          DATETIME_FORMAT({Day}, 'YYYY-MM-DD') = '${recordDate}'
        )`
      }).firstPage();

      if (existingRecords.length > 0) {
        // Update the first existing record found
        const existingRecordId = existingRecords[0].id;
        recordsToUpdate.push({
          id: existingRecordId,
          fields: {
            'Impr.': record.impressions,
            'Clicks': record.clicks,
            'Cost': record.cost,
            'Book Now - Step 1: Email Signup': record.step1Value,
            'Book Now - Step 6: Booking Confirmation': record.step6Value
          }
        });
        console.log(`Record prepared for update for Campaign Name: ${campaignName} on Date: ${recordDate}`);
      } else {
        // No existing records found, create a new one
        recordsToCreate.push({
          fields: {
            'Day': recordDate,
            'Campaign': campaignName,
            'Impr.': record.impressions,
            'Clicks': record.clicks,
            'Cost': record.cost,
            'Book Now - Step 1: Email Signup': record.step1Value,
            'Book Now - Step 6: Booking Confirmation': record.step6Value
          }
        });
        console.log(`Record prepared for creation for Campaign Name: ${campaignName} on Date: ${recordDate}`);
      }
    } catch (error) {
      console.error(`Error processing record for Campaign Name: ${campaignName} on Date: ${recordDate}`, error);
    }
  }

  // Process updates in batches
  const batchProcessUpdates = async (records) => {
    for (let i = 0; i < records.length; i += 10) {
      const batch = records.slice(i, i + 10);
      await base('Table 1').update(batch);
      console.log(`Updated ${batch.length} records in batch.`);
    }
  };

  // Process creations in batches
  const batchProcessCreations = async (records) => {
    for (let i = 0; i < records.length; i += 10) {
      const batch = records.slice(i, i + 10);
      await base('Table 1').create(batch);
      console.log(`Created ${batch.length} records in batch.`);
    }
  };

  // Update existing records if any
  if (recordsToUpdate.length > 0) {
    await batchProcessUpdates(recordsToUpdate);
    console.log("Automation Update Done");
  }

  // Create new records if any
  if (recordsToCreate.length > 0) {
    await batchProcessCreations(recordsToCreate);
    console.log("Automation Create Done");
  }
}

async function testFilterByFormula() {
  try {
    // Use DATETIME_FORMAT to match the date field correctly
    const matchingRecords = await base('Table 1').select({
      filterByFormula: `AND(
        {Campaign} = 'ACQ_AZ_Gilbert_Search_Brand_85295', 
        DATETIME_FORMAT({Day}, 'YYYY-MM-DD') = '2024-10-31'
      )`
    }).firstPage();

    if (matchingRecords.length > 0) {
      console.log(`Found matching record(s):`);
      matchingRecords.forEach(record => {
        console.log(`Record ID: ${record.id}, Fields:`, record.fields);
      });
    } else {
      console.log(`No matching records found. Logging all records to verify fields:`);
      
      // Log all records to help debug potential mismatches
      const allRecords = await base('Table 1').select().firstPage();
      allRecords.forEach(record => {
        console.log(`Record ID: ${record.id}, Campaign: ${record.fields['Campaign']}, Day: ${record.fields['Day']}`);
      });
    }
  } catch (error) {
    console.error('Error testing filterByFormula:', error);
  }
};

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
  redirectToGoogle,
  handleOAuthCallback,
  fetchTest,
  testFilterByFormula
};
