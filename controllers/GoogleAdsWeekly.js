const Airtable = require('airtable');
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_HISKIN);
const { client } = require('../configs/googleAdsConfig');
const { getStoredRefreshToken } = require('./GoogleAuth');

const refreshToken_Google = getStoredRefreshToken();

if (!refreshToken_Google) {
  throw new Error("Access token is missing. Please authenticate.");
}

const dateRanges = [
  { start: '2024-09-13', end: '2024-09-19' },
  { start: '2024-09-20', end: '2024-09-26' },
  { start: '2024-09-27', end: '2024-10-03' },
  { start: '2024-10-04', end: '2024-10-10' },
  { start: '2024-10-11', end: '2024-10-17' },
  { start: '2024-10-18', end: '2024-10-24' },
];

const getCustomer = () => client.Customer({
  customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
  refresh_token: refreshToken_Google,
  login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
});

const fetchWeeklyData = async (customer, metricsQuery, conversionQuery) => {
  const formattedMetricsMap = {};
  for (const { start, end } of dateRanges) {
    let metricsPageToken = null, conversionPageToken = null;
    do {
      const metricsResponse = await customer.query(metricsQuery(start, end));
      metricsResponse.forEach(campaign => {
        const key = `week-${start}`;
        formattedMetricsMap[key] = formattedMetricsMap[key] || {
          date: `${start} - ${end}`, impressions: 0, clicks: 0, cost: 0, step1Value: 0, step6Value: 0
        };
        formattedMetricsMap[key].impressions += campaign.metrics.impressions;
        formattedMetricsMap[key].clicks += campaign.metrics.clicks;
        formattedMetricsMap[key].cost += campaign.metrics.cost_micros / 1_000_000;
      });
      metricsPageToken = metricsResponse.next_page_token;
    } while (metricsPageToken);

    do {
      const conversionBatchResponse = await customer.query(conversionQuery(start, end));
      conversionBatchResponse.forEach(conversion => {
        const key = `week-${start}`;
        if (formattedMetricsMap[key]) {
          if (conversion.conversion_action.name === 'Book Now - Step 1: Email Signup') {
            formattedMetricsMap[key].step1Value += conversion.metrics.all_conversions;
          } else if (conversion.conversion_action.name === 'Book Now - Step 6: Booking Confirmation') {
            formattedMetricsMap[key].step6Value += conversion.metrics.all_conversions;
          }
        }
      });
      conversionPageToken = conversionBatchResponse.next_page_token;
    } while (conversionPageToken);
  }
  return Object.values(formattedMetricsMap);
};

const sendToAirtable = async (data, tableName, field) => {
  for (const record of data) {
    const dateField = record.date;
    const recordDate = dateField.split(' - ')[0];
    try {
      const existingRecords = await base(tableName).select({
        filterByFormula: `{${field}} = '${dateField}'`
      }).firstPage();
      const recordFields = {
        [field]: dateField, 
        'Impr.': record.impressions, 
        'Clicks': record.clicks,
        'Cost': record.cost, 
        'Book Now - Step 1: Email Signup': record.step1Value,
        'Book Now - Step 6: Booking Confirmation': record.step6Value
      };
      if (existingRecords.length > 0) {
        await base(tableName).update(existingRecords[0].id, recordFields);
        console.log(`Record updated for Date: ${recordDate} Table: ${tableName}`);
      } else {
        await base(tableName).create(recordFields);
        console.log(`Record created for Date: ${recordDate} Table: ${tableName}`);
      }
    } catch (error) {
      console.error(`Error processing record for Date: ${recordDate}`, error);
    }
  }
};

const fetchReportDataWeekly = async (req, res) => {
  try {
    const customer = getCustomer();
    const metricsQuery = (start, end) => `
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
        segments.date BETWEEN '${start}' AND '${end}' 
      ORDER BY 
        segments.date DESC`;
    const conversionQuery = (start, end) => `
      SELECT 
        conversion_action.name, 
        metrics.all_conversions, 
        segments.date 
      FROM 
        conversion_action WHERE segments.date BETWEEN '${start}' AND '${end}' 
        AND conversion_action.name IN ('Book Now - Step 1: Email Signup', 'Book Now - Step 6: Booking Confirmation') 
      ORDER BY 
        segments.date DESC 
      LIMIT 100`;
    const data = await fetchWeeklyData(customer, metricsQuery, conversionQuery);
    await sendToAirtable(data, 'All Weekly Report', 'All Search');
    return data;
    // res.json(data);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
};

const fetchReportDataWeeklyBrand = async (req, res) => {
  try {
    const customer = getCustomer();
    const metricsQuery = (start, end) => `
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
        segments.date BETWEEN '${start}' AND '${end}' 
        AND campaign.name LIKE '%Brand%' 
      ORDER BY 
        segments.date DESC`;
    const conversionQuery = (start, end) => `
      SELECT 
        conversion_action.name, 
        metrics.all_conversions, 
        segments.date 
      FROM 
        conversion_action 
      WHERE 
        segments.date BETWEEN '${start}' AND '${end}' 
        AND conversion_action.name IN ('Book Now - Step 1: Email Signup', 'Book Now - Step 6: Booking Confirmation') 
      ORDER BY 
        segments.date DESC 
      LIMIT 100`;
    const data = await fetchWeeklyData(customer, metricsQuery, conversionQuery);
    await sendToAirtable(data, 'Brand Weekly Report', 'Brand');
    return data;
    // res.json(data);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
};

const fetchReportDataWeeklyNB = async (req, res) => {
  try {
    const customer = getCustomer();
    const metricsQuery = (start, end) => `
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
        segments.date BETWEEN '${start}' AND '${end}' 
        AND campaign.name LIKE '%NB%' 
      ORDER BY 
        segments.date DESC`;
    const conversionQuery = (start, end) => `
      SELECT 
        conversion_action.name, 
        metrics.all_conversions, 
        segments.date 
      FROM 
        conversion_action 
      WHERE 
        segments.date BETWEEN '${start}' AND '${end}' 
        AND conversion_action.name IN ('Book Now - Step 1: Email Signup', 'Book Now - Step 6: Booking Confirmation') 
      ORDER BY 
        segments.date DESC 
      LIMIT 100`;
    const data = await fetchWeeklyData(customer, metricsQuery, conversionQuery);
    await sendToAirtable(data, 'NB Weekly Report', 'No Brand');
    return data;
    // res.json(data);
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).send('Error fetching report data');
  }
};

const sendFinalReportToAirtable = async () => {
  try {
    const weeklyData = await fetchReportDataWeekly();
    const brandData = await fetchReportDataWeeklyBrand();
    const noBrandData = await fetchReportDataWeeklyNB();

    const records = [];

    records.push({ fields: { Week: 'All Search' } });
    weeklyData.forEach(record => {
      records.push({
        fields: {
          'Week': record.date,
          'Impr.': record.impressions,
          'Clicks': record.clicks,
          'Cost': record.cost,
          'Book Now - Step 1: Email Signup': record.step1Value,
          'Book Now - Step 6: Booking Confirmation': record.step6Value,
        }
      });
    });

    records.push({ fields: {} });
    records.push({ fields: { Week: 'Brand' } });

    brandData.forEach(record => {
      records.push({
        fields: {
          'Week': record.date,
          'Impr.': record.impressions,
          'Clicks': record.clicks,
          'Cost': record.cost,
          'Book Now - Step 1: Email Signup': record.step1Value,
          'Book Now - Step 6: Booking Confirmation': record.step6Value,
        }
      });
    });

    records.push({ fields: {} });
    records.push({ fields: { Week: 'No Brand' } });

    noBrandData.forEach(record => {
      records.push({
        fields: {
          'Week': record.date,
          'Impr.': record.impressions,
          'Clicks': record.clicks,
          'Cost': record.cost,
          'Book Now - Step 1: Email Signup': record.step1Value,
          'Book Now - Step 6: Booking Confirmation': record.step6Value,
        }
      });
    });

    const batchSize = 10;
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      await base('Final Report').create(batch);
      console.log(`Batch of ${batch.length} records sent to Airtable successfully!`);
    }

    console.log('Final report sent to Airtable successfully!');
  } catch (error) {
    console.error('Error sending final report to Airtable:', error);
  }
};

const testFetchWeekly = async (req, res) => {
  res.json(await fetchWeeklyData(getCustomer(), req.query.metricsQuery, req.query.conversionQuery));
};

module.exports = {
  fetchReportDataWeekly,
  fetchReportDataWeeklyBrand,
  fetchReportDataWeeklyNB,
  sendFinalReportToAirtable,
  testFetchWeekly
};
