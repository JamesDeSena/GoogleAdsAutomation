const schedule = require("node-schedule");
const Airtable = require("airtable");
const { google } = require('googleapis');
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_HISKIN
);
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDateRanges = null;

const generateWeeklyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentStartDate = new Date(startDate);

  const adjustedEndDate = new Date(endDate);
  const daysToSunday = (7 - adjustedEndDate.getDay()) % 7;
  adjustedEndDate.setDate(adjustedEndDate.getDate() + daysToSunday);

  while (currentStartDate <= adjustedEndDate) {
    let currentEndDate = new Date(currentStartDate);
    currentEndDate.setDate(currentStartDate.getDate() + 6);

    dateRanges.push({
      start: currentStartDate.toISOString().split("T")[0],
      end: currentEndDate.toISOString().split("T")[0],
    });

    currentStartDate.setDate(currentStartDate.getDate() + 7);
  }

  return dateRanges;
};

const getOrGenerateDateRanges = (inputStartDate = null) => {
  const today = new Date();
  const dayOfWeek = today.getDay();
  const daysSinceLast = (dayOfWeek + 6) % 7; //Friday (dayOfWeek + 1) % 7; Monday (dayOfWeek + 6) % 7;

  const previousLast = new Date(today);
  previousLast.setDate(today.getDate() - daysSinceLast);

  const currentDay = new Date(previousLast);
  currentDay.setDate(previousLast.getDate() + 6);

  const startDate = '2024-11-11'; //previousFriday 2024-09-13 / 11-11
  // const fixedEndDate = '2024-11-07'; // currentDay

  const endDate = currentDay; //new Date(fixedEndDate); //currentDay;

  if (inputStartDate) {
    return generateWeeklyDateRanges(inputStartDate, new Date(new Date(inputStartDate).setDate(new Date(inputStartDate).getDate() + 6)));
  } else {
    if (
      !storedDateRanges ||
      new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate
    ) {
      storedDateRanges = generateWeeklyDateRanges(startDate, endDate);
    }
    return storedDateRanges;
  }
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const sendToAirtable = async (data, tableName, field) => {
  for (const record of data) {
    const dateField = record.date;
    const recordDate = dateField.split(" - ")[0];
    try {
      const existingRecords = await base(tableName)
        .select({
          filterByFormula: `{${field}} = '${dateField}'`,
        })
        .firstPage();
      const recordFields = {
        [field]: dateField,
        "Impr.": record.impressions,
        Clicks: record.clicks,
        Spend: record.cost,
        "Book Now - Step 1: Locations": record.step1Value,
        "Book Now - Step 5: Confirm Booking": record.step5Value,
        "Book Now - Step 6: Booking Confirmation": record.step6Value,
        "BookingConfirmed": record.bookingRecord,
        "Purchase": record.purchase
      };
      if (existingRecords.length > 0) {
        await base(tableName).update(existingRecords[0].id, recordFields);
      } else {
        await base(tableName).create(recordFields);
      }

      console.log(`${tableName} sent to Airtable successfully!`);
    } catch (error) {
      console.error(`Error processing record for Date: ${recordDate}`, error);
    }
  }
};

const fetchReportDataWeekly = async (dateRanges) => {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    // const dateRanges = getOrGenerateDateRanges();

    const aggregateDataForWeek = async (startDate, endDate) => {
      const aggregatedData = {
        date: `${startDate} - ${endDate}`,
        impressions: 0,
        clicks: 0,
        cost: 0,
        step1Value: 0,
        step5Value: 0,
        step6Value: 0,
        bookingConfirmed: 0,
        purchase: 0,
      };

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
          segments.date BETWEEN '${startDate}' AND '${endDate}'
        ORDER BY
          segments.date DESC
      `;

      const conversionQuery = `
        SELECT 
          metrics.all_conversions,
          conversion_action.name,
          segments.date 
        FROM 
          conversion_action
        WHERE 
          segments.date BETWEEN '${startDate}' AND '${endDate}'
          AND conversion_action.name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation', 'BookingConfirmed', 'Purchase') 
        ORDER BY 
          segments.date DESC
      `;

      let metricsPageToken = null;
      do {
        const metricsResponse = await customer.query(metricsQuery);
        metricsResponse.forEach((campaign) => {
          aggregatedData.impressions += campaign.metrics.impressions || 0;
          aggregatedData.clicks += campaign.metrics.clicks || 0;
          aggregatedData.cost +=
            (campaign.metrics.cost_micros || 0) / 1_000_000;
        });
        metricsPageToken = metricsResponse.next_page_token;
      } while (metricsPageToken);

      let conversionPageToken = null;
      do {
        const conversionBatchResponse = await customer.query(conversionQuery);
        conversionBatchResponse.forEach((conversion) => {
          const conversionValue = conversion.metrics.all_conversions || 0;
          if (conversion.conversion_action.name === "Book Now - Step 1: Locations") {
            aggregatedData.step1Value += conversionValue;
          } else if (conversion.conversion_action.name === "Book Now - Step 5:Confirm Booking (Initiate Checkout)") {
            aggregatedData.step5Value += conversionValue;
          } else if (conversion.conversion_action.name === "Book Now - Step 6: Booking Confirmation") {
            aggregatedData.step6Value += conversionValue;
          } else if (conversion.conversion_action.name === "BookingConfirmed") {
            aggregatedData.bookingConfirmed += conversionValue;
          } else if (conversion.conversion_action.name === "Purchase") {
            aggregatedData.purchase += conversionValue;
          }
        });
        conversionPageToken = conversionBatchResponse.next_page_token;
      } while (conversionPageToken);

      return aggregatedData;
    };

    const allWeeklyData = [];
    for (const { start, end } of dateRanges) {
      const weeklyData = await aggregateDataForWeek(start, end);
      allWeeklyData.push(weeklyData);
    }

    // await sendToAirtable(allWeeklyData, "All Weekly Report", "All Search");
    return allWeeklyData;

    // res.json(allWeeklyData);
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const aggregateDataForWeek = async (
  customer,
  startDate,
  endDate,
  campaignNameFilter
) => {
  const aggregatedData = {
    date: `${startDate} - ${endDate}`,
    impressions: 0,
    clicks: 0,
    cost: 0,
    step1Value: 0,
    step5Value: 0,
    step6Value: 0,
    bookingConfirmed: 0,
    purchase: 0,
  };

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
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name LIKE '%${campaignNameFilter}%'
    ORDER BY
      segments.date DESC
  `;

  const conversionQuery = `
    SELECT 
      campaign.id,
      metrics.all_conversions,
      segments.conversion_action_name,
      segments.date 
    FROM 
      campaign
    WHERE 
      segments.date BETWEEN '${startDate}' AND '${endDate}'
      AND campaign.name LIKE '%${campaignNameFilter}%'
      AND segments.conversion_action_name IN ('Book Now - Step 1: Locations', 'Book Now - Step 5:Confirm Booking (Initiate Checkout)', 'Book Now - Step 6: Booking Confirmation', 'BookingConfirmed', 'Purchase')
    ORDER BY 
      segments.date DESC
  `;

  let metricsPageToken = null;
  do {
    const metricsResponse = await customer.query(metricsQuery);
    metricsResponse.forEach((campaign) => {
      aggregatedData.impressions += campaign.metrics.impressions || 0;
      aggregatedData.clicks += campaign.metrics.clicks || 0;
      aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  let conversionPageToken = null;
  do {
    const conversionBatchResponse = await customer.query(conversionQuery);
    conversionBatchResponse.forEach((conversion) => {
      const conversionValue = conversion.metrics.all_conversions || 0;
      if (conversion.segments.conversion_action_name === "Book Now - Step 1: Locations") {
        aggregatedData.step1Value += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 5:Confirm Booking (Initiate Checkout)") {
        aggregatedData.step5Value += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Book Now - Step 6: Booking Confirmation") {
        aggregatedData.step6Value += conversionValue;
      } else if (conversion.segments.conversion_action_name === "BookingConfirmed") {
        aggregatedData.bookingConfirmed += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Purchase") {
        aggregatedData.purchase += conversionValue;
      }
    });
    conversionPageToken = conversionBatchResponse.next_page_token;
  } while (conversionPageToken);

  return aggregatedData;
};

const fetchReportDataWeeklyFilter = async (req, res, campaignNameFilter, reportName, dateRanges) => {
  const refreshToken_Google = getStoredRefreshToken();

  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_HISKIN,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });
    
    // const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForWeek(customer, start, end, campaignNameFilter);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    // if (campaignNameFilter === "Brand" || campaignNameFilter === "NB") {
    //   await sendToAirtable(allWeeklyData, `${reportName} Weekly Report`, campaignNameFilter);
    // }

    return allWeeklyData;

  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const fetchReportDataWeeklyBrand = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Brand", "Brand", dateRanges);
};

const fetchReportDataWeeklyNB = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "NB", "NB", dateRanges);
};

const fetchReportDataWeeklyGilbert = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Gilbert", "Gilbert", dateRanges);
};

const fetchReportDataWeeklyMKT = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "MKT", "MKT", dateRanges);
};

const fetchReportDataWeeklyPhoenix = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Phoenix", "Phoenix", dateRanges);
};

const fetchReportDataWeeklyScottsdale = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Scottsdale", "Scottsdale", dateRanges);
};

const fetchReportDataWeeklyUptownPark = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Uptown", "Uptown", dateRanges);
};

const fetchReportDataWeeklyMontrose = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Montrose", "Montrose", dateRanges);
};

const fetchReportDataWeeklyRiceVillage = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "RiceVillage", "RiceVillage", dateRanges);
};

const fetchReportDataWeeklyMosaic = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "Mosaic", "Mosaic", dateRanges);
};

const fetchReportDataWeekly14thSt = (req, res, dateRanges) => {
  return fetchReportDataWeeklyFilter(req, res, "14thSt", "14thSt", dateRanges);
};

const sendFinalWeeklyReportToAirtable = async (req, res) => {
  try {
    const date = req?.params?.date;

    const dateRanges = getOrGenerateDateRanges(date);

    const weeklyData = await fetchReportDataWeekly(dateRanges);
    const brandData = await fetchReportDataWeeklyBrand(req, res, dateRanges);
    const noBrandData = await fetchReportDataWeeklyNB(req, res, dateRanges);
    const gilbertData = await fetchReportDataWeeklyGilbert(req, res, dateRanges);
    const mktData = await fetchReportDataWeeklyMKT(req, res, dateRanges);
    const phoenixData = await fetchReportDataWeeklyPhoenix(req, res, dateRanges);
    const scottsdaleData = await fetchReportDataWeeklyScottsdale(req, res, dateRanges);
    const uptownParkData = await fetchReportDataWeeklyUptownPark(req, res, dateRanges);
    const montroseData = await fetchReportDataWeeklyMontrose(req, res, dateRanges);
    const riceVillageData = await fetchReportDataWeeklyRiceVillage(req, res, dateRanges);
    const mosaicData = await fetchReportDataWeeklyMosaic(req, res, dateRanges);
    const fourteenthStData = await fetchReportDataWeekly14thSt(req, res, dateRanges);

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const addWoWVariance = (lastRecord, secondToLastRecord, filter) => {
      records.push({
        fields: {
          Week: "Wow Variance %",
          Filter: filter,
          "Impr. Raw": calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions),
          'Clicks Raw': calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks),
          'Cost Raw': calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost),
          "Book Now - Step 1: Locations Raw": calculateWoWVariance(lastRecord.step1Value, secondToLastRecord.step1Value),
          "Book Now - Step 5: Confirm Booking Raw": calculateWoWVariance(lastRecord.step5Value, secondToLastRecord.step5Value),
          "Book Now - Step 6: Booking Confirmation Raw": calculateWoWVariance(lastRecord.step6Value, secondToLastRecord.step6Value),
          "CPC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks),
          "CTR Raw": calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions),
          "Step 1 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step1Value, secondToLastRecord.cost / secondToLastRecord.step1Value),
          "Step 5 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step5Value, secondToLastRecord.cost / secondToLastRecord.step5Value),
          "Step 6 CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.step6Value, secondToLastRecord.cost / secondToLastRecord.step6Value),
          "Step 1 Conv Rate Raw": calculateWoWVariance(lastRecord.step1Value / lastRecord.clicks, secondToLastRecord.step1Value / secondToLastRecord.clicks),
          "Step 5 Conv Rate Raw": calculateWoWVariance(lastRecord.step5Value / lastRecord.clicks, secondToLastRecord.step5Value / secondToLastRecord.clicks),
          "Step 6 Conv Rate Raw": calculateWoWVariance(lastRecord.step6Value / lastRecord.clicks, secondToLastRecord.step6Value / secondToLastRecord.clicks),
          "Booking Confirmed Raw": calculateWoWVariance(lastRecord.bookingConfirmed, secondToLastRecord.bookingConfirmed),
          "Booking CAC Raw": calculateWoWVariance(lastRecord.cost / lastRecord.bookingConfirmed, secondToLastRecord.cost / secondToLastRecord.bookingConfirmed),
          "Booking Conv Rate Raw": calculateWoWVariance(lastRecord.bookingConfirmed / lastRecord.clicks, secondToLastRecord.bookingConfirmed / secondToLastRecord.clicks),
          "Purchase Raw": calculateWoWVariance(lastRecord.purchase, secondToLastRecord.purchase),
        },
      });
    };

    const addDataToRecords = (data, filter) => {
      data.forEach((record) => {
        records.push({
          fields: {
            Week: record.date,
            Filter: filter,
            "Impr. Raw": record.impressions,
            'Clicks Raw': record.clicks,
            'Cost Raw': record.cost,
            "Book Now - Step 1: Locations Raw": record.step1Value,
            "Book Now - Step 5: Confirm Booking Raw": record.step5Value,
            "Book Now - Step 6: Booking Confirmation Raw": record.step6Value,
            "CPC Raw": record.cost / record.clicks,
            "CTR Raw": (record.clicks / record.impressions) * 100,
            "Step 1 CAC Raw": record.cost / record.step1Value,
            "Step 5 CAC Raw": record.cost / record.step5Value,
            "Step 6 CAC Raw": record.cost / record.step6Value,
            "Step 1 Conv Rate Raw": (record.step1Value / record.clicks) * 100,
            "Step 5 Conv Rate Raw": (record.step5Value / record.clicks) * 100,
            "Step 6 Conv Rate Raw": (record.step6Value / record.clicks) * 100,
            "Booking Confirmed Raw": record.bookingConfirmed,
            "Booking CAC Raw": record.cost / record.bookingConfirmed,
            "Booking Conv Rate Raw": (record.bookingConfirmed / record.clicks) * 100,
            "Purchase Raw": record.purchase,
          },
        });
      });
    };

    addDataToRecords(weeklyData, "1 - All Search");
    addDataToRecords(brandData, "2 - Brand");
    addDataToRecords(noBrandData, "3 - NB");
    addDataToRecords(gilbertData, "4 - Gilbert");
    addDataToRecords(mktData, "5 - MKT");
    addDataToRecords(phoenixData, "6 - Phoenix");
    addDataToRecords(scottsdaleData, "7 - Scottsdale");
    addDataToRecords(uptownParkData, "8 - UptownPark");
    addDataToRecords(montroseData, "9 - Montrose");
    addDataToRecords(riceVillageData, "10 - RiceVillage");
    addDataToRecords(mosaicData, "11 - Mosaic");
    addDataToRecords(fourteenthStData, "12 - 14thSt");

    if (!date || date.trim() === '') {
      addWoWVariance(weeklyData.slice(-2)[0], weeklyData.slice(-3)[0], "1 - All Search");
      addWoWVariance(brandData.slice(-2)[0], brandData.slice(-3)[0], "2 - Brand");
      addWoWVariance(noBrandData.slice(-2)[0], noBrandData.slice(-3)[0], "3 - NB");
      addWoWVariance(gilbertData.slice(-2)[0], gilbertData.slice(-3)[0], "4 - Gilbert");
      addWoWVariance(mktData.slice(-2)[0], mktData.slice(-3)[0], "5 - MKT");
      addWoWVariance(phoenixData.slice(-2)[0], phoenixData.slice(-3)[0], "6 - Phoenix");
      addWoWVariance(scottsdaleData.slice(-2)[0], scottsdaleData.slice(-3)[0], "7 - Scottsdale");
      addWoWVariance(uptownParkData.slice(-2)[0], uptownParkData.slice(-3)[0], "8 - UptownPark");
      addWoWVariance(montroseData.slice(-2)[0], montroseData.slice(-3)[0], "9 - Montrose");
      addWoWVariance(riceVillageData.slice(-2)[0], riceVillageData.slice(-3)[0], "10 - RiceVillage");
      addWoWVariance(mosaicData.slice(-2)[0], mosaicData.slice(-3)[0], "11 - Mosaic");
      addWoWVariance(fourteenthStData.slice(-2)[0], fourteenthStData.slice(-3)[0], "12 - 14thSt");
    }

    const table = base("Final Report");
    const existingRecords = await table.select().all();

    const recordExists = (week, filter) => {
      return existingRecords.some(record =>
        record.fields.Week === week && record.fields.Filter === filter
      );
    };

    const updateRecord = async (id, fields) => {
      await table.update(id, fields);
    };

    const createNewRecord = async (fields) => {
      await table.create([{ fields }]);
    };

    const isDataEqual = (existingRecord, newRecordFields) => {
      return (
        existingRecord.fields["Impr. Raw"] === newRecordFields["Impr. Raw"] &&
        existingRecord.fields["Clicks Raw"] === newRecordFields["Clicks Raw"] &&
        existingRecord.fields["Cost Raw"] === newRecordFields["Cost Raw"] &&
        existingRecord.fields["Book Now - Step 1: Locations Raw"] === newRecordFields["Book Now - Step 1: Locations Raw"] &&
        existingRecord.fields["Book Now - Step 5: Confirm Booking Raw"] === newRecordFields["Book Now - Step 5: Confirm Booking Raw"] &&
        existingRecord.fields["Book Now - Step 6: Booking Confirmation Raw"] === newRecordFields["Book Now - Step 6: Booking Confirmation Raw"] &&
        existingRecord.fields["CPC Raw"] === newRecordFields["CPC Raw"] &&
        existingRecord.fields["CTR Raw"] === newRecordFields["CTR Raw"] &&
        existingRecord.fields["Step 1 CAC Raw"] === newRecordFields["Step 1 CAC Raw"] &&
        existingRecord.fields["Step 5 CAC Raw"] === newRecordFields["Step 5 CAC Raw"] &&
        existingRecord.fields["Step 6 CAC Raw"] === newRecordFields["Step 6 CAC Raw"] &&
        existingRecord.fields["Step 1 Conv Rate Raw"] === newRecordFields["Step 1 Conv Rate Raw"] &&
        existingRecord.fields["Step 5 Conv Rate Raw"] === newRecordFields["Step 5 Conv Rate Raw"] &&
        existingRecord.fields["Step 6 Conv Rate Raw"] === newRecordFields["Step 6 Conv Rate Raw"] &&
        existingRecord.fields["Booking Confirmed Raw"] === newRecordFields["Booking Confirmed Raw"] &&
        existingRecord.fields["Booking CAC Raw"] === newRecordFields["Booking CAC Raw"] &&
        existingRecord.fields["Booking Conv Rate Raw"] === newRecordFields["Booking Conv Rate Raw"] &&
        existingRecord.fields["Purchase Raw"] === newRecordFields["Purchase Raw"]
      );
    };

    for (const record of records) {
      const exists = recordExists(record.fields.Week, record.fields.Filter);

      if (exists) {
        const existingRecord = existingRecords.find(r => r.fields.Week === record.fields.Week && r.fields.Filter === record.fields.Filter);

        if (!isDataEqual(existingRecord, record.fields)) {
          await updateRecord(existingRecord.id, record.fields);
        }
      } else {
        await createNewRecord(record.fields);
      }
    }

    console.log("Process completed successfully. Records updated and/or created.");

    // const deletePromises = existingRecords.map(record => table.destroy(record.id));
    // await Promise.all(deletePromises);

    // const batchSize = 10;
    // for (let i = 0; i < records.length; i += batchSize) {
    //   const batch = records.slice(i, i + batchSize);
    //   await table.create(batch);
    //   console.log(`Batch of ${batch.length} records sent to Airtable successfully!`);
    // }

    console.log("Final weekly report sent to Airtable successfully!");
  } catch (error) {
    console.error("Error sending final report to Airtable:", error);
  }
};

const sendFinalWeeklyReportToGoogleSheets = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.HI_SKIN_SPREADSHEET;
  const headerRange = 'Live!A1:U1';
  const dataRange = 'Live!A2:U';
  const dataRangeRaw = 'Raw!A2:U'; 

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const weeklyData = await fetchReportDataWeekly(dateRanges);
    const brandData = await fetchReportDataWeeklyBrand(req, res, dateRanges);
    const noBrandData = await fetchReportDataWeeklyNB(req, res, dateRanges);
    const gilbertData = await fetchReportDataWeeklyGilbert(req, res, dateRanges);
    const mktData = await fetchReportDataWeeklyMKT(req, res, dateRanges);
    const phoenixData = await fetchReportDataWeeklyPhoenix(req, res, dateRanges);
    const scottsdaleData = await fetchReportDataWeeklyScottsdale(req, res, dateRanges);
    const uptownParkData = await fetchReportDataWeeklyUptownPark(req, res, dateRanges);
    const montroseData = await fetchReportDataWeeklyMontrose(req, res, dateRanges);
    const riceVillageData = await fetchReportDataWeeklyRiceVillage(req, res, dateRanges);
    const mosaicData = await fetchReportDataWeeklyMosaic(req, res, dateRanges);
    const fourteenthStData = await fetchReportDataWeekly14thSt(req, res, dateRanges);

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2); // 2 decimals if not a whole number

    const addWoWVariance = (lastRecord, secondToLastRecord, filter, filter2) => {
      records.push({
        Week: "WoW Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions)),
        'Clicks': formatPercentage(calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks)),
        'Cost': formatPercentage(calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost)),
        "Book Now - Step 1: Locations": formatPercentage(calculateWoWVariance(lastRecord.step1Value, secondToLastRecord.step1Value)),
        "Book Now - Step 5: Confirm Booking": formatPercentage(calculateWoWVariance(lastRecord.step5Value, secondToLastRecord.step5Value)),
        "Book Now - Step 6: Booking Confirmation": formatPercentage(calculateWoWVariance(lastRecord.step6Value, secondToLastRecord.step6Value)),
        "CPC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
        "Step 1 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step1Value, secondToLastRecord.cost / secondToLastRecord.step1Value)),
        "Step 5 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step5Value, secondToLastRecord.cost / secondToLastRecord.step5Value)),
        "Step 6 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step6Value, secondToLastRecord.cost / secondToLastRecord.step6Value)),
        "Step 1 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step1Value / lastRecord.clicks, secondToLastRecord.step1Value / secondToLastRecord.clicks)),
        "Step 5 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step5Value / lastRecord.clicks, secondToLastRecord.step5Value / secondToLastRecord.clicks)),
        "Step 6 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step6Value / lastRecord.clicks, secondToLastRecord.step6Value / secondToLastRecord.clicks)),
        "Booking Confirmed": formatPercentage(calculateWoWVariance(lastRecord.bookingConfirmed, secondToLastRecord.bookingConfirmed)),
        "Booking CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.bookingConfirmed, secondToLastRecord.cost / secondToLastRecord.bookingConfirmed)),
        "Booking Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.bookingConfirmed / lastRecord.clicks, secondToLastRecord.bookingConfirmed / secondToLastRecord.clicks)),
        "Purchase": formatPercentage(calculateWoWVariance(lastRecord.purchase, secondToLastRecord.purchase)),
      });
    };

    const addDataToRecords = (data, filter, filter2) => {
      data.forEach((record) => {
        records.push({
          Week: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          'Clicks': formatNumber(record.clicks),
          'Cost': formatCurrency(record.cost),
          "Book Now - Step 1: Locations": formatNumber(record.step1Value),
          "Book Now - Step 5: Confirm Booking": formatNumber(record.step5Value),
          "Book Now - Step 6: Booking Confirmation": formatNumber(record.step6Value),
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          "Step 1 CAC": formatCurrency(record.cost / record.step1Value),
          "Step 5 CAC": formatCurrency(record.cost / record.step5Value),
          "Step 6 CAC": formatCurrency(record.cost / record.step6Value),
          "Step 1 Conv Rate": formatPercentage((record.step1Value / record.clicks) * 100),
          "Step 5 Conv Rate": formatPercentage((record.step5Value / record.clicks) * 100),
          "Step 6 Conv Rate": formatPercentage((record.step6Value / record.clicks) * 100),
          "Booking Confirmed": formatNumber(record.bookingConfirmed),
          "Booking CAC": formatCurrency(record.cost / record.bookingConfirmed),
          "Booking Conv Rate": formatPercentage((record.bookingConfirmed / record.clicks) * 100),
          "Purchase": formatNumber(record.purchase),
        });
      });
    };

    addDataToRecords(weeklyData, "All Search", 1);
    addDataToRecords(brandData, "Brand", 2);
    addDataToRecords(noBrandData, "NB", 3);
    addDataToRecords(gilbertData, "Gilbert", 4);
    addDataToRecords(mktData, "MKT", 5);
    addDataToRecords(phoenixData, "Phoenix", 6);
    addDataToRecords(scottsdaleData, "Scottsdale", 7);
    addDataToRecords(uptownParkData, "UptownPark", 8);
    addDataToRecords(montroseData, "Montrose", 9);
    addDataToRecords(riceVillageData, "RiceVillage", 10);
    addDataToRecords(mosaicData, "Mosaic", 11);
    addDataToRecords(fourteenthStData, "14thSt", 12);

    if (!date || date.trim() === '') {
      addWoWVariance(weeklyData.slice(-2)[0], weeklyData.slice(-3)[0], "All Search", 1);
      addWoWVariance(brandData.slice(-2)[0], brandData.slice(-3)[0], "Brand", 2);
      addWoWVariance(noBrandData.slice(-2)[0], noBrandData.slice(-3)[0], "NB", 3);
      addWoWVariance(gilbertData.slice(-2)[0], gilbertData.slice(-3)[0], "Gilbert", 4);
      addWoWVariance(mktData.slice(-2)[0], mktData.slice(-3)[0], "MKT", 5);
      addWoWVariance(phoenixData.slice(-2)[0], phoenixData.slice(-3)[0], "Phoenix", 6);
      addWoWVariance(scottsdaleData.slice(-2)[0], scottsdaleData.slice(-3)[0], "Scottsdale", 7);
      addWoWVariance(uptownParkData.slice(-2)[0], uptownParkData.slice(-3)[0], "UptownPark", 8);
      addWoWVariance(montroseData.slice(-2)[0], montroseData.slice(-3)[0], "Montrose", 9);
      addWoWVariance(riceVillageData.slice(-2)[0], riceVillageData.slice(-3)[0], "RiceVillage", 10);
      addWoWVariance(mosaicData.slice(-2)[0], mosaicData.slice(-3)[0], "Mosaic", 11);
      addWoWVariance(fourteenthStData.slice(-2)[0], fourteenthStData.slice(-3)[0], "14thSt", 12);
    }

    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
    
      records.forEach((record, index) => {
        // Only process new groups
        if (record.Filter !== currentGroup) {
          // Add the custom header row for this group
          finalRecords.push({
            Week: `${record.Filter}`, // Dynamically use record.Filter as the group name
            Filter: "Filter",
            Filter2: "Filter2",
            "Impr.": "Impr.",
            "Clicks": "Clicks",
            "Cost": "Cost",
            "Book Now - Step 1: Locations": "Book Now - Step 1: Locations",
            "Book Now - Step 5: Confirm Booking": "Book Now - Step 5: Confirm Booking",
            "Book Now - Step 6: Booking Confirmation": "Book Now - Step 6: Booking Confirmation",
            "CPC": "CPC",
            "CTR": "CTR",
            "Step 1 CAC": "Step 1 CAC",
            "Step 5 CAC": "Step 5 CAC",
            "Step 6 CAC": "Step 6 CAC",
            "Step 1 Conv Rate": "Step 1 Conv Rate",
            "Step 5 Conv Rate": "Step 5 Conv Rate",
            "Step 6 Conv Rate": "Step 6 Conv Rate",
            "Booking Confirmed": "Booking Confirmed",
            "Booking CAC": "Booking CAC",
            "Booking Conv Rate": "Booking Conv Rate",
            "Purchase": "Purchase",
            isBold: true,
          });
    
          // Update current group
          currentGroup = record.Filter;
        }
    
        // Add the current record for this group
        finalRecords.push({
          ...record,
          isBold: false,
        });
    
        // Add WoW Variance % row after each group's records
        if (record.Week === "WoW Variance %") {
          finalRecords.push({ Week: "", Filter: "", Filter2: "" , isBold: false});
        }
      });
    }
    
    processGroup(records);

    const sheetData = finalRecords.map(record => [
      record.Week,
      record.Filter,
      record.Filter2,
      record["Impr."], 
      record["Clicks"],
      record["Cost"],
      record["Book Now - Step 1: Locations"],
      record["Book Now - Step 5: Confirm Booking"],
      record["Book Now - Step 6: Booking Confirmation"],
      record["CPC"],
      record["CTR"],
      record["Step 1 CAC"],
      record["Step 5 CAC"],
      record["Step 6 CAC"],
      record["Step 1 Conv Rate"],
      record["Step 5 Conv Rate"],
      record["Step 6 Conv Rate"],
      record["Booking Confirmed"],
      record["Booking CAC"],
      record["Booking Conv Rate"],
      record["Purchase"],
    ]);

    const resource = {
      values: sheetData,
    };
    
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRange,
    });

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRangeRaw,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRange,
      valueInputOption: "RAW",
      resource,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRangeRaw,
      valueInputOption: "RAW",
      resource,
    });

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      resource: {
        requests: [
          {
            repeatCell: {
              range: {
                sheetId: 0,
                startRowIndex: 1,
                endRowIndex: sheetData.length + 1,
                startColumnIndex: 0,
                endColumnIndex: 21,
              },
              cell: {
                userEnteredFormat: {
                  horizontalAlignment: 'RIGHT',
                },
              },
              fields: 'userEnteredFormat.horizontalAlignment',
            },
          },
        ],
      },
    });

    console.log("Final weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  fetchReportDataWeekly,
  fetchReportDataWeeklyBrand,
  fetchReportDataWeeklyNB,
  sendFinalWeeklyReportToAirtable,
  sendFinalWeeklyReportToGoogleSheets
};
