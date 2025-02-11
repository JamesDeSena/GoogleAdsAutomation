const { google } = require('googleapis');
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

const fetchReportDataWeeklyCampaignHS = async (dateRanges) => {
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
          campaign.id,
          metrics.all_conversions,
          segments.conversion_action_name,
          segments.date 
        FROM 
          campaign
        WHERE 
          segments.date BETWEEN '${startDate}' AND '${endDate}'
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

const fetchReportDataWeeklySearchHS = async (dateRanges) => {
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
          AND campaign.name LIKE '%Search%'
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
          AND campaign.name LIKE '%Search%'
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

    const allSearchWeeklyData = [];
    for (const { start, end } of dateRanges) {
      const weeklySearchData = await aggregateDataForWeek(start, end);
      allSearchWeeklyData.push(weeklySearchData);
    }

    return allSearchWeeklyData;

    // res.json(allSearchWeeklyData);
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const aggregateDataForWeek = async (customer, startDate, endDate, campaignNameFilter, brandNBFilter) => {
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
      AND campaign.name LIKE '%${campaignNameFilter}%' AND campaign.name LIKE '%${brandNBFilter}%'
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
      AND campaign.name LIKE '%${campaignNameFilter}%' AND campaign.name LIKE '%${brandNBFilter}%'
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

const fetchReportDataWeeklyHSFilter = async (req, res, campaignNameFilter, reportName, brandNBFilter, dateRanges) => {
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
      return aggregateDataForWeek(customer, start, end, campaignNameFilter, brandNBFilter);
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

const createFetchFunction = (campaignNameFilter, reportName, brandNBFilter = "") => {
  return (req, res, dateRanges) => fetchReportDataWeeklyHSFilter(req, res, campaignNameFilter, reportName, brandNBFilter, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklyHSBrand: createFetchFunction("Brand", "Brand"),
  fetchReportDataWeeklyHSNB: createFetchFunction("NB", "NB"),
  fetchReportDataWeeklyHSPmax: createFetchFunction("Pmax", "Pmax"),
  fetchReportDataWeeklyHSGilbert: createFetchFunction("Gilbert", "Gilbert"),
  fetchReportDataWeeklyHSGilbertBrand: createFetchFunction("Gilbert", "Gilbert", "Brand"),
  fetchReportDataWeeklyHSGilbertNB: createFetchFunction("Gilbert", "Gilbert", "NB"),
  fetchReportDataWeeklyHSMKT: createFetchFunction("MKT", "MKT"),
  fetchReportDataWeeklyHSMKTBrand: createFetchFunction("MKT", "MKT", "Brand"),
  fetchReportDataWeeklyHSMKTNB: createFetchFunction("MKT", "MKT", "NB"),
  fetchReportDataWeeklyHSPhoenix: createFetchFunction("Phoenix", "Phoenix"),
  fetchReportDataWeeklyHSPhoenixBrand: createFetchFunction("Phoenix", "Phoenix", "Brand"),
  fetchReportDataWeeklyHSPhoenixNB: createFetchFunction("Phoenix", "Phoenix", "NB"),
  fetchReportDataWeeklyHSScottsdale: createFetchFunction("Scottsdale", "Scottsdale"),
  fetchReportDataWeeklyHSScottsdaleBrand: createFetchFunction("Scottsdale", "Scottsdale", "Brand"),
  fetchReportDataWeeklyHSScottsdaleNB: createFetchFunction("Scottsdale", "Scottsdale", "NB"),
  fetchReportDataWeeklyHSUptownPark: createFetchFunction("Uptown", "Uptown"),
  fetchReportDataWeeklyHSUptownParkBrand: createFetchFunction("Uptown", "Uptown", "Brand"),
  fetchReportDataWeeklyHSUptownParkNB: createFetchFunction("Uptown", "Uptown", "NB"),
  fetchReportDataWeeklyHSMontrose: createFetchFunction("Montrose", "Montrose"),
  fetchReportDataWeeklyHSMontroseBrand: createFetchFunction("Montrose", "Montrose", "Brand"),
  fetchReportDataWeeklyHSMontroseNB: createFetchFunction("Montrose", "Montrose", "NB"),
  fetchReportDataWeeklyHSRiceVillage: createFetchFunction("RiceVillage", "RiceVillage"),
  fetchReportDataWeeklyHSRiceVillageBrand: createFetchFunction("RiceVillage", "RiceVillage", "Brand"),
  fetchReportDataWeeklyHSRiceVillageNB: createFetchFunction("RiceVillage", "RiceVillage", "NB"),
  fetchReportDataWeeklyHSMosaic: createFetchFunction("Mosaic", "Mosaic"),
  fetchReportDataWeeklyHSMosaicBrand: createFetchFunction("Mosaic", "Mosaic", "Brand"),
  fetchReportDataWeeklyHSMosaicNB: createFetchFunction("Mosaic", "Mosaic", "NB"),
  fetchReportDataWeeklyHS14thSt: createFetchFunction("14thSt", "14thSt"),
  fetchReportDataWeeklyHS14thStBrand: createFetchFunction("14thSt", "14thSt", "Brand"),
  fetchReportDataWeeklyHS14thStNB: createFetchFunction("14thSt", "14thSt", "NB"),
};

const executeSpecificFetchFunctionHS = async (req, res, dateRanges) => {
  const functionName = "fetchReportDataWeeklyHSGilbertNB";
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

const sendFinalWeeklyReportToGoogleSheetsHS = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.HI_SKIN_SPREADSHEET;
  const dataRanges = {
    Live: 'Live View!A2:U',
    AllBNB: 'Reporting Overview!A2:U',
    Gilbert: 'Gilbert!A2:U',
    MKT: 'MKT!A2:U',
    Phoenix: 'Phoenix!A2:U',
    Scottsdale: 'Scottsdale!A2:U',
    UptownPark: 'UptownPark!A2:U',
    Montrose: 'Montrose!A2:U',
    RiceVillage: 'RiceVillage!A2:U',
    Mosaic: 'Mosaic!A2:U',
    FourteenthSt: '14thSt!A2:U',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const weeklyCampaignData = await fetchReportDataWeeklyCampaignHS(dateRanges);
    const weeklySearchData = await fetchReportDataWeeklySearchHS(dateRanges);
    const brandData = await fetchFunctions.fetchReportDataWeeklyHSBrand(req, res, dateRanges);
    const noBrandData = await fetchFunctions.fetchReportDataWeeklyHSNB(req, res, dateRanges);
    const pmaxData = await fetchFunctions.fetchReportDataWeeklyHSPmax(req, res, dateRanges);
    const gilbertData = await fetchFunctions.fetchReportDataWeeklyHSGilbert(req, res, dateRanges);
    const gilbertDataBrand = await fetchFunctions.fetchReportDataWeeklyHSGilbertBrand(req, res, dateRanges);
    const gilbertDataNB = await fetchFunctions.fetchReportDataWeeklyHSGilbertNB(req, res, dateRanges);
    const mktData = await fetchFunctions.fetchReportDataWeeklyHSMKT(req, res, dateRanges);
    const mktDataBrand = await fetchFunctions.fetchReportDataWeeklyHSMKTBrand(req, res, dateRanges);
    const mktDataNB = await fetchFunctions.fetchReportDataWeeklyHSMKTNB(req, res, dateRanges);
    const phoenixData = await fetchFunctions.fetchReportDataWeeklyHSPhoenix(req, res, dateRanges);
    const phoenixDataBrand = await fetchFunctions.fetchReportDataWeeklyHSPhoenixBrand(req, res, dateRanges);
    const phoenixDataNB = await fetchFunctions.fetchReportDataWeeklyHSPhoenixNB(req, res, dateRanges);
    const scottsdaleData = await fetchFunctions.fetchReportDataWeeklyHSScottsdale(req, res, dateRanges);
    const scottsdaleDataBrand = await fetchFunctions.fetchReportDataWeeklyHSScottsdaleBrand(req, res, dateRanges);
    const scottsdaleDataNB = await fetchFunctions.fetchReportDataWeeklyHSScottsdaleNB(req, res, dateRanges);
    const uptownParkData = await fetchFunctions.fetchReportDataWeeklyHSUptownPark(req, res, dateRanges);
    const uptownParkDataBrand = await fetchFunctions.fetchReportDataWeeklyHSUptownParkBrand(req, res, dateRanges);
    const uptownParkDataNB = await fetchFunctions.fetchReportDataWeeklyHSUptownParkNB(req, res, dateRanges);
    const montroseData = await fetchFunctions.fetchReportDataWeeklyHSMontrose(req, res, dateRanges);
    const montroseDataBrand = await fetchFunctions.fetchReportDataWeeklyHSMontroseBrand(req, res, dateRanges);
    const montroseDataNB = await fetchFunctions.fetchReportDataWeeklyHSMontroseNB(req, res, dateRanges);
    const riceVillageData = await fetchFunctions.fetchReportDataWeeklyHSRiceVillage(req, res, dateRanges);
    const riceVillageDataBrand = await fetchFunctions.fetchReportDataWeeklyHSRiceVillageBrand(req, res, dateRanges);
    const riceVillageDataNB = await fetchFunctions.fetchReportDataWeeklyHSRiceVillageNB(req, res, dateRanges);
    const mosaicData = await fetchFunctions.fetchReportDataWeeklyHSMosaic(req, res, dateRanges);
    const mosaicDataBrand = await fetchFunctions.fetchReportDataWeeklyHSMosaicBrand(req, res, dateRanges);
    const mosaicDataNB = await fetchFunctions.fetchReportDataWeeklyHSMosaicNB(req, res, dateRanges);
    const fourteenthStData = await fetchFunctions.fetchReportDataWeeklyHS14thSt(req, res, dateRanges);
    const fourteenthStDataBrand = await fetchFunctions.fetchReportDataWeeklyHS14thStBrand(req, res, dateRanges);
    const fourteenthStDataNB = await fetchFunctions.fetchReportDataWeeklyHS14thStNB(req, res, dateRanges);

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

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

    addDataToRecords(weeklyCampaignData, "All Campaign", 1);
    addDataToRecords(weeklySearchData, "All Search", 2);
    addDataToRecords(brandData, "Brand Search", 3);
    addDataToRecords(noBrandData, "NB Search", 4);
    addDataToRecords(pmaxData, "Pmax", 5);
    addDataToRecords(gilbertData, "Gilbert", 6);
    addDataToRecords(gilbertDataBrand, "Gilbert Brand", 7);
    addDataToRecords(gilbertDataNB, "Gilbert NB", 8);
    addDataToRecords(mktData, "MKT", 9);
    addDataToRecords(mktDataBrand, "MKT Brand", 10);
    addDataToRecords(mktDataNB, "MKT NB", 11);
    addDataToRecords(phoenixData, "Phoenix", 12);
    addDataToRecords(phoenixDataBrand, "Phoenix Brand", 13);
    addDataToRecords(phoenixDataNB, "Phoenix NB", 14);
    addDataToRecords(scottsdaleData, "Scottsdale", 15);
    addDataToRecords(scottsdaleDataBrand, "Scottsdale Brand", 16);
    addDataToRecords(scottsdaleDataNB, "Scottsdale NB", 17);
    addDataToRecords(uptownParkData, "UptownPark", 18);
    addDataToRecords(uptownParkDataBrand, "UptownPark Brand", 19);
    addDataToRecords(uptownParkDataNB, "UptownPark NB", 20);
    addDataToRecords(montroseData, "Montrose", 21);
    addDataToRecords(montroseDataBrand, "Montrose Brand", 22);
    addDataToRecords(montroseDataNB, "Montrose NB", 23);
    addDataToRecords(riceVillageData, "RiceVillage", 24);
    addDataToRecords(riceVillageDataBrand, "RiceVillage Brand", 25);
    addDataToRecords(riceVillageDataNB, "RiceVillage NB", 26);
    addDataToRecords(mosaicData, "Mosaic", 27);
    addDataToRecords(mosaicDataBrand, "Mosaic Brand", 28);
    addDataToRecords(mosaicDataNB, "Mosaic NB", 29);
    addDataToRecords(fourteenthStData, "14thSt", 30);
    addDataToRecords(fourteenthStDataBrand, "14thSt Brand", 31);
    addDataToRecords(fourteenthStDataNB, "14thSt NB", 32);


    if (!date || date.trim() === '') {
      addWoWVariance(weeklyCampaignData.slice(-2)[0], weeklyCampaignData.slice(-3)[0], "All Campaign", 1);
      addWoWVariance(weeklySearchData.slice(-2)[0], weeklySearchData.slice(-3)[0], "All Search", 2);
      addWoWVariance(brandData.slice(-2)[0], brandData.slice(-3)[0], "Brand Search", 3);
      addWoWVariance(noBrandData.slice(-2)[0], noBrandData.slice(-3)[0], "NB Search", 4);
      addWoWVariance(pmaxData.slice(-2)[0], pmaxData.slice(-3)[0], "Pmax", 5);
      addWoWVariance(gilbertData.slice(-2)[0], gilbertData.slice(-3)[0], "Gilbert", 6);
      addWoWVariance(gilbertDataBrand.slice(-2)[0], gilbertDataBrand.slice(-3)[0], "Gilbert Brand", 7);
      addWoWVariance(gilbertDataNB.slice(-2)[0], gilbertDataNB.slice(-3)[0], "Gilbert NB", 8);
      addWoWVariance(mktData.slice(-2)[0], mktData.slice(-3)[0], "MKT", 9);
      addWoWVariance(mktDataBrand.slice(-2)[0], mktDataBrand.slice(-3)[0], "MKT Brand", 10);
      addWoWVariance(mktDataNB.slice(-2)[0], mktDataNB.slice(-3)[0], "MKT NB", 11);
      addWoWVariance(phoenixData.slice(-2)[0], phoenixData.slice(-3)[0], "Phoenix", 12);
      addWoWVariance(phoenixDataBrand.slice(-2)[0], phoenixDataBrand.slice(-3)[0], "Phoenix Brand", 13);
      addWoWVariance(phoenixDataNB.slice(-2)[0], phoenixDataNB.slice(-3)[0], "Phoenix NB", 14);
      addWoWVariance(scottsdaleData.slice(-2)[0], scottsdaleData.slice(-3)[0], "Scottsdale", 15);
      addWoWVariance(scottsdaleDataBrand.slice(-2)[0], scottsdaleDataBrand.slice(-3)[0], "Scottsdale Brand", 16);
      addWoWVariance(scottsdaleDataNB.slice(-2)[0], scottsdaleDataNB.slice(-3)[0], "Scottsdale NB", 17);
      addWoWVariance(uptownParkData.slice(-2)[0], uptownParkData.slice(-3)[0], "UptownPark", 18);
      addWoWVariance(uptownParkDataBrand.slice(-2)[0], uptownParkDataBrand.slice(-3)[0], "UptownPark Brand", 19);
      addWoWVariance(uptownParkDataNB.slice(-2)[0], uptownParkDataNB.slice(-3)[0], "UptownPark NB", 20);
      addWoWVariance(montroseData.slice(-2)[0], montroseData.slice(-3)[0], "Montrose", 21);
      addWoWVariance(montroseDataBrand.slice(-2)[0], montroseDataBrand.slice(-3)[0], "Montrose Brand", 22);
      addWoWVariance(montroseDataNB.slice(-2)[0], montroseDataNB.slice(-3)[0], "Montrose NB", 23);
      addWoWVariance(riceVillageData.slice(-2)[0], riceVillageData.slice(-3)[0], "RiceVillage", 24);
      addWoWVariance(riceVillageDataBrand.slice(-2)[0], riceVillageDataBrand.slice(-3)[0], "RiceVillage Brand", 25);
      addWoWVariance(riceVillageDataNB.slice(-2)[0], riceVillageDataNB.slice(-3)[0], "RiceVillage NB", 26);
      addWoWVariance(mosaicData.slice(-2)[0], mosaicData.slice(-3)[0], "Mosaic", 27);
      addWoWVariance(mosaicDataBrand.slice(-2)[0], mosaicDataBrand.slice(-3)[0], "Mosaic Brand", 28);
      addWoWVariance(mosaicDataNB.slice(-2)[0], mosaicDataNB.slice(-3)[0], "Mosaic NB", 29);
      addWoWVariance(fourteenthStData.slice(-2)[0], fourteenthStData.slice(-3)[0], "14thSt", 30);
      addWoWVariance(fourteenthStDataBrand.slice(-2)[0], fourteenthStDataBrand.slice(-3)[0], "14thSt Brand", 31);
      addWoWVariance(fourteenthStDataNB.slice(-2)[0], fourteenthStDataNB.slice(-3)[0], "14thSt NB", 32);
    }
    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          finalRecords.push({
            Week: record.Filter,
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
          currentGroup = record.Filter;
        }
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "WoW Variance %") {
          finalRecords.push({ Week: "", Filter: "", Filter2: "", isBold: false });
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
      record["Booking Confirmed"],
      record["Booking CAC"],
      record["Booking Conv Rate"],
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
      record["Purchase"],
    ]);

    const dataToSend = {
      Live: sheetData,
      AllBNB: sheetData.filter(row => ["All Search", "Brand Search", "NB Search"].includes(row[0]) || ["All Search", "Brand Search", "NB Search"].includes(row[1])),
      Gilbert: sheetData.filter(row => ["Gilbert", "Gilbert Brand", "Gilbert NB"].includes(row[0]) || ["Gilbert", "Gilbert Brand", "Gilbert NB"].includes(row[1])),
      MKT: sheetData.filter(row => ["MKT", "MKT Brand", "MKT NB"].includes(row[0]) || ["MKT", "MKT Brand", "MKT NB"].includes(row[1])),
      Phoenix: sheetData.filter(row => ["Phoenix", "Phoenix Brand", "Phoenix NB"].includes(row[0]) || ["Phoenix", "Phoenix Brand", "Phoenix NB"].includes(row[1])),
      Scottsdale: sheetData.filter(row => ["Scottsdale", "Scottsdale Brand", "Scottsdale NB"].includes(row[0]) || ["Scottsdale", "Scottsdale Brand", "Scottsdale NB"].includes(row[1])),
      UptownPark: sheetData.filter(row => ["UptownPark", "UptownPark Brand", "UptownPark NB"].includes(row[0]) || ["UptownPark", "UptownPark Brand", "UptownPark NB"].includes(row[1])),
      Montrose: sheetData.filter(row => ["Montrose", "Montrose Brand", "Montrose NB"].includes(row[0]) || ["Montrose", "Montrose Brand", "Montrose NB"].includes(row[1])),
      RiceVillage: sheetData.filter(row => ["RiceVillage", "RiceVillage Brand", "RiceVillage NB"].includes(row[0]) || ["RiceVillage", "RiceVillage Brand", "RiceVillage NB"].includes(row[1])),
      Mosaic: sheetData.filter(row => ["Mosaic", "Mosaic Brand", "Mosaic NB"].includes(row[0]) || ["Mosaic", "Mosaic Brand", "Mosaic NB"].includes(row[1])),
      FourteenthSt: sheetData.filter(row => ["14thSt", "14thSt Brand", "14thSt NB"].includes(row[0]) || ["14thSt", "14thSt Brand", "14thSt NB"].includes(row[1])),
    };    

    const formatSheets = async (sheetName, data) => {
      await sheets.spreadsheets.values.clear({ spreadsheetId, range: dataRanges[sheetName] });
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: dataRanges[sheetName],
        valueInputOption: "RAW",
        resource: { values: data },
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
                  endRowIndex: data.length + 1,
                  startColumnIndex: 0,
                  endColumnIndex: 6,
                },
                cell: {
                  userEnteredFormat: { horizontalAlignment: 'RIGHT' },
                },
                fields: 'userEnteredFormat.horizontalAlignment',
              },
            },
          ],
        },
      });
    };

    for (const [sheetName, data] of Object.entries(dataToSend)) {
      await formatSheets(sheetName, data);
    }

    console.log("Final Hi, Skin weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const sendBlendedCACToGoogleSheetsHS = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const sourceSpreadsheetId = process.env.BLENDED_SPREADSHEET;
  const sourceDataRange = 'MAA - Daily!A2:W';
  const targetSpreadsheetId = process.env.HI_SKIN_SPREADSHEET;
  const targetDataRange = 'MAA - Daily!A2:C';

  try {
    const sourceResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: sourceSpreadsheetId,
      range: sourceDataRange,
    });

    const sourceRows = sourceResponse.data.values;

    if (!sourceRows || sourceRows.length === 0) {
      console.log("No data found in the source sheet.");
      return;
    }

    const filteredData = sourceRows.map(row => [
      row[1] || null,
      row[21] || null,
      row[22] || null
    ]);

    const targetResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: targetSpreadsheetId,
      range: targetDataRange,
    });

    const targetRows = targetResponse.data.values || [];

    if (JSON.stringify(filteredData) === JSON.stringify(targetRows)) {
      console.log("Data is already up to date. Skipping update.");
      return;
    }

    await sheets.spreadsheets.values.update({
      spreadsheetId: targetSpreadsheetId,
      range: targetDataRange,
      valueInputOption: 'RAW',
      requestBody: {
        values: filteredData,
      },
    });

    console.log("Data successfully written to target sheet.");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  fetchReportDataWeeklyCampaignHS,
  fetchReportDataWeeklySearchHS,
  executeSpecificFetchFunctionHS,
  sendFinalWeeklyReportToGoogleSheetsHS,
  sendBlendedCACToGoogleSheetsHS
};
