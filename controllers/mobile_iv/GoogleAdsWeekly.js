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

const aggregateDataForWeek = async (customer, startDate, endDate ) => {
  const aggregatedData = {
    date: `${startDate} - ${endDate}`,
    impressions: 0,
    clicks: 0,
    cost: 0,
    conversions: 0,
    interactions: 0,
    calls: 0,
    books: 0,
    phone: 0,
    // conv_date: 0,
  };

  const metricsQuery = `
    SELECT
      campaign.id,
      campaign.name,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.conversions,
      metrics.interactions,
      metrics.conversions_value_by_conversion_date,
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
      AND segments.conversion_action_name IN ('Calls from Ads - Local SEO', 'Book Now Form Local SEO', 'Phone No. Click Local SEO')
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
      aggregatedData.conversions += campaign.metrics.conversions || 0;
      aggregatedData.interactions += campaign.metrics.interactions || 0;
      // aggregatedData.conv_date += campaign.metrics.conversions_value_by_conversion_date || 0;
    });
    metricsPageToken = metricsResponse.next_page_token;
  } while (metricsPageToken);

  let conversionPageToken = null;
  do {
    const conversionBatchResponse = await customer.query(conversionQuery);
    conversionBatchResponse.forEach((conversion) => {
      const conversionValue = conversion.metrics.all_conversions || 0;
      if (conversion.segments.conversion_action_name === "Calls from Ads - Local SEO") {
        aggregatedData.calls += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Book Now Form Local SEO") {
        aggregatedData.books += conversionValue;
      } else if (conversion.segments.conversion_action_name === "Phone No. Click Local SEO") {
        aggregatedData.phone += conversionValue;
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
      customer_id: campaignNameFilter,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });
    
    // const dateRanges = getOrGenerateDateRanges();

    const allWeeklyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForWeek(customer, start, end);
    });

    const allWeeklyData = await Promise.all(allWeeklyDataPromises);

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const createFetchFunction = (campaignNameFilter, reportName) => {
  return (req, res, dateRanges) => fetchReportDataWeeklyFilter(req, res, campaignNameFilter, reportName, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklyAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ, "Mobile IV Drip AZ"),
  fetchReportDataWeeklyLV: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV, "Mobile IV Drip LV"),
  fetchReportDataWeeklyNYC: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC, "Mobile IV Drip NYC"),
};

const executeSpecificFetchFunctionMIV = async (req, res) => {
  const functionName = "fetchReportDataWeeklyAZ";
  const dateRanges = getOrGenerateDateRanges();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](req, res, dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

const sendFinalWeeklyReportToGoogleSheetsMIV = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_MOBILE_DRIP;
  const dataRanges = {
    AZLive: 'AZ Weekly!A2:U',
    LVLive: 'LV Weekly!A2:S',
    NYCLive: 'NYC Weekly!A2:S',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const dripAZ = await fetchFunctions.fetchReportDataWeeklyAZ(req, res, dateRanges);
    const dripLV = await fetchFunctions.fetchReportDataWeeklyLV(req, res, dateRanges);
    const dripNYC = await fetchFunctions.fetchReportDataWeeklyNYC(req, res, dateRanges);

    const bookingData = await sendBookings(req, res);

    const bookingAZ = bookingData.AZ || [];
    const bookingAllAZ = bookingData.allAZ || [];
    const bookingLV = bookingData.LV || [];
    const bookingNY = bookingData.NYC || [];

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

    const addWoWVariance = (lastRecord, secondToLastRecord, bookingAZ, bookingRecords, filter, filter2) => {
      const bookingLastRecord = bookingRecords.find(j => j.week === lastRecord.date) || { data: {} };
      const bookingSecondToLastRecord = bookingRecords.find(j => j.week === secondToLastRecord.date) || { data: {} };

      const bookingLastRecordAZ = bookingAZ.find(j => j.week === lastRecord.date) || { data: {} };
      const bookingSecondToLastRecordAZ = bookingAZ.find(j => j.week === secondToLastRecord.date) || { data: {} };
    
      const baseRecord = {
        Week: "WoW Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions)),
        "Clicks": formatPercentage(calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks)),
        "Cost": formatPercentage(calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost)),
        "CPC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
        // "Booking Total": formatPercentage(calculateWoWVariance(bookingLastRecord.data.count2, bookingSecondToLastRecord.data.count2)),
        // "CAC": formatPercentage(calculateWoWVariance(lastRecord.cost /(bookingLastRecord.data.count2 || 0), secondToLastRecord.cost (bookingSecondToLastRecord.data.count2 || 0))),
        // "Appt Conv Rate":formatPercentage(calculateWoWVariance((bookingLastRecord.data.count2 || 0) / lastRecord.clicks,(bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks.count2)),
      };

      Object.assign(baseRecord, {
        "Booking Total": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              (bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0),
              (bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0)
            ))
          : formatPercentage(calculateWoWVariance(bookingLastRecord.data.count2, bookingSecondToLastRecord.data.count2)),
        "CAC": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              lastRecord.cost / ((bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0)),
              secondToLastRecord.cost / ((bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0))
            ))
          : formatPercentage(calculateWoWVariance(
              lastRecord.cost / (bookingLastRecord.data.count2 || 0),
              secondToLastRecord.cost / (bookingSecondToLastRecord.data.count2 || 0)
            )),
        "Appt Conv Rate": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              ((bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0) / lastRecord.clicks),
              ((bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0) / secondToLastRecord.clicks)
            ))
          : formatPercentage(calculateWoWVariance(
              (bookingLastRecord.data.count2 || 0) / lastRecord.clicks,
              (bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks
            )),
        "New Requests": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              (bookingLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingLastRecordAZ.data?.Tucson?.count1 || 0),
              (bookingSecondToLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count1 || 0)
            ))
          : formatPercentage(calculateWoWVariance(bookingLastRecord.data.count1, bookingSecondToLastRecord.data.count1)),
        "New Requests CAC": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              lastRecord.cost / ((bookingLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingLastRecordAZ.data?.Tucson?.count1 || 0)),
              secondToLastRecord.cost / ((bookingSecondToLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count1 || 0))
            ))
          : formatPercentage(calculateWoWVariance(
              lastRecord.cost / (bookingLastRecord.data.count1 || 0),
              secondToLastRecord.cost / (bookingSecondToLastRecord.data.count1 || 0)
            )),
      });

      if (filter === "AZ") {
        Object.assign(baseRecord, {
          "Booking Total Phoenix": formatPercentage(calculateWoWVariance(bookingLastRecordAZ.data?.Phoenix?.count2, bookingSecondToLastRecordAZ.data?.Phoenix?.count2)),
          "Booking Total Tucson": formatPercentage(calculateWoWVariance(bookingLastRecordAZ.data?.Tucson?.count2, bookingSecondToLastRecordAZ.data?.Tucson?.count2)),
        });
      }
    
      Object.assign(baseRecord, {
        "Conversion": formatPercentage(calculateWoWVariance(lastRecord.conversions, secondToLastRecord.conversions)),
        "Cost Per Conv": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
        "Conv. Rate": formatPercentage(calculateWoWVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
        "Calls from Ads - Local SEO": formatPercentage(calculateWoWVariance(lastRecord.calls, secondToLastRecord.calls)),
        "Book Now Form Local SEO": formatPercentage(calculateWoWVariance(lastRecord.books, secondToLastRecord.books)),
        "Phone No. Click Local SEO": formatPercentage(calculateWoWVariance(lastRecord.phone, secondToLastRecord.phone)),
      });
    
      records.push(baseRecord);
    };    

    const addBiWeeklyVariance = (previousRecord, secondToPreviousRecord, lastRecord, secondToLastRecord, bookingAZ, bookingRecords, filter, filter2) => {
      const bookingPreviousRecord = bookingRecords.find(j => j.week === previousRecord.date) || { data: {} };
      const bookingSecondToPreviousRecord = bookingRecords.find(j => j.week === secondToPreviousRecord.date) || { data: {} };

      const bookingLastRecord = bookingRecords.find(j => j.week === lastRecord.date) || { data: {} };
      const bookingSecondToLastRecord = bookingRecords.find(j => j.week === secondToLastRecord.date) || { data: {} };

      const bookingPreviousRecordAZ = bookingAZ.find(j => j.week === previousRecord.date) || { data: {} };
      const bookingSecondToPreviousRecordAZ = bookingAZ.find(j => j.week === secondToPreviousRecord.date) || { data: {} };

      const bookingLastRecordAZ = bookingAZ.find(j => j.week === lastRecord.date) || { data: {} };
      const bookingSecondToLastRecordAZ = bookingAZ.find(j => j.week === secondToLastRecord.date) || { data: {} };
    
      const baseRecord = {
        Week: "Biweekly Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance((previousRecord.impressions + secondToPreviousRecord.impressions), (lastRecord.impressions + secondToLastRecord.impressions))),
        "Clicks": formatPercentage(calculateWoWVariance((previousRecord.clicks + secondToPreviousRecord.clicks), (lastRecord.clicks + secondToLastRecord.clicks))),
        "Cost": formatPercentage(calculateWoWVariance((previousRecord.cost + secondToPreviousRecord.cost), (lastRecord.cost + secondToLastRecord.cost))),
        "CPC": formatPercentage(calculateWoWVariance(((previousRecord.cost / previousRecord.clicks) + (secondToPreviousRecord.cost / secondToPreviousRecord.clicks)), ((lastRecord.cost / lastRecord.clicks) + ( secondToLastRecord.cost / secondToLastRecord.clicks)))),
        "CTR": formatPercentage(calculateWoWVariance(((previousRecord.clicks / previousRecord.impressions) + (secondToPreviousRecord.clicks / secondToPreviousRecord.impressions)), ((lastRecord.clicks / lastRecord.impressions) + (secondToLastRecord.clicks / secondToLastRecord.impressions)))),
        // "Booking Total": formatPercentage(calculateWoWVariance(bookingLastRecord.data.count2, bookingSecondToLastRecord.data.count2)),
        // "CAC": formatPercentage(calculateWoWVariance(lastRecord.cost /(bookingLastRecord.data.count2 || 0), secondToLastRecord.cost (bookingSecondToLastRecord.data.count2 || 0))),
        // "Appt Conv Rate":formatPercentage(calculateWoWVariance((bookingLastRecord.data.count2 || 0) / lastRecord.clicks,(bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks.count2)),
      };

      Object.assign(baseRecord, {
        "Booking Total": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              (((bookingPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingPreviousRecordAZ.data?.Tucson?.count2 || 0)) +
              ((bookingSecondToPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToPreviousRecordAZ.data?.Tucson?.count2 || 0))),
              (((bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0)) +
              ((bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0)))
            ))
          : formatPercentage(calculateWoWVariance(
              ((bookingPreviousRecord.data.count2 || 0) + (bookingSecondToPreviousRecord.data.count2 || 0)), 
              ((bookingLastRecord.data.count2 || 0) + (bookingSecondToLastRecord.data.count2 || 0))
            )),
        "CAC": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              ((previousRecord.cost / ((bookingPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingPreviousRecordAZ.data?.Tucson?.count2 || 0))) + 
              (secondToPreviousRecord.cost / ((bookingSecondToPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToPreviousRecordAZ.data?.Tucson?.count2 || 0)))),
              ((lastRecord.cost / ((bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0))) + 
              (secondToLastRecord.cost / ((bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0))))
            ))
          : formatPercentage(calculateWoWVariance(
              ((previousRecord.cost / (bookingPreviousRecord.data.count2 || 0)) + (secondToPreviousRecord.cost / (bookingSecondToPreviousRecord.data.count2 || 0))),
              ((lastRecord.cost / (bookingLastRecord.data.count2 || 0)) + ( secondToLastRecord.cost / (bookingSecondToLastRecord.data.count2 || 0)))
            )),
        "Appt Conv Rate": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              (((((bookingPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingPreviousRecordAZ.data?.Tucson?.count2 || 0)) / previousRecord.clicks)  +
              (((bookingSecondToPreviousRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToPreviousRecordAZ.data?.Tucson?.count2 || 0)) / secondToPreviousRecord.clicks))),
              (((((bookingLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingLastRecordAZ.data?.Tucson?.count2 || 0)) / lastRecord.clicks) + 
              (((bookingSecondToLastRecordAZ.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count2 || 0)) / secondToLastRecord.clicks)))
            ))
          : formatPercentage(calculateWoWVariance(
              (((bookingPreviousRecord.data.count2 || 0) / previousRecord.clicks) + ((bookingSecondToPreviousRecord.data.count2 || 0) / secondToPreviousRecord.clicks)),
              (((bookingLastRecord.data.count2 || 0) / lastRecord.clicks) + ((bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks))
            )),
        "New Requests": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              (((bookingPreviousRecordAZ.data?.Phoenix?.count1 || 0) + (bookingPreviousRecordAZ.data?.Tucson?.count1 || 0)) + 
              ((bookingSecondToPreviousRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToPreviousRecordAZ.data?.Tucson?.count1 || 0))),
              (((bookingLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingLastRecordAZ.data?.Tucson?.count1 || 0)) + 
              ((bookingSecondToLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count1 || 0)))
            ))
          : formatPercentage(calculateWoWVariance(
              (bookingPreviousRecord.data.count1 + bookingSecondToPreviousRecord.data.count1),
              (bookingLastRecord.data.count1 + bookingSecondToLastRecord.data.count1)
            )),
        "New Requests CAC": filter === "AZ"
          ? formatPercentage(calculateWoWVariance(
              ((previousRecord.cost / ((bookingPreviousRecordAZ.data?.Phoenix?.count1 || 0) + (bookingPreviousRecordAZ.data?.Tucson?.count1 || 0))) + 
              (secondToPreviousRecord.cost / ((bookingSecondToPreviousRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToPreviousRecordAZ.data?.Tucson?.count1 || 0)))),
              ((lastRecord.cost / ((bookingLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingLastRecordAZ.data?.Tucson?.count1 || 0))) + 
              (secondToLastRecord.cost / ((bookingSecondToLastRecordAZ.data?.Phoenix?.count1 || 0) + (bookingSecondToLastRecordAZ.data?.Tucson?.count1 || 0))))
            ))
          : formatPercentage(calculateWoWVariance(
              ((previousRecord.cost / (bookingPreviousRecord.data.count1 || 0)) + (secondToPreviousRecord.cost / (bookingSecondToPreviousRecord.data.count1 || 0))),
              ((lastRecord.cost / (bookingLastRecord.data.count1 || 0)) + (secondToLastRecord.cost / (bookingSecondToLastRecord.data.count1 || 0)))
            )),
      });

      if (filter === "AZ") {
        Object.assign(baseRecord, {
          "Booking Total Phoenix": formatPercentage(calculateWoWVariance(
            (bookingPreviousRecordAZ.data?.Phoenix?.count2 + bookingSecondToPreviousRecordAZ.data?.Phoenix?.count2),
            (bookingLastRecordAZ.data?.Phoenix?.count2 + bookingSecondToLastRecordAZ.data?.Phoenix?.count2)
          )),
          "Booking Total Tucson": formatPercentage(calculateWoWVariance(
            (bookingPreviousRecordAZ.data?.Tucson?.count2 + bookingSecondToPreviousRecordAZ.data?.Tucson?.count2),
            (bookingLastRecordAZ.data?.Tucson?.count2 + bookingSecondToLastRecordAZ.data?.Tucson?.count2)
          )),
        });
      }
    
      Object.assign(baseRecord, {
        "Conversion": formatPercentage(calculateWoWVariance((previousRecord.conversions + secondToPreviousRecord.conversions), (lastRecord.conversions + secondToLastRecord.conversions))),
        "Cost Per Conv": formatPercentage(calculateWoWVariance(((previousRecord.cost / previousRecord.conversions) + (secondToPreviousRecord.cost / secondToPreviousRecord.conversions)), ((lastRecord.cost / lastRecord.conversions) + (secondToLastRecord.cost / secondToLastRecord.conversions)))),
        "Conv. Rate": formatPercentage(calculateWoWVariance(((previousRecord.conversions / previousRecord.interactions) + (secondToPreviousRecord.conversions / secondToPreviousRecord.interactions)), ((lastRecord.conversions / lastRecord.interactions) + (secondToLastRecord.conversions / secondToLastRecord.interactions)))),
        "Calls from Ads - Local SEO": formatPercentage(calculateWoWVariance((previousRecord.calls + secondToPreviousRecord.calls), (lastRecord.calls + secondToLastRecord.calls))),
        "Book Now Form Local SEO": formatPercentage(calculateWoWVariance((previousRecord.books + secondToPreviousRecord.books), (lastRecord.books + secondToLastRecord.books))),
        "Phone No. Click Local SEO": formatPercentage(calculateWoWVariance((previousRecord.phone + secondToPreviousRecord.phone), (lastRecord.phone + secondToLastRecord.phone))),
      });
    
      records.push(baseRecord);
    }; 

    const addDataToRecords = (data, bookingDataAZ, bookingData, filter, filter2) => { 
      data.forEach((record) => {
        const bookingRecord = bookingData.find(j => j.week === record.date) || { data: {} };
        const bookingRecordAZ = bookingDataAZ.find(j => j.week === record.date) || { data: {} };
        const baseRecord = {
          Week: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          "Clicks": formatNumber(record.clicks),
          "Cost": formatCurrency(record.cost),
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          // "Booking Total": formatNumber(bookingRecord.data.count2 || 0),
          // "CAC": formatCurrency(record.cost / (bookingRecord.data.count2 || 0) || 0),
          // "Appt Conv Rate": formatPercentage(((bookingRecord.data.count2 || 0) / record.clicks) * 100 || 0),
        };

        Object.assign(baseRecord, {
          "Booking Total": filter === "AZ"
            ? formatNumber((bookingRecordAZ.data?.Phoenix?.count2 || 0) + (bookingRecordAZ.data?.Tucson?.count2 || 0)) 
            : formatNumber(bookingRecord.data.count2 || 0),
          "CAC": filter === "AZ"
            ? formatCurrency(record.cost / ((bookingRecordAZ.data?.Phoenix?.count2 || 0) + (bookingRecordAZ.data?.Tucson?.count2 || 0)) || 0) 
            : formatCurrency(record.cost / (bookingRecord.data.count2 || 0) || 0),
          "Appt Conv Rate": filter === "AZ"
            ? formatPercentage((((bookingRecordAZ.data?.Phoenix?.count2 || 0) + (bookingRecordAZ.data?.Tucson?.count2 || 0)) / record.clicks) * 100 || 0) 
            : formatPercentage(((bookingRecord.data.count2 || 0) / record.clicks) * 100 || 0),
          "New Requests": filter === "AZ"
            ? formatNumber((bookingRecordAZ.data?.Phoenix?.count1 || 0) + (bookingRecordAZ.data?.Tucson?.count1 || 0)) 
            : formatNumber(bookingRecord.data.count1 || 0),
          "New Requests CAC": filter === "AZ"
            ? formatCurrency(record.cost / ((bookingRecordAZ.data?.Phoenix?.count1 || 0) + (bookingRecordAZ.data?.Tucson?.count1 || 0)) || 0) 
            : formatCurrency(record.cost / (bookingRecord.data.count1 || 0) || 0),
        });
    
        if (filter === "AZ") {
          Object.assign(baseRecord, {
            "Booking Total Phoenix": formatNumber(bookingRecordAZ.data?.Phoenix?.count2 || 0),
            "Booking Total Tucson": formatNumber(bookingRecordAZ.data?.Tucson?.count2 || 0),
          });
        }

        Object.assign(baseRecord, {
          "Conversion": formatNumber(record.conversions),
          "Cost Per Conv": formatCurrency(record.cost / record.conversions),
          "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
          "Calls from Ads - Local SEO": formatNumber(record.calls),
          "Book Now Form Local SEO": formatNumber(record.books),
          "Phone No. Click Local SEO": formatNumber(record.phone),
        });
    
        records.push(baseRecord);
      });
    };    

    addDataToRecords(dripAZ, bookingAZ, bookingAllAZ, "AZ", 1);
    addDataToRecords(dripLV, [], bookingLV, "LV", 2);
    addDataToRecords(dripNYC, [], bookingNY, "NYC", 3);

    if (!date || date.trim() === '') {
      addWoWVariance(dripAZ.slice(-2)[0], dripAZ.slice(-3)[0], bookingAZ, bookingAllAZ, "AZ", 1);
      addWoWVariance(dripLV.slice(-2)[0], dripLV.slice(-3)[0], [], bookingLV, "LV", 2);
      addWoWVariance(dripNYC.slice(-2)[0], dripNYC.slice(-3)[0], [], bookingNY, "NYC", 3);
    }

    if (!date || date.trim() === '') {
      addBiWeeklyVariance(dripAZ.slice(-2)[0], dripAZ.slice(-3)[0], dripAZ.slice(-4)[0], dripAZ.slice(-5)[0], bookingAZ, bookingAllAZ, "AZ", 1);
      addBiWeeklyVariance(dripLV.slice(-2)[0], dripLV.slice(-3)[0], dripLV.slice(-4)[0], dripLV.slice(-5)[0], [], bookingLV, "LV", 2);
      addBiWeeklyVariance(dripNYC.slice(-2)[0], dripNYC.slice(-3)[0], dripNYC.slice(-4)[0], dripNYC.slice(-5)[0], [], bookingNY, "NYC", 3);
    }

    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
    
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          const baseHeader = {
            Week: record.Filter,
            Filter: "Filter",
            Filter2: "Filter2",
            "Impr.": "Impr.",
            "Clicks": "Clicks",
            "Cost": "Cost",
            "CPC": "CPC",
            "CTR": "CTR",
            "Booking Total": "Booking Total",
            "CAC": "CAC",
            "Appt Conv Rate": "Appt Conv Rate",
            "New Requests": "New Requests",
            "New Requests CAC": "New Requests CAC",
          };    
    
          if (record.Filter.trim().toUpperCase() === "AZ") {
            Object.assign(baseHeader, {
              "Booking Total Phoenix": "Booking Total Phoenix",
              "Booking Total Tucson": "Booking Total Tucson",
            });
          }
    
          Object.assign(baseHeader, {
            "Conversion": "Conversion",
            "Cost Per Conv": "Cost Per Conv",
            "Conv. Rate": "Conv. Rate",
            "Calls from Ads - Local SEO": "Calls from Ads - Local SEO",
            "Book Now Form Local SEO": "Book Now Form Local SEO",
            "Phone No. Click Local SEO": "Phone No. Click Local SEO",
          });

          finalRecords.push(baseHeader);
          currentGroup = record.Filter;
        }
    
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "Biweekly Variance %") {
          finalRecords.push({ Week: "", Filter: "", Filter2: "", isBold: false });
        }
      });
    }    

    processGroup(records);

    const sheetData = finalRecords.map(record => {
      const baseData = [
        record.Week,
        record.Filter,
        record.Filter2,
        record["Impr."],
        record["Clicks"],
        record["Cost"],
        record["CPC"],
        record["CTR"],
        record["Booking Total"],
        record["CAC"],
        record["Appt Conv Rate"],
        record["New Requests"],
        record["New Requests CAC"],
      ];
    
      if (record.Filter === "AZ") {
        baseData.push(
          record["Booking Total Phoenix"],
          record["Booking Total Tucson"]
        );
      }
    
      baseData.push(
        record["Conversion"],
        record["Cost Per Conv"],
        record["Conv. Rate"],
        record["Calls from Ads - Local SEO"],
        record["Book Now Form Local SEO"],
        record["Phone No. Click Local SEO"]
      );
      
      return baseData;
    });    

    const dataToSend = {
      AZLive: sheetData.filter(row => ["AZ"].includes(row[1])),
      LVLive: sheetData.filter(row => ["LV"].includes(row[1])),
      NYCLive: sheetData.filter(row => ["NYC"].includes(row[1])),
    };    

    const formatSheets = async (sheetName, data) => {
      await sheets.spreadsheets.values.clear({
        spreadsheetId,
        range: dataRanges[sheetName],
      });
    
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
                  sheetId: 2045694540,
                  startRowIndex: 1,
                  endRowIndex: data.length + 1,
                  startColumnIndex: 0,
                  endColumnIndex: 6,
                },
                cell: {
                  userEnteredFormat: {
                    horizontalAlignment: "LEFT",
                  },
                },
                fields: "userEnteredFormat.horizontalAlignment",
              },
            },
          ],
        },
      });
    };
    
    for (const [sheetName, data] of Object.entries(dataToSend)) {
      await formatSheets(sheetName, data);
    }    

    console.log("Final Mobile IV Drip weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const sendBookings = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  const sourceSpreadsheetId = process.env.SHEET_MOBILE_DRIP;
  const sourceDataRanges = {
    "AZ_Phoenix": "AZ Phoenix Bookings Data!A2:D",
    "AZ_Tucson": "AZ Tucson Bookings Data!A2:D",
    "allAZ": "AZ Bookings Data!A2:D",
    "LV": "LV Bookings Data!A2:D",
    "NYC": "NY Bookings Data!A2:D",
  };

  try {
    const startDate = new Date('2024-11-11');
    const result = { AZ: [], allAZ: [], LV: [], NYC: [] };
    const weeklyData = { AZ: {}, allAZ: {}, LV: {}, NYC: {} };

    for (const [location, range] of Object.entries(sourceDataRanges)) {
      const { data: { values: rows } } = await sheets.spreadsheets.values.get({
        spreadsheetId: sourceSpreadsheetId,
        range,
      });
      if (!rows) continue;

      rows.forEach(([date, count1, , count2]) => {

        if (!date || !count1 || !count2 || isNaN(count2) || isNaN(count2) || new Date(date) < startDate) return;
      
        const currentRowDate = new Date(date);
        const dayOfWeek = currentRowDate.getDay();
        const diffToMonday = (dayOfWeek + 6) % 7;

        const weekStart = new Date(currentRowDate);
        weekStart.setDate(currentRowDate.getDate() - diffToMonday);
      
        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekStart.getDate() + 6);

        const weekLabel = `${weekStart.getFullYear()}-${(weekStart.getMonth() + 1).toString().padStart(2, '0')}-${weekStart.getDate().toString().padStart(2, '0')} - ${weekEnd.getFullYear()}-${(weekEnd.getMonth() + 1).toString().padStart(2, '0')}-${weekEnd.getDate().toString().padStart(2, '0')}`;
      
        if (location.startsWith("AZ_")) {
          const subLocation = location.replace("AZ_", "");
          weeklyData.AZ[weekLabel] = weeklyData.AZ[weekLabel] || { Phoenix: { count1: 0, count2: 0 }, Tucson: { count1: 0, count2: 0 } };
          weeklyData.AZ[weekLabel][subLocation].count1 += parseInt(count1, 10);
          weeklyData.AZ[weekLabel][subLocation].count2 += parseInt(count2, 10);
        } else {
          weeklyData[location][weekLabel] = weeklyData[location][weekLabel] || { count1: 0, count2: 0 };
          weeklyData[location][weekLabel].count1 += parseInt(count1, 10);
          weeklyData[location][weekLabel].count2 += parseInt(count2, 10);
        }
      });      
    }

    for (const [week, data] of Object.entries(weeklyData.AZ)) {
      result.AZ.push({ week, data });
    }
    for (const [week, data] of Object.entries(weeklyData.allAZ)) {
      result.allAZ.push({ week, data });
    }
    for (const [week, data] of Object.entries(weeklyData.LV)) {
      result.LV.push({ week, data });
    }
    for (const [week, data] of Object.entries(weeklyData.NYC)) {
      result.NYC.push({ week, data });
    }

    return result;
  } catch (error) {
    console.error("Error generating weekly data:", error);
    res.status(500).json({ error: "Internal server error" });
  }
};

module.exports = {
  executeSpecificFetchFunctionMIV,
  sendFinalWeeklyReportToGoogleSheetsMIV,
  sendBookings
};
