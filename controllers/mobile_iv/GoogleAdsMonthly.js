const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredRefreshToken } = require("../GoogleAuth");

let storedDateRanges = null;

const generateMonthlyDateRanges = (startDate, endDate) => {
  const dateRanges = [];
  let currentMonthStart = new Date(`${startDate}-01T00:00:00Z`); // Normalize to UTC

  while (currentMonthStart <= endDate) {
    const currentMonthEnd = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1, // Move to next month
      0 // Last day of the current month
    ));

    const adjustedEndDate = currentMonthEnd > endDate ? endDate : currentMonthEnd;

    dateRanges.push({
      start: currentMonthStart.toISOString().split('T')[0],
      end: adjustedEndDate.toISOString().split('T')[0],
    });

    // Move to the 1st of the next month
    currentMonthStart = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() + 1,
      1
    ));
  }

  return dateRanges;
};

const getOrGenerateDateRanges = () => {
  const today = new Date();
  const startDate = '2025-04';
  const endDate = today; 

  if (!storedDateRanges || new Date(storedDateRanges[storedDateRanges.length - 1].end) < endDate) {
    storedDateRanges = generateMonthlyDateRanges(startDate, endDate);
  }

  return storedDateRanges;
};

setInterval(getOrGenerateDateRanges, 24 * 60 * 60 * 1000);

const aggregateDataForMonth = async (customer, startDate, endDate ) => {
  const startDateObj = new Date(startDate);
  const formattedDate = `${startDateObj.getFullYear()}-${(startDateObj.getMonth() + 1).toString().padStart(2, '0')}`;
  
  const aggregatedData = {
    date: formattedDate,
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
      AND segments.conversion_action_name IN ('MobileIVDrip.com Book Now Confirmed', 'MobileIVDrip.com Click - Call Now Button')
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
      if (conversion.segments.conversion_action_name === "MobileIVDrip.com Book Now Confirmed") {
        aggregatedData.books += conversionValue;
      } else if (conversion.segments.conversion_action_name === "MobileIVDrip.com Click - Call Now Button") {
        aggregatedData.phone += conversionValue;
      }
    });
    conversionPageToken = conversionBatchResponse.next_page_token;
  } while (conversionPageToken);

  return aggregatedData;
};

const fetchReportDataMonthlyFilter = async (req, res, campaignNameFilter, reportName, dateRanges) => {
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

    const allMonthlyDataPromises = dateRanges.map(({ start, end }) => {
      return aggregateDataForMonth(customer, start, end);
    });

    const allMonthlyData = await Promise.all(allMonthlyDataPromises);

    return allMonthlyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(500).send("Error fetching report data");
  }
};

const createFetchFunction = (campaignNameFilter, reportName) => {
  return (req, res, dateRanges) => fetchReportDataMonthlyFilter(req, res, campaignNameFilter, reportName, dateRanges);
};

const fetchFunctions = {
  fetchReportDataMonthlyAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ, "Mobile IV Drip AZ"),
  fetchReportDataMonthlyLV: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV, "Mobile IV Drip LV"),
  fetchReportDataMonthlyNYC: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC, "Mobile IV Drip NYC"),
};

const executeSpecificFetchFunctionMIV = async (req, res) => {
  const functionName = "fetchReportDataMonthlyAZ";
  const dateRanges = getOrGenerateDateRanges();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](req, res, dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

const sendFinalMonthlyReportToGoogleSheetsMIV = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.SHEET_MOBILE_DRIP;
  const dataRanges = {
    Monthly: 'Monthly View!A2:S',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const dripAZ = await fetchFunctions.fetchReportDataMonthlyAZ(req, res, dateRanges);
    const dripLV = await fetchFunctions.fetchReportDataMonthlyLV(req, res, dateRanges);
    const dripNYC = await fetchFunctions.fetchReportDataMonthlyNYC(req, res, dateRanges);

    const bookingData = await sendBookings(req, res);

    const bookingAZ = bookingData.AZ || [];
    const bookingLV = bookingData.LV || [];
    const bookingNY = bookingData.NYC || [];

    const mergeAndSum = (datasets) => {
      const mergedData = {};

      datasets.forEach((data) => {
        data.forEach((entry) => {
          const { date, month, data, ...metrics } = entry;
          const key = date || month;

          if (!mergedData[key]) {
            mergedData[key] = { date: key };
          }

          Object.keys(metrics).forEach((k) => {
            mergedData[key][k] = (mergedData[key][k] || 0) + (Number(metrics[k]) || 0);
          });
        });
      });

      return Object.values(mergedData);
    };

    const total = mergeAndSum([dripAZ, dripLV, dripNYC]);

    const addTotalBookings = (totalData, bookings) => {
      bookings.forEach((booking) => {
        const { month, data } = booking;
        const key = month;

        const totalEntry = totalData.find((entry) => entry.date === key);
        if (totalEntry) {
          let totalBookings = 0;
          let totalNewBookings = 0;
    
          if (typeof data === "object") {
            Object.values(data).forEach((num) => {
              if (typeof num === "object") {
                totalBookings += num.count1 || 0;
                totalNewBookings += num.count2 || 0;
              } else {
                if (data.count1 !== undefined) totalBookings = data.count1;
                if (data.count2 !== undefined) totalNewBookings = data.count2;
              }
            });
          }
    
          totalEntry.totalBook = (totalEntry.totalBook || 0) + totalBookings;
          totalEntry.totalNewBook = (totalEntry.totalNewBook || 0) + totalNewBookings;
        }
      });
    };

    addTotalBookings(total, bookingAZ);
    addTotalBookings(total, bookingLV);
    addTotalBookings(total, bookingNY);

    const records = [];
    const calculateMoMVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

    const getDaysInMonth = (dateString) => {
      const [year, month] = dateString.split("-").map(Number);
      return new Date(year, month, 0).getDate();
    };

    const addMoMVariance = (lastRecord, secondToLastRecord, bookingRecords, filter, filter2) => {
      const daysInMonthLast = getDaysInMonth(lastRecord.date);
      const daysInMonthSecond = getDaysInMonth(secondToLastRecord.date);
      if(filter === "Total"){
        const baseRecord = {
          Month: "MoM Variance %",
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatPercentage(calculateMoMVariance(lastRecord.impressions, secondToLastRecord.impressions)),
          "Clicks": formatPercentage(calculateMoMVariance(lastRecord.clicks, secondToLastRecord.clicks)),
          "Cost": formatPercentage(calculateMoMVariance(lastRecord.cost, secondToLastRecord.cost)),
          "CPC": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
          "CTR": formatPercentage(calculateMoMVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
          "Booking Total": formatPercentage(calculateMoMVariance(lastRecord.totalBook, secondToLastRecord.totalBook)),
          "CAC": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.totalBook, secondToLastRecord.cost / secondToLastRecord.totalBook)),
          "Daily Requests": formatPercentage(calculateMoMVariance(lastRecord.totalBook / daysInMonthLast, secondToLastRecord.totalBook / daysInMonthSecond)),
          "Appt Conv Rate": formatPercentage(calculateMoMVariance(lastRecord.totalBook / lastRecord.clicks, secondToLastRecord.totalBook / secondToLastRecord.clicks)),
          "New Requests": formatPercentage(calculateMoMVariance(lastRecord.totalNewBook, secondToLastRecord.totalNewBook)),
          "New Requests CAC": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.totalNewBook, secondToLastRecord.cost / secondToLastRecord.totalNewBook)),
          "Conversion": formatPercentage(calculateMoMVariance(lastRecord.conversions, secondToLastRecord.conversions)),
          "Cost Per Conv": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
          "Conv. Rate": formatPercentage(calculateMoMVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
          "MobileIVDrip.com Book Now Confirmed": formatPercentage(calculateMoMVariance(lastRecord.books, secondToLastRecord.books)),
          "MobileIVDrip.com Click - Call Now Button": formatPercentage(calculateMoMVariance(lastRecord.phone, secondToLastRecord.phone)),
        };

        records.push(baseRecord);
      } else {
        const bookingLastRecord = bookingRecords.find(j => j.month === lastRecord.date) || { data: {} };
        const bookingSecondToLastRecord = bookingRecords.find(j => j.month === secondToLastRecord.date) || { data: {} };
      
        const baseRecord = {
          Month: "MoM Variance %",
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatPercentage(calculateMoMVariance(lastRecord.impressions, secondToLastRecord.impressions)),
          "Clicks": formatPercentage(calculateMoMVariance(lastRecord.clicks, secondToLastRecord.clicks)),
          "Cost": formatPercentage(calculateMoMVariance(lastRecord.cost, secondToLastRecord.cost)),
          "CPC": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
          "CTR": formatPercentage(calculateMoMVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),   
          // "Booking Total": formatPercentage(calculateMoMVariance(bookingLastRecord.data.count2, bookingSecondToLastRecord.data.count2)),
          // "CAC": formatPercentage(calculateMoMVariance(lastRecord.cost / (bookingLastRecord.data.count2 || 0), secondToLastRecord.cost / (bookingSecondToLastRecord.data.count2 || 0))),
          // "Daily Requests": formatPercentage(calculateMoMVariance((bookingLastRecord.data.count2 || 0) / daysInMonthLast, (bookingSecondToLastRecord.data.count2 || 0) / daysInMonthSecond)),
          // "Appt Conv Rate": formatPercentage(calculateMoMVariance((bookingLastRecord.data.count2 || 0) / lastRecord.clicks, (bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks)),
          // "New Requests": formatPercentage(calculateMoMVariance(bookingLastRecord.data.count1, bookingSecondToLastRecord.data.count1)),
          // "New Requests CAC": formatPercentage(calculateMoMVariance(lastRecord.cost / (bookingLastRecord.data.count1 || 1), secondToLastRecord.cost / (bookingSecondToLastRecord.data.count1 || 0))),
          // "Conversion": formatPercentage(calculateMoMVariance(lastRecord.conversions, secondToLastRecord.conversions)),
          // "Cost Per Conv": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
          // "Conv. Rate": formatPercentage(calculateMoMVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
          // "MobileIVDrip.com Book Now Confirmed": formatPercentage(calculateMoMVariance(lastRecord.books, secondToLastRecord.books)),
          // "MobileIVDrip.com Click - Call Now Button": formatPercentage(calculateMoMVariance(lastRecord.phone, secondToLastRecord.phone)),
        };

        Object.assign(baseRecord, {
          "Booking Total": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                (bookingLastRecord.data?.Phoenix?.count2 || 0) + (bookingLastRecord.data?.Tucson?.count2 || 0),
                (bookingSecondToLastRecord.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecord.data?.Tucson?.count2 || 0)
              ))
            : formatPercentage(calculateMoMVariance(bookingLastRecord.data.count2, bookingSecondToLastRecord.data.count2)),
          "CAC": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                lastRecord.cost / ((bookingLastRecord.data?.Phoenix?.count2 || 0) + (bookingLastRecord.data?.Tucson?.count2 || 0)),
                secondToLastRecord.cost / ((bookingSecondToLastRecord.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecord.data?.Tucson?.count2 || 0))
              ))
            : formatPercentage(calculateMoMVariance(lastRecord.cost / (bookingLastRecord.data.count2 || 0), secondToLastRecord.cost / (bookingSecondToLastRecord.data.count2 || 0))),
          "Daily Requests": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                ((bookingLastRecord.data?.Phoenix?.count2 || 0) + (bookingLastRecord.data?.Tucson?.count2 || 0)) / daysInMonthLast,
                ((bookingSecondToLastRecord.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecord.data?.Tucson?.count2 || 0)) / daysInMonthSecond
              ))
            : formatPercentage(calculateMoMVariance((bookingLastRecord.data.count2 || 0) / daysInMonthLast, (bookingSecondToLastRecord.data.count2 || 0) / daysInMonthSecond)),
          "Appt Conv Rate": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                ((bookingLastRecord.data?.Phoenix?.count2 || 0) + (bookingLastRecord.data?.Tucson?.count2 || 0)) / lastRecord.clicks,
                ((bookingSecondToLastRecord.data?.Phoenix?.count2 || 0) + (bookingSecondToLastRecord.data?.Tucson?.count2 || 0)) / secondToLastRecord.clicks
              ))
            : formatPercentage(calculateMoMVariance((bookingLastRecord.data.count2 || 0) / lastRecord.clicks, (bookingSecondToLastRecord.data.count2 || 0) / secondToLastRecord.clicks)),
          "New Requests": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                ((bookingLastRecord.data?.Phoenix?.count1 || 0) + (bookingLastRecord.data?.Tucson?.count1 || 0)) / lastRecord.clicks,
                ((bookingSecondToLastRecord.data?.Phoenix?.count1 || 0) + (bookingSecondToLastRecord.data?.Tucson?.count1 || 0)) / secondToLastRecord.clicks
              ))
            : formatPercentage(calculateMoMVariance(bookingLastRecord.data.count1, bookingSecondToLastRecord.data.count1)),
          "New Requests CAC": filter === "AZ"
            ? formatPercentage(calculateMoMVariance(
                lastRecord.cost / ((bookingLastRecord.data?.Phoenix.count1 || 0) + (bookingLastRecord.data?.Tucson.count1 || 0)),
                secondToLastRecord.cost / ((bookingSecondToLastRecord.data?.Phoenix.count1 || 0) + (bookingSecondToLastRecord.data?.Tucson.count1 || 0))
              ))
            : formatPercentage(calculateMoMVariance(lastRecord.cost / (bookingLastRecord.data.count1 || 1), secondToLastRecord.cost / (bookingSecondToLastRecord.data.count1 || 0))),
        });

        Object.assign(baseRecord, {
          "Conversion": formatPercentage(calculateMoMVariance(lastRecord.conversions, secondToLastRecord.conversions)),
          "Cost Per Conv": formatPercentage(calculateMoMVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
          "Conv. Rate": formatPercentage(calculateMoMVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
          "MobileIVDrip.com Book Now Confirmed": formatPercentage(calculateMoMVariance(lastRecord.books, secondToLastRecord.books)),
          "MobileIVDrip.com Click - Call Now Button": formatPercentage(calculateMoMVariance(lastRecord.phone, secondToLastRecord.phone)),
        });

        records.push(baseRecord);
      }
    };    
   
    const addDataToRecords = (data, bookingData, filter, filter2) => { 
      if(filter === "Total"){
        data.forEach((record) => {
          const daysInMonth = getDaysInMonth(record.date);
          const baseRecord = {
            Month: record.date,
            Filter: filter,
            Filter2: filter2,
            "Impr.": formatNumber(record.impressions),
            "Clicks": formatNumber(record.clicks),
            "Cost": formatCurrency(record.cost),
            "CPC": formatCurrency(record.cost / record.clicks),
            "CTR": formatPercentage((record.clicks / record.impressions) * 100),
            "Booking Total": formatNumber(record.totalBook || 0),
            "CAC": formatCurrency(record.cost / (record.totalBook || 0) || 1),
            "Daily Requests": formatNumber((record.totalBook || 0) / daysInMonth),
            "Appt Conv Rate": formatPercentage(((record.totalBook || 0) / record.clicks) * 100 || 1),
            "New Requests": formatNumber(record.totalNewBook || 0),
            "New Requests CAC": formatCurrency((record.cost / ( record.totalNewBook || 0)) || 1),
            "Conversion": formatNumber(record.conversions),
            "Cost Per Conv": formatCurrency(record.cost / record.conversions),
            "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
            "MobileIVDrip.com Book Now Confirmed": formatNumber(record.books),
            "MobileIVDrip.com Click - Call Now Button": formatNumber(record.phone),
          };
      
          records.push(baseRecord);
        });
      } else {
        data.forEach((record) => {
          const daysInMonth = getDaysInMonth(record.date);
          const bookingRecord = bookingData.find(j => j.month === record.date) || { data: {} };
          const baseRecord = {
            Month: record.date,
            Filter: filter,
            Filter2: filter2,
            "Impr.": formatNumber(record.impressions),
            "Clicks": formatNumber(record.clicks),
            "Cost": formatCurrency(record.cost),
            "CPC": formatCurrency(record.cost / record.clicks),
            "CTR": formatPercentage((record.clicks / record.impressions) * 100),
            // "Booking Total": formatNumber(bookingRecord.data.count2 || 0),
            // "CAC": formatCurrency(record.cost / (bookingRecord.data.count2 || 0) || 1),
            // "Daily Requests": formatNumber(((bookingRecord.data.count2 || 0) / daysInMonth) || 1),
            // "Appt Conv Rate": formatPercentage(((bookingRecord.data.count2 || 0) / record.clicks) * 100 || 1),
            // "New Requests": formatNumber(bookingRecord.data.count1 || 0),
            // "New Requests CAC": formatCurrency((record.cost / (bookingRecord.data.count1 || 0)) || 1),
            // "Conversion": formatNumber(record.conversions),
            // "Cost Per Conv": formatCurrency(record.cost / record.conversions),
            // "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
            // "MobileIVDrip.com Book Now Confirmed": formatNumber(record.books),
            // "MobileIVDrip.com Click - Call Now Button": formatNumber(record.phone),
          };
          
          Object.assign(baseRecord, {
            "Booking Total": filter === "AZ"
              ? formatNumber((bookingRecord.data?.Phoenix?.count2 || 0) + (bookingRecord.data?.Tucson?.count2 || 0))
              : formatNumber(bookingRecord.data.count2 || 0),
            "CAC": filter === "AZ"
              ? formatCurrency(record.cost / ((bookingRecord.data?.Phoenix?.count2 || 0) + (bookingRecord.data?.Tucson?.count2 || 0)) || 1) 
              : formatCurrency(record.cost / (bookingRecord.data.count2 || 0) || 1),
            "Daily Requests": filter === "AZ"
              ? formatNumber((((bookingRecord.data?.Phoenix?.count2 || 0) + (bookingRecord.data?.Tucson?.count2 || 0)) / daysInMonth) || 1) 
              : formatNumber(((bookingRecord.data.count2 || 0) / daysInMonth) || 1),
            "Appt Conv Rate": filter === "AZ"
              ? formatPercentage((((bookingRecord.data?.Phoenix?.count2 || 0) + (bookingRecord.data?.Tucson?.count2 || 0)) / record.clicks) * 100 || 1) 
              : formatPercentage(((bookingRecord.data.count2 || 0) / record.clicks) * 100 || 1),
            "New Requests": filter === "AZ"
              ? formatNumber((bookingRecord.data?.Phoenix?.count1 || 0) + (bookingRecord.data?.Tucson?.count1  || 0))
              : formatNumber(bookingRecord.data.count1 || 0),
            "New Requests CAC": filter === "AZ"
              ? formatCurrency(record.cost / ((bookingRecord.data?.Phoenix?.count1  || 0) + (bookingRecord.data?.Tucson?.count1  || 0)) || 1) 
              : formatCurrency((record.cost / (bookingRecord.data.count1 || 0))),
          });

          Object.assign(baseRecord, {
            "Conversion": formatNumber(record.conversions),
            "Cost Per Conv": formatCurrency(record.cost / record.conversions),
            "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
            "MobileIVDrip.com Book Now Confirmed": formatNumber(record.books),
            "MobileIVDrip.com Click - Call Now Button": formatNumber(record.phone),
          });
      
          records.push(baseRecord);
        });
      }
    };    

    addDataToRecords(total, [], "Total", 1)
    addDataToRecords(dripAZ, bookingAZ, "AZ", 2);
    addDataToRecords(dripLV, bookingLV, "LV", 3);
    addDataToRecords(dripNYC, bookingNY, "NYC", 4);
    
    if (!date || date.trim() === '') {
      addMoMVariance(total.slice(-2)[0], total.slice(-3)[0], [], "Total", 1);
      addMoMVariance(dripAZ.slice(-2)[0], dripAZ.slice(-3)[0], bookingAZ, "AZ", 2);
      addMoMVariance(dripLV.slice(-2)[0], dripLV.slice(-3)[0], bookingLV, "LV", 3);
      addMoMVariance(dripNYC.slice(-2)[0], dripNYC.slice(-3)[0], bookingNY, "NYC", 4);
    }

    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
    
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          const baseHeader = {
            Month: record.Filter,
            Filter: "Filter",
            Filter2: "Filter2",
            "Impr.": "Impr.",
            "Clicks": "Clicks",
            "Cost": "Cost",
            "CPC": "CPC",
            "CTR": "CTR",
            "Booking Total": "Booking Total",
            "CAC": "CAC",
            "Daily Requests": "Daily Requests",
            "Appt Conv Rate": "Appt Conv Rate",
            "New Requests": "New Requests",
            "New Requests CAC": "New Requests CAC",
            "Conversion": "Conversion",
            "Cost Per Conv": "Cost Per Conv",
            "Conv. Rate": "Conv. Rate",
            "MobileIVDrip.com Book Now Confirmed": "MobileIVDrip.com Book Now Confirmed",
            "MobileIVDrip.com Click - Call Now Button": "MobileIVDrip.com Click - Call Now Button",
          };

          finalRecords.push(baseHeader);
          currentGroup = record.Filter;
        }
    
        finalRecords.push({ ...record, isBold: false });
        if (record.Month === "MoM Variance %") {
          finalRecords.push({ Month: "", Filter: "", Filter2: "", isBold: false });
        }
      });
    }    

    processGroup(records);

    const sheetData = finalRecords.map(record => {
      const baseData = [
        record.Month,
        record.Filter,
        record.Filter2,
        record["Impr."],
        record["Clicks"],
        record["Cost"],
        record["CPC"],
        record["CTR"],
        record["Booking Total"],
        record["CAC"],
        record["Daily Requests"],
        record["Appt Conv Rate"],
        record["New Requests"],
        record["New Requests CAC"],
        record["Conversion"],
        record["Cost Per Conv"],
        record["Conv. Rate"],
        record["MobileIVDrip.com Book Now Confirmed"],
        record["MobileIVDrip.com Click - Call Now Button"]
      ];
      
      return baseData;
    });    

    const dataToSend = {
      // AZ: sheetData.filter(row => ["AZ"].includes(row[0]) || ["AZ"].includes(row[1])),
      // LV: sheetData.filter(row => ["LV"].includes(row[0]) || ["LV"].includes(row[1])),
      // NYC: sheetData.filter(row => ["NYC"].includes(row[0]) || ["NYC"].includes(row[1])),
      Monthly: sheetData,
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
                    horizontalAlignment: "RIGHT",
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

    console.log("Final Mobile IV Drip monthly report sent to Google Sheets successfully!");
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
    const result = { AZ: [], allAZ:[], LV: [], NYC: [] };
    const monthlyData = { AZ: {}, allAZ:{}, LV: {}, NYC: {} };

    for (const [location, range] of Object.entries(sourceDataRanges)) {
      const { data: { values: rows } } = await sheets.spreadsheets.values.get({
        spreadsheetId: sourceSpreadsheetId,
        range,
      });
      if (!rows) continue;
    
      rows.forEach(([date, count1, , count2]) => {
        if (!date || !count1 || !count2 || isNaN(count1) || isNaN(count2) || new Date(date) < startDate) return; 
    
        const currentRowDate = new Date(date);
        const monthLabel = `${currentRowDate.getFullYear()}-${(currentRowDate.getMonth() + 1).toString().padStart(2, '0')}`;
    
        if (location.startsWith("AZ_")) {
          const subLocation = location.replace("AZ_", "");
          monthlyData.AZ[monthLabel] = monthlyData.AZ[monthLabel] || { Phoenix: { count1: 0, count2: 0 }, Tucson: { count1: 0, count2: 0 } };
          monthlyData.AZ[monthLabel][subLocation].count1 += parseInt(count1, 10);
          monthlyData.AZ[monthLabel][subLocation].count2 += parseInt(count2, 10);
        } else {
          monthlyData[location][monthLabel] = monthlyData[location][monthLabel] || { count1: 0, count2: 0 };
          monthlyData[location][monthLabel].count1 += parseInt(count1, 10);
          monthlyData[location][monthLabel].count2 += parseInt(count2, 10);
        }
      });      
    }
    
    for (const [month, data] of Object.entries(monthlyData.AZ)) {
      result.AZ.push({ month, data });
    }
    for (const [month, data] of Object.entries(monthlyData.allAZ)) {
      result.allAZ.push({ month, data });
    }
    for (const [month, data] of Object.entries(monthlyData.LV)) {
      result.LV.push({ month, data });
    }
    for (const [month, data] of Object.entries(monthlyData.NYC)) {
      result.NYC.push({ month, data });
    }    

    return result;
  } catch (error) {
    console.error("Error generating monthly data:", error);
    res.status(500).json({ error: "Internal server error" });
  }
};

module.exports = {
  executeSpecificFetchFunctionMIV,
  sendFinalMonthlyReportToGoogleSheetsMIV,
  sendBookings,
};
