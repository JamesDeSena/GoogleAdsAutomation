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
    AZLive: 'AZ Weekly!A2:Z',
    LVLive: 'LV Weekly!A2:X',
    NYCLive: 'NYC Weekly!A2:X',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const dripAZ = await fetchFunctions.fetchReportDataWeeklyAZ(req, res, dateRanges);
    const dripLV = await fetchFunctions.fetchReportDataWeeklyLV(req, res, dateRanges);
    const dripNYC = await fetchFunctions.fetchReportDataWeeklyNYC(req, res, dateRanges);

    const janeData = await sendJaneToGoogleSheetsMIV(req, res);
    const bookingData = await sendBookings(req, res);

    const janeAZ = janeData.Arizona || [];
    const janeLV = janeData.LasVegas || [];
    const janeNYC = janeData.NewYork || [];

    const bookingAZ = bookingData.AZ || [];
    const bookingLV = bookingData.LV || [];
    const bookingNY = bookingData.NYC || [];

    const records = [];
    const calculateWoWVariance = (current, previous) => ((current - previous) / previous) * 100;

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);

    const addWoWVariance = (lastRecord, secondToLastRecord, janeRecords, bookingRecords, filter, filter2) => {
      const janeLastRecord = janeRecords.find(j => j.week === lastRecord.date) || {};
      const janeSecondToLastRecord = janeRecords.find(j => j.week === secondToLastRecord.date) || {};
      const bookingLastRecord = bookingRecords.find(j => j.week === lastRecord.date) || {};
      const bookingSecondToLastRecord = bookingRecords.find(j => j.week === secondToLastRecord.date) || {};

      const baseRecord = {
        Week: "WoW Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(lastRecord.impressions, secondToLastRecord.impressions)),
        'Clicks': formatPercentage(calculateWoWVariance(lastRecord.clicks, secondToLastRecord.clicks)),
        'Cost': formatPercentage(calculateWoWVariance(lastRecord.cost, secondToLastRecord.cost)),
        "CPC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
        "Conversion": formatPercentage(calculateWoWVariance(lastRecord.conversions, secondToLastRecord.conversions)),
        "Cost Per Conv": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.conversions, secondToLastRecord.cost / secondToLastRecord.conversions)),
        "Conv. Rate": formatPercentage(calculateWoWVariance(lastRecord.conversions / lastRecord.interactions, secondToLastRecord.conversions / secondToLastRecord.interactions)),
        "Leads": formatPercentage(calculateWoWVariance(janeLastRecord.allBook , janeSecondToLastRecord.allBook)),
        "CPL": formatPercentage(calculateWoWVariance(lastRecord.cost / janeLastRecord.booked, secondToLastRecord.cost / janeSecondToLastRecord.booked)),
        "Calls from Ads - Local SEO": formatPercentage(calculateWoWVariance(lastRecord.calls, secondToLastRecord.calls)),
        "Book Now Form Local SEO": formatPercentage(calculateWoWVariance(lastRecord.books, secondToLastRecord.books)),
        "Phone No. Click Local SEO": formatPercentage(calculateWoWVariance(lastRecord.phone, secondToLastRecord.phone)),
        "Booked": formatPercentage(calculateWoWVariance(janeLastRecord.booked , janeSecondToLastRecord.booked)),
        "Arrived": formatPercentage(calculateWoWVariance(janeLastRecord.arrived, janeSecondToLastRecord.arrived)),
        "Archived": formatPercentage(calculateWoWVariance(janeLastRecord.archived, janeSecondToLastRecord.archived)),
        "Cancelled": formatPercentage(calculateWoWVariance(janeLastRecord.cancelled, janeSecondToLastRecord.cancelled)),
        "No Show": formatPercentage(calculateWoWVariance(janeLastRecord.no_show, janeSecondToLastRecord.no_show)),
        "Never Booked": formatPercentage(calculateWoWVariance(janeLastRecord.never_booked, janeSecondToLastRecord.never_booked)),
        "Rescheduled": formatPercentage(calculateWoWVariance(janeLastRecord.rescheduled, janeSecondToLastRecord.rescheduled)),
        // "Conv Value per Time": formatPercentage(calculateWoWVariance(lastRecord.conv_date, secondToLastRecord.conv_date)),
      };

      if (filter === "AZ") {
        Object.assign(baseRecord, {
          "Number of Appt Requests Phoenix": formatPercentage(calculateWoWVariance(bookingLastRecord.data?.Phoenix, bookingSecondToLastRecord.data?.Phoenix)),
          "Number of Appt Requests Tucson": formatPercentage(calculateWoWVariance(bookingLastRecord.data?.Tucson, bookingSecondToLastRecord.data?.Tucson)),
          "Number of Appt Requests Total": formatPercentage(calculateWoWVariance(bookingLastRecord.data?.Phoenix + bookingLastRecord.data?.Tucson, bookingSecondToLastRecord.data?.Phoenix + bookingSecondToLastRecord.data?.Tucson)),
        });
      } else {
        Object.assign(baseRecord, {
          "Number of Appt Requests": formatPercentage(calculateWoWVariance(bookingLastRecord.data, bookingSecondToLastRecord.data)),
        });
      }
      records.push(baseRecord);
    };

    const addDataToRecords = (data, janeData, bookingData, filter, filter2) => {
      data.forEach((record) => {
        const janeRecord = janeData.find(j => j.week === record.date) || {};
        const bookingRecord = bookingData.find(j => j.week === record.date) || {};
        const baseRecord = {
          Week: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          'Clicks': formatNumber(record.clicks),
          'Cost': formatCurrency(record.cost),
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          "Conversion": formatNumber(record.conversions),
          "Cost Per Conv": formatCurrency(record.cost / record.conversions),
          "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
          "Leads": formatNumber(janeRecord.allBook || 0),
          "CPL": janeRecord.booked ? formatCurrency(record.cost / janeRecord.booked) : formatCurrency(0),
          "Calls from Ads - Local SEO": formatNumber(record.calls),
          "Book Now Form Local SEO": formatNumber(record.books),
          "Phone No. Click Local SEO": formatNumber(record.phone),
          "Booked": formatNumber(janeRecord.booked || 0),
          "Arrived": formatNumber(janeRecord.arrived || 0),
          "Archived": formatNumber(janeRecord.archived || 0),
          "Cancelled": formatNumber(janeRecord.cancelled || 0),
          "No Show": formatNumber(janeRecord.no_show || 0),
          "Never Booked": formatNumber(janeRecord.never_booked || 0),
          "Rescheduled": formatNumber(janeRecord.rescheduled || 0),
          // "Conv Value per Time": record.conv_date,
        };

        if (filter === "AZ") {
          console.log("ok")
          console.log(bookingRecord)
          Object.assign(baseRecord, {
            "Number of Appt Requests Phoenix": formatNumber(bookingRecord.data?.Phoenix || 0),
            "Number of Appt Requests Tucson": formatNumber(bookingRecord.data?.Tucson || 0),
            "Number of Appt Requests Total": formatNumber((bookingRecord.data?.Phoenix || 0) + (bookingRecord.data?.Tucson || 0)),
          });
        } else {
          console.log("go")
          Object.assign(baseRecord, {
            "Number of Appt Requests": formatNumber(bookingRecord.data || 0),
          });
        }
        records.push(baseRecord);
      });
    };

    addDataToRecords(dripAZ, janeAZ, bookingAZ, "AZ", 1);
    addDataToRecords(dripLV, janeLV, bookingLV, "LV", 2);
    addDataToRecords(dripNYC, janeNYC, bookingNY, "NYC", 3);

    if (!date || date.trim() === '') {
      addWoWVariance(dripAZ.slice(-2)[0], dripAZ.slice(-3)[0], janeAZ, bookingAZ, "AZ", 1);
      addWoWVariance(dripLV.slice(-2)[0], dripLV.slice(-3)[0], janeLV, bookingLV, "LV", 2);
      addWoWVariance(dripNYC.slice(-2)[0], dripNYC.slice(-3)[0], janeNYC, bookingNY, "NYC", 3);
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
            "Conversion": "Conversion",
            "Cost Per Conv": "Cost Per Conv",
            "Conv. Rate": "Conv. Rate",
            "Leads": "Leads",
            "CPL": "CPL",
            "Calls from Ads - Local SEO": "Calls from Ads - Local SEO",
            "Book Now Form Local SEO": "Book Now Form Local SEO",
            "Phone No. Click Local SEO": "Phone No. Click Local SEO",
            "Booked": "Booked",
            "Arrived": "Arrived",
            "Archived": "Archived",
            "Cancelled": "Cancelled",
            "No Show": "No Show",
            "Never Booked": "Never Booked",
            "Rescheduled": "Rescheduled",
          };
          if (record.Filter === "AZ") {
            Object.assign(baseHeader, {
              "Number of Appt Requests Phoenix": "Number of Appt Requests Phoenix",
              "Number of Appt Requests Tucson": "Number of Appt Requests Tucson",
              "Number of Appt Requests Total": "Number of Appt Requests Total",
            });
          } else {
            Object.assign(baseHeader, {
              "Number of Appt Requests": "Number of Appt Requests",
            });
          }

          finalRecords.push(baseHeader);
          currentGroup = record.Filter;
        }
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "WoW Variance %") {
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
        record["Conversion"],
        record["Cost Per Conv"],
        record["Conv. Rate"],
        record["Leads"],
        record["CPL"],
        record["Calls from Ads - Local SEO"],
        record["Book Now Form Local SEO"],
        record["Phone No. Click Local SEO"],
        record["Booked"],
        record["Arrived"],
        record["Archived"],
        record["Cancelled"],
        record["No Show"],
        record["Never Booked"],
        record["Rescheduled"]
        // record["Conv Value per Time"],
      ];
    
      if (record.Filter === "AZ") {
        baseData.push(
          record["Number of Appt Requests Phoenix"],
          record["Number of Appt Requests Tucson"],
          record["Number of Appt Requests Total"]
        );
      } else {
        baseData.push(record["Number of Appt Requests"]);
      }
    
      return baseData;
    });

    const dataToSend = {
      // AZ: sheetData.filter(row => ["AZ"].includes(row[0]) || ["AZ"].includes(row[1])),
      // LV: sheetData.filter(row => ["LV"].includes(row[0]) || ["LV"].includes(row[1])),
      // NYC: sheetData.filter(row => ["NYC"].includes(row[0]) || ["NYC"].includes(row[1])),
      AZLive: sheetData.filter(row => ["AZ"].includes(row[0]) || ["AZ"].includes(row[1])),
      LVLive: sheetData.filter(row => ["LV"].includes(row[0]) || ["LV"].includes(row[1])),
      NYCLive: sheetData.filter(row => ["NYC"].includes(row[0]) || ["NYC"].includes(row[1])),
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

    console.log("Final Mobile IV Drip weekly report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

const sendJaneToGoogleSheetsMIV = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const sourceSpreadsheetId = process.env.SHEET_JANE;
  const sourceDataRange = 'Data Record!A2:Y';

  const sheetNames = {
    "Mobile IV Drip - Las Vegas": "LasVegas",
    "Mobile IV Drip - Arizona": "Arizona",
    "Mobile IV Drip - New York": "NewYork",
  };

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sourceSpreadsheetId,
      range: sourceDataRange,
    });

    const rows = response.data.values;

    if (!rows || rows.length === 0) {
      return res.json({ message: "No data found" });
    }

    const validStatuses = new Set(["arrived", "booked", "archived", "cancelled", "no_show", "never_booked", "rescheduled"]);
    const startDate = new Date('2024-11-11');
    const weeksByLocation = {};
    const totalBookedByWeek = {};

    rows.forEach(row => {
      const name = row[1];
      const date = row[2] ? row[2].split(" ")[0] : null;
      const status = row[14];

      if (!date || !validStatuses.has(status) || new Date(date) < startDate) return;

      const currentRowDate = new Date(date);
      const dayOfWeek = currentRowDate.getDay();
      const diffToMonday = (dayOfWeek + 6) % 7;

      const weekStart = new Date(currentRowDate);
      weekStart.setDate(currentRowDate.getDate() - diffToMonday);

      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);

      const weekLabel = `${weekStart.getFullYear()}-${(weekStart.getMonth() + 1).toString().padStart(2, '0')}-${weekStart.getDate().toString().padStart(2, '0')} - ${weekEnd.getFullYear()}-${(weekEnd.getMonth() + 1).toString().padStart(2, '0')}-${weekEnd.getDate().toString().padStart(2, '0')}`;

      if (!weeksByLocation[name]) {
        weeksByLocation[name] = {};
      }

      if (!weeksByLocation[name][weekLabel]) {
        weeksByLocation[name][weekLabel] = { arrived: 0, booked: 0, archived: 0, cancelled: 0, no_show: 0, never_booked: 0, rescheduled: 0 };
      }

      weeksByLocation[name][weekLabel][status]++;

      if (!totalBookedByWeek[weekLabel]) {
        totalBookedByWeek[weekLabel] = 0;
      }

      if (status === "booked") {
        totalBookedByWeek[weekLabel]++;
      }
    });

    const result = {};
    for (const name in weeksByLocation) {
      const sheetKey = sheetNames[name] || "Other";
      result[sheetKey] = Object.keys(weeksByLocation[name]).map(weekLabel => {
        const weekData = weeksByLocation[name][weekLabel];
        return {
          week: weekLabel,
          allData: (weekData.arrived + weekData.booked + weekData.archived + weekData.cancelled + weekData.no_show + weekData.never_booked + weekData.rescheduled) || 0, 
          allBook: totalBookedByWeek[weekLabel] || 0,
          arrived: weekData.arrived || 0,
          booked: weekData.booked || 0,
          archived: weekData.archived || 0,
          cancelled: weekData.cancelled || 0,
          no_show: weekData.no_show || 0,
          never_booked: weekData.never_booked || 0,
          rescheduled: weekData.rescheduled || 0,
        };
      });
    }
    return result;
  } catch (error) {
    console.error("Error generating test data:", error);
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
    "LV": "LV Bookings Data!A2:D",
    "NYC": "NY Bookings Data!A2:D",
  };

  try {
    const startDate = new Date('2024-11-11');
    const result = { AZ: [], LV: [], NYC: [] };
    const weeklyData = { AZ: {}, LV: {}, NYC: {} };

    for (const [location, range] of Object.entries(sourceDataRanges)) {
      const { data: { values: rows } } = await sheets.spreadsheets.values.get({
        spreadsheetId: sourceSpreadsheetId,
        range,
      });
      if (!rows) continue;

      rows.forEach(([date, , , count]) => {

        if (!date || !count || isNaN(count) || new Date(date) < startDate) return;
      
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
          weeklyData.AZ[weekLabel] = weeklyData.AZ[weekLabel] || { Phoenix: 0, Tucson: 0 };
          weeklyData.AZ[weekLabel][subLocation] += parseInt(count, 10);
        } else {
          weeklyData[location][weekLabel] = (weeklyData[location][weekLabel] || 0) + parseInt(count, 10);
        }
      });      
    }

    for (const [week, data] of Object.entries(weeklyData.AZ)) {
      result.AZ.push({ week, data });
    }
    for (const [week, count] of Object.entries(weeklyData.LV)) {
      result.LV.push({ week, data: count });
    }
    for (const [week, count] of Object.entries(weeklyData.NYC)) {
      result.NYC.push({ week, data: count });
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
  sendJaneToGoogleSheetsMIV,
  sendBookings
};
