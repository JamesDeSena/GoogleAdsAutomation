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
  const startDate = '2024-12';
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
  fetchReportDatamonthlyAZ: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPAZ, "Mobile IV Drip AZ"),
  fetchReportDatamonthlyLV: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPLV, "Mobile IV Drip LV"),
  fetchReportDatamonthlyNYC: createFetchFunction(process.env.GOOGLE_ADS_CUSTOMER_ID_DRIPNYC, "Mobile IV Drip NYC"),
};

const executeSpecificFetchFunctionMIV = async (req, res) => {
  const functionName = "fetchReportDatamonthlyAZ";
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
    Monthly: 'Monthly View!A2:T',
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const dripAZ = await fetchFunctions.fetchReportDatamonthlyAZ(req, res, dateRanges);
    const dripLV = await fetchFunctions.fetchReportDatamonthlyLV(req, res, dateRanges);
    const dripNYC = await fetchFunctions.fetchReportDatamonthlyNYC(req, res, dateRanges);

    const janeData = await sendJaneToGoogleSheetsMIV(req, res);
    const bookingData = await sendBookings(req, res);

    const janeAZ = janeData.Arizona || [];
    const janeLV = janeData.LasVegas || [];
    const janeNYC = janeData.NewYork || [];

    const bookingAZ = bookingData.AZ || [];
    const bookingLV = bookingData.LV || [];
    const bookingNY = bookingData.NYC || [];

    const records = [];

    const formatCurrency = (value) => `$${value.toFixed(2)}`;
    const formatPercentage = (value) => `${value.toFixed(2)}%`;
    const formatNumber = (value) => value % 1 === 0 ? value : value.toFixed(2);
   
    const addDataToRecords = (data, janeData, bookingData, filter, filter2) => { 
      data.forEach((record) => {
        const janeRecord = janeData.find(j => j.month === record.date) || {};
        const bookingRecord = bookingData.find(j => j.month === record.date) || {};
        const baseRecord = {
          month: record.date,
          Filter: filter,
          Filter2: filter2,
          "Impr.": formatNumber(record.impressions),
          "Clicks": formatNumber(record.clicks),
          "Cost": formatCurrency(record.cost),
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          "Conversion": formatNumber(record.conversions),
          "Cost Per Conv": formatCurrency(record.cost / record.conversions),
          "Conv. Rate": formatPercentage((record.conversions / record.interactions) * 100),
          "Leads": formatNumber(janeRecord.allBook || 0),
          "CPL": janeRecord.booked ? formatCurrency(record.cost / janeRecord.booked) : formatCurrency(0),
        };
    
        if (filter === "AZ") {
          Object.assign(baseRecord, {
            "Number of Appt Requests Total": formatNumber(
              (bookingRecord.data?.Phoenix || 0) + (bookingRecord.data?.Tucson || 0)
            ),
          });
        } else {
          Object.assign(baseRecord, {
            "Number of Appt Requests": formatNumber(bookingRecord.data || 0),
          });
        }
        
        Object.assign(baseRecord, {
          "CAC": filter === "AZ"
            ? formatCurrency(record.cost / ((bookingRecord.data?.Phoenix || 0) + (bookingRecord.data?.Tucson || 0)) || 1) 
            : formatCurrency(record.cost / (bookingRecord.data || 0) || 1),
          "Calls from Ads - Local SEO": formatNumber(record.calls),
          "Book Now Form Local SEO": formatNumber(record.books),
          "Phone No. Click Local SEO": formatNumber(record.phone),
        });
    
        if (filter === "AZ") {
          Object.assign(baseRecord, {
            "Number of Appt Requests Phoenix": formatNumber(bookingRecord.data?.Phoenix || 0),
            "Number of Appt Requests Tucson": formatNumber(bookingRecord.data?.Tucson || 0),
          });
        }
    
        records.push(baseRecord);
      });
    };    

    addDataToRecords(dripAZ, janeAZ, bookingAZ, "AZ", 1);
    addDataToRecords(dripLV, janeLV, bookingLV, "LV", 2);
    addDataToRecords(dripNYC, janeNYC, bookingNY, "NYC", 3);

    records.sort((a, b) => a.Filter2 - b.Filter2);

    const finalRecords = [];

    function processGroup(records) {
      let currentGroup = '';
    
      records.forEach(record => {
        if (record.Filter !== currentGroup) {
          const baseHeader = {
            month: record.Filter,
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
          };
    
          if (record.Filter.trim().toUpperCase() === "AZ") {
            Object.assign(baseHeader, {
              "Number of Appt Requests Total": "Number of Appt Requests Total",
            });
          } else {
            Object.assign(baseHeader, {
              "Number of Appt Requests": "Number of Appt Requests",
            });
          }          
    
          Object.assign(baseHeader, {
            "CAC": "CAC",
            "Calls from Ads - Local SEO": "Calls from Ads - Local SEO",
            "Book Now Form Local SEO": "Book Now Form Local SEO",
            "Phone No. Click Local SEO": "Phone No. Click Local SEO",
          });
    
          if (record.Filter.trim().toUpperCase() === "AZ") {
            Object.assign(baseHeader, {
              "Number of Appt Requests Phoenix": "Number of Appt Requests Phoenix",
              "Number of Appt Requests Tucson": "Number of Appt Requests Tucson",
            });
          }

          finalRecords.push(baseHeader);
          currentGroup = record.Filter;
        }
    
        finalRecords.push({ ...record, isBold: false });
      });
    }    

    processGroup(records);

    const sheetData = finalRecords.map(record => {
      const baseData = [
        record.month,
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
      ];
    
      if (record.Filter === "AZ") {
        baseData.push(record["Number of Appt Requests Total"]);
      } else {
        baseData.push(record["Number of Appt Requests"]);
      }
    
      baseData.push(
        record["CAC"],
        record["Calls from Ads - Local SEO"],
        record["Book Now Form Local SEO"],
        record["Phone No. Click Local SEO"]
      );
    
      if (record.Filter === "AZ") {
        baseData.push(
          record["Number of Appt Requests Phoenix"],
          record["Number of Appt Requests Tucson"]
        );
      }
      
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
    const startDate = new Date('2024-11-01');
    const monthsByLocation = {};
    const totalBookedByMonth = {};

    rows.forEach(row => {
      const name = row[1];
      const date = row[2] ? row[2].split(" ")[0] : null;
      const status = row[14];

      if (!date || !validStatuses.has(status) || new Date(date) < startDate) return;

      const currentRowDate = new Date(date);
      const monthLabel = `${currentRowDate.getFullYear()}-${(currentRowDate.getMonth() + 1).toString().padStart(2, '0')}`;

      if (!monthsByLocation[name]) {
        monthsByLocation[name] = {};
      }

      if (!monthsByLocation[name][monthLabel]) {
        monthsByLocation[name][monthLabel] = { arrived: 0, booked: 0, archived: 0, cancelled: 0, no_show: 0, never_booked: 0, rescheduled: 0 };
      }

      monthsByLocation[name][monthLabel][status]++;

      if (!totalBookedByMonth[monthLabel]) {
        totalBookedByMonth[monthLabel] = 0;
      }

      if (status === "booked") {
        totalBookedByMonth[monthLabel]++;
      }
    });

    const result = {};
    for (const name in monthsByLocation) {
      const sheetKey = sheetNames[name] || "Other";
      result[sheetKey] = Object.keys(monthsByLocation[name]).map(monthLabel => {
        const monthData = monthsByLocation[name][monthLabel];
        return {
          month: monthLabel,
          allData: (monthData.arrived + monthData.booked + monthData.archived + monthData.cancelled + monthData.no_show + monthData.never_booked + monthData.rescheduled) || 0, 
          allBook: totalBookedByMonth[monthLabel] || 0,
          arrived: monthData.arrived || 0,
          booked: monthData.booked || 0,
          archived: monthData.archived || 0,
          cancelled: monthData.cancelled || 0,
          no_show: monthData.no_show || 0,
          never_booked: monthData.never_booked || 0,
          rescheduled: monthData.rescheduled || 0,
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
    const monthlyData = { AZ: {}, LV: {}, NYC: {} };

    for (const [location, range] of Object.entries(sourceDataRanges)) {
      const { data: { values: rows } } = await sheets.spreadsheets.values.get({
        spreadsheetId: sourceSpreadsheetId,
        range,
      });
      if (!rows) continue;

      rows.forEach(([date, , , count]) => {
        if (!date || !count || isNaN(count) || new Date(date) < startDate) return;

        const currentRowDate = new Date(date);
        const monthLabel = `${currentRowDate.getFullYear()}-${(currentRowDate.getMonth() + 1).toString().padStart(2, '0')}`;

        if (location.startsWith("AZ_")) {
          const subLocation = location.replace("AZ_", "");
          monthlyData.AZ[monthLabel] = monthlyData.AZ[monthLabel] || { Phoenix: 0, Tucson: 0 };
          monthlyData.AZ[monthLabel][subLocation] += parseInt(count, 10);
        } else {
          monthlyData[location][monthLabel] = (monthlyData[location][monthLabel] || 0) + parseInt(count, 10);
        }
      });      
    }

    for (const [month, data] of Object.entries(monthlyData.AZ)) {
      result.AZ.push({ month, data });
    }
    for (const [month, count] of Object.entries(monthlyData.LV)) {
      result.LV.push({ month, data: count });
    }
    for (const [month, count] of Object.entries(monthlyData.NYC)) {
      result.NYC.push({ month, data: count });
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
  sendJaneToGoogleSheetsMIV,
  sendBookings
};
