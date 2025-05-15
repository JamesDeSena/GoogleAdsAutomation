const axios = require('axios');
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
  const daysSinceLast = dayOfWeek % 7; //Friday (dayOfWeek + 1) % 7; Monday (dayOfWeek + 6) % 7;

  const previousLast = new Date(today);
  previousLast.setDate(today.getDate() - daysSinceLast);

  const currentDay = new Date(previousLast);
  currentDay.setDate(previousLast.getDate() + 6);

  const startDate = '2021-10-03'; //previousFriday 2024-09-13 / 11-11
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

async function getRawCampaigns() {
  try {
    const initialResponse = await axios.get(
      "https://api.lawmatics.com/v1/prospects?page=1&fields=created_at,stage,custom_field_values",
      { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
    );

    const totalPages = initialResponse.data.meta?.total_pages || 1;
    const requests = Array.from({ length: totalPages }, (_, i) =>
      axios.get(
        `https://api.lawmatics.com/v1/prospects?page=${i + 1}&fields=created_at,stage,custom_field_values`,
        { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
      )
    );
    const responses = await Promise.all(requests);

    const allCampaigns = responses.flatMap(response =>
      response.data.stages || response.data.data || response.data.results || []
    );

    const formatDateToMMDDYYYY = (dateString) => {
      if (!dateString) return null;
      const date = new Date(dateString);
      const month = String(date.getMonth() + 1).padStart(2, "0");
      const day = String(date.getDate()).padStart(2, "0");
      const year = date.getFullYear();
      return `${month}/${day}/${year}`;
    };

    const filteredCampaigns = allCampaigns
      .filter(({ attributes }) => {
        const createdAt = attributes?.created_at;
        if (!createdAt) return false;

        const createdDate = new Date(
          new Date(createdAt).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
        );

        return createdDate >= new Date("2021-10-03T00:00:00-08:00");
      })
      .map(({ attributes, relationships }) => ({
        created_at: formatDateToMMDDYYYY(attributes.created_at),
        stage_id: relationships?.stage?.data?.id || null,
        jurisdiction: attributes?.custom_field_values?.["562886"]?.formatted_value || null,
      }));

    const eventInitialResponse = await axios.get(
      "https://api.lawmatics.com/v1/events?fields=id,name,start_date",
      { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
    );

    const eventTotalPages = eventInitialResponse.data.meta?.total_pages || 1;
    const eventRequests = Array.from({ length: eventTotalPages }, (_, i) =>
      axios.get(
        `https://api.lawmatics.com/v1/events?page=${i + 1}&fields=id,name,start_date`,
        { headers: { Authorization: `Bearer ${process.env.LAWMATICS_TOKEN}` }, maxBodyLength: Infinity }
      )
    );
    const eventResponses = await Promise.all(eventRequests);

    const allEvents = eventResponses.flatMap(response =>
      response.data.data || []
    );

    const strategySessions = allEvents
      .filter(event => {
        const eventName = event.attributes?.name;
        const startDate = event.attributes?.start_date;
        if (!eventName || !startDate) return false;

        const eventDate = new Date(
          new Date(startDate).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
        );

        return (eventName === "Strategy Session" || eventName === "AZ - Strategy Session") && eventDate >= new Date("2021-10-03T00:00:00-08:00");
      })
      .map(event => ({
        event_start: formatDateToMMDDYYYY(event.attributes?.start_date),
        event_id: event.id,
        jurisdiction: event.attributes?.name,
      }));

    const combinedData = {campaigns: filteredCampaigns, events: strategySessions};
    // console.log("Final Campaigns:", JSON.stringify(combinedData, null, 2));

    return combinedData;
  } catch (error) {
    throw new Error(
      error.response ? error.response.data : error.message
    );
  }
};

const fetchAndAggregateLPCData = async (filter) => {
  const refreshToken_Google = getStoredRefreshToken();
  if (!refreshToken_Google) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  try {
    const customer = client.Customer({
      customer_id: process.env.GOOGLE_ADS_CUSTOMER_ID_LPC,
      refresh_token: refreshToken_Google,
      login_customer_id: process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID,
    });

    const dateRanges = getOrGenerateDateRanges();
    const lpcData = await Promise.all(
      dateRanges.map(async ({ start, end }) => {
        const aggregatedData = {
          date: `${start} - ${end}`,
          clicks: 0,
          cost: 0,
        };

        let whereClause = `segments.date BETWEEN '${start}' AND '${end}'`;

        if (filter === "%CA%") {
          whereClause += ` AND campaign.name NOT LIKE '%AZ%'`;
        } else if (filter === "%AZ%") {
          whereClause += ` AND campaign.name LIKE '%AZ%'`;
        } else {
          whereClause += ` AND campaign.name LIKE '${filter}'`;
        }

        const metricsQuery = `
          SELECT
            campaign.id,
            campaign.name,
            metrics.clicks,
            metrics.cost_micros,
            segments.date
          FROM
            campaign
          WHERE
            ${whereClause}
          ORDER BY
            segments.date DESC
        `;

        let metricsPageToken = null;
        do {
          const metricsResponse = await customer.query(metricsQuery);
          metricsResponse.forEach((campaign) => {
            aggregatedData.clicks += campaign.metrics.clicks || 0;
            aggregatedData.cost += (campaign.metrics.cost_micros || 0) / 1_000_000;
          });
          metricsPageToken = metricsResponse.next_page_token;
        } while (metricsPageToken);

        return aggregatedData;
      })
    );

    return lpcData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const sendFinalWeeklyReportToGoogleSheetsLPC = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: "serviceToken.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const dataRanges = {
    CA: "CA Weekly Report",
    AZ: "AZ Weekly Report",
  };

  try {
    const { campaigns, events } = await getRawCampaigns();
    const caData = await fetchAndAggregateLPCData("%CA%");
    await new Promise((resolve) => setTimeout(resolve, 500));
    const azData = await fetchAndAggregateLPCData("%AZ%");

    const startDate = new Date("2021-10-03");
    const today = new Date();
    const weeks = { CA: {}, AZ: {} };
    const nopeStagesCA = new Set(["80193", "113690", "21589"]);
    const nopeStagesAZ = new Set(["111597", "111596"]);
    const eventLikeStagesCA = new Set(["21590", "37830", "21574", "81918", "60522", "21576", "21600", "36749", "58113", "21591", "21575"]);
    const eventLikeStagesAZ = new Set(["111631", "126229", "111632", "111633", "111634", "111635", "111636"]);

    const formatDate = (date) =>
      `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;

    const processDate = (date) => {
      if (!date) return null;
      const parsedDate = new Date(
        new Date(date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
      );
      return parsedDate < startDate ? null : parsedDate;
    };

    const processWeek = (date) => {
      let weekStart = new Date(date);
      weekStart.setDate(date.getDate() - date.getDay());
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);
      return {
        label: `${formatDate(weekStart)} - ${formatDate(weekEnd)}`,
        weekStart,
      };
    };

    campaigns.forEach(({ created_at, stage_id, jurisdiction }) => {
      const createdDate = processDate(created_at);
      if (!createdDate || createdDate > today) return;
    
      const region = jurisdiction?.toLowerCase() === "arizona" ? "AZ" : "CA";
      if (!region) return;
    
      const { label } = processWeek(createdDate);
      if (!weeks[region][label]) {
        weeks[region][label] = [label, 0, 0, 0, 0, 0, 0];
      }
    
      weeks[region][label][1]++;
      if (
        stage_id &&
        ((region === "CA" && nopeStagesCA.has(stage_id)) || (region === "AZ" && nopeStagesAZ.has(stage_id)))
      ) {
        weeks[region][label][2]++;
      }
    });

    campaigns.forEach(({ created_at, stage_id, jurisdiction }) => {
      const createdDate = processDate(created_at);
      if (!createdDate || createdDate > today) return;
    
      const region = jurisdiction?.toLowerCase() === "arizona" ? "AZ" : "CA";
      if (!region) return;
    
      const { label } = processWeek(createdDate);
      if (!weeks[region][label]) {
        weeks[region][label] = [label, 0, 0, 0, 0, 0, 0];
      }
    
      if (
        stage_id &&
        ((region === "CA" && eventLikeStagesCA.has(stage_id)) ||
         (region === "AZ" && eventLikeStagesAZ.has(stage_id)))
      ) {
        weeks[region][label][4]++;
      }
    });

    events.forEach(({ event_id, event_start, jurisdiction }) => {
      const eventDate = processDate(event_start);
      if (!eventDate || eventDate > today) return;
    
      const region = jurisdiction === "AZ - Strategy Session" ? "AZ" : "CA";
      if (!region) return;
    
      const { label } = processWeek(eventDate);
      if (!weeks[region][label]) {
        weeks[region][label] = [label, 0, 0, 0, 0, 0, 0];
      }
    
      weeks[region][label][4]++;
    });    

    if (Array.isArray(caData)) {
      caData.forEach(({ date, clicks, cost }) => {
        if (weeks["CA"][date]) {
          weeks["CA"][date][5] = cost;
          weeks["CA"][date][6] = clicks;
        }
      });
    }
    
    if (Array.isArray(azData)) {
      azData.forEach(({ date, clicks, cost }) => {
        if (weeks["AZ"][date]) {
          weeks["AZ"][date][5] = cost;
          weeks["AZ"][date][6] = clicks;
        }
      });
    }    
    
    Object.keys(weeks).forEach((region) => {
      Object.values(weeks[region]).forEach((week) => {
        week[3] = week[1] - week[2];
      });
    });

    const calculateVariance = (current, previous) => {
      if (previous === 0 || isNaN(current) || isNaN(previous)) return 0;
      return ((current - previous) / previous) * 100;
    };

    const formatPercentage = (value) =>
      isNaN(value) || !isFinite(value) ? "0.00%" : `${value.toFixed(2)}%`;

    const addWoWRow = (records) => {
      if (records.length < 3) return;

      const lastWeek = records[records.length - 2];
      const twoWeeksAgo = records[records.length - 3];

      const row = [
        "WoW Variance %",
        formatPercentage(calculateVariance(lastWeek[1], twoWeeksAgo[1])), // MQL
        formatPercentage(calculateVariance(lastWeek[2], twoWeeksAgo[2])), // Nopes
        formatPercentage(calculateVariance(lastWeek[3], twoWeeksAgo[3])), // SQL
        formatPercentage(calculateVariance(lastWeek[4], twoWeeksAgo[4])), // SS
        formatPercentage(calculateVariance(lastWeek[5], twoWeeksAgo[5])), // Cost
        formatPercentage(calculateVariance(lastWeek[6], twoWeeksAgo[6])), // Clicks
      ];

      records.push(row);
    };

    const addBiWeeklyRow = (records) => {
      if (records.length < 6) return;

      const last2Weeks = [records[records.length - 3], records[records.length - 4]];
      const prev2Weeks = [records[records.length - 5], records[records.length - 6]];

      const averageMetric = (index, weeks) =>
        weeks.reduce((sum, row) => sum + (parseFloat(row[index]) || 0), 0) / weeks.length;

      const row = [
        "Biweekly Variance %",
        formatPercentage(calculateVariance(
          averageMetric(1, last2Weeks), averageMetric(1, prev2Weeks)
        )),
        formatPercentage(calculateVariance(
          averageMetric(2, last2Weeks), averageMetric(2, prev2Weeks)
        )),
        formatPercentage(calculateVariance(
          averageMetric(3, last2Weeks), averageMetric(3, prev2Weeks)
        )),
        formatPercentage(calculateVariance(
          averageMetric(4, last2Weeks), averageMetric(4, prev2Weeks)
        )),
        formatPercentage(calculateVariance(
          averageMetric(5, last2Weeks), averageMetric(5, prev2Weeks)
        )),
        formatPercentage(calculateVariance(
          averageMetric(6, last2Weeks), averageMetric(6, prev2Weeks)
        )),
      ];

      records.push(row);
    };

    await Promise.all(
      Object.entries(weeks).map(async ([region, data]) => {
        console.log(`Processing region: ${region}`);

        const sortedWeeks = Object.values(data).sort(
          (a, b) => new Date(a[0].split(" - ")[0]) - new Date(b[0].split(" - ")[0])
        );

        if (sortedWeeks.length >= 5) {
          addWoWRow(sortedWeeks);
          addBiWeeklyRow(sortedWeeks);
        }

        const existingData = await sheets.spreadsheets.values.get({
          spreadsheetId,
          range: dataRanges[region],
        });

        const existingRows = existingData.data.values || [];
        const existingWeeks = new Map(
          existingRows.map((row, index) => {
            const weekLabel = row[0]?.trim();
            return [weekLabel, index];
          })
        );

        const batchUpdates = [];
        const newValues = [];

        sortedWeeks.forEach((weekData) => {
          const weekLabel = weekData[0].trim();
    
          if (existingWeeks.has(weekLabel)) {
            const rowIndex = existingWeeks.get(weekLabel);
            batchUpdates.push({
              range: `${dataRanges[region]}!A${rowIndex + 1}:G${rowIndex + 1}`,
              values: [weekData],
            });
          } else {
            newValues.push(weekData);
          }
        });

        try {
          if (batchUpdates.length > 0) {
            await sheets.spreadsheets.values.batchUpdate({
              spreadsheetId,
              resource: {
                valueInputOption: "RAW",
                data: batchUpdates,
              },
            });
          }
        } catch (error) {
          console.error(`Error updating ${region} sheet:`, error.response?.data || error.message);
        }

        try {
          if (newValues.length > 0) {
            await sheets.spreadsheets.values.append({
              spreadsheetId,
              range: `${dataRanges[region]}!A2:G`,
              valueInputOption: "RAW",
              insertDataOption: "INSERT_ROWS",
              resource: { values: newValues },
            });
          }
        } catch (error) {
          console.error(`Error appending to ${region} sheet:`, error.response?.data || error.message);
        }
      })
    );

    console.log("Weekly LPC Report data successfully updated!");
  } catch (error) {
    console.error("Error processing weekly campaigns:", error);
  }
};

module.exports = {
  getRawCampaigns,
  sendFinalWeeklyReportToGoogleSheetsLPC,
};
