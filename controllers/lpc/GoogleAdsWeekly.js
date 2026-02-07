const axios = require('axios');
const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const fs = require('fs');

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
  const formatDateToMMDDYYYY = (dateString) => {
    if (!dateString) return null;
    const date = new Date(dateString);
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  };

  const fetchPaginatedData = async (baseUrl, token, batchSize) => {
    const allData = [];
    const headers = { Authorization: `Bearer ${token}` };

    const initialResponse = await axios.get(`${baseUrl}&page=1`, { headers, maxBodyLength: Infinity });
    const totalPages = initialResponse.data.meta?.total_pages || 1;

    for (let i = 0; i < totalPages; i += batchSize) {
      const batchPromises = [];
      const endOfBatch = Math.min(i + batchSize, totalPages);

      for (let j = i; j < endOfBatch; j++) {
        const pageNumber = j + 1;
        batchPromises.push(
          axios.get(`${baseUrl}&page=${pageNumber}`, { headers, maxBodyLength: Infinity })
        );
      }
      const batchResponses = await Promise.all(batchPromises);
      batchResponses.forEach(response => allData.push(...(response.data.data || [])));
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    return allData;
  };

  try {
    const LAWMATICS_TOKEN = process.env.LAWMATICS_TOKEN;
    const BATCH_SIZE = 20;

    const [allCampaignsData, allEventsData] = await Promise.all([
      fetchPaginatedData("https://api.lawmatics.com/v1/prospects?fields=created_at,stage,custom_field_values,utm_source", LAWMATICS_TOKEN, BATCH_SIZE),
      fetchPaginatedData("https://api.lawmatics.com/v1/events?fields=id,name,start_date,canceled_at,attendee_name", LAWMATICS_TOKEN, BATCH_SIZE)
    ]);
    
    const filteredCampaigns = allCampaignsData
      .filter(({ attributes }) => {
        if (!attributes?.created_at) return false;
        const createdDate = new Date(new Date(attributes.created_at).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
        if (createdDate < new Date("2024-01-T00:00:00-08:00")) return false;
        // if (!/(google|bing)/i.test(attributes?.utm_source || "")) return false;
        if ((attributes?.utm_source || "").toLowerCase() === "metaads") return false;
        return true;
      })
      .map(({ attributes, relationships }) => ({
        created_at: formatDateToMMDDYYYY(attributes.created_at),
        stage_id: relationships?.stage?.data?.id || null,
        jurisdiction: attributes?.custom_field_values?.["635624"]?.formatted_value || null,
        source: attributes?.utm_source || null,
      }));

    const strategySessions = allEventsData
      .filter(event => {
        const { name, start_date, canceled_at } = event.attributes || {};
        if (!name || !start_date) return false;
        if (canceled_at) return false;
        const eventDate = new Date(new Date(start_date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
        return eventDate >= new Date("2021-10-03T00:00:00-08:00");
      })
      .map(event => ({
        event_start: formatDateToMMDDYYYY(event.attributes?.start_date),
        event_id: event.id,
        jurisdiction: event.attributes?.name,
        name: event.attributes?.attendee_name
      }));

    return { campaigns: filteredCampaigns, events: strategySessions };
  } catch (error) {
    if (error.response) {
      console.error("API Error:", error.response.status, error.response.data);
      throw new Error(`API returned status ${error.response.status}`);
    } else if (error.request) {
      console.error("Network Error:", error.message);
      throw new Error("Network error or timeout connecting to API.");
    } else {
      console.error("Script Error:", error.message);
      throw new Error(`Script error: ${error.message}`);
    }
  }
}

const fetchAndAggregateLPCData = async (filter) => {
  const refreshToken_Google = getStoredGoogleToken();
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

        if (filter === "CA") {
          whereClause += ` AND campaign.name REGEXP_MATCH '.*CA_.*'`;
        } else if (filter === "AZ") {
          whereClause += ` AND campaign.name REGEXP_MATCH '.*AZ_.*'`;
        } else if (filter === "WA") {
          whereClause += ` AND campaign.name REGEXP_MATCH '.*WA_.*'`;
        } else {
          whereClause += ` AND campaign.name REGEXP_MATCH '.*${filter}.*'`;
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
}

const sendFinalWeeklyReportToGoogleSheetsLPC = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: "serviceToken.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_LPC;
  const dataRanges = { CA: "CA Weekly Report", AZ: "AZ Weekly Report", WA: "WA Weekly Report" };

  try {
    const { campaigns, events } = await getRawCampaigns();
    
    const caLpc = await fetchAndAggregateLPCData("CA");
    await new Promise((r) => setTimeout(r, 5000)); 
    const azLpc = await fetchAndAggregateLPCData("AZ");
    await new Promise((r) => setTimeout(r, 5000));
    const waLpc = await fetchAndAggregateLPCData("WA");

    const startDate = new Date("2021-10-03");
    
    const today = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));

    // 1. Initialize Object-Based Storage
    const weeks = { CA: {}, AZ: {}, WA: {} };

    const nopeStages = {
      CA: new Set(["21589", "80193", "113690", "26783"]),
      AZ: new Set(["111596", "111597", "111599"]),
      WA: new Set(["110790", "110791", "110793", "110794"]),
    };

    const eventLikeStages = {
      CA: new Set(["21590", "37830", "21574", "135261", "81918", "60522", "21576", "21600", "36749", "58113", "21591", "21575"]),
      AZ: new Set(["111631", "126229", "111632", "111633", "111634", "129101", "111635", "111636"]),
      WA: new Set(["144176", "144177", "143884", "144178", "144179", "144180", "144181", "144182", "144183"]),
    };

    const formatDate = (date) =>
      `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;

    const processDate = (date) => {
      if (!date) return null;
      const parsedDate = new Date(new Date(date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
      
      if (parsedDate > today) return null; 
      
      return parsedDate < startDate ? null : parsedDate;
    };

    const getWeekLabel = (date) => {
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - date.getDay()); // Sunday
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6); // Saturday
      return { 
        label: `${formatDate(weekStart)} - ${formatDate(weekEnd)}`, 
        sortDate: weekStart 
      };
    };

    const getWeekEntry = (region, label) => {
      if (!weeks[region][label]) {
        weeks[region][label] = {
          label: label,
          leads: 0, 
          nopes: 0, 
          ss: 0, // Events only
          cost: 0, 
          clicks: 0
        };
      }
      return weeks[region][label];
    };

    // --- 2. Process Campaigns (Leads & Nopes Only) ---
    campaigns.forEach(({ created_at, stage_id, jurisdiction }) => {
      const createdDate = processDate(created_at);
      if (!createdDate) return;

      const { label } = getWeekLabel(createdDate);

      // Region Detection
      const region =
        (eventLikeStages.AZ.has(stage_id) || nopeStages.AZ.has(stage_id)) ? "AZ" :
        (eventLikeStages.CA.has(stage_id) || nopeStages.CA.has(stage_id)) ? "CA" :
        (eventLikeStages.WA.has(stage_id) || nopeStages.WA.has(stage_id)) ? "WA" :
        jurisdiction?.toLowerCase() === "arizona" ? "AZ" :
        jurisdiction?.toLowerCase() === "california" ? "CA" :
        jurisdiction?.toLowerCase() === "washington" ? "WA" : null;

      if (!region) return;

      const weekData = getWeekEntry(region, label);

      weekData.leads++;
      if (nopeStages[region].has(stage_id)) weekData.nopes++;
    });

    // --- 3. Process Events (Strategy Sessions) ---
    events.forEach(({ event_start, jurisdiction }) => {
      const eventDate = processDate(event_start);
      if (!eventDate) return;

      const { label } = getWeekLabel(eventDate);
      const j = jurisdiction?.trim() || "";
      let region = null;

      if (j === "AZ - Strategy Session" || j.startsWith("AZ - Strategy Session -")) region = "AZ";
      else if (j === "CA - Strategy Session" || j.startsWith("CA - Strategy Session -")) region = "CA";
      else if (j === "WA - Strategy Session" || j.startsWith("WA - Strategy Session -")) region = "WA";

      if (!region && j === "Strategy Session" &&
          eventDate.getFullYear() === 2025 &&
          eventDate.getMonth() < 11) {
        region = "CA";
      }

      if (region) {
        const weekData = getWeekEntry(region, label);
        weekData.ss++;
      }
    });

    // --- 4. Integrate LPC Marketing Data ---
    const applyLPC = (data, region) => {
      if (!Array.isArray(data)) return;
      data.forEach(({ date, clicks, cost }) => {
        if (weeks[region][date]) {
          weeks[region][date].cost = parseFloat(cost) || 0;
          weeks[region][date].clicks = parseFloat(clicks) || 0;
        }
      });
    };

    applyLPC(caLpc, "CA");
    applyLPC(azLpc, "AZ");
    applyLPC(waLpc, "WA");

    // --- 5. Format Data for Google Sheets ---
    const formatRowForSheets = (weekData) => {
      const confirmed = weekData.leads - weekData.nopes;
      const cpl = confirmed > 0 ? weekData.cost / confirmed : 0;
      const cvr = weekData.clicks > 0 ? (weekData.leads / weekData.clicks) * 100 : 0;

      // Columns: [Date, Total Forms, No Shows, Confirmed, SS, Cost, Clicks, CPL, CVR]
      return [
        weekData.label,
        "'" + weekData.leads,
        "'" + weekData.nopes,
        "'" + confirmed,
        "'" + weekData.ss,
        "$" + (weekData.cost || 0).toFixed(2),
        "'" + weekData.clicks,
        "$" + cpl.toFixed(2),
        cvr.toFixed(2) + "%"
      ];
    };

    const calculateVariance = (current, previous) => {
      const clean = (val) => parseFloat(String(val).replace(/[$,%']/g, '')) || 0;
      const currVal = clean(current);
      const prevVal = clean(previous);
      
      if (prevVal === 0) return null;
      return ((currVal - prevVal) / prevVal) * 100;
    };

    const finalData = {};

    Object.keys(weeks).forEach((region) => {
      const sortedKeys = Object.keys(weeks[region]).sort((a, b) => 
        new Date(a.split(" - ")[0]) - new Date(b.split(" - ")[0])
      );

      let records = sortedKeys.map(key => formatRowForSheets(weeks[region][key]));
      
      // Variance Logic: Compare Last Completed Week vs Week Before That
      // (Excludes the "Current" incomplete week from variance calc)
      if (records.length >= 3) {
        const lastComplete = records[records.length - 2];
        const prevComplete = records[records.length - 3];

        const wowRow = ["WoW Variance %"];
        for (let i = 1; i <= 8; i++) {
          const v = calculateVariance(lastComplete[i], prevComplete[i]);
          wowRow.push(v === null ? "N/A" : v.toFixed(2) + "%");
        }
        records.push(wowRow);
      }

      if (records.length >= 6) {
        const last2 = [records[records.length - 2], records[records.length - 3]];
        const prev2 = [records[records.length - 4], records[records.length - 5]];

        const avg = (idx, arr) => {
          const sum = arr.reduce((acc, row) => acc + (parseFloat(String(row[idx]).replace(/[$,%']/g, '')) || 0), 0);
          return sum / arr.length;
        };

        const biRow = ["Biweekly Variance %"];
        for (let i = 1; i <= 8; i++) {
          const v = calculateVariance(avg(i, last2), avg(i, prev2));
          biRow.push(v === null ? "N/A" : v.toFixed(2) + "%");
        }
        records.push(biRow);
      }

      finalData[region] = records;
    });

    // --- 6. Sync to Sheets ---
    await Promise.all(Object.entries(finalData).map(async ([region, sortedWeeks]) => {
      const existingData = await sheets.spreadsheets.values.get({ spreadsheetId, range: dataRanges[region] });
      const existingRows = existingData.data.values || [];
      const existingWeeks = new Map(existingRows.map((row, i) => [row[0]?.trim(), i]));

      const batchUpdates = [];
      const newValues = [];
      
      sortedWeeks.forEach(weekData => {
        const label = weekData[0].trim();
        
        if (existingWeeks.has(label)) {
          batchUpdates.push({ 
            range: `${dataRanges[region]}!A${existingWeeks.get(label) + 1}`, 
            values: [weekData] 
          });
        } else {
          newValues.push(weekData);
        }
      });

      if (batchUpdates.length > 0) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId,
          resource: { valueInputOption: "USER_ENTERED", data: batchUpdates }
        });
      }
      if (newValues.length > 0) {
        await sheets.spreadsheets.values.append({
          spreadsheetId,
          range: dataRanges[region],
          valueInputOption: "USER_ENTERED",
          resource: { values: newValues },
        });
      }
    }));

    console.log("Weekly LPC Report data successfully updated!");
  } catch (error) {
    console.error("Error processing weekly campaigns:", error);
  }
};

async function testLawmatics() {
  const formatDateToMMDDYYYY = (dateString) => {
    if (!dateString) return null;
    const date = new Date(dateString);
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  };

  const fetchPaginatedData = async (baseUrl, token, batchSize) => {
    const allData = [];
    const headers = { Authorization: `Bearer ${token}` };

    const initialResponse = await axios.get(`${baseUrl}&page=1`, { headers, maxBodyLength: Infinity });
    const totalPages = initialResponse.data.meta?.total_pages || 1;

    for (let i = 0; i < totalPages; i += batchSize) {
      const batchPromises = [];
      const endOfBatch = Math.min(i + batchSize, totalPages);

      for (let j = i; j < endOfBatch; j++) {
        const pageNumber = j + 1;
        batchPromises.push(
          axios.get(`${baseUrl}&page=${pageNumber}`, { headers, maxBodyLength: Infinity })
        );
      }

      const batchResponses = await Promise.all(batchPromises);
      batchResponses.forEach((response) => allData.push(...(response.data.data || [])));
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    return allData;
  };

  try {
    const LAWMATICS_TOKEN = process.env.LAWMATICS_TOKEN;
    const BATCH_SIZE = 20;

    const allCampaignsData = await fetchPaginatedData(
      "https://api.lawmatics.com/v1/prospects?fields=created_at,stage,custom_field_values,utm_source",
      LAWMATICS_TOKEN,
      BATCH_SIZE
    );

    const startDate = new Date("2025-10-01T00:00:00-07:00"); 
    const endDate = new Date("2025-11-30T23:59:59-07:00");

    const excludedStageIds = new Set([
      "144176","144177","143884","144179","144180","144181","144182", "144183"
    ]);

    const filteredCampaigns = allCampaignsData
      .filter(({ attributes, relationships }) => {
        if (!attributes?.created_at) return false;

        const stageId = relationships?.stage?.data?.id;
        if (!excludedStageIds.has(stageId)) return false;

        const jurisdiction = attributes?.custom_field_values?.["562886"]?.formatted_value || null;
        if (jurisdiction && !/AZ|Arizona/i.test(jurisdiction)) return false;

        const createdDate = new Date(
          new Date(attributes.created_at).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
        );
        if (createdDate < startDate || createdDate > endDate) return false;

        // ✅ only allow google utm_source
        // if ((attributes?.utm_source || "").toLowerCase() !== "google") return false;
        if ((attributes?.utm_source || "").toLowerCase() === "metaads") return false;

        return true;
      })
      .map(({ attributes, relationships }) => ({
        created_at: formatDateToMMDDYYYY(attributes.created_at),
        stage_id: relationships?.stage?.data?.id || null,
        jurisdiction: attributes?.custom_field_values?.["562886"]?.formatted_value || null,
        source: attributes?.utm_source || null,
      }));

    // --- Weekly grouping ---
    const groupedByWeek = {};
    const weekStartRef = new Date("2025-07-27T00:00:00");

    filteredCampaigns.forEach((campaign) => {
      const date = new Date(campaign.created_at);
      const diffDays = Math.floor((date - weekStartRef) / (1000 * 60 * 60 * 24));
      const weekIndex = Math.floor(diffDays / 7);

      const weekStart = new Date(weekStartRef);
      weekStart.setDate(weekStartRef.getDate() + weekIndex * 7);
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);

      const weekLabel = `${formatDateToMMDDYYYY(weekStart)} - ${formatDateToMMDDYYYY(weekEnd)}`;

      if (!groupedByWeek[weekLabel]) {
        groupedByWeek[weekLabel] = { count: 0, campaigns: [] };
      }

      groupedByWeek[weekLabel].campaigns.push(campaign);
      groupedByWeek[weekLabel].count++;
    });

    fs.writeFileSync("campaigns_weekly_report.json", JSON.stringify(groupedByWeek, null, 2));

    return groupedByWeek;
  } catch (error) {
    if (error.response) {
      console.error("API Error:", error.response.status, error.response.data);
      throw new Error(`API returned status ${error.response.status}`);
    } else if (error.request) {
      console.error("Network Error:", error.message);
      throw new Error("Network error or timeout connecting to API.");
    } else {
      console.error("Script Error:", error.message);
      throw new Error(`Script error: ${error.message}`);
    }
  }
}

module.exports = {
  testLawmatics,
  getRawCampaigns,
  sendFinalWeeklyReportToGoogleSheetsLPC,
};
