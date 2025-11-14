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
      fetchPaginatedData("https://api.lawmatics.com/v1/events?fields=id,name,start_date", LAWMATICS_TOKEN, BATCH_SIZE)
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
        jurisdiction: attributes?.custom_field_values?.["562886"]?.formatted_value || null,
        source: attributes?.utm_source || null,
      }));

    const strategySessions = allEventsData
      .filter(event => {
        const { name, start_date } = event.attributes || {};
        if (!name || !start_date) return false;
        const eventDate = new Date(new Date(start_date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
        return (name === "Strategy Session" || (/^AZ\b/.test(name))) && eventDate >= new Date("2021-10-03T00:00:00-08:00");
      })
      .map(event => ({
        event_start: formatDateToMMDDYYYY(event.attributes?.start_date),
        event_id: event.id,
        jurisdiction: event.attributes?.name,
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
    const caData = await fetchAndAggregateLPCData("CA");
    await new Promise((r) => setTimeout(r, 10000));
    const azData = await fetchAndAggregateLPCData("AZ");
    await new Promise((r) => setTimeout(r, 10000));
    const waData = await fetchAndAggregateLPCData("WA");

    const startDate = new Date("2021-10-03");
    const today = new Date();
    const weeks = { CA: {}, AZ: {}, WA: {} };

    const nopeStages = {
      CA: new Set(["80193", "113690", "21589"]), //CA - Initial Call - Not something we handle, CA - At Capacity - Refer Out, CA - Initial Call - Not Moving Forward
      AZ: new Set(["111596", "111597"]), //AZ - Initial Call Not Moving Forward, AZ - Initial Call Not Something We Handle, 
      WA: new Set(["110790", "110790", "110793"]), //WA - Initial Call - Not Moving Forward, WA - Initial Call - Not Something We Handle, WA - At Capacity - Refer Out
    };

    const eventLikeStages = {
      CA: new Set(["21590","37830","21574","81918","60522","21576","21600","36749","58113","21591","21575"]), //Strategy Session xStrategy Session - Completed xOn Hold / Asked to reach out at a later date
      AZ: new Set(["111631","126229","111632","111633","111634","111635","111636"]), //Strategy Session xAZ - Strategy Session - Completed xAZ - On Hold
      WA: new Set(["144176","144177","143884","144179","144180","144181","144182", "144183"]), //Strategy Session xAZ - Strategy Session - Completed
    };

    const formatDate = (date) =>
      `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,"0")}-${String(date.getDate()).padStart(2,"0")}`;

    const processDate = (date) => {
      if (!date) return null;
      const parsedDate = new Date(new Date(date).toLocaleString("en-US", { timeZone: "America/Los_Angeles" }));
      return parsedDate < startDate ? null : parsedDate;
    };

    const processWeek = (date) => {
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - date.getDay());
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);
      return { label: `${formatDate(weekStart)} - ${formatDate(weekEnd)}`, weekStart };
    };

    const addWeekEntry = (region, label) => {
      if (!weeks[region][label]) weeks[region][label] = [label,0,0,0,0,0,0,0,0];
      return weeks[region][label];
    };

    campaigns.forEach(({ created_at, stage_id, jurisdiction }) => {
      const createdDate = processDate(created_at);
      if (!createdDate || createdDate > today) return;

      const { label } = processWeek(createdDate);
      const region =
        (eventLikeStages.AZ.has(stage_id) || nopeStages.AZ.has(stage_id)) ? "AZ" :
        (eventLikeStages.CA.has(stage_id) || nopeStages.CA.has(stage_id)) ? "CA" :
        (eventLikeStages.WA.has(stage_id) || nopeStages.WA.has(stage_id)) ? "WA" :
        jurisdiction?.toLowerCase() === "arizona" ? "AZ" :
        jurisdiction?.toLowerCase() === "washington" ? "WA" : "CA";

      const weekData = addWeekEntry(region, label);
      weekData[1]++;
      if (nopeStages[region].has(stage_id)) weekData[2]++;
      if (eventLikeStages[region].has(stage_id)) weekData[4]++;
    });

    events.forEach(({ event_start, stage_id, jurisdiction }) => {
      const eventDate = processDate(event_start);
      if (!eventDate || eventDate > today) return;

      const { label } = processWeek(eventDate);

      const region =
        (eventLikeStages.AZ.has(stage_id) || nopeStages.AZ.has(stage_id)) ? "AZ" :
        (eventLikeStages.CA.has(stage_id) || nopeStages.CA.has(stage_id)) ? "CA" :
        (eventLikeStages.WA.has(stage_id) || nopeStages.WA.has(stage_id)) ? "WA" :
        /^AZ - Strategy Session/i.test(jurisdiction) ? "AZ" :
        /^WA - Strategy Session/i.test(jurisdiction) ? "WA" : "CA";

      const weekData = addWeekEntry(region, label);
      weekData[4]++;
    });

    const applyLPCData = (data, region) => {
      data.forEach(({ date, clicks, cost }) => {
        if (weeks[region][date]) {
          weeks[region][date][5] = cost;
          weeks[region][date][6] = clicks;
        }
      });
    };

    if (Array.isArray(caData)) applyLPCData(caData, "CA");
    if (Array.isArray(azData)) applyLPCData(azData, "AZ");
    if (Array.isArray(waData)) applyLPCData(waData, "WA");

    Object.keys(weeks).forEach((region) => {
      Object.values(weeks[region]).forEach((week) => {
        const totalForms = week[1], noShows = week[2];
        const confirmed = totalForms - noShows;
        const cost = parseFloat(week[5]) || 0;
        const clicks = parseFloat(week[6]) || 0;

        week[3] = confirmed;
        week[7] = confirmed > 0 ? cost / confirmed : 0;
        week[8] = clicks > 0 ? (totalForms / clicks) * 100 : 0;
      });
    });

    const calculateVariance = (current, previous) => {
      current = parseFloat(current) || 0;
      previous = parseFloat(previous) || 0;
      if (previous === 0) return null;
      return ((current - previous) / previous) * 100;
    };

    const addWoWRow = (records) => {
      if (records.length < 3) return;
      const lastWeek = records[records.length-2].map(v => parseFloat(v) || 0);
      const twoWeeksAgo = records[records.length-3].map(v => parseFloat(v) || 0);
      records.push(["WoW Variance %", ...[1,2,3,4,5,6,7,8].map(i => calculateVariance(lastWeek[i], twoWeeksAgo[i]))]);
    };

    const addBiWeeklyRow = (records) => {
      if (records.length < 6) return;
      const last2Weeks = [records[records.length-3], records[records.length-4]];
      const prev2Weeks = [records[records.length-5], records[records.length-6]];
      const avg = (idx, weeksArr) => weeksArr.reduce((s,r)=>s+(parseFloat(r[idx])||0),0)/weeksArr.length;
      records.push(["Biweekly Variance %", ...[1,2,3,4,5,6,7,8].map(i => calculateVariance(avg(i,last2Weeks), avg(i,prev2Weeks)))]);
    };

    const finalData = {};

    Object.keys(weeks).forEach((region) => {
      const records = Object.values(weeks[region]).sort((a,b)=>new Date(a[0].split(" - ")[0]) - new Date(b[0].split(" - ")[0]));

      addWoWRow(records);
      addBiWeeklyRow(records);

      finalData[region] = records;
    });

    const columnSigns = ["date","number","number","number","number","currency","number","currency","percent"];
    const formatRow = (row) => {
      const isVarianceRow = /variance %/i.test(row[0]);

      return row.map((value, idx) => {
        if (idx === 0) return value;
        if (isVarianceRow) {
          return value === null ? "N/A" : parseFloat(value).toFixed(2) + "%";
        }

        const type = columnSigns[idx];
        if (type === "currency")
          return "$" + (parseFloat(value) || 0).toFixed(2);
        if (type === "percent") {
          return value === null ? "N/A" : parseFloat(value).toFixed(2) + "%";
        }
        if (type === "number") return "'" + (parseFloat(value) || 0);

        return value;
      });
    };

    Object.keys(finalData).forEach((region) => {
      finalData[region] = finalData[region].map((row) => formatRow(row));
    });

    await Promise.all(Object.entries(finalData).map(async ([region, sortedWeeks]) => {
      const existingData = await sheets.spreadsheets.values.get({ spreadsheetId, range: dataRanges[region] });
      const existingRows = existingData.data.values || [];
      const existingWeeks = new Map(existingRows.map((row,i)=>[row[0]?.trim(),i]));

      const batchUpdates = [], newValues = [];
      
      sortedWeeks.forEach(weekData => {
        const label = weekData[0].trim();
        if(existingWeeks.has(label)) {
          batchUpdates.push({ range: `${dataRanges[region]}!A${existingWeeks.get(label)+1}`, values:[weekData] });
        } else {
          newValues.push(weekData);
        }
      });

      if(batchUpdates.length > 0) {
        await sheets.spreadsheets.values.batchUpdate({ 
          spreadsheetId, 
          resource:{ valueInputOption:"USER_ENTERED", data:batchUpdates }
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
}

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
