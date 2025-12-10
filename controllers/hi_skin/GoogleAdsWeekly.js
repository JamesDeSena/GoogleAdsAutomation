const axios = require("axios");
const { google } = require('googleapis');
const { client } = require("../../configs/googleAdsConfig");
const { getStoredGoogleToken } = require("../GoogleAuth");
const { getStoredBingToken } = require("../BingAuth");

const fs = require("fs");
const path = require("path");
const https = require("https");
const { execSync } = require("child_process");
const AdmZip = require("adm-zip");

const csvFilePath = path.join(__dirname, 'report.csv');

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

async function generateHSBing() {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const today = new Date();
  const day = today.getDate();
  const month = today.getMonth() + 1;
  const year = today.getFullYear();

  const requestBody = `
    <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
        <Action mustUnderstand="1">SubmitGenerateReport</Action>
        <AuthenticationToken>${token.accessToken_Bing}</AuthenticationToken>
        <CustomerAccountId>${process.env.BING_ADS_ACCOUNT_ID_HS}</CustomerAccountId>
        <CustomerId>${process.env.BING_ADS_CID}</CustomerId>
        <DeveloperToken>${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
      </s:Header>
      <s:Body>
        <SubmitGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
            <ReportRequest i:type="CampaignPerformanceReportRequest" xmlns:a="https://bingads.microsoft.com/Reporting/v13">
                <a:ExcludeColumnHeaders>false</a:ExcludeColumnHeaders>
                <a:ExcludeReportFooter>true</a:ExcludeReportFooter>
                <a:ExcludeReportHeader>true</a:ExcludeReportHeader>
                <a:Format>Csv</a:Format>
                <a:FormatVersion>2.0</a:FormatVersion>
                <a:ReportName>CampaignPerformanceReport</a:ReportName> <!-- Include Date -->
                <a:ReturnOnlyCompleteData>false</a:ReturnOnlyCompleteData>
                <a:Aggregation>Daily</a:Aggregation>
                <a:Columns>
                  <a:CampaignPerformanceReportColumn>TimePeriod</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>CampaignName</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Impressions</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Clicks</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Spend</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Goal</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>Conversions</a:CampaignPerformanceReportColumn>
                  <a:CampaignPerformanceReportColumn>AllConversions</a:CampaignPerformanceReportColumn>
                </a:Columns>
                <a:Scope>
                  <a:AccountIds xmlns:b="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                    <b:long>${process.env.BING_ADS_ACCOUNT_ID_HS}</b:long>
                  </a:AccountIds>
                </a:Scope>
                <a:Time>
                  <a:CustomDateRangeEnd>
                    <a:Day>${day}</a:Day>
                    <a:Month>${month}</a:Month>
                    <a:Year>${year}</a:Year>
                  </a:CustomDateRangeEnd>
                  <a:CustomDateRangeStart>
                    <a:Day>11</a:Day>
                    <a:Month>11</a:Month>
                    <a:Year>2024</a:Year>
                  </a:CustomDateRangeStart>
                  <a:ReportTimeZone>PacificTimeUSCanadaTijuana</a:ReportTimeZone>
                </a:Time>
            </ReportRequest>
        </SubmitGenerateReportRequest>
      </s:Body>
    </s:Envelope>
  `;

  try {
    const response = await axios.post(
      "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc?singleWsdl",
      requestBody,
      {
        headers: {
          "Content-Type": "text/xml;charset=utf-8",
          SOAPAction: "SubmitGenerateReport",
        },
        timeout: 10000,
      }
    );

    const match = response.data.match(/<ReportRequestId>(.*?)<\/ReportRequestId>/)?.[1];
    return match;
  } catch (error) {
    console.error("Error fetching Bing data:", error.response ? error.response.data : error.message);
    throw error;
  }
};

async function pollingHSBing() {
  const token = getStoredBingToken();
  if (!token.accessToken_Bing) {
    console.error("Access token is missing. Please authenticate.");
    return;
  }

  const key = await generateHSBing();

  const requestBody = `
    <s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
        <Action mustUnderstand="1">SubmitGenerateReport</Action>
        <AuthenticationToken>${token.accessToken_Bing}</AuthenticationToken>
        <CustomerAccountId>${process.env.BING_ADS_ACCOUNT_ID_HS}</CustomerAccountId>
        <CustomerId>${process.env.BING_ADS_CID}</CustomerId>
        <DeveloperToken>${process.env.BING_ADS_DEVELOPER_TOKEN}</DeveloperToken>
      </s:Header>
      <s:Body>
        <PollGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
          <ReportRequestId>${key}</ReportRequestId>
        </PollGenerateReportRequest>
      </s:Body>
    </s:Envelope>
  `;

  let retries = 5;
  let reportUrl = null;

  while (retries > 0 && !reportUrl) {
    try {
      const response = await axios.post(
        "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc?singleWsdl",
        requestBody,
        {
          headers: {
            "Content-Type": "text/xml;charset=utf-8",
            SOAPAction: "PollGenerateReport",
          },
          timeout: 10000,
        }
      );

      let match = response.data.match(/<ReportDownloadUrl>(.*?)<\/ReportDownloadUrl>/)?.[1];
      
      if (match) {
        reportUrl = match.replace(/&amp;/g, "&");
        return reportUrl;
      }

      console.error("ReportDownloadUrl not found, retrying...");
      retries -= 1;
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for 5 seconds before retrying

    } catch (error) {
      console.error("Error fetching Bing data:", error.response ? error.response.data : error.message);
      retries -= 1;
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for 5 seconds before retrying
    }
  }

  throw new Error("Failed to retrieve report URL after multiple attempts.");
};

async function downloadAndExtractHSBing() {
  const url = await pollingHSBing();
  if (!url) return;

  const zip = path.join(__dirname, 'report.zip');
  const dir = path.join(__dirname, 'bing_report');
  const csv = csvFilePath

  fs.mkdirSync(dir, { recursive: true });

  await new Promise((res, rej) =>
    https.get(url, r => r.pipe(fs.createWriteStream(zip)).on("finish", res).on("error", rej))
  );

  if (process.platform === "win32") {
    const zipFile = new AdmZip(zip);
    zipFile.extractAllTo(dir, true);
  } else {
    execSync(`unzip ${zip} -d ${dir}`);
  }

  const file = fs.readdirSync(dir).find(f => f.endsWith(".csv"));
  if (!file) throw new Error("CSV not found.");

  fs.renameSync(path.join(dir, file), csv);
  [zip, dir].forEach(f => fs.rmSync(f, { recursive: true, force: true }));

  console.log("Saved CSV:", csv);
  return csv;
};

const aggregateWeeklyDataFromCSV = async () => {
  const csv = csvFilePath;

  if (!fs.existsSync(csv)) {
    await downloadAndExtractHSBing();
  }

  const fileContent = fs.readFileSync(csv, 'utf-8');
  const lines = fileContent.trim().split('\n');

  const weeklyData = {};
  const weekRanges = getOrGenerateDateRanges();

  weekRanges.forEach(({ start, end }) => {
    const key = `${start} - ${end}`;
    weeklyData[key] = {
      date: key,
      impressions: 0,
      clicks: 0,
      cost: 0,
      step1Value: 0,
      step5Value: 0,
      step6Value: 0,
      bookingConfirmed: 0,
      purchase: 0,
    };
  });

  const parseDate = (dateStr) => {
    const [year, month, day] = dateStr.split('-').map(Number);
    return new Date(year, month - 1, day);
  };

  lines.slice(1).forEach(line => {
    const values = line.split('","').map(v => v.replace(/^"|"$/g, ''));
    if (values.length < 8) return;

    const [timePeriod, campaignName, impressions, clicks, cost, goal] = values;

    const date = parseDate(timePeriod);
    const matchingWeek = weekRanges.find(({ start, end }) => {
      const startDate = parseDate(start);
      const endDate = parseDate(end);
      return date >= startDate && date <= endDate;
    });

    if (!matchingWeek) return;

    const key = `${matchingWeek.start} - ${matchingWeek.end}`;
    const entry = weeklyData[key];

    entry.impressions += parseInt(impressions) || 0;
    entry.clicks += parseInt(clicks) || 0;
    entry.cost += parseFloat(cost) || 0;

    const normalizedGoal = (goal || '').trim().toLowerCase();
    if (normalizedGoal === 'booking confirmed') {
      entry.bookingConfirmed += 1;
    } else if (normalizedGoal === 'purchase') {
      entry.purchase += 1;
    }
  });

  const result = Object.values(weeklyData).sort((a, b) => {
    const [startA] = a.date.split(' - ');
    const [startB] = a.date.split(' - ');
    return new Date(startA) - new Date(startB);
  });

  return result;
};

const fetchReportDataWeeklyCampaignHS = async (dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

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

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
  }
};

const fetchReportDataWeeklySearchHS = async (dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

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
    // res.status(300).send("Error fetching report data");
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

const fetchReportDataWeeklyHSFilter = async (req, res, campaignNameFilter, brandNBFilter, dateRanges) => {
  const refreshToken_Google = getStoredGoogleToken();

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

    return allWeeklyData;
  } catch (error) {
    console.error("Error fetching report data:", error);
    // res.status(300).send("Error fetching report data");
  }
};

const createFetchFunction = (campaignNameFilter, brandNBFilter = "") => {
  return (req, res, dateRanges) => fetchReportDataWeeklyHSFilter(req, res, campaignNameFilter, brandNBFilter, dateRanges);
};

const fetchFunctions = {
  fetchReportDataWeeklyHSBrand: createFetchFunction("Brand", "Search"),
  fetchReportDataWeeklyHSNBTotal: createFetchFunction("NB", ""),
  fetchReportDataWeeklyHSNB: createFetchFunction("NB", "Search"),
  fetchReportDataWeeklyHSDSA: createFetchFunction("DSA", "NB"),
  fetchReportDataWeeklyHSGilbert: createFetchFunction("Gilbert", ""),
  fetchReportDataWeeklyHSGilbertBrand: createFetchFunction("Gilbert", "Brand"),
  fetchReportDataWeeklyHSGilbertNB: createFetchFunction("Gilbert", "NB"),
  fetchReportDataWeeklyHSMKT: createFetchFunction("MKT", ""),
  fetchReportDataWeeklyHSMKTBrand: createFetchFunction("MKT", "Brand"),
  fetchReportDataWeeklyHSMKTNB: createFetchFunction("MKT", "NB"),
  fetchReportDataWeeklyHSPhoenix: createFetchFunction("Phoenix", ""),
  fetchReportDataWeeklyHSPhoenixBrand: createFetchFunction("Phoenix", "Brand"),
  fetchReportDataWeeklyHSPhoenixNB: createFetchFunction("Phoenix", "NB"),
  fetchReportDataWeeklyHSScottsdale: createFetchFunction("Scottsdale", ""),
  fetchReportDataWeeklyHSScottsdaleBrand: createFetchFunction("Scottsdale", "Brand"),
  fetchReportDataWeeklyHSScottsdaleNB: createFetchFunction("Scottsdale", "NB"),
  fetchReportDataWeeklyHSUptownPark: createFetchFunction("Uptown", ""),
  fetchReportDataWeeklyHSUptownParkBrand: createFetchFunction("Uptown", "Brand"),
  fetchReportDataWeeklyHSUptownParkNB: createFetchFunction("Uptown", "NB"),
  fetchReportDataWeeklyHSMontrose: createFetchFunction("Montrose", ""),
  fetchReportDataWeeklyHSMontroseBrand: createFetchFunction("Montrose", "Brand"),
  fetchReportDataWeeklyHSMontroseNB: createFetchFunction("Montrose", "NB"),
  fetchReportDataWeeklyHSRiceVillage: createFetchFunction("RiceVillage", ""),
  fetchReportDataWeeklyHSRiceVillageBrand: createFetchFunction("RiceVillage", "Brand"),
  fetchReportDataWeeklyHSRiceVillageNB: createFetchFunction("RiceVillage", "NB"),
  fetchReportDataWeeklyHSMosaic: createFetchFunction("Mosaic", ""),
  fetchReportDataWeeklyHSMosaicBrand: createFetchFunction("Mosaic", "Brand"),
  fetchReportDataWeeklyHSMosaicNB: createFetchFunction("Mosaic", "NB"),
  fetchReportDataWeeklyHS14thSt: createFetchFunction("14thSt", ""),
  fetchReportDataWeeklyHS14thStBrand: createFetchFunction("14thSt", "Brand"),
  fetchReportDataWeeklyHS14thStNB: createFetchFunction("14thSt", "NB"),
  fetchReportDataWeeklyHSPmax: createFetchFunction("Pmax", ""),
  fetchReportDataWeeklyHSPmaxBrand: createFetchFunction("Pmax", "Brand"),
  fetchReportDataWeeklyHSPmaxNB: createFetchFunction("Pmax", "NB"),
  fetchReportDataWeeklyHSShopping: createFetchFunction("Shopping", ""),
  fetchReportDataWeeklyHSDemandGen: createFetchFunction("DemandGen", ""),
  fetchReportDataWeeklyHSBing: createFetchFunction("Bing", ""),
};

const executeSpecificFetchFunctionHS = async (req, res) => {
  const functionName = "fetchReportDataWeeklyHSDSA";
  const dateRanges = getOrGenerateDateRanges();
  if (fetchFunctions[functionName]) {
    const data = await fetchFunctions[functionName](dateRanges);
    res.json(data);
  } else {
    console.error(`Function ${functionName} does not exist.`);
    res.status(404).send("Function not found");
  }
};

let lastApiCallTime = 0;
const MIN_DELAY_BETWEEN_CALLS_MS = 3000;

const createThrottledFetch = (fetchFn) => async (...args) => {
  const now = Date.now();
  const timeSinceLastCall = now - lastApiCallTime;
  const delayNeeded = MIN_DELAY_BETWEEN_CALLS_MS - timeSinceLastCall;

  if (delayNeeded > 0) {
    console.log(`Throttling: Waiting for ${delayNeeded}ms before calling ${fetchFn.name || 'a function'}.`);
    await new Promise(resolve => setTimeout(resolve, delayNeeded));
  }

  const result = await fetchFn(...args);
  lastApiCallTime = Date.now();
  
  return result;
};

const throttledFetchFunctions = {
    weeklyCampaignData: createThrottledFetch(fetchReportDataWeeklyCampaignHS),
    weeklySearchData: createThrottledFetch(fetchReportDataWeeklySearchHS),
    brandData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSBrand),
    noBrandTotalData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSNBTotal),
    noBrandSearchData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSNB),
    dsaData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSDSA),
    gilbertData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSGilbert),
    gilbertDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSGilbertBrand),
    gilbertDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSGilbertNB),
    mktData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMKT),
    mktDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMKTBrand),
    mktDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMKTNB),
    phoenixData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPhoenix),
    phoenixDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPhoenixBrand),
    phoenixDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPhoenixNB),
    scottsdaleData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSScottsdale),
    scottsdaleDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSScottsdaleBrand),
    scottsdaleDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSScottsdaleNB),
    uptownParkData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSUptownPark),
    uptownParkDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSUptownParkBrand),
    uptownParkDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSUptownParkNB),
    montroseData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMontrose),
    montroseDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMontroseBrand),
    montroseDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMontroseNB),
    riceVillageData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSRiceVillage),
    riceVillageDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSRiceVillageBrand),
    riceVillageDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSRiceVillageNB),
    mosaicData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMosaic),
    mosaicDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMosaicBrand),
    mosaicDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSMosaicNB),
    fourteenthStData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHS14thSt),
    fourteenthStDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHS14thStBrand),
    fourteenthStDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHS14thStNB),
    pmaxData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPmax),
    pmaxDataBrand: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPmaxBrand),
    pmaxDataNB: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSPmaxNB),
    shoppingData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSShopping),
    demandGenData: createThrottledFetch(fetchFunctions.fetchReportDataWeeklyHSDemandGen),
    bingData: createThrottledFetch(aggregateWeeklyDataFromCSV),
};

const sendFinalWeeklyReportToGoogleSheetsHS = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.SHEET_HI_SKIN;
  const dataRanges = {
    Live: 'Weekly Performance!A2:U',
    Brand: 'Brand Weekly Performance!A2:U',
    NB: 'NB Weekly Performance!A2:U'
  };

  try {
    const date = req?.params?.date;
    const dateRanges = getOrGenerateDateRanges(date);

    const {
      weeklyCampaignData: throttledWeeklyCampaignDataFetch,
      weeklySearchData: throttledWeeklySearchDataFetch,
      brandData: throttledBrandDataFetch,
      noBrandTotalData: throttledNoBrandTotalDataFetch,
      noBrandSearchData: throttledNoBrandSearchDataFetch,  
      dsaData: throttledDSADataFetch,
      gilbertData: throttledGilbertDataFetch,
      gilbertDataBrand: throttledGilbertDataBrandFetch,
      gilbertDataNB: throttledGilbertDataNBFetch,
      mktData: throttledMktDataFetch,
      mktDataBrand: throttledMktDataBrandFetch,
      mktDataNB: throttledMktDataNBFetch,
      phoenixData: throttledPhoenixDataFetch,
      phoenixDataBrand: throttledPhoenixDataBrandFetch,
      phoenixDataNB: throttledPhoenixDataNBFetch,
      scottsdaleData: throttledScottsdaleDataFetch,
      scottsdaleDataBrand: throttledScottsdaleDataBrandFetch,
      scottsdaleDataNB: throttledScottsdaleDataNBFetch,
      uptownParkData: throttledUptownParkDataFetch,
      uptownParkDataBrand: throttledUptownParkDataBrandFetch,
      uptownParkDataNB: throttledUptownParkDataNBFetch,
      montroseData: throttledMontroseDataFetch,
      montroseDataBrand: throttledMontroseDataBrandFetch,
      montroseDataNB: throttledMontroseDataNBFetch,
      riceVillageData: throttledRiceVillageDataFetch,
      riceVillageDataBrand: throttledRiceVillageDataBrandFetch,
      riceVillageDataNB: throttledRiceVillageDataNBFetch,
      mosaicData: throttledMosaicDataFetch,
      mosaicDataBrand: throttledMosaicDataBrandFetch,
      mosaicDataNB: throttledMosaicDataNBFetch,
      fourteenthStData: throttledFourteenthStDataFetch,
      fourteenthStDataBrand: throttledFourteenthStDataBrandFetch,
      fourteenthStDataNB: throttledFourteenthStDataNBFetch,
      pmaxData: throttledPmaxDataFetch,
      pmaxDataBrand: throttledPmaxDataBrandFetch,
      pmaxDataNB: throttledPmaxDataNBFetch,
      shoppingData: throttledShoppingDataFetch,
      demandGenData: throttledDemandGenFetch,
      bingData: throttledBingDataFetch,
    } = throttledFetchFunctions;

    const weeklyCampaignData = await throttledWeeklyCampaignDataFetch(dateRanges);
    const weeklySearchData = await throttledWeeklySearchDataFetch(dateRanges);
    const brandData = await throttledBrandDataFetch(req, res, dateRanges);
    const noBrandTotalData = await throttledNoBrandTotalDataFetch(req, res, dateRanges);
    const noBrandSearchData = await throttledNoBrandSearchDataFetch(req, res, dateRanges);
    const dsaData = await throttledDSADataFetch(req, res, dateRanges);
    const gilbertData = await throttledGilbertDataFetch(req, res, dateRanges);
    const gilbertDataBrand = await throttledGilbertDataBrandFetch(req, res, dateRanges);
    const gilbertDataNB = await throttledGilbertDataNBFetch(req, res, dateRanges);
    const mktData = await throttledMktDataFetch(req, res, dateRanges);
    const mktDataBrand = await throttledMktDataBrandFetch(req, res, dateRanges);
    const mktDataNB = await throttledMktDataNBFetch(req, res, dateRanges);
    const phoenixData = await throttledPhoenixDataFetch(req, res, dateRanges);
    const phoenixDataBrand = await throttledPhoenixDataBrandFetch(req, res, dateRanges);
    const phoenixDataNB = await throttledPhoenixDataNBFetch(req, res, dateRanges);
    const scottsdaleData = await throttledScottsdaleDataFetch(req, res, dateRanges);
    const scottsdaleDataBrand = await throttledScottsdaleDataBrandFetch(req, res, dateRanges);
    const scottsdaleDataNB = await throttledScottsdaleDataNBFetch(req, res, dateRanges);
    const uptownParkData = await throttledUptownParkDataFetch(req, res, dateRanges);
    const uptownParkDataBrand = await throttledUptownParkDataBrandFetch(req, res, dateRanges);
    const uptownParkDataNB = await throttledUptownParkDataNBFetch(req, res, dateRanges);
    const montroseData = await throttledMontroseDataFetch(req, res, dateRanges);
    const montroseDataBrand = await throttledMontroseDataBrandFetch(req, res, dateRanges);
    const montroseDataNB = await throttledMontroseDataNBFetch(req, res, dateRanges);
    const riceVillageData = await throttledRiceVillageDataFetch(req, res, dateRanges);
    const riceVillageDataBrand = await throttledRiceVillageDataBrandFetch(req, res, dateRanges);
    const riceVillageDataNB = await throttledRiceVillageDataNBFetch(req, res, dateRanges);
    const mosaicData = await throttledMosaicDataFetch(req, res, dateRanges);
    const mosaicDataBrand = await throttledMosaicDataBrandFetch(req, res, dateRanges);
    const mosaicDataNB = await throttledMosaicDataNBFetch(req, res, dateRanges);
    const fourteenthStData = await throttledFourteenthStDataFetch(req, res, dateRanges);
    const fourteenthStDataBrand = await throttledFourteenthStDataBrandFetch(req, res, dateRanges);
    const fourteenthStDataNB = await throttledFourteenthStDataNBFetch(req, res, dateRanges);
    const pmaxData = await throttledPmaxDataFetch(req, res, dateRanges);
    const pmaxDataBrand = await throttledPmaxDataBrandFetch(req, res, dateRanges);
    const pmaxDataNB = await throttledPmaxDataNBFetch(req, res, dateRanges);
    const shoppingData = await throttledShoppingDataFetch(req, res, dateRanges);
    const demandGenData = await throttledDemandGenFetch(req, res, dateRanges);
    const bingData = await throttledBingDataFetch();

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
        "CPC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.clicks, secondToLastRecord.cost / secondToLastRecord.clicks)),
        "CTR": formatPercentage(calculateWoWVariance(lastRecord.clicks / lastRecord.impressions, secondToLastRecord.clicks / secondToLastRecord.impressions)),
        "Booking Confirmed": formatPercentage(calculateWoWVariance(lastRecord.bookingConfirmed, secondToLastRecord.bookingConfirmed)),
        "Booking CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.bookingConfirmed, secondToLastRecord.cost / secondToLastRecord.bookingConfirmed)),
        "Booking Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.bookingConfirmed / lastRecord.clicks, secondToLastRecord.bookingConfirmed / secondToLastRecord.clicks)),
        "Book Now - Step 1: Locations": formatPercentage(calculateWoWVariance(lastRecord.step1Value, secondToLastRecord.step1Value)),
        "Book Now - Step 5: Confirm Booking": formatPercentage(calculateWoWVariance(lastRecord.step5Value, secondToLastRecord.step5Value)),
        "Book Now - Step 6: Booking Confirmation": formatPercentage(calculateWoWVariance(lastRecord.step6Value, secondToLastRecord.step6Value)),
        "Step 1 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step1Value, secondToLastRecord.cost / secondToLastRecord.step1Value)),
        "Step 5 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step5Value, secondToLastRecord.cost / secondToLastRecord.step5Value)),
        "Step 6 CAC": formatPercentage(calculateWoWVariance(lastRecord.cost / lastRecord.step6Value, secondToLastRecord.cost / secondToLastRecord.step6Value)),
        "Step 1 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step1Value / lastRecord.clicks, secondToLastRecord.step1Value / secondToLastRecord.clicks)),
        "Step 5 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step5Value / lastRecord.clicks, secondToLastRecord.step5Value / secondToLastRecord.clicks)),
        "Step 6 Conv Rate": formatPercentage(calculateWoWVariance(lastRecord.step6Value / lastRecord.clicks, secondToLastRecord.step6Value / secondToLastRecord.clicks)),
        "Purchase": formatPercentage(calculateWoWVariance(lastRecord.purchase, secondToLastRecord.purchase)),
      });
    };

    const addBiWeeklyVariance = (previousRecord, secondToPreviousRecord, lastRecord, secondToLastRecord, filter, filter2) => {
      records.push({
        Week: "Biweekly Variance %",
        Filter: filter,
        Filter2: filter2,
        "Impr.": formatPercentage(calculateWoWVariance(
          previousRecord.impressions + secondToPreviousRecord.impressions, 
          lastRecord.impressions + secondToLastRecord.impressions)),
        "Clicks": formatPercentage(calculateWoWVariance(
          previousRecord.clicks + secondToPreviousRecord.clicks,
          lastRecord.clicks + secondToLastRecord.clicks)),
        "Cost": formatPercentage(calculateWoWVariance(
          previousRecord.cost + secondToPreviousRecord.cost, 
          lastRecord.cost + secondToLastRecord.cost)),
        "CPC": formatPercentage(calculateWoWVariance(
          (previousRecord.cost + secondToPreviousRecord.cost) / (previousRecord.clicks + secondToPreviousRecord.clicks), 
          (lastRecord.cost + secondToLastRecord.cost) / (lastRecord.clicks + secondToLastRecord.clicks))),
        "CTR": formatPercentage(calculateWoWVariance(
          (previousRecord.clicks + secondToPreviousRecord.clicks) / (previousRecord.impressions + secondToPreviousRecord.impressions), 
          (lastRecord.clicks + secondToLastRecord.clicks) / (lastRecord.impressions + secondToLastRecord.impressions))),
        "Booking Confirmed": formatPercentage(calculateWoWVariance(
          previousRecord.bookingConfirmed + secondToPreviousRecord.bookingConfirmed,
          lastRecord.bookingConfirmed + secondToLastRecord.bookingConfirmed)),
        "Booking CAC": formatPercentage(calculateWoWVariance(
          (previousRecord.cost + secondToPreviousRecord.cost) / (previousRecord.bookingConfirmed + secondToPreviousRecord.bookingConfirmed),
          (lastRecord.cost + secondToLastRecord.cost) / (lastRecord.bookingConfirmed + secondToLastRecord.bookingConfirmed))),
        "Booking Conv Rate": formatPercentage(calculateWoWVariance(
          (previousRecord.bookingConfirmed + secondToPreviousRecord.bookingConfirmed) / (previousRecord.clicks + secondToPreviousRecord.clicks),
          (lastRecord.bookingConfirmed + secondToLastRecord.bookingConfirmed) / (lastRecord.clicks + secondToLastRecord.clicks))),
        "Book Now - Step 1: Locations": formatPercentage(calculateWoWVariance(
          previousRecord.step1Value + secondToPreviousRecord.step1Value,
          lastRecord.step1Value + secondToLastRecord.step1Value)),
        "Book Now - Step 5: Confirm Booking": formatPercentage(calculateWoWVariance(
          previousRecord.step5Value + secondToPreviousRecord.step5Value,
          lastRecord.step5Value + secondToLastRecord.step5Value)),
        "Book Now - Step 6: Booking Confirmation": formatPercentage(calculateWoWVariance(
          previousRecord.step6Value + secondToPreviousRecord.step6Value,
          lastRecord.step6Value + secondToLastRecord.step6Value)),
        "Step 1 CAC": formatPercentage(calculateWoWVariance(
          (previousRecord.cost + secondToPreviousRecord.cost) / (previousRecord.step1Value + secondToPreviousRecord.step1Value),
          (lastRecord.cost + secondToLastRecord.cost) / (lastRecord.step1Value + secondToLastRecord.step1Value))),
        "Step 5 CAC": formatPercentage(calculateWoWVariance(
          (previousRecord.cost + secondToPreviousRecord.cost) / (previousRecord.step5Value + secondToPreviousRecord.step5Value),
          (lastRecord.cost + secondToLastRecord.cost) / (lastRecord.step5Value + secondToLastRecord.step5Value))),
        "Step 6 CAC": formatPercentage(calculateWoWVariance(
          (previousRecord.cost + secondToPreviousRecord.cost) / (previousRecord.step6Value + secondToPreviousRecord.step6Value),
          (lastRecord.cost + secondToLastRecord.cost) / (lastRecord.step6Value + secondToLastRecord.step6Value))),
        "Step 1 Conv Rate": formatPercentage(calculateWoWVariance(
          (previousRecord.step1Value + secondToPreviousRecord.step1Value) / (previousRecord.clicks + secondToPreviousRecord.clicks),
          (lastRecord.step1Value + secondToLastRecord.step1Value) / (lastRecord.clicks + secondToLastRecord.clicks))),
        "Step 5 Conv Rate": formatPercentage(calculateWoWVariance(
          (previousRecord.step5Value + secondToPreviousRecord.step5Value) / (previousRecord.clicks + secondToPreviousRecord.clicks),
          (lastRecord.step5Value + secondToLastRecord.step5Value) / (lastRecord.clicks + secondToLastRecord.clicks))),
        "Step 6 Conv Rate": formatPercentage(calculateWoWVariance(
          (previousRecord.step6Value + secondToPreviousRecord.step6Value) / (previousRecord.clicks + secondToPreviousRecord.clicks),
          (lastRecord.step6Value + secondToLastRecord.step6Value) / (lastRecord.clicks + secondToLastRecord.clicks))),
        "Purchase": formatPercentage(calculateWoWVariance(
          previousRecord.purchase + secondToPreviousRecord.purchase,
          lastRecord.purchase + secondToLastRecord.purchase))
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
          "CPC": formatCurrency(record.cost / record.clicks),
          "CTR": formatPercentage((record.clicks / record.impressions) * 100),
          "Booking Confirmed": formatNumber(record.bookingConfirmed),
          "Booking CAC": formatCurrency(record.cost / record.bookingConfirmed),
          "Booking Conv Rate": formatPercentage((record.bookingConfirmed / record.clicks) * 100),
          "Book Now - Step 1: Locations": formatNumber(record.step1Value),
          "Book Now - Step 5: Confirm Booking": formatNumber(record.step5Value),
          "Book Now - Step 6: Booking Confirmation": formatNumber(record.step6Value),
          "Step 1 CAC": formatCurrency(record.cost / record.step1Value),
          "Step 5 CAC": formatCurrency(record.cost / record.step5Value),
          "Step 6 CAC": formatCurrency(record.cost / record.step6Value),
          "Step 1 Conv Rate": formatPercentage((record.step1Value / record.clicks) * 100),
          "Step 5 Conv Rate": formatPercentage((record.step5Value / record.clicks) * 100),
          "Step 6 Conv Rate": formatPercentage((record.step6Value / record.clicks) * 100),
          "Purchase": formatNumber(record.purchase),
        });
      });
    };

    addDataToRecords(weeklyCampaignData, "All Campaign", 1);
    addDataToRecords(weeklySearchData, "All Search", 2);
    addDataToRecords(brandData, "Brand Search", 3);
    addDataToRecords(noBrandTotalData, "NB Total", 4);
    addDataToRecords(noBrandSearchData, "NB Search", 5);
    addDataToRecords(dsaData, "DSA NB", 6);
    addDataToRecords(pmaxData, "Pmax", 7);
    addDataToRecords(pmaxDataBrand, "Pmax Brand", 8);
    addDataToRecords(pmaxDataNB, "Pmax NB", 9);
    addDataToRecords(shoppingData, "Shopping", 10);
    addDataToRecords(demandGenData, "DemandGen", 11);
    addDataToRecords(bingData, "Bing", 12);
    addDataToRecords(gilbertData, "Gilbert", 13);
    addDataToRecords(gilbertDataBrand, "Gilbert Brand", 14);
    addDataToRecords(gilbertDataNB, "Gilbert NB", 15);
    addDataToRecords(mktData, "MKT", 16);
    addDataToRecords(mktDataBrand, "MKT Brand", 17);
    addDataToRecords(mktDataNB, "MKT NB", 18);
    addDataToRecords(phoenixData, "Phoenix", 19);
    addDataToRecords(phoenixDataBrand, "Phoenix Brand", 20);
    addDataToRecords(phoenixDataNB, "Phoenix NB", 21);
    addDataToRecords(scottsdaleData, "Scottsdale", 22);
    addDataToRecords(scottsdaleDataBrand, "Scottsdale Brand", 23);
    addDataToRecords(scottsdaleDataNB, "Scottsdale NB", 24);
    addDataToRecords(uptownParkData, "UptownPark", 25);
    addDataToRecords(uptownParkDataBrand, "UptownPark Brand", 26);
    addDataToRecords(uptownParkDataNB, "UptownPark NB", 27);
    addDataToRecords(montroseData, "Montrose", 28);
    addDataToRecords(montroseDataBrand, "Montrose Brand", 29);
    addDataToRecords(montroseDataNB, "Montrose NB", 30);
    addDataToRecords(riceVillageData, "RiceVillage", 31);
    addDataToRecords(riceVillageDataBrand, "RiceVillage Brand", 32);
    addDataToRecords(riceVillageDataNB, "RiceVillage NB", 33);
    addDataToRecords(mosaicData, "Mosaic", 34);
    addDataToRecords(mosaicDataBrand, "Mosaic Brand", 35);
    addDataToRecords(mosaicDataNB, "Mosaic NB", 36);
    addDataToRecords(fourteenthStData, "14thSt", 37);
    addDataToRecords(fourteenthStDataBrand, "14thSt Brand", 38);
    addDataToRecords(fourteenthStDataNB, "14thSt NB", 39);

    if (!date || date.trim() === '') {
      addWoWVariance(weeklyCampaignData.slice(-2)[0], weeklyCampaignData.slice(-3)[0], "All Campaign", 1);
      addWoWVariance(weeklySearchData.slice(-2)[0], weeklySearchData.slice(-3)[0], "All Search", 2);
      addWoWVariance(brandData.slice(-2)[0], brandData.slice(-3)[0], "Brand Search", 3);
      addWoWVariance(noBrandTotalData.slice(-2)[0], noBrandTotalData.slice(-3)[0], "NB Total", 4);
      addWoWVariance(noBrandSearchData.slice(-2)[0], noBrandSearchData.slice(-3)[0], "NB Search", 5);
      addWoWVariance(dsaData.slice(-2)[0], dsaData.slice(-3)[0], "DSA NB", 6);
      addWoWVariance(pmaxData.slice(-2)[0], pmaxData.slice(-3)[0], "Pmax", 7);
      addWoWVariance(pmaxDataBrand.slice(-2)[0], pmaxDataBrand.slice(-3)[0], "Pmax Brand", 8);
      addWoWVariance(pmaxDataNB.slice(-2)[0], pmaxDataNB.slice(-3)[0], "Pmax NB", 9);
      addWoWVariance(shoppingData.slice(-2)[0], shoppingData.slice(-3)[0], "Shopping", 10);
      addWoWVariance(demandGenData.slice(-2)[0], demandGenData.slice(-3)[0], "DemandGen", 11);
      addWoWVariance(bingData.slice(-2)[0], bingData.slice(-3)[0], "Bing", 12);
      addWoWVariance(gilbertData.slice(-2)[0], gilbertData.slice(-3)[0], "Gilbert", 13);
      addWoWVariance(gilbertDataBrand.slice(-2)[0], gilbertDataBrand.slice(-3)[0], "Gilbert Brand", 14);
      addWoWVariance(gilbertDataNB.slice(-2)[0], gilbertDataNB.slice(-3)[0], "Gilbert NB", 15);
      addWoWVariance(mktData.slice(-2)[0], mktData.slice(-3)[0], "MKT", 16);
      addWoWVariance(mktDataBrand.slice(-2)[0], mktDataBrand.slice(-3)[0], "MKT Brand", 17);
      addWoWVariance(mktDataNB.slice(-2)[0], mktDataNB.slice(-3)[0], "MKT NB", 18);
      addWoWVariance(phoenixData.slice(-2)[0], phoenixData.slice(-3)[0], "Phoenix", 19);
      addWoWVariance(phoenixDataBrand.slice(-2)[0], phoenixDataBrand.slice(-3)[0], "Phoenix Brand", 20);
      addWoWVariance(phoenixDataNB.slice(-2)[0], phoenixDataNB.slice(-3)[0], "Phoenix NB", 21);
      addWoWVariance(scottsdaleData.slice(-2)[0], scottsdaleData.slice(-3)[0], "Scottsdale", 22);
      addWoWVariance(scottsdaleDataBrand.slice(-2)[0], scottsdaleDataBrand.slice(-3)[0], "Scottsdale Brand", 23);
      addWoWVariance(scottsdaleDataNB.slice(-2)[0], scottsdaleDataNB.slice(-3)[0], "Scottsdale NB", 24);
      addWoWVariance(uptownParkData.slice(-2)[0], uptownParkData.slice(-3)[0], "UptownPark", 25);
      addWoWVariance(uptownParkDataBrand.slice(-2)[0], uptownParkDataBrand.slice(-3)[0], "UptownPark Brand", 26);
      addWoWVariance(uptownParkDataNB.slice(-2)[0], uptownParkDataNB.slice(-3)[0], "UptownPark NB", 27);
      addWoWVariance(montroseData.slice(-2)[0], montroseData.slice(-3)[0], "Montrose", 28);
      addWoWVariance(montroseDataBrand.slice(-2)[0], montroseDataBrand.slice(-3)[0], "Montrose Brand", 29);
      addWoWVariance(montroseDataNB.slice(-2)[0], montroseDataNB.slice(-3)[0], "Montrose NB", 30);
      addWoWVariance(riceVillageData.slice(-2)[0], riceVillageData.slice(-3)[0], "RiceVillage", 31);
      addWoWVariance(riceVillageDataBrand.slice(-2)[0], riceVillageDataBrand.slice(-3)[0], "RiceVillage Brand", 32);
      addWoWVariance(riceVillageDataNB.slice(-2)[0], riceVillageDataNB.slice(-3)[0], "RiceVillage NB", 33);
      addWoWVariance(mosaicData.slice(-2)[0], mosaicData.slice(-3)[0], "Mosaic", 34);
      addWoWVariance(mosaicDataBrand.slice(-2)[0], mosaicDataBrand.slice(-3)[0], "Mosaic Brand", 35);
      addWoWVariance(mosaicDataNB.slice(-2)[0], mosaicDataNB.slice(-3)[0], "Mosaic NB", 36);
      addWoWVariance(fourteenthStData.slice(-2)[0], fourteenthStData.slice(-3)[0], "14thSt", 37);
      addWoWVariance(fourteenthStDataBrand.slice(-2)[0], fourteenthStDataBrand.slice(-3)[0], "14thSt Brand", 38);
      addWoWVariance(fourteenthStDataNB.slice(-2)[0], fourteenthStDataNB.slice(-3)[0], "14thSt NB", 39);
    }
    records.sort((a, b) => a.Filter2 - b.Filter2);

    if (!date || date.trim() === '') {
      addBiWeeklyVariance(weeklyCampaignData.slice(-2)[0], weeklyCampaignData.slice(-3)[0], weeklyCampaignData.slice(-4)[0], weeklyCampaignData.slice(-5)[0], "All Campaign", 1);
      addBiWeeklyVariance(weeklySearchData.slice(-2)[0], weeklySearchData.slice(-3)[0], weeklySearchData.slice(-4)[0], weeklySearchData.slice(-5)[0], "All Search", 2);
      addBiWeeklyVariance(brandData.slice(-2)[0], brandData.slice(-3)[0], brandData.slice(-4)[0], brandData.slice(-5)[0], "Brand Search", 3);
      addBiWeeklyVariance(noBrandTotalData.slice(-2)[0], noBrandTotalData.slice(-3)[0], noBrandTotalData.slice(-4)[0], noBrandTotalData.slice(-5)[0], "NB Total", 4);
      addBiWeeklyVariance(noBrandSearchData.slice(-2)[0], noBrandSearchData.slice(-3)[0], noBrandSearchData.slice(-4)[0], noBrandSearchData.slice(-5)[0], "NB Search", 5);
      addBiWeeklyVariance(dsaData.slice(-2)[0], dsaData.slice(-3)[0], dsaData.slice(-4)[0], dsaData.slice(-5)[0], "DSA NB", 6);
      addBiWeeklyVariance(pmaxData.slice(-2)[0], pmaxData.slice(-3)[0], pmaxData.slice(-4)[0], pmaxData.slice(-5)[0], "Pmax", 7);
      addBiWeeklyVariance(pmaxDataBrand.slice(-2)[0], pmaxDataBrand.slice(-3)[0], pmaxDataBrand.slice(-4)[0], pmaxDataBrand.slice(-5)[0], "Pmax Brand", 8);
      addBiWeeklyVariance(pmaxDataNB.slice(-2)[0], pmaxDataNB.slice(-3)[0], pmaxDataNB.slice(-4)[0], pmaxDataNB.slice(-5)[0], "Pmax NB", 9);
      addBiWeeklyVariance(shoppingData.slice(-2)[0], shoppingData.slice(-3)[0], shoppingData.slice(-4)[0], shoppingData.slice(-5)[0], "Shopping", 10);
      addBiWeeklyVariance(demandGenData.slice(-2)[0], demandGenData.slice(-3)[0], demandGenData.slice(-4)[0], demandGenData.slice(-5)[0], "DemandGen", 11);
      addBiWeeklyVariance(bingData.slice(-2)[0], bingData.slice(-3)[0], bingData.slice(-4)[0], bingData.slice(-5)[0], "Bing", 12);
      addBiWeeklyVariance(gilbertData.slice(-2)[0], gilbertData.slice(-3)[0], gilbertData.slice(-4)[0], gilbertData.slice(-5)[0], "Gilbert", 13);
      addBiWeeklyVariance(gilbertDataBrand.slice(-2)[0], gilbertDataBrand.slice(-3)[0], gilbertDataBrand.slice(-4)[0], gilbertDataBrand.slice(-5)[0], "Gilbert Brand", 14);
      addBiWeeklyVariance(gilbertDataNB.slice(-2)[0], gilbertDataNB.slice(-3)[0], gilbertDataNB.slice(-4)[0], gilbertDataNB.slice(-5)[0], "Gilbert NB", 15);
      addBiWeeklyVariance(mktData.slice(-2)[0], mktData.slice(-3)[0], mktData.slice(-4)[0], mktData.slice(-5)[0], "MKT", 16);
      addBiWeeklyVariance(mktDataBrand.slice(-2)[0], mktDataBrand.slice(-3)[0], mktDataBrand.slice(-4)[0], mktDataBrand.slice(-5)[0], "MKT Brand", 17);
      addBiWeeklyVariance(mktDataNB.slice(-2)[0], mktDataNB.slice(-3)[0], mktDataNB.slice(-4)[0], mktDataNB.slice(-5)[0], "MKT NB", 18);
      addBiWeeklyVariance(phoenixData.slice(-2)[0], phoenixData.slice(-3)[0], phoenixData.slice(-4)[0], phoenixData.slice(-5)[0], "Phoenix", 19);
      addBiWeeklyVariance(phoenixDataBrand.slice(-2)[0], phoenixDataBrand.slice(-3)[0], phoenixDataBrand.slice(-4)[0], phoenixDataBrand.slice(-5)[0], "Phoenix Brand", 20);
      addBiWeeklyVariance(phoenixDataNB.slice(-2)[0], phoenixDataNB.slice(-3)[0], phoenixDataNB.slice(-4)[0], phoenixDataNB.slice(-5)[0], "Phoenix NB", 21);
      addBiWeeklyVariance(scottsdaleData.slice(-2)[0], scottsdaleData.slice(-3)[0], scottsdaleData.slice(-4)[0], scottsdaleData.slice(-5)[0], "Scottsdale", 22);
      addBiWeeklyVariance(scottsdaleDataBrand.slice(-2)[0], scottsdaleDataBrand.slice(-3)[0], scottsdaleDataBrand.slice(-4)[0], scottsdaleDataBrand.slice(-5)[0], "Scottsdale Brand", 23);
      addBiWeeklyVariance(scottsdaleDataNB.slice(-2)[0], scottsdaleDataNB.slice(-3)[0], scottsdaleDataNB.slice(-4)[0], scottsdaleDataNB.slice(-5)[0], "Scottsdale NB", 24);
      addBiWeeklyVariance(uptownParkData.slice(-2)[0], uptownParkData.slice(-3)[0], uptownParkData.slice(-4)[0], uptownParkData.slice(-5)[0], "UptownPark", 25);
      addBiWeeklyVariance(uptownParkDataBrand.slice(-2)[0], uptownParkDataBrand.slice(-3)[0], uptownParkDataBrand.slice(-4)[0], uptownParkDataBrand.slice(-5)[0], "UptownPark Brand", 26);
      addBiWeeklyVariance(uptownParkDataNB.slice(-2)[0], uptownParkDataNB.slice(-3)[0], uptownParkDataNB.slice(-4)[0], uptownParkDataNB.slice(-5)[0], "UptownPark NB", 27);
      addBiWeeklyVariance(montroseData.slice(-2)[0], montroseData.slice(-3)[0], montroseData.slice(-4)[0], montroseData.slice(-5)[0], "Montrose", 28);
      addBiWeeklyVariance(montroseDataBrand.slice(-2)[0], montroseDataBrand.slice(-3)[0], montroseDataBrand.slice(-4)[0], montroseDataBrand.slice(-5)[0], "Montrose Brand", 29);
      addBiWeeklyVariance(montroseDataNB.slice(-2)[0], montroseDataNB.slice(-3)[0], montroseDataNB.slice(-4)[0], montroseDataNB.slice(-5)[0], "Montrose NB", 30);
      addBiWeeklyVariance(riceVillageData.slice(-2)[0], riceVillageData.slice(-3)[0], riceVillageData.slice(-4)[0], riceVillageData.slice(-5)[0], "RiceVillage", 31);
      addBiWeeklyVariance(riceVillageDataBrand.slice(-2)[0], riceVillageDataBrand.slice(-3)[0], riceVillageDataBrand.slice(-4)[0], riceVillageDataBrand.slice(-5)[0], "RiceVillage Brand", 32);
      addBiWeeklyVariance(riceVillageDataNB.slice(-2)[0], riceVillageDataNB.slice(-3)[0], riceVillageDataNB.slice(-4)[0], riceVillageDataNB.slice(-5)[0], "RiceVillage NB", 33);
      addBiWeeklyVariance(mosaicData.slice(-2)[0], mosaicData.slice(-3)[0], mosaicData.slice(-4)[0], mosaicData.slice(-5)[0], "Mosaic", 34);
      addBiWeeklyVariance(mosaicDataBrand.slice(-2)[0], mosaicDataBrand.slice(-3)[0], mosaicDataBrand.slice(-4)[0], mosaicDataBrand.slice(-5)[0], "Mosaic Brand", 35);
      addBiWeeklyVariance(mosaicDataNB.slice(-2)[0], mosaicDataNB.slice(-3)[0], mosaicDataNB.slice(-4)[0], mosaicDataNB.slice(-5)[0], "Mosaic NB", 36);
      addBiWeeklyVariance(fourteenthStData.slice(-2)[0], fourteenthStData.slice(-3)[0], fourteenthStData.slice(-4)[0], fourteenthStData.slice(-5)[0], "14thSt", 37);
      addBiWeeklyVariance(fourteenthStDataBrand.slice(-2)[0], fourteenthStDataBrand.slice(-3)[0], fourteenthStDataBrand.slice(-4)[0], fourteenthStDataBrand.slice(-5)[0], "14thSt Brand", 38);
      addBiWeeklyVariance(fourteenthStDataNB.slice(-2)[0], fourteenthStDataNB.slice(-3)[0], fourteenthStDataNB.slice(-4)[0], fourteenthStDataNB.slice(-5)[0], "14thSt NB", 39);
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
            "CPC": "CPC",
            "CTR": "CTR",
            "Booking Confirmed": "Booking Confirmed",
            "Booking CAC": "Booking CAC",
            "Booking Conv Rate": "Booking Conv Rate",
            "Book Now - Step 1: Locations": "Book Now - Step 1: Locations",
            "Book Now - Step 5: Confirm Booking": "Book Now - Step 5: Confirm Booking",
            "Book Now - Step 6: Booking Confirmation": "Book Now - Step 6: Booking Confirmation",     
            "Step 1 CAC": "Step 1 CAC",
            "Step 5 CAC": "Step 5 CAC",
            "Step 6 CAC": "Step 6 CAC",
            "Step 1 Conv Rate": "Step 1 Conv Rate",
            "Step 5 Conv Rate": "Step 5 Conv Rate",
            "Step 6 Conv Rate": "Step 6 Conv Rate",
            "Purchase": "Purchase",
            isBold: true,
          });
          currentGroup = record.Filter;
        }
        finalRecords.push({ ...record, isBold: false });
        if (record.Week === "Biweekly Variance %") {
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
      record["CPC"],
      record["CTR"],
      record["Booking Confirmed"],
      record["Booking CAC"],
      record["Booking Conv Rate"],
      record["Book Now - Step 1: Locations"],
      record["Book Now - Step 5: Confirm Booking"],
      record["Book Now - Step 6: Booking Confirmation"],
      record["Step 1 CAC"],
      record["Step 5 CAC"],
      record["Step 6 CAC"],
      record["Step 1 Conv Rate"],
      record["Step 5 Conv Rate"],
      record["Step 6 Conv Rate"],
      record["Purchase"],
    ]);

    const dataToSend = {
      Live: sheetData.filter(row => ["Brand Search", "NB Search", "DSA NB", "Pmax", "Pmax Brand", "Pmax NB", "Shopping", "DemandGen", "Bing", "Gilbert Brand", "MKT Brand", "Phoenix Brand", "Scottsdale Brand", "UptownPark Brand", "Montrose Brand", "RiceVillage Brand", "Mosaic Brand", "14thSt Brand"].includes(row[0]) || ["Brand Search", "NB Search", "DSA NB", "Pmax", "Pmax Brand", "Pmax NB", "Shopping", "DemandGen", "Bing", "Gilbert Brand", "MKT Brand", "Phoenix Brand", "Scottsdale Brand", "UptownPark Brand", "Montrose Brand", "RiceVillage Brand", "Mosaic Brand", "14thSt Brand"].includes(row[1])),
      Brand: sheetData.filter(row => ["Brand Search", "Gilbert Brand", "MKT Brand", "Phoenix Brand", "Scottsdale Brand", "UptownPark Brand", "Montrose Brand", "RiceVillage Brand", "Mosaic Brand", "14thSt Brand"].includes(row[0]) || ["Brand Search", "Gilbert Brand", "MKT Brand", "Phoenix Brand", "Scottsdale Brand", "UptownPark Brand", "Montrose Brand", "RiceVillage Brand", "Mosaic Brand", "14thSt Brand"].includes(row[1])),
      NB: sheetData.filter(row => ["NB Total", "NB Search", "DSA NB", "Pmax NB", "Shopping", "DemandGen"].includes(row[0]) || ["NB Total", "NB Search", "DSA NB", "Pmax NB", "Shopping", "DemandGen"].includes(row[1])),
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
    console.error("Error sending Hi, Skin weekly report to Google Sheets:", error);
  }
};

const sendBlendedCACToGoogleSheetsHS = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const sourceSpreadsheetId = process.env.SHEET_BLENDED;
  const sourceDataRange = 'MAA - Daily!A2:X';
  const targetSpreadsheetId = process.env.SHEET_HI_SKIN;
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
      row[22] || null,
      row[23] || null
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
  downloadAndExtractHSBing,
  aggregateWeeklyDataFromCSV,
  executeSpecificFetchFunctionHS,
  sendFinalWeeklyReportToGoogleSheetsHS,
  sendBlendedCACToGoogleSheetsHS
};