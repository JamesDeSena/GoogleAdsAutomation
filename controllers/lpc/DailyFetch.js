const { google } = require('googleapis');

const ExportToReport = async (req, res) => {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'serviceToken.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const spreadsheetId = process.env.LPC_BUDGET_SPREADSHEET;
  const dataRanges = {
    Export: 'Daily Export!A2:C',
    Report: 'Daily Report!A2:E',
  };

  try {
    const reportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Report,
    });
    let reportData = reportResponse.data.values || [];

    const exportResponse = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: dataRanges.Export,
    });
    let exportData = exportResponse.data.values || [];

    const validValuesForD = [
      "Initial Call - Not something we handle",
      "At Capacity - Refer Out",
      "Initial Call - Not Moving Forward"
    ];

    const validValuesForE = [
      "Strategy Session Scheduled",
      "Strategy Session - Not moving forward",
      "Strategy Session - Time Remaining"
    ];

    // Store previous "Yes" values mapped by Date + Time
    let previousYesMap = new Map();
    reportData.forEach(([prevDate, prevTime, prevC]) => {
      if (prevC === "Yes") {
        previousYesMap.set(`${prevDate} ${prevTime}`, true);
      }
    });

    const transformedData = exportData
      .map(([createdAt, columnB, columnC]) => {
        if (!createdAt) return null; // Ignore rows with no date

        const [date, time] = createdAt.split(" ");
        let columnCReport = previousYesMap.has(`${date} ${time}`) ? "Yes" : "";
        let columnDValue = validValuesForD.includes(columnB) ? columnB : "";
        let columnEValue = validValuesForE.includes(columnB) ? `${columnC ? columnC + ", " : ""}${columnB}` : "";

        if (columnB === "Initial PCs" && !columnCReport) {
          columnCReport = "Yes";
        }

        // Remove rows where C, D, and E are all empty
        if (!columnCReport && !columnDValue && !columnEValue) return null;

        return [date, time, columnCReport, columnDValue, columnEValue];
      })
      .filter(row => row !== null); // Remove null rows

    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: dataRanges.Report,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: dataRanges.Report,
      valueInputOption: "RAW",
      resource: { values: transformedData },
    });

    console.log("LPC Daily report sent to Google Sheets successfully!");
  } catch (error) {
    console.error("Error sending final report to Google Sheets:", error);
  }
};

module.exports = {
  ExportToReport,
};
