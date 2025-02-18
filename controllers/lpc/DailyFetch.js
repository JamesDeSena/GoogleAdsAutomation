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

    const transformedData = exportData.map(([createdAt, value, columnC], index) => {
      if (!createdAt) return ["", "", "", "", ""];

      const [date, time] = createdAt.split(" ");
      let columnCValue = "";
      let columnDValue = value !== "Initial PCs" ? value : ""; 
      let columnEValue = columnC || "";
      let columnCReport = "";

      if (value === "Initial PCs" && !reportData[index]?.[2]) {
        columnCReport = "Yes"; // Set "Yes" only if Column C is empty (never overwrite existing "Yes")
      } else if (reportData[index]?.[2] === "Yes") {
        columnCReport = "Yes"; // Keep "Yes" if it's already set in Column C
      }

      return [date, time, columnCReport, columnDValue, columnEValue];
    });

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
