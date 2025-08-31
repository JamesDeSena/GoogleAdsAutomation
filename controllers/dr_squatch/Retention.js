const { google } = require('googleapis');
const schedule = require('node-schedule');
const { ROOT_OUTPUT_FOLDER_ID, RETENTION_DAYS } = require('./ScraperConfig');
require('dotenv').config();

const auth = new google.auth.GoogleAuth({
  keyFile: 'serviceToken.json',
  scopes: ['https://www.googleapis.com/auth/drive'],
});

const drive = google.drive({ version: 'v3', auth });

async function cleanupOldFolders() {
  console.log('🗑️ Starting retention cleanup...');
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - RETENTION_DAYS);

  try {
    // Get all region folders
    const regionFoldersRes = await drive.files.list({
      q: `'${ROOT_OUTPUT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
      fields: 'files(id, name)',
    });

    if (!regionFoldersRes.data.files || regionFoldersRes.data.files.length === 0) {
      console.log('No region folders found.');
      return;
    }

    for (const regionFolder of regionFoldersRes.data.files) {
      // Get all date-stamped subfolders
      const dateFoldersRes = await drive.files.list({
        q: `'${regionFolder.id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
        fields: 'files(id, name)',
      });

      if (!dateFoldersRes.data.files) continue;

      for (const dateFolder of dateFoldersRes.data.files) {
        const folderDate = new Date(dateFolder.name);
        if (!isNaN(folderDate.getTime()) && folderDate < cutoffDate) {
          console.log(`Trashing old folder: ${regionFolder.name}/${dateFolder.name}`);
          await drive.files.update({
            fileId: dateFolder.id,
            requestBody: { trashed: true },
          });
        }
      }
    }

    console.log('✅ Retention cleanup complete.');
  } catch (err) {
    console.error('❌ Error during retention cleanup:', err.message);
  }
}

// Schedule cleanup every day at 2 AM
schedule.scheduleJob('0 2 * * *', () => {
  cleanupOldFolders();
});

module.exports = {
  cleanupOldFolders,
};
