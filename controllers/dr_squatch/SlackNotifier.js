const { WebClient } = require('@slack/web-api');

const slackClient = new WebClient(process.env.SLACK_BOT_STOCK);
const slackChannel = 'C09B3TXTJN8';

const sendSlackSummary = async (summaryData) => {
  const { region, date, totalProducts, uniqueUrls, changesCount, unknownCount, reviewSheetUrl, driveFolderUrl } = summaryData;

  const blocks = [
    {
      type: 'header',
      text: {
        type: 'plain_text',
        text: '✅ Stock Status Verification Run Complete',
        emoji: true,
      },
    },
    {
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*Region:*\n${region}` },
        { type: 'mrkdwn', text: `*Date:*\n${date}` },
        { type: 'mrkdwn', text: `*Products Processed:*\n${totalProducts}` },
        { type: 'mrkdwn', text: `*Discrepancies Found:*\n${changesCount}` },
      ],
    },
    {
      type: 'divider',
    },
    {
      type: 'actions',
      elements: [
        {
          type: 'button',
          text: {
              type: 'plain_text',
              text: 'Review Changes',
              emoji: true,
          },
          style: 'primary',
          url: reviewSheetUrl,
          action_id: 'review_changes_button'
        },
        {
          type: 'button',
          text: {
              type: 'plain_text',
              text: 'View Full Logs',
              emoji: true,
          },
          url: driveFolderUrl,
          action_id: 'view_logs_button'
        },
      ],
    },
  ];

  if (unknownCount > 0) {
    blocks.push({
      type: 'context',
      elements: [
        {
          type: 'mrkdwn',
          text: `*⚠️ ATTENTION:* ${unknownCount} products had an UNKNOWN status and require investigation.`,
        },
      ],
    });
  }

  try {
    await slackClient.chat.postMessage({
      channel: slackChannel,
      text: `Stock Status Run Complete for ${region}`, // Fallback text for notifications
      blocks: blocks,
    });
    console.log(`✅ Summary sent to Slack channel.`);
  } catch (e) {
      console.error(`Error sending Slack message: ${e.message}`);
  }
};

const sendValidationSummary = async (summaryData) => {
  const { region, date, totalCompared, discrepancyCount, changedCount, newCount, removedCount, unknownCount, reviewSheetUrl, driveFolderUrl } = summaryData;

  const blocks = [
    {
      type: 'header',
      text: { type: 'plain_text', text: '📊 Weekend Data Validation Complete', emoji: true },
    },
    {
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*Region:*\n${region}` },
        { type: 'mrkdwn', text: `*Date of Run:*\n${date}` },
        { type: 'mrkdwn', text: `*Total Items Compared:*\n${totalCompared}` },
        { type: 'mrkdwn', text: `*Discrepancies Found:*\n*${discrepancyCount}*` }, // <-- NEW FIELD
      ],
    },
    {
        type: 'section',
        fields: [
            { type: 'mrkdwn', text: `*Changed:*\n${changedCount}` },
            { type: 'mrkdwn', text: `*New:*\n${newCount}` },
            { type: 'mrkdwn', text: `*Removed:*\n${removedCount}` },
        ]
    },
    {
      type: 'divider',
    },
    {
      type: 'actions',
      elements: [
        {
          type: 'button',
          text: { type: 'plain_text', text: 'Review in Google Sheets', emoji: true },
          style: 'primary',
          url: reviewSheetUrl,
          action_id: 'review_validation_button'
        },
        {
          type: 'button',
          text: { type: 'plain_text', text: 'View CSV Archive', emoji: true },
          url: driveFolderUrl,
          action_id: 'view_archive_button'
        },
      ],
    },
  ];

  if (unknownCount > 0) {
    blocks.push({
      type: 'context',
      elements: [
        { type: 'mrkdwn', text: `*⚠️ ATTENTION:* ${unknownCount} products have an UNKNOWN status.` },
      ],
    });
  }

  try {
    await slackClient.chat.postMessage({
      channel: slackChannel,
      text: `Weekend Data Validation Complete for ${region}`, // Fallback text for notifications
      blocks: blocks,
    });
    console.log(`✅ Summary sent to Slack channel for ${region}.`);
  } catch (e) {
    console.error(`Error sending Slack message: ${e.message}`);
  }
};

module.exports = { 
  sendSlackSummary,
  sendValidationSummary
};