const { WebClient } = require('@slack/web-api');
const { getStoredRefreshToken } = require("../controllers/GoogleAuth");
const { getStoredAccessToken } = require("../controllers/BingAuth");

const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN);
const slackChannel = 'C07VBABS9RT';

const sendSlackMessage = async (channel, text) => {
  try {
    await slackClient.chat.postMessage({
      channel: channel,
      text: "Token is missing. Please authenticate.",
      attachments: [
        {
          color: '#2eb67d',
          blocks: [
            {
              type: 'section',
              text: {
                type: 'mrkdwn',
                text: `${text}`
              }
            }
          ]
        }
      ]
    });
    console.log("Message sent to Slack.");
  } catch (error) {
    console.error("Error sending message to Slack:", error.message);
  }
};

const checkTokensAndNotify = () => {
  const token = getStoredAccessToken();
  const refreshToken_Google = getStoredRefreshToken();

  let message = '';
  let bingLinkAdded = false;
  let googleLinkAdded = false;

  if (!token || !token.accessToken_Bing) {
    message += "⚠️ Bing access token is missing. Please authenticate. \n";
    bingLinkAdded = true;
  }

  if (!refreshToken_Google) {
    message += "⚠️ Google refresh token is missing. Please authenticate. \n";
    googleLinkAdded = true;
  }

  if (bingLinkAdded && googleLinkAdded) {
    message += "\nBing authentication link: https://googleadsautomation.onrender.com/api/auth/bing\nGoogle authentication link: https://googleadsautomation.onrender.com/api/auth/google";
  } else {
    if (bingLinkAdded) {
      message += "\nBing authentication link: https://googleadsautomation.onrender.com/api/auth/bing";
    }

    if (googleLinkAdded) {
      message += "\nGoogle authentication link: https://googleadsautomation.onrender.com/api/auth/google";
    }
  }

  if (message) {
    console.warn(message);
    sendSlackMessage(slackChannel, message);
  }
};

let googleTokenLastChecked = null;

const remindGoogleTokenRefresh = () => {
  const refreshToken_Google = getStoredRefreshToken();

  if (refreshToken_Google && !googleTokenLastChecked) {
    googleTokenLastChecked = new Date();
  }

  if (googleTokenLastChecked) {
    const currentDate = new Date();
    const diffDays = Math.floor((currentDate - googleTokenLastChecked) / (1000 * 60 * 60 * 24));

    if (diffDays >= 5) {
      sendSlackMessage(slackChannel, "⚠️ It's been 5 days since the Google token was last refreshed. Please refresh the Google token.");
      googleTokenLastChecked = currentDate;
    }
  }
};

checkTokensAndNotify();

// setInterval(checkTokensAndNotify, 2400000);  // 40 minutes in milliseconds
setInterval(checkTokensAndNotify, 60000);
setInterval(remindGoogleTokenRefresh, 86400000);

module.exports = { 
  checkTokensAndNotify,
  remindGoogleTokenRefresh
};
