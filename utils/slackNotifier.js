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
                text: `${text}\n\n${text.includes('Bing') ? 'Bing' : 'Google'} authentication link: https://googleadsautomation.onrender.com/api/auth/${text.includes('Bing') ? 'bing' : 'google'}`
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

  if (!token.accessToken_Bing) {
    console.error("Bing access token is missing. Please authenticate.");
    sendSlackMessage(slackChannel, "⚠️ Bing access token is missing. Please authenticate.");
  }

  if (!refreshToken_Google) {
    console.error("Google refresh token is missing. Please authenticate.");
    sendSlackMessage(slackChannel, "⚠️ Google refresh token is missing. Please authenticate.");
  }
};

checkTokensAndNotify();

setInterval(checkTokensAndNotify, 2400000);  // 40 minutes in milliseconds
// setInterval(checkTokensAndNotify, 60000);  // 1 minute in milliseconds

module.exports = { 
  checkTokensAndNotify
};