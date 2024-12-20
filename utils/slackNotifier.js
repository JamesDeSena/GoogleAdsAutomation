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
                text: `${text}\n\n${text.includes('Bing') || text.includes('Google') ? 'Please authenticate using the following links:' : ''} \nBing authentication link: https://googleadsautomation.onrender.com/api/auth/bing\nGoogle authentication link: https://googleadsautomation.onrender.com/api/auth/google`
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

  if (!token.accessToken_Bing && !refreshToken_Google) {
    message = "⚠️ Both Bing access token and Google refresh token are missing. Please authenticate.";
  } else {
    if (!token.accessToken_Bing) {
      message += "⚠️ Bing access token is missing. Please authenticate. \n";
    }

    if (!refreshToken_Google) {
      message += "⚠️ Google refresh token is missing. Please authenticate. \n";
    }
  }

  if (message) {
    console.warn(message);
    sendSlackMessage(slackChannel, message);
  }
};

checkTokensAndNotify();

// setInterval(checkTokensAndNotify, 2400000);  // 40 minutes in milliseconds
setInterval(checkTokensAndNotify, 60000);  // 1 minute in milliseconds

module.exports = { 
  checkTokensAndNotify
};