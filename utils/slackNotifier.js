const { WebClient } = require('@slack/web-api');
const { getStoredGoogleToken } = require("../controllers/GoogleAuth");
const { getStoredBingToken } = require("../controllers/BingAuth");
// const { getStoredLinkedinToken } = require("../controllers/LinkedinAuth");

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
  const token = getStoredBingToken();
  const refreshToken_Google = getStoredGoogleToken();
  // const accessToken_Linkedin = getStoredLinkedinToken()

  let message = '';
  let bingLinkAdded = false;
  let googleLinkAdded = false;
  // let linkedinLinkAdded = false;

  if (!token || !token.accessToken_Bing) {
    message += "⚠️ Bing access token is missing. Please authenticate. \n";
    bingLinkAdded = true;
  }

  if (!refreshToken_Google) {
    message += "⚠️ Google refresh token is missing. Please authenticate. \n";
    googleLinkAdded = true;
  }

  // if (!accessToken_Linkedin) {
  //   message += "⚠️ Linkedin refresh token is missing. Please authenticate. \n";
  //   linkedinLinkAdded = true;
  // }

  if (bingLinkAdded && googleLinkAdded /*&& linkedinLinkAdded*/) {
    message += "\nBing authentication link: https://googleadsautomation.onrender.com/api/auth/bing\nGoogle authentication link: https://googleadsautomation.onrender.com/api/auth/google"; //\nLinkedin authentication link: https://googleadsautomation.onrender.com/api/auth/linkedin
  } else {
    if (bingLinkAdded) {
      message += "\nBing authentication link: https://googleadsautomation.onrender.com/api/auth/bing";
    }

    if (googleLinkAdded) {
      message += "\nGoogle authentication link: https://googleadsautomation.onrender.com/api/auth/google";
    }

    // if (linkedinLinkAdded) {
    //   message += "\nLinkedin authentication link: https://googleadsautomation.onrender.com/api/auth/linkedin";
    // }
  }

  if (message) {
    console.warn(message);
    sendSlackMessage(slackChannel, message);
  }
};

const remindGoogleTokenRefresh = () => {
  const refreshToken_Google = getStoredGoogleToken();

  if (refreshToken_Google) {
    sendSlackMessage(slackChannel, "⚠️ It's time to refresh the Google token. Please refresh the token.");
  }
};

checkTokensAndNotify();

setInterval(checkTokensAndNotify, 2400000);  // 40 minutes in milliseconds
// setInterval(checkTokensAndNotify, 60000);
setInterval(remindGoogleTokenRefresh, 345600000);

module.exports = { 
  checkTokensAndNotify,
  remindGoogleTokenRefresh
};
