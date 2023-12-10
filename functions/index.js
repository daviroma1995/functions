const { onDocumentCreated } = require('firebase-functions/v2/firestore');
const { google } = require('googleapis');
const moment = require('moment');
const { onRequest } = require('firebase-functions/v2/https');
const { onSchedule } = require('firebase-functions/v2/scheduler');

const admin = require('firebase-admin');

admin.initializeApp();

exports.sendNotification = onDocumentCreated("notifications/{docId}", async (event) => {
  const data = event.data.data();

  const notificationContent = {
    notification: {
      title: data.title,
      body: data.body,
    },
    data: {
      id: JSON.stringify(data.id),
      title: data.title,
      body: data.body,
      senderImage: JSON.stringify(data.senderImage),
      senderId: JSON.stringify(data.senderId),
      type: JSON.stringify(data.type),
      appointmentId: data.appointmentId,
    },
    apns: {
      payload: {
        aps: {
          alert: {
            title: data.title,
            body: data.body,
          },
          sound: "default",
        },
      },
    },
    topic: data.receiverId,
  };

  await admin.messaging().send(notificationContent);
});

exports.createUserwithEmail = onRequest(async (req, res) => {
  const { email, password } = req.body;
  try {
    const userData = await admin.auth().createUser({
      email,
      password,
    });
    res.status(200).send(userData);
  } catch (err) {
    res.status(500).send(err);
  }
});

exports.deleteUser = onRequest(async (req, res) => {
  const { uid } = req.body;
  console.log(uid);
  try {
    const user = await admin.auth().deleteUser(uid);
    res.status(200).send(user);
  } catch (ex) {
    console.log("Error: " + ex);
    res.status(500).send(ex);
  }
});

exports.scheduledCalendarSync = onSchedule('0 */2 * * *', async (event) => {
  const credentials = await getGoogleOAuthConfig();

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });

  const calendar = google.calendar({
    version: "v3",
    auth,
  });

  const startOfMonth = moment().startOf('month').toISOString();
  const endOfNextMonth = moment().add(1, 'month').endOf('month').toISOString();

  try {
    const res = await calendar.events.list({
      calendarId: 'davideromano5991@gmail.com',
      timeMin: startOfMonth,
      timeMax: endOfNextMonth,
      singleEvents: true,
      orderBy: 'startTime',
    });

    console.log('Full API response:', JSON.stringify(res.data, null, 2));
    const events = res.data.items || [];

    console.log(`Fetched ${events.length} events from Google Calendar:`);

    events.forEach(event => {
      console.log(`- ${event.summary} | ${event.start?.dateTime || event.start?.date}`);
    });

    console.log('Calendar sync completed.');
    return null;
  } catch (error) {
    console.error('Error fetching Google Calendar events:', error);
    throw new Error('Failed to fetch Google Calendar events');
  }
});

async function getGoogleOAuthConfig() {
  const storageBucket = admin.storage().bucket();
  const file = storageBucket.file("google_calendar_config/credentials.json");

  const fileContents = await file.download();
  return JSON.parse(fileContents.toString());
}
