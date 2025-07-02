const { onDocumentCreated } = require('firebase-functions/v2/firestore');
const { onRequest } = require('firebase-functions/v2/https');
const { google } = require('googleapis');

const moment = require('moment-timezone');

const { onSchedule } = require("firebase-functions/v2/scheduler");

const admin = require("firebase-admin");
admin.initializeApp(); 

const { Timestamp } = require("firebase-admin/firestore");
const db = admin.firestore();

const { logger } = require("firebase-functions"); // <-- Make sure this is included

exports.sendNotification = onDocumentCreated("scheduled_appointment_notifications/{docId}", async (event) => {
  const data = event.data.data();
  if (!data) {
    logger.warn("📭 No notification data found in event.");
    return;
  }

  const { receiverId, title, body } = data;

  if (!receiverId || !Array.isArray(receiverId) || receiverId.length === 0) {
    logger.error("❌ receiverId is missing or not an array of user IDs.");
    return;
  }

  try {
    // 🔍 Fetch FCM tokens from Firestore for each user ID
    const tokenFetches = await Promise.all(
      receiverId.map((userId) =>
        admin.firestore().collection("clients").doc(userId).get()
      )
    );

    const tokens = tokenFetches
      .map((docSnap, idx) => {
        if (!docSnap.exists) {
          logger.warn(`⚠️ User ${receiverId[idx]} not found.`);
          return null;
        }
        const token = docSnap.data().fcmToken;
        if (!token) {
          logger.warn(`⚠️ No FCM token for user ${receiverId[idx]}`);
        }
        return token;
      })
      .filter((token) => typeof token === "string" && token.length > 0);

    if (tokens.length === 0) {
      logger.warn("🚫 No valid FCM tokens found. Notification not sent.");
      return;
    }

    // 🔔 Construct the notification payload
    const message = {
      tokens,
      notification: {
        title: title || "Notifica",
        body: body || "",
      },
      data: {
        id: String(data.id || ""),
        title: title || "",
        body: body || "",
        senderImage: String(data.senderImage || ""),
        senderId: String(data.senderId || ""),
        type: String(data.type || ""),
        appointmentId: data.appointmentId || "",
      },
      apns: {
        payload: {
          aps: {
            alert: { title: title || "", body: body || "" },
            sound: "default",
          },
        },
      },
    };

    // 📡 Send the push notification
    const response = await admin.messaging().sendEachForMulticast(message);

    logger.info(`📨 Notification multicast response → ✅ ${response.successCount} sent, ❌ ${response.failureCount} failed`);

    // 🧾 Log failures
    response.responses.forEach((resp, idx) => {
      if (!resp.success) {
        logger.error(`❌ Token[${idx}] ${tokens[idx]} failed:`, resp.error);
      }
    });

  } catch (error) {
    logger.error("🔥 Error sending push notification:", error);
  }
});


exports.createUserwithEmail = onRequest(async (req, res) => {
  const { email, password } = req.body;
  try {
    const userData = await admin.auth().createUser({ email, password });
    res.status(200).send(userData);
  } catch (err) {
    res.status(500).send(err);
  }
});

exports.deleteUser = onRequest(async (req, res) => {
  const { uid } = req.body;
  try {
    const user = await admin.auth().deleteUser(uid);
    res.status(200).send(user);
  } catch (ex) {
    res.status(500).send(ex);
  }
});

exports.scheduledCalendarSync = onSchedule("0 */2 * * *", async () => {
  const credentials = await getGoogleOAuthConfig();
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });
  const calendar = google.calendar({ version: "v3", auth });

  const startOfMonth = moment().startOf("month").toISOString();
  const endOfNextMonth = moment().add(1, "month").endOf("month").toISOString();

  try {
    const res = await calendar.events.list({
      calendarId: "davideromano5991@gmail.com",
      timeMin: startOfMonth,
      timeMax: endOfNextMonth,
      singleEvents: true,
      orderBy: "startTime",
    });

    const events = res.data.items || [];
    console.log(`Fetched ${events.length} events from Google Calendar.`);

    const batch = admin.firestore().batch();
    const collectionRef = admin.firestore().collection("appointments");

    for (const event of events) {
      const hasAppointmentId = event.extendedProperties?.private?.appointment_id;
      const hasCalendarEventId = event.extendedProperties?.private?.calendar_event_id;

      if (hasAppointmentId || hasCalendarEventId) {
        console.log(`Skipping event ${event.id} → appointment_id: ${hasAppointmentId}, calendar_event_id: ${hasCalendarEventId}`);
        continue;
      }

      const startDate = event.start?.dateTime || event.start?.date;
      const endDate = event.end?.dateTime || event.end?.date;

      if (!startDate || !endDate) {
        console.warn(`Skipping event ${event.id} → missing start or end date.`);
        continue;
      }

      const startMoment = moment(startDate).tz("Europe/Rome");
      const endMoment = moment(endDate).tz("Europe/Rome");
      const durationInMinutes = endMoment.diff(startMoment, "minutes");

      const docRef = collectionRef.doc();
      const appointmentId = docRef.id;

      const appointmentDoc = {
        google_calendar_event_id: event.id || "",
        title: event.summary || "",
        notes: event.description || "",
        start_time: startMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
        end_time: endMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
        appointment_time: Timestamp.fromDate(startMoment.toDate()),
        date_timestamp: Timestamp.fromDate(startMoment.startOf("day").toDate()),
        date: startMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
        time: startMoment.format("HH:mm"),
        total_duration: durationInMinutes,
        color_id: "WHGQm6aJH4wdT8RIqbT1",
        client_id: "",
        email: "",
        employee_id_list: [],
        is_regular: true,
        from_calendar_appointment: false,
        number: "",
        room_id_list: [],
        status_id: "",
        treatment_id_list: [],
      };

      batch.set(docRef, appointmentDoc);
      console.log(`Created Firestore appointment ${appointmentId} for event ${event.id}: ${event.summary}`);

      const updatedEvent = {
        ...event,
        extendedProperties: {
          private: {
            ...(event.extendedProperties?.private || {}),
            appointment_id: appointmentId,
            calendar_event_id: event.id,
          },
        },
      };

      await calendar.events.update({
        calendarId: "davideromano5991@gmail.com",
        eventId: event.id,
        requestBody: updatedEvent,
      });

      console.log(`Updated Google Calendar event ${event.id} with appointment_id ${appointmentId}`);
    }

    await batch.commit();
    console.log("Calendar sync completed.");
    return null;
  } catch (error) {
    console.error("Error fetching Google Calendar events:", error);
    throw new Error("Failed to fetch Google Calendar events");
  }
});

// When an appointment is created, schedule a follow-up notification
exports.scheduleFollowUpNotification = onDocumentCreated(
  "appointments/{appointmentId}",
  async (event) => {
    const appointment = event.data.data();
    const appointmentId = event.params.appointmentId;

    if (!appointment || !appointment.end_time || !appointment.client_id) {
      console.warn("Missing required appointment fields");
      return;
    }

    const endTime = new Date(appointment.end_time);

    // Get client full name
    let clientName = "Client";
    const clientDoc = await db.collection("clients").doc(appointment.client_id).get();
    if (clientDoc.exists) {
      const clientData = clientDoc.data();
      clientName = `${clientData.firstName || ""} ${clientData.lastName || ""}`.trim() || "Client";
    }

    // Get admin user IDs
    const adminUsers = await db.collection("clients").where("isAdmin", "==", true).get();
    const adminIds = adminUsers.docs.map(doc => doc.id);

    if (adminIds.length === 0) {
      console.warn("No admin users found");
      return;
    }

    // Prepare document for delayed notification
    const notificationRef = db.collection("scheduled_appointment_notifications").doc(appointmentId);

    await notificationRef.set({
      appointmentId,
      sendTime: Timestamp.fromDate(endTime),
      createdAt: Timestamp.now(),
      client_id: appointment.client_id,
      clientName,
      receiverIds: adminIds,
      status: "pending", // pending, sent, failed
      ttl: Timestamp.fromMillis(endTime.getTime() + 7 * 24 * 60 * 60 * 1000) // auto-delete in 7 days
    });

    console.log(`Scheduled follow-up notification for appointment ${appointmentId}`);
  }
);

exports.handleNotificationAction = onRequest(async (req, res) => {
  try {
    const { appointmentId, action, userId } = req.body;

    if (!appointmentId || !action || !userId) {
      return res.status(400).json({ error: "appointmentId, action, and userId are required" });
    }

    const appointmentRef = db.collection("appointments").doc(appointmentId);
    const appointmentSnap = await appointmentRef.get();

    if (!appointmentSnap.exists) {
      return res.status(404).json({ error: "Appointment not found" });
    }

    const appointmentData = appointmentSnap.data();
    const now = Timestamp.now();

    // Status IDs from your system
    const STATUS_ARCHIVIATO = "88aa7cf3-c6b6-4cab-91eb-247aa6445a05";
    const STATUS_NO_SHOW = "88aa7cf3-c6b6-4cab-91eb-247aa6445a4g";

    let newStatusId;
    let statusLabel;

    if (action === "appointment_attended") {
      newStatusId = STATUS_ARCHIVIATO;
      statusLabel = "archiviato";
    } else if (action === "appointment_no_show") {
      newStatusId = STATUS_NO_SHOW;
      statusLabel = "no-show";
    } else {
      return res.status(400).json({ error: "Invalid action" });
    }

    // Update appointment document
    await appointmentRef.update({
      status_id: newStatusId,
      status_updated_by: userId,
      status_updated_at: now,
      followup_action: action,
      followup_action_at: now,
    });

    // Log the action for audit/history
    await db.collection("appointment_actions").add({
      appointmentId,
      action,
      userId,
      timestamp: now,
      previousStatus: appointmentData.status_id || null,
      newStatus: newStatusId,
    });

    return res.status(200).json({
      status: "success",
      message: `Appointment marked as ${statusLabel}`,
      appointmentId,
      action,
    });

  } catch (error) {
    console.error("❌ handleNotificationAction error:", error);
    return res.status(500).json({ error: "Internal server error", details: error.message });
  }
});

// Run every 5 minutes to send scheduled follow-up notifications
exports.processScheduledNotifications = onSchedule({
  schedule: "*/5 * * * *",
  timeZone: "Europe/Rome",
}, async () => {
  const now = Timestamp.now();
  const fiveMinutesAgo = Timestamp.fromMillis(Date.now() - 5 * 60 * 1000);

  const snapshot = await db
    .collection("scheduled_appointment_notifications")
    .where("sendTime", ">", fiveMinutesAgo)
    .where("sendTime", "<=", now)
    .where("status", "==", "pending")
    .get();

  if (snapshot.empty) {
    console.log("✅ No scheduled notifications to process at this time.");
    return null;
  }

  const adminUsers = await db.collection("clients").where("isAdmin", "==", true).get();
  const adminIds = adminUsers.docs.map(doc => doc.id);

  for (const doc of snapshot.docs) {
    const data = doc.data();
    const appointmentId = data.appointmentId;

    try {
      const appointmentSnap = await db.collection("appointments").doc(appointmentId).get();
      if (!appointmentSnap.exists) throw new Error("Appointment not found");

      const appointmentData = appointmentSnap.data();
      let clientName = "Cliente";

      if (appointmentData.client_id) {
        const clientSnap = await db.collection("clients").doc(appointmentData.client_id).get();
        if (clientSnap.exists) {
          const client = clientSnap.data();
          clientName = `${client.firstName || ""} ${client.lastName || ""}`.trim() || "Cliente";
        }
      }

      const notificationRef = db.collection("scheduled_appointment_notifications").doc();
      await notificationRef.set({
        id: notificationRef.id,
        title: "Follow-up appuntamento",
        body: `Il cliente si è presentato all'appuntamento? (${clientName})`,
        receiverId: adminIds,
        createdAt: Timestamp.now(),
        desc: `Follow-up dell'appuntamento per ${clientName}`,
        type: "appointment_followup",
        appointmentId,
        client_id: appointmentData.client_id || null,
        actions: [
          { id: "yes", title: "Si", action: "appointment_attended" },
          { id: "no", title: "No", action: "appointment_no_show" }
        ],
        status: adminIds,
        ttl: Timestamp.fromMillis(Date.now() + 7 * 24 * 60 * 60 * 1000)
      });

      await doc.ref.update({
        status: "sent",
        sentAt: Timestamp.now(),
      });

    } catch (error) {
      console.error(`❌ Error sending notification for appointment ${appointmentId}:`, error.message);
      await doc.ref.update({
        status: "failed",
        error: error.message,
        failedAt: Timestamp.now(),
      });
    }
  }

  return null;
});

// Expire pending notifications older than 2 days
exports.expireOldPendingNotifications = onSchedule({
  schedule: "0 3 * * *",
  timeZone: "Europe/Rome"
}, async () => {
  const now = Timestamp.now();
  const twoDaysAgo = Timestamp.fromMillis(Date.now() - 2 * 24 * 60 * 60 * 1000);

  const snapshot = await db
    .collection("scheduled_appointment_notifications")
    .where("status", "==", "pending")
    .where("sendTime", "<=", twoDaysAgo)
    .get();

  if (snapshot.empty) {
    console.log("✅ No old pending notifications to expire.");
    return null;
  }

  for (const doc of snapshot.docs) {
    await doc.ref.update({
      status: "failed",
      expiredAt: now,
    });
    console.log(`⏳ Marked old pending notification ${doc.id} as failed`);
  }

  return null;
});

async function getGoogleOAuthConfig() {
  const storageBucket = admin.storage().bucket();
  const file = storageBucket.file("google_calendar_config/credentials.json");
  const [fileContents] = await file.download();
  return JSON.parse(fileContents.toString());
}
