const { onDocumentCreated, onDocumentDeleted } = require('firebase-functions/v2/firestore');
const { onRequest } = require('firebase-functions/v2/https');
const { google } = require('googleapis');
const { onSchedule } = require("firebase-functions/v2/scheduler");
const moment = require('moment-timezone');
const admin = require("firebase-admin");
const { logger } = require("firebase-functions");

// Initialize Firebase Admin once
admin.initializeApp(); 
const db = admin.firestore();

// Import time utilities
const { 
  getNowRome, 
  toRomeMoment, 
  getTimestampNowRome, 
  createTTLFromRomeMoment, 
  toTimestampRome,        
  EUROPE_ROME 
} = require('./timeUtils');

// ========== CONSTANTS ==========
const STATUS_ARCHIVIATO = "88aa7cf3-c6b6-4cab-91eb-247aa6445a05";
const STATUS_NO_SHOW = "88aa7cf3-c6b6-4cab-91eb-247aa6445a4g";
const CALENDAR_ID = "davideromano5991@gmail.com";

// Calendar sync optimization constants
const AUTH_CACHE_DURATION = 50 * 60 * 1000; // 50 minutes
const MAX_EVENTS = 2500;
const BATCH_SIZE = 500;
const DATE_CACHE_DURATION = 30 * 60 * 1000; // 30 minutes

// Action mappings for cleaner code
const NOTIFICATION_ACTIONS = {
  appointment_attended: { statusId: STATUS_ARCHIVIATO, label: "archiviato" },
  appointment_no_show: { statusId: STATUS_NO_SHOW, label: "no-show" },
};

// ========== MODULE-LEVEL CACHING ==========
let cachedAuth = null;
let authLastCreated = null;
let dateRangeCache = null;
let dateRangeCacheTime = null;

// ========== UTILITY FUNCTIONS ==========
const uniq = (arr) => Array.from(new Set(arr.filter(Boolean)));

// Optimized date range caching
function getDateRanges() {
  const now = Date.now();
  if (dateRangeCache && dateRangeCacheTime && (now - dateRangeCacheTime < DATE_CACHE_DURATION)) {
    return dateRangeCache;
  }
  
  dateRangeCache = {
    startOfMonth: moment().startOf("month").toISOString(),
    endOfNextMonth: moment().add(1, "month").endOf("month").toISOString()
  };
  dateRangeCacheTime = now;
  return dateRangeCache;
}

// Optimized Google Auth with caching
async function getAuth() {
  const now = Date.now();
  if (cachedAuth && authLastCreated && (now - authLastCreated < AUTH_CACHE_DURATION)) {
    return cachedAuth;
  }
  
  const credentials = await getGoogleOAuthConfig();
  if (!credentials) throw new Error("No credentials available");
  
  cachedAuth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/calendar", "https://www.googleapis.com/auth/calendar.events"],
  });
  
  authLastCreated = now;
  return cachedAuth;
}

function clearAuthCache() {
  cachedAuth = null;
  authLastCreated = null;
}

// Enhanced error handling with context
function handleCalendarError(error, context = '') {
  const { code, message } = error;
  if (code === 401 || code === 403) clearAuthCache();
  
  const errorMessages = {
    403: "Insufficient permissions. Check service account access.",
    401: "Authentication failed. Check credentials.",
    404: `Calendar not found: ${CALENDAR_ID}`,
    429: "Rate limit exceeded. Function running too frequently.",
    400: `Bad request: ${message}`
  };
  
  const errorMsg = errorMessages[code] || `Calendar API error: ${message}`;
  throw new Error(`${context} - ${errorMsg}`);
}

// Memory-optimized event processing
function processEvent(event) {
  const hasAppointmentId = event.extendedProperties?.private?.appointment_id;
  const hasCalendarEventId = event.extendedProperties?.private?.calendar_event_id;
  
  if (hasAppointmentId || hasCalendarEventId) {
    return { skip: true, reason: 'already_processed' };
  }
  
  const startDate = event.start?.dateTime || event.start?.date;
  const endDate = event.end?.dateTime || event.end?.date;
  
  if (!startDate || !endDate) {
    return { skip: true, reason: 'missing_dates' };
  }
  
  const startMoment = toRomeMoment(startDate);
  const endMoment = toRomeMoment(endDate);
  
  return {
    skip: false,
    startMoment,
    endMoment,
    durationInMinutes: endMoment.diff(startMoment, "minutes"),
    eventData: {
      id: event.id,
      summary: event.summary || "",
      description: event.description || ""
    }
  };
}

function createAppointmentDoc(processedEvent, appointmentId) {
  const { startMoment, endMoment, durationInMinutes, eventData } = processedEvent;
  
  return {
    google_calendar_event_id: eventData.id,
    title: eventData.summary,
    notes: eventData.description,
    start_time: startMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
    end_time: endMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
    appointment_time: toTimestampRome(startMoment),
    date_timestamp: toTimestampRome(startMoment.clone().startOf("day")),
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
    created_at: admin.firestore.FieldValue.serverTimestamp(),
  };
}

// Batch calendar updates for performance
async function updateCalendarEventsBatch(calendar, updates) {
  const promises = updates.map(({ eventId, appointmentId, originalEvent }) =>
    calendar.events.update({
      calendarId: CALENDAR_ID,
      eventId,
      requestBody: {
        ...originalEvent,
        extendedProperties: {
          private: {
            ...(originalEvent.extendedProperties?.private || {}),
            appointment_id: appointmentId,
            calendar_event_id: eventId,
          },
        },
      },
    }).catch(error => {
      logger.error(`Failed to update event ${eventId}:`, error.message);
      if (error.code === 401 || error.code === 403) clearAuthCache();
      return null;
    })
  );
  
  return Promise.allSettled(promises);
}

async function getGoogleOAuthConfig() {
  const storageBucket = admin.storage().bucket();
  const file = storageBucket.file("google_calendar_config/credentials.json");
  const [fileContents] = await file.download();
  return JSON.parse(fileContents.toString());
}

// Optimized admin user fetching with caching
let adminUsersCache = null;
let adminUsersCacheTime = null;
const ADMIN_CACHE_DURATION = 10 * 60 * 1000; // 10 minutes

async function getAdminUsers() {
  const now = Date.now();
  if (adminUsersCache && adminUsersCacheTime && (now - adminUsersCacheTime < ADMIN_CACHE_DURATION)) {
    return adminUsersCache;
  }
  
  const adminUsers = await db.collection("clients").where("isAdmin", "==", true).get();
  adminUsersCache = uniq(adminUsers.docs.map(d => d.id));
  adminUsersCacheTime = now;
  
  if (adminUsersCache.length === 0) {
    logger.warn("No admin users found");
  }
  
  return adminUsersCache;
}

// Optimized client name resolution
async function resolveClientName(clientId) {
  try {
    const clientDoc = await db.collection("clients").doc(clientId).get();
    if (clientDoc.exists) {
      const client = clientDoc.data();
      return `${client.first_name || ""} ${client.last_name || ""}`.trim() || "Cliente";
    }
  } catch (e) {
    logger.warn(`Unable to resolve client name for ${clientId}:`, e?.message);
  }
  return "Cliente";
}

// ========== HTTP ENDPOINTS ==========

exports.handleNotificationAction = onRequest(async (req, res) => {
  // CORS handling
  if (req.method === "OPTIONS") {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
    res.set("Access-Control-Allow-Headers", "content-type, authorization");
    return res.status(204).send("");
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  res.set("Access-Control-Allow-Origin", "*");

  const { appointmentId, action, userId } = req.body || {};
  if (!appointmentId || !action || !userId) {
    return res.status(400).json({ error: "appointmentId, action, and userId are required" });
  }

  const nextAction = NOTIFICATION_ACTIONS[action];
  if (!nextAction) {
    return res.status(400).json({ error: "Invalid action" });
  }

  const appointmentRef = db.collection("appointments").doc(appointmentId);
  const now = getTimestampNowRome();
  let resultPayload = null;

  try {
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(appointmentRef);
      if (!snap.exists) {
        throw Object.assign(new Error("Appointment not found"), { code: 404 });
      }

      const appt = snap.data() || {};
      const previousAction = appt.followup_action ?? null;

      // Idempotent check
      if (previousAction === action) {
        resultPayload = {
          status: "success",
          message: `Appointment already marked as ${NOTIFICATION_ACTIONS[previousAction].label}`,
          appointmentId,
          action,
          idempotent: true,
        };
        return;
      }

      // Conflict check
      if (previousAction && previousAction !== action) {
        throw Object.assign(new Error(`Already handled with: ${previousAction}`), { code: 409 });
      }

      // Update appointment
      tx.update(appointmentRef, {
        status_id: nextAction.statusId,
        status_updated_by: userId,
        status_updated_at: now,
        followup_action: action,
        followup_action_at: now,
      });

      // Create audit records in batch
      const actionsCol = appointmentRef.collection("actions");
      const auditRef = db.collection("appointment_actions").doc();
      
      const actionData = {
        appointmentId,
        action,
        userId,
        timestamp: now,
        previousStatus: appt.status_id ?? null,
        newStatus: nextAction.statusId,
        source: "notification",
      };

      tx.set(actionsCol.doc(), actionData);
      tx.set(auditRef, actionData);

      resultPayload = {
        status: "success",
        message: `Appointment marked as ${nextAction.label}`,
        appointmentId,
        action,
      };
    });

    return res.status(200).json(resultPayload);

  } catch (error) {
    const code = error.code || 500;
    const errorResponses = {
      404: { status: 404, error: "Appointment not found" },
      409: { status: 409, error: "Appointment already processed with a different action" },
    };

    if (errorResponses[code]) {
      return res.status(errorResponses[code].status).json({ error: errorResponses[code].error });
    }

    logger.error("handleNotificationAction error:", error);
    return res.status(500).json({ error: "Internal server error", details: error.message });
  }
});

// Consolidated user management
exports.manageUsers = onRequest(async (req, res) => {
  const { action, email, password, uid } = req.body;
  
  try {
    switch (action) {
      case 'create':
        if (!email || !password) {
          return res.status(400).json({ error: "Email and password required for creation" });
        }
        const userData = await admin.auth().createUser({ email, password });
        return res.status(200).json(userData);
        
      case 'delete':
        if (!uid) {
          return res.status(400).json({ error: "UID required for deletion" });
        }
        await admin.auth().deleteUser(uid);
        return res.status(200).json({ success: true, message: "User deleted" });
        
      default:
        return res.status(400).json({ error: "Invalid action. Use 'create' or 'delete'" });
    }
  } catch (error) {
    logger.error(`User management error for action ${action}:`, error);
    return res.status(500).json({ error: error.message });
  }
});

// ========== FIRESTORE TRIGGERS ==========

exports.scheduleFollowUpNotification = onDocumentCreated("appointments/{appointmentId}", async (event) => {
  const appointment = event.data.data();
  const appointmentId = event.params.appointmentId;

  if (!appointment?.end_time || !appointment?.client_id) {
    logger.warn(`Missing required fields for appointment ${appointmentId}`);
    return;
  }

  try {
    const endTimeRome = toRomeMoment(appointment.end_time);
    const sendTime = toTimestampRome(endTimeRome);
    const createdAt = getTimestampNowRome();
    const ttl = createTTLFromRomeMoment(endTimeRome);

    // Resolve client name and admin users in parallel
    const [client_name, adminIds] = await Promise.all([
      resolveClientName(appointment.client_id),
      getAdminUsers()
    ]);

    if (adminIds.length === 0) {
      logger.warn("No admin users found for notifications");
      return;
    }

    const payload = {
      appointmentId,
      sendTime,
      createdAt,
      client_id: appointment.client_id,
      client_name,
      receiverIds: adminIds,
      status: "pending",
      ttl
    };

    await db.collection("scheduled_appointment_notifications").doc(appointmentId).set(payload, { merge: true });
    logger.info(`Scheduled follow-up for ${client_name} (${appointmentId})`);

  } catch (error) {
    logger.error(`Error scheduling notification for appointment ${appointmentId}:`, error);
    throw error;
  }
});

exports.deleteFollowUpNotification = onDocumentDeleted("appointments/{appointmentId}", async (event) => {
  const appointmentId = event.params.appointmentId;
  
  try {
    // Parallel deletion of scheduled and active notifications
    const [scheduledRef, notificationsQuery] = await Promise.all([
      db.collection("scheduled_appointment_notifications").doc(appointmentId).get(),
      db.collection("new_notification")
        .where("appointmentId", "==", appointmentId)
        .where("type", "==", "appointment_followup")
        .get()
    ]);

    const batch = db.batch();

    // Delete scheduled notification
    if (scheduledRef.exists) {
      batch.delete(scheduledRef.ref);
    }

    // Delete related notifications
    notificationsQuery.docs.forEach(doc => {
      batch.delete(doc.ref);
    });

    await batch.commit();
    logger.info(`Cleaned up notifications for deleted appointment ${appointmentId}`);

  } catch (error) {
    logger.error(`Error cleaning up notifications for appointment ${appointmentId}:`, error);
    throw error;
  }
});

exports.sendNotification = onDocumentCreated("new_notification/{docId}", async (event) => {
  const data = event.data.data();
  if (!data) {
    logger.warn("No notification data found in event.");
    return;
  }

  const { receiverIds, title, body } = data;

  if (!receiverIds?.length) {
    logger.error("receiverIds are missing or empty.");
    return;
  }

  try {
    // Batch fetch FCM tokens
    const tokenFetches = await Promise.all(
      receiverIds.map((userId) => db.collection("clients").doc(userId).get())
    );

    const tokens = uniq(
      tokenFetches.map((docSnap, idx) => {
        if (!docSnap.exists) {
          logger.warn(`User ${receiverIds[idx]} not found.`);
          return null;
        }
        const token = docSnap.data().fcmToken;
        if (!token) logger.warn(`No FCM token for user ${receiverIds[idx]}`);
        return typeof token === "string" && token.length > 0 ? token : null;
      })
    );

    if (tokens.length === 0) {
      logger.warn("No valid FCM tokens found. Notification not sent.");
      return;
    }

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
        appointmentId: String(data.appointmentId || ""),
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

    const response = await admin.messaging().sendEachForMulticast(message);
    logger.info(`Notification sent: ${response.successCount} success, ${response.failureCount} failed`);

    // Log failures for debugging
    response.responses.forEach((resp, idx) => {
      if (!resp.success) {
        logger.error(`Token[${idx}] failed:`, resp.error);
      }
    });

  } catch (error) {
    logger.error("Error sending push notification:", error);
  }
});

// ========== SCHEDULED FUNCTIONS ==========

exports.processScheduledNotifications = onSchedule({
  schedule: "*/5 * * * *",
  timeZone: EUROPE_ROME,
}, async () => {
  logger.info("Running scheduled job: processScheduledNotifications");

  try {
    const nowRome = getNowRome();
    const fiveMinutesAgoRome = nowRome.clone().subtract(5, 'minutes');
    const now = toTimestampRome(nowRome);
    const fiveMinutesAgo = toTimestampRome(fiveMinutesAgoRome);

    const snapshot = await db
      .collection("scheduled_appointment_notifications")
      .where("sendTime", ">", fiveMinutesAgo)
      .where("sendTime", "<=", now)
      .where("status", "==", "pending")
      .get();

    if (snapshot.empty) {
      logger.info("No scheduled notifications to process.");
      return null;
    }

    logger.info(`Found ${snapshot.size} pending notifications to process.`);

    // Process notifications with transactional leasing
    for (const pendingDoc of snapshot.docs) {
      const docRef = pendingDoc.ref;

      // Acquire lease transactionally
      const leased = await db.runTransaction(async (tx) => {
        const snap = await tx.get(docRef);
        if (!snap.exists || snap.get("status") !== "pending") return false;

        tx.update(docRef, {
          status: "processing",
          processingAt: getTimestampNowRome(),
        });
        return true;
      });

      if (!leased) {
        logger.info(`Skipping ${pendingDoc.id} (already processing or not pending)`);
        continue;
      }

      // Process the notification
      const dataSnap = await docRef.get();
      const data = dataSnap.data();
      const appointmentId = data.appointmentId;

      logger.info(`Processing notification for appointment: ${appointmentId}`);

      try {
        // Verify appointment exists
        const appointmentSnap = await db.collection("appointments").doc(appointmentId).get();
        if (!appointmentSnap.exists) {
          logger.warn(`Appointment ${appointmentId} no longer exists - removing notification`);
          await docRef.delete();
          continue;
        }

        const appointmentData = appointmentSnap.data();
        const client_name = data.client_name || "Cliente";

        // Create notification
        const notificationRef = db.collection("new_notification").doc();
        const notificationPayload = {
          id: notificationRef.id,
          title: "Follow-up appuntamento",
          body: `${client_name} si è presentato/a all'appuntamento?`,
          receiverIds: data.receiverIds,
          createdAt: getTimestampNowRome(),
          createdAtFormatted: nowRome.format("YYYY-MM-DD HH:mm:ss"),
          desc: `Follow-up dell'appuntamento per ${client_name}`,
          type: "appointment_followup",
          appointmentId,
          client_id: appointmentData.client_id || null,
          actions: [
            { id: "yes", title: "È venuto", action: "appointment_attended" },
            { id: "no", title: "Non si è presentato", action: "appointment_no_show" }
          ],
          status: "pending",
          ttl: toTimestampRome(nowRome.clone().add(7, 'days')),
        };

        await notificationRef.set(notificationPayload);

        // Mark as sent
        await docRef.update({
          status: "sent",
          sentAt: getTimestampNowRome(),
          sentAtFormatted: nowRome.format("YYYY-MM-DD HH:mm:ss"),
        });

        logger.info(`Notification for appointment ${appointmentId} created and marked as sent.`);

      } catch (error) {
        logger.error(`Error processing appointmentId ${appointmentId}:`, error.message);
        await docRef.update({
          status: "failed",
          error: error.message,
          failedAt: getTimestampNowRome(),
        });
      }
    }

    logger.info("Finished processing scheduled notifications.");
    return null;

  } catch (globalError) {
    logger.error("Global error in processScheduledNotifications:", globalError);
    throw globalError;
  }
});

exports.scheduledCalendarSync = onSchedule("*/30 * * * *", async () => {
  const startTime = Date.now();
  logger.info("Starting optimized calendar sync...");
  
  try {
    const auth = await getAuth();
    const calendar = google.calendar({ version: "v3", auth });
    const { startOfMonth, endOfNextMonth } = getDateRanges();
    
    // Fetch events with optimized parameters
    const res = await calendar.events.list({
      calendarId: CALENDAR_ID,
      timeMin: startOfMonth,
      timeMax: endOfNextMonth,
      singleEvents: true,
      orderBy: "startTime",
      maxResults: MAX_EVENTS,
      fields: "items(id,summary,description,start,end,extendedProperties/private)"
    }).catch(error => handleCalendarError(error, "Event fetch failed"));

    const events = res.data.items || [];
    logger.info(`Fetched ${events.length} events`);
    
    if (events.length === 0) {
      return { success: true, totalEvents: 0, processedEvents: 0, skippedEvents: 0, executionTime: Date.now() - startTime };
    }

    // Check existing events in Firestore (handle Firestore's 10-item limit for 'in' queries)
    const existingEventIds = new Set();
    const eventIds = events.map(event => event.id).filter(Boolean);
    
    // Process in chunks of 10 due to Firestore 'in' query limitation
    for (let i = 0; i < eventIds.length; i += 10) {
      const chunk = eventIds.slice(i, i + 10);
      const existingQuery = await db.collection("appointments")
        .where("google_calendar_event_id", "in", chunk)
        .get();
      
      existingQuery.docs.forEach(doc => {
        existingEventIds.add(doc.data().google_calendar_event_id);
      });
    }
    
    logger.info(`Found ${existingEventIds.size} existing appointments in Firestore`);

    const collectionRef = db.collection("appointments");
    const calendarUpdates = [];
    let processedCount = 0;
    let skippedCount = 0;
    let appCreatedCount = 0;
    
    // Process events in batches
    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const eventBatch = events.slice(i, i + BATCH_SIZE);
      let batchOperations = 0;
      
      for (const event of eventBatch) {
        try {
          // First check: Was this appointment created from the app?
          const hasAppointmentId = event.extendedProperties?.private?.appointment_id;
          const hasCalendarEventId = event.extendedProperties?.private?.calendar_event_id;
          
          if (hasAppointmentId || hasCalendarEventId) {
            logger.debug(`Skipping app-created appointment: ${event.id}`);
            appCreatedCount++;
            continue;
          }

          // Second check: Does this calendar event already exist in Firestore?
          if (existingEventIds.has(event.id)) {
            logger.debug(`Skipping existing calendar event: ${event.id}`);
            skippedCount++;
            continue;
          }

          // Third check: Basic event validation
          const startDate = event.start?.dateTime || event.start?.date;
          const endDate = event.end?.dateTime || event.end?.date;
          
          if (!startDate || !endDate) {
            logger.warn(`Skipping event with missing dates: ${event.id}`);
            skippedCount++;
            continue;
          }

          // Validate and process dates
          const startMoment = toRomeMoment(startDate);
          const endMoment = toRomeMoment(endDate);
          
          if (!startMoment.isValid() || !endMoment.isValid()) {
            logger.warn(`Skipping event with invalid dates: ${event.id}`);
            skippedCount++;
            continue;
          }
          
          const durationInMinutes = endMoment.diff(startMoment, "minutes");
          
          const docRef = collectionRef.doc();
          const appointmentDoc = {
            google_calendar_event_id: event.id,
            title: event.summary || "",
            notes: event.description || "",
            start_time: startMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
            end_time: endMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
            appointment_time: toTimestampRome(startMoment),
            date_timestamp: toTimestampRome(startMoment.clone().startOf("day")),
            date: startMoment.format("YYYY-MM-DD HH:mm:ss.SSS"),
            time: startMoment.format("HH:mm"),
            total_duration: durationInMinutes,
            color_id: "WHGQm6aJH4wdT8RIqbT1",
            client_id: "",
            email: "",
            employee_id_list: [],
            is_regular: true,
            from_calendar_appointment: true,
            number: "",
            room_id_list: [],
            status_id: "",
            treatment_id_list: [],
            created_at: admin.firestore.FieldValue.serverTimestamp(),
          };
          
          batch.set(docRef, appointmentDoc);
          batchOperations++;
          
          calendarUpdates.push({
            eventId: event.id,
            appointmentId: docRef.id,
            originalEvent: event
          });
          
          processedCount++;
          
        } catch (eventError) {
          logger.error(`Error processing event ${event.id}:`, eventError.message);
          skippedCount++;
        }
      }
      
      // Commit batch if there are operations
      if (batchOperations > 0) {
        await batch.commit().catch(error => {
          logger.error("Firestore batch commit failed:", error);
          throw new Error(`Firestore batch failed: ${error.message}`);
        });
        logger.info(`Committed batch of ${batchOperations} new appointments`);
      }
    }
    
    // Process calendar updates
    if (calendarUpdates.length > 0) {
      for (let i = 0; i < calendarUpdates.length; i += 10) {
        const updateBatch = calendarUpdates.slice(i, i + 10);
        await updateCalendarEventsBatch(calendar, updateBatch);
      }
    }
    
    const executionTime = Date.now() - startTime;
    logger.info(`Sync completed in ${executionTime}ms:`);
    logger.info(`  Total events: ${events.length}`);
    logger.info(`  App-created (skipped): ${appCreatedCount}`);
    logger.info(`  Already in Firestore (skipped): ${skippedCount}`);
    logger.info(`  New calendar appointments: ${processedCount}`);
    
    return {
      success: true,
      totalEvents: events.length,
      processedEvents: processedCount,
      skippedEvents: skippedCount,
      appCreatedEvents: appCreatedCount,
      executionTime
    };
    
  } catch (error) {
    const executionTime = Date.now() - startTime;
    logger.error(`Calendar sync failed after ${executionTime}ms:`, error.message);
    throw error;
  }
});

exports.expireOldPendingNotifications = onSchedule({
  schedule: "0 3 * * *",
  timeZone: EUROPE_ROME
}, async () => {
  const nowRome = getNowRome();
  const twoDaysAgoRome = nowRome.clone().subtract(2, "days");
  const now = toTimestampRome(nowRome);
  const twoDaysAgo = toTimestampRome(twoDaysAgoRome);

  try {
    const snapshot = await db
      .collection("new_notification")
      .where("status", "==", "pending")
      .where("createdAt", "<=", twoDaysAgo)
      .get();

    if (snapshot.empty) {
      logger.info("No old pending notifications to expire.");
      return null;
    }

    // Batch expire notifications
    const batch = db.batch();
    snapshot.docs.forEach(doc => {
      batch.update(doc.ref, {
        status: "failed",
        expiredAt: now,
      });
    });

    await batch.commit();
    logger.info(`Expired ${snapshot.size} old pending notifications.`);

  } catch (error) {
    logger.error("Error expiring old pending notifications:", error);
  }

  return null;
});