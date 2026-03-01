const { onDocumentCreated, onDocumentDeleted, onDocumentUpdated, onDocumentWritten } = require('firebase-functions/v2/firestore');
const { onRequest, onCall, HttpsError } = require('firebase-functions/v2/https');
const { google } = require('googleapis');
const { onSchedule } = require("firebase-functions/v2/scheduler");
const moment = require('moment-timezone');
const admin = require("firebase-admin");
const { logger } = require("firebase-functions");

// Initialize Firebase Admin once
admin.initializeApp(); 
const db = admin.firestore();
const {  
  EUROPE_ROME,
  // ✅ Timezone-agnostic functions for app appointments
  extractTimeFromInput,         // For: time field (HH:mm exactly as selected)
  toTimestampFromInput,         // For: ALL date fields (no timezone shift)
  // ✅ Firestore Timestamp helpers
  formatDateTimeRome,           // For: createdAtFormatted
  getNowTimestamp,              // For: created_at, updated_at
  createTTL,                    // For: ttl fields
  getTimestampDaysAgo,          // For: cleanup queries
  getDurationMinutes,           // For: calculating duration (Date objects)
  // ✅ Timezone-agnostic functions for Google Calendar sync
  toLocalTimestamp,             // For: storing local time from calendar events
  formatTimeLocal,              // For: formatting time using event's timezone
  isPastLocal,                  // For: checking if past using event's timezone
  // ✅ Defensive parsing
  parseAsUTC,                   // For: listAppointments date filters
} = require('./timeUtils');

// ========== CONSTANTS ==========
const STATUS_ARCHIVIATO = "88aa7cf3-c6b6-4cab-91eb-247aa6445a05";
const STATUS_NO_SHOW = "88aa7cf3-c6b6-4cab-91eb-247aa6445a4g";
const STATUS_CONFERMATO = "88aa7cf3-c6b6-4cab-91eb-247aa6445a0a"; // ✅ ADD THIS
const CALENDAR_ID = "davideromano5991@gmail.com";

const DEFAULT_COLOR_ID = "BQk2GETESHgJQZb9C59r";

   
/* =========================================================
  🗄️ TREATMENT & CATEGORY CACHE
  ========================================================= */
let treatmentCache = null;
let categoryCache = null;
let cacheLoadedAt = null;
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour TTL

/* =========================================================
   👤 USER CACHE
   ========================================================= */
   const userCache = new Map();
   const USER_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
   const MAX_USER_CACHE_SIZE = 500;
   
   /**
    * Get user name from cache or Firestore
    */

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

// ========== SECURITY HELPERS ==========

/**
 * Server-side admin verification — never trust isAdmin from client payload.
 * Reuses the same pattern as unlockClient.
 *
 * @param {string} uid - Firebase Auth UID of the caller
 * @returns {Promise<boolean>} - True if user is admin
 */
async function verifyIsAdmin(uid) {
  if (!uid) return false;
  const callerDoc = await db.collection("clients").doc(uid).get();
  return callerDoc.exists && callerDoc.data().isAdmin === true;
}

/**
 * Checks if an appointment starts within the next hour.
 * Used to block last-minute edits/deletes for non-admin users.
 *
 * @param {object} appointmentData - Firestore document data
 * @returns {boolean} - True if appointment starts within 1 hour from now
 */
function isWithinOneHour(appointmentData) {
  const startTime = appointmentData.start_time?.toDate
    ? appointmentData.start_time.toDate()
    : new Date(appointmentData.start_time);

  if (isNaN(startTime.getTime())) return false;

  const oneHourFromNow = new Date(Date.now() + 60 * 60 * 1000);
  return startTime <= oneHourFromNow;
}


/* =========================================================
   🔁 Google Calendar Sync Helper (calls your onRequest endpoint)
   ========================================================= */
/**
 * 🔁 Google Calendar Sync Helper (calls your onRequest endpoint)
 */
/**
 * Syncs appointment with Google Calendar
 * @param {string} operation - "CREATE", "UPDATE", or "DELETE"
 * @param {string} appointmentId - Firestore appointment document ID
 * @param {object} appointmentData - Complete appointment data
 * @returns {Promise<boolean>} - True if sync succeeded, false otherwise
 */
async function syncWithGoogleCalendar(operation, appointmentId, appointmentData = {}) {
  const endpoint =
    process.env.GOOGLE_CALENDAR_FUNCTION_URL ||
    "https://us-central1-aetherium-salon.cloudfunctions.net/googleCalendarEvent";

  // ✅ Validate required fields before attempting sync
  if (!appointmentId) {
    console.error("❌ [Calendar Sync] Missing appointmentId");
    return false;
  }

  if (!operation || !["CREATE", "UPDATE", "DELETE"].includes(operation)) {
    console.error(`❌ [Calendar Sync] Invalid operation: ${operation}`);
    return false;
  }

  // ✅ For UPDATE operations, verify we have a Google Calendar event ID
  if (operation === "UPDATE" && !appointmentData.google_calendar_event_id) {
    console.warn(`⚠️ [Calendar Sync] UPDATE requested but no google_calendar_event_id found for ${appointmentId}`);
    console.warn(`⚠️ [Calendar Sync] Skipping sync - appointment may not be in Google Calendar yet`);
    return false;
  }

  // ✅ For DELETE operations, we need the event ID
  if (operation === "DELETE" && !appointmentData.google_calendar_event_id) {
    console.warn(`⚠️ [Calendar Sync] DELETE requested but no google_calendar_event_id found for ${appointmentId}`);
    return false;
  }


  const body = {
    operation,
    appointment_id: appointmentId,
    appointment: appointmentData,
  };

  // ✅ Log the full payload (for debugging)
  console.log(`📤 [Calendar Sync] Full payload:`);
  console.log(JSON.stringify(body, null, 2));
  console.log(`📤 ========================================`);

  // ✅ Retry logic with exponential backoff
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`📡 [Calendar Sync] Attempt ${attempt}/3 - Sending ${operation} request...`);
      
      const res = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${process.env.CALENDAR_API_KEY}`,
          "X-Request-ID": `${appointmentId}-${Date.now()}` // For tracking
        },
        body: JSON.stringify(body),
      });

      console.log(`📡 [Calendar Sync] Response status: ${res.status} ${res.statusText}`);

      if (res.ok) {
        try {
          const responseData = await res.json();
          console.log(`✅ ========================================`);
          console.log(`✅ [Calendar Sync] ${operation} SUCCEEDED for ${appointmentId}`);
          console.log(`✅ [Calendar Sync] Response:`, JSON.stringify(responseData, null, 2));
          console.log(`✅ ========================================`);
          return true;
        } catch (parseError) {
          // Response is OK but not JSON
          const text = await res.text();
          console.log(`✅ [Calendar Sync] ${operation} succeeded (non-JSON response): ${text}`);
          return true;
        }
      } else {
        // ✅ Enhanced error logging
        const text = await res.text();
        console.error(`❌ ========================================`);
        console.error(`❌ [Calendar Sync] ${operation} FAILED`);
        console.error(`❌ [Calendar Sync] Attempt: ${attempt}/3`);
        console.error(`❌ [Calendar Sync] Status: ${res.status} ${res.statusText}`);
        console.error(`❌ [Calendar Sync] Response: ${text}`);
        
        try {
          const errorJson = JSON.parse(text);
          console.error(`❌ [Calendar Sync] Error details:`, JSON.stringify(errorJson, null, 2));
          
          // ✅ Log specific error types
          if (errorJson.error) {
            console.error(`❌ [Calendar Sync] Error message: ${errorJson.error}`);
          }
          if (errorJson.code) {
            console.error(`❌ [Calendar Sync] Error code: ${errorJson.code}`);
          }
        } catch (e) {
          console.error(`❌ [Calendar Sync] Raw error text: ${text.substring(0, 500)}`);
        }
        console.error(`❌ ========================================`);

        // ✅ Don't retry on certain errors (4xx client errors)
        if (res.status >= 400 && res.status < 500) {
          console.error(`⚠️ [Calendar Sync] Client error (${res.status}), not retrying`);
          break;
        }
      }
    } catch (err) {
      console.error(`❌ ========================================`);
      console.error(`❌ [Calendar Sync] ${operation} attempt ${attempt}/3 EXCEPTION`);
      console.error(`❌ [Calendar Sync] Error type: ${err.constructor.name}`);
      console.error(`❌ [Calendar Sync] Error message: ${err.message}`);
      console.error(`❌ [Calendar Sync] Stack trace:`, err.stack);
      console.error(`❌ ========================================`);
    }

    // ✅ Exponential backoff before retry (1s, 2s, 4s)
    if (attempt < 3) {
      const delay = 1000 * Math.pow(2, attempt - 1);
      console.log(`⏳ [Calendar Sync] Waiting ${delay}ms before retry...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  // ✅ All attempts failed - queue for retry
  console.log(`⚠️ ========================================`);
  console.log(`⚠️ [Calendar Sync] All 3 attempts failed for ${appointmentId}`);
  console.log(`⚠️ [Calendar Sync] Queueing for retry...`);
  
  try {
    await db.collection("calendar_sync_queue").doc(appointmentId).set(
      cleanUndefined({
        appointmentId,
        operation,
        appointment: cleanUndefined(appointmentData || {}),
        status: "failed",
        attempts: admin.firestore.FieldValue.increment(1),
        lastErrorAt: admin.firestore.Timestamp.now(),
        createdAt: admin.firestore.Timestamp.now(),
        nextRetryAt: admin.firestore.Timestamp.fromDate(
          new Date(Date.now() + 5 * 60 * 1000) // Retry in 5 minutes
        ),
      }),
      { merge: true }
    );
    console.log(`✅ [Calendar Sync] Successfully queued ${appointmentId} for retry`);
  } catch (queueError) {
    console.error(`❌ [Calendar Sync] Failed to queue retry:`, queueError.message);
  }
  
  console.log(`⚠️ ========================================`);
  return false;
}

  /**
   * 🧹 Helper: remove undefined fields before writing to Firestore
   */
function cleanUndefined(obj) {
  return Object.fromEntries(Object.entries(obj).filter(([_, v]) => v !== undefined));
}

/**
 * VALIDATION: Ensures a notification has a valid structure before saving.
 * Throws an error with a readable message if invalid.
 */
function validateNotification(doc) {
  const errors = [];

  if (!doc.type) errors.push("Missing required field: type");
  if (!doc.title) errors.push("Missing required field: title");
  if (!doc.body) errors.push("Missing required field: body");

  if (!Array.isArray(doc.receiverIds) || doc.receiverIds.length === 0) {
    errors.push("receiverIds must be a non-empty array");
  }

  // appointment_followup notifications must include actions[]
  if (doc.type === "appointment_followup") {
    if (!Array.isArray(doc.actions) || doc.actions.length === 0) {
      errors.push("appointment_followup requires actions[]");
    }
  }

  // If link is present it must be a string
  if (doc.link && typeof doc.link !== "string") {
    errors.push("link must be a string");
  }

  // TTL must exist and be a Firestore Timestamp
  if (!doc.ttl) {
    errors.push("TTL missing");
  }

  if (errors.length > 0) {
    throw new Error("Notification validation failed: " + errors.join("; "));
  }
}

function buildScheduledNotification(payload) {
  const doc = cleanUndefined({
    id: payload.id,
    appointmentId: payload.appointmentId,
    clientId: payload.clientId,
    client_name: payload.client_name,
    receiverIds: payload.receiverIds || [],
    status: payload.receiverIds || [],
    action: payload.action,
    sendTime: payload.sendTime,
    notification_status: "pending",
    // ✅ FIXED: Use new timestamp helpers
    createdAt: getNowTimestamp(),
    createdAtFormatted: formatDateTimeRome(new Date()),
    ttl: createTTL(payload.ttlDays || 30),
  });

  // scheduled notifications do NOT have type/title/body/actions — skip validation
  // But ensure we have required scheduling fields:
  if (!doc.sendTime) throw new Error("Scheduled notification missing sendTime");
  if (!doc.receiverIds.length) throw new Error("Scheduled notification missing receiverIds");

  return doc;
}


function buildNotification(payload) {
  const doc = cleanUndefined({
    id: payload.id,
    title: payload.title,
    body: payload.body,
    type: payload.type,
    receiverIds: payload.receiverIds || [],
    status: payload.receiverIds || [],
    appointmentId: payload.appointmentId,
    actions: payload.actions,
    link: payload.link,
    createdAt: getNowTimestamp(),
    createdAtFormatted: formatDateTimeRome(new Date()),
    notification_status: "pending",
    ttl: createTTL(payload.ttlDays || 30),
  });

  validateNotification(doc);
  return doc;
}

/**
 * 👥 Helper: Resolve notification recipients
 * - Admin actor → notify ONLY the client (if exists)
 * - Client actor → notify ALL admins (excluding the actor)
 * - Always excludes the actor from receiving their own notification
 */
async function getNotificationRecipients(isAdmin, currentUserId, clientId) {
  const adminIds = await getAdminUsers();

  // ---------------------------
  // CASE 1 — Actor is a CLIENT
  // ---------------------------
  if (!isAdmin) {
    // Client performed an action → notify ALL admins (excluding self if they're also admin)
    return adminIds.filter(id => id !== currentUserId);
  }

  // ---------------------------
  // CASE 2 — Actor is an ADMIN
  // ---------------------------
  const recipients = [];

  // ✅ ALWAYS notify other admins (excluding creator)
  const otherAdmins = adminIds.filter(id => id !== currentUserId);
  recipients.push(...otherAdmins);

  // ✅ ALSO notify the client (if exists and is not the creator)
  if (clientId && clientId.trim() !== "" && clientId !== currentUserId) {
    recipients.push(clientId);
  }

  return recipients;
}
    
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
  try {
    const storageBucket = admin.storage().bucket();
    const file = storageBucket.file("google_calendar_config/credentials.json");
    const [fileContents] = await file.download();
    return JSON.parse(fileContents.toString());
  } catch (e) {
    logger.error("Failed to load Google Calendar credentials:", e.message);
    throw new Error("Missing or malformed credentials for Google Calendar");
  }
}

// Optimized admin user fetching with caching
let adminUsersCache = null;
let adminUsersCacheTime = null;
const ADMIN_CACHE_DURATION = 10 * 60 * 1000; // 10 minutes

async function getAdminUsers() {
  const now = Date.now();

  // Use cached admin list for 10 minutes
  if (
    adminUsersCache &&
    adminUsersCacheTime &&
    now - adminUsersCacheTime < ADMIN_CACHE_DURATION
  ) {
    return adminUsersCache;
  }

  logger.info("Refreshing admin users cache…");

  const snap = await db.collection("clients")
    .where("isAdmin", "==", true)
    .get();

  const admins = snap.docs.map((d) => d.id);
  adminUsersCache = uniq(admins);
  adminUsersCacheTime = now;

  if (adminUsersCache.length === 0) {
    logger.warn("⚠️ No admin users found in collection 'clients'.");
  } else {
    logger.info(`👑 Loaded ${adminUsersCache.length} admin users.`);
  }

  return adminUsersCache;
}


/**
 * 🏆 Award membership points for past appointments within active membership year
 */
async function awardPointsForPastAppointment(clientId, appointmentDateISO) {
  if (!clientId || !appointmentDateISO) return;

  const membershipRef = db.collection("client_memberships").doc(clientId);
  const membershipSnap = await membershipRef.get();

  if (!membershipSnap.exists) {
    logger.warn(`⚠️ No membership found for client ${clientId}`);
    return;
  }

  const membership = membershipSnap.data();
  const { start_date, points: currentPoints = 0 } = membership;

  if (!start_date) {
    logger.warn(`⚠️ Missing start_date for membership of client ${clientId}`);
    return;
  }

  // membership start date is DD/MM/YYYY
  const [day, month, year] = start_date.split("/");
  const startDay = parseInt(day, 10);
  const startMonth = parseInt(month, 10);

  const appointmentMoment = moment.tz(appointmentDateISO, EUROPE_ROME);
  const appointmentYear = appointmentMoment.year();

  // Build active membership window
  let startMoment = moment.tz(
    `${appointmentYear}-${String(startMonth).padStart(2, "0")}-${String(startDay).padStart(2,"0")}`,
    "YYYY-MM-DD",
    EUROPE_ROME
  ).startOf("day");

  let endMoment = startMoment.clone().add(1, "year").endOf("day");

  // If appointment precedes the year window → shift back one year
  if (appointmentMoment.isBefore(startMoment)) {
    startMoment = startMoment.subtract(1, "year");
    endMoment = startMoment.clone().add(1, "year").endOf("day");
  }

  // Only award points for *past* appointments in the active window
  if (
    appointmentMoment.isBefore(moment.tz(EUROPE_ROME)) &&
    appointmentMoment.isBetween(startMoment, endMoment, null, "[]")
  ) {
    const newPoints = Math.min(currentPoints + 25, 300);

    if (newPoints > currentPoints) {
      await membershipRef.update({ points: newPoints });
      logger.info(
        `🏆 Points awarded (${appointmentDateISO}) — ${clientId}: ${currentPoints} → ${newPoints}`
      );
    } else {
      logger.info(`ℹ️ Membership cap (300) reached for ${clientId}`);
    }
  } else {
    logger.debug(
      `No points: ${appointmentDateISO} outside membership window ${startMoment.format("DD/MM/YYYY")} → ${endMoment.format("DD/MM/YYYY")}`
    );
  }
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

exports.handleNotificationAction = onRequest(async (req, res) => {
  // CORS
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).send('');
  }

  const { appointmentId, action, userId } = req.body || {};
  
  logger.info("📥 handleNotificationAction called", {
    appointmentId,
    action,
    userId,
    timestamp: new Date().toISOString()
  });

  if (!appointmentId || !action || !userId) {
    return res.status(400).json({
      error: "appointmentId, action, and userId are required",
    });
  }

  const nextAction = NOTIFICATION_ACTIONS[action];
  if (!nextAction) {
    return res.status(400).json({ error: "Invalid action" });
  }

  const appointmentRef = db.collection("appointments").doc(appointmentId);
  const now = admin.firestore.Timestamp.now();

  try {
    // ✅ OPTIMIZATION: Read appointment OUTSIDE transaction first
    const apptSnap = await appointmentRef.get();
    
    if (!apptSnap.exists) {
      return res.status(404).json({ error: "Appointment not found" });
    }

    const appt = apptSnap.data() || {};
    const previousAction = appt.followup_action ?? null;
    const clientId = appt.client_id;

    // ✅ IDEMPOTENT CHECK - before transaction
    if (previousAction === action) {
      logger.info("✅ Idempotent - action already applied", {
        appointmentId,
        action
      });
      return res.status(200).json({
        notification_status: "success",
        message: `Appointment already marked as ${NOTIFICATION_ACTIONS[previousAction].label}`,
        appointmentId,
        action,
        idempotent: true,
      });
    }

    // ✅ CONFLICT CHECK - before transaction
    if (previousAction && previousAction !== action) {
      logger.warn("❌ CONFLICT DETECTED", {
        appointmentId,
        previousAction,
        requestedAction: action,
      });
      return res.status(409).json({ 
        error: "Appointment already processed with a different action" 
      });
    }

    // ✅ Read flag and membership OUTSIDE transaction (non-critical data)
    const [flagSnap, membershipSnap] = await Promise.all([
      db.collection("clients").doc(clientId).collection("flags").doc("no_show").get(),
      db.collection("client_memberships").doc(clientId).get(),
    ]);

    const currentNoShowCount = flagSnap.exists ? (flagSnap.data()?.count || 0) : 0;
    const hasMembership = membershipSnap.exists;

    // ✅ LIGHTWEIGHT TRANSACTION - only critical writes
    await db.runTransaction(async (tx) => {
      // Quick recheck (race condition protection)
      const freshSnap = await tx.get(appointmentRef);
      if (!freshSnap.exists) {
        throw Object.assign(new Error("Appointment not found"), { code: 404 });
      }

      const freshData = freshSnap.data();
      if (freshData.followup_action && freshData.followup_action !== action) {
        throw Object.assign(
          new Error(`Already handled with: ${freshData.followup_action}`),
          { code: 409 }
        );
      }

      // Update appointment
      tx.update(appointmentRef, {
        status_id: nextAction.statusId,
        status_updated_by: userId,
        status_updated_at: now,
        followup_action: action,
        followup_action_at: now,
      });

      // Update no-show flag if applicable
      if (action === "appointment_no_show") {
        const newCount = currentNoShowCount + 1;
        const flagRef = db.collection("clients").doc(clientId).collection("flags").doc("no_show");
        
        tx.set(flagRef, {
          count: newCount,
          updated_at: now,
        }, { merge: true });

        // Lock client if threshold reached
        if (hasMembership && newCount >= 3) {
          const clientRef = db.collection("clients").doc(clientId);
          tx.update(clientRef, { 
            is_locked: true,
            locked_at: now,
            locked_reason: "no_show_threshold",
          });
        }
      }
    });

    logger.info("✅ Transaction complete", { appointmentId, action });
    
    return res.status(200).json({
      notification_status: "success",
      message: `Appointment marked as ${nextAction.label}`,
      appointmentId,
      action,
    });

  } catch (error) {
    const code = error.code || 500;

    logger.error("❌ handleNotificationAction error", {
      appointmentId,
      action,
      errorCode: code,
      errorMessage: error.message,
    });

    if (code === 404) {
      return res.status(404).json({ error: "Appointment not found" });
    }

    if (code === 409) {
      return res.status(409).json({ error: "Appointment already processed with a different action" });
    }

    return res.status(500).json({
      error: "Internal server error",
      details: error.message,
    });
  }
});

// Auto-invalidate when treatments change
exports.onTreatmentChange = onDocumentWritten("treatments/{treatmentId}", async (event) => {
  console.log("📝 Treatment changed:", event.params.treatmentId);
  invalidateTreatmentCache();
  await loadTreatmentCache(true);
});

// Auto-invalidate when categories change
exports.onTreatmentCategoryChange = onDocumentWritten("treatment_categories/{categoryId}", async (event) => {
  console.log("📝 Category changed:", event.params.categoryId);
  invalidateTreatmentCache();
  await loadTreatmentCache(true);
});

  
  exports.onAppointmentCompleted = onDocumentUpdated("appointments/{appointmentId}", async (event) => {
    const appointmentId = event.params.appointmentId;
    const before = event.data?.before?.data();
    const after = event.data?.after?.data();
  
    logger.info(`🔄 Trigger fired for appointment ${appointmentId}`);
  
    if (!before || !after) {
      logger.error(`❌ Missing before/after snapshot for ${appointmentId}`);
      return;
    }
  
    logger.info(`📌 Status change: ${before.status_id} → ${after.status_id}`);
  
    const STATUS_COMPLETED = STATUS_ARCHIVIATO;
    const wasCompleted = before.status_id === STATUS_COMPLETED;
    const isCompleted = after.status_id === STATUS_COMPLETED;
  
    // ――― SKIP CONDITIONS ―――
    if (!isCompleted) {
      logger.info(`⏭️ Not completed → Skipping appointment ${appointmentId}`);
      return;
    }
  
    if (wasCompleted) {
      logger.info(`⏭️ Already completed previously → Skipping ${appointmentId}`);
      return;
    }
  
    const clientId = after.client_id;
    if (!clientId) {
      logger.warn(`⚠️ Appointment ${appointmentId} has NO client_id`);
      return;
    }
  
    logger.info(`🎯 Completed appointment for client ${clientId}`);
  
    try {
      // ------------------------
      // Check if client already prompted
      // ------------------------
      const clientSnap = await db.collection("clients").doc(clientId).get();
      const alreadyPrompted = clientSnap.exists && clientSnap.data().open_review_prompt === true;
  
      logger.info(`👤 open_review_prompt flag for ${clientId}: ${alreadyPrompted}`);
  
      if (alreadyPrompted) {
        logger.info(`⏭️ Client ${clientId} already received a prompt`);
        return;
      }
  
      // ------------------------
      // Check if already scheduled
      // ------------------------
      const existing = await db
        .collection("scheduled_appointment_notifications")
        .where("clientId", "==", clientId)
        .where("action", "==", "open_review_prompt")
        .limit(1)
        .get();
  
      logger.info(`🔎 Existing scheduled review prompts for ${clientId}: ${existing.size}`);
  
      if (!existing.empty) {
        logger.info(`⏭️ Review prompt already scheduled for ${clientId}`);
        return;
      }
  
      // ------------------------
      // Schedule new review
      // ------------------------
      // ✅ FIXED: Handle Firestore Timestamp
      const endTimeValue = after.end_time?.toDate 
        ? after.end_time.toDate() 
        : new Date(after.end_time);
      const followupTime = new Date(endTimeValue);
      followupTime.setMinutes(followupTime.getMinutes() + 5);
  
      logger.info(`⏰ Scheduling review prompt at: ${followupTime.toISOString()}`);
  
      const scheduledRef = db.collection("scheduled_appointment_notifications").doc();
  
      const payload = buildScheduledNotification({
        id: scheduledRef.id,
        appointmentId,
        clientId,
        client_name: await resolveClientName(clientId),
        receiverIds: [clientId],
        sendTime: admin.firestore.Timestamp.fromDate(followupTime),
        action: "open_review_prompt",
        ttlDays: 30,
      });
  
      await scheduledRef.set(payload);
  
      logger.info(`✅ Review prompt scheduled → scheduledId: ${scheduledRef.id}`);
  
    } catch (error) {
      logger.error(`❌ Failed scheduling review prompt for ${clientId}:`, error);
    }
  });
  
  


exports.backfillRenewalDates = onRequest(async (req, res) => {
  // Migration complete — endpoint disabled
  return res.status(410).json({
    error: "Migration complete",
    message: "This endpoint is no longer available.",
  });
});

exports.unlockClient = onCall(async (request) => {
  const { clientId } = request.data || {};
  const callerUid = request.auth?.uid;

  if (!callerUid) {
    throw new HttpsError("unauthenticated", "Authentication required.");
  }

  if (!clientId) {
    throw new HttpsError("invalid-argument", "Missing clientId.");
  }

  const clientRef = db.collection("clients").doc(clientId);
  const flagRef = clientRef.collection("flags").doc("no_show");

  try {
    const callerDoc = await db.collection("clients").doc(callerUid).get();
    const isAdmin = callerDoc.exists && callerDoc.data().isAdmin === true;

    if (!isAdmin) {
      throw new HttpsError("permission-denied", "Only admins can unlock clients.");
    }

    const now = admin.firestore.Timestamp.now();

    await db.runTransaction(async (tx) => {
      // Get current flag document
      const flagDoc = await tx.get(flagRef);
      
      // Update client is_locked
      tx.update(clientRef, { 
        is_locked: false,
        updated_at: now
      });
      
      // Reset no_show count to 1 (use set with merge to create if doesn't exist)
      tx.set(flagRef, {
        count: 1,
        updated_at: now,
        unlocked_by: callerUid,
        unlocked_at: now
      }, { merge: true });
    });

    console.log(`Client ${clientId} unlocked by admin ${callerUid}. No-show count reset to 1.`);
    
    return { 
      status: "success", 
      message: `Client ${clientId} unlocked and no-show count reset to 1.` 
    };

  } catch (error) {
    console.error("unlockClient error:", error);
    if (error instanceof HttpsError) throw error;
    throw new HttpsError("internal", error.message);
  }
});

exports.ignoreNotification = onRequest(async (req, res) => {
  const requestId = `REQ-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  
  logger.info(`📩 ========================================`);
  logger.info(`📩 [${requestId}] ignoreNotification STARTED`);
  logger.info(`📩 [${requestId}] Timestamp: ${new Date().toISOString()}`);
  logger.info(`📩 ========================================`);

  // CORS
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    logger.info(`📩 [${requestId}] OPTIONS request - returning 204`);
    return res.status(204).send('');
  }

  if (req.method !== "POST") {
    logger.warn(`📩 [${requestId}] Invalid method: ${req.method}`);
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  // Log raw request body
  logger.info(`📩 [${requestId}] Raw request body: ${JSON.stringify(req.body)}`);

  const { user_id, notification_id } = req.body;

  logger.info(`📩 [${requestId}] Parsed parameters:`);
  logger.info(`   user_id: ${user_id} (type: ${typeof user_id})`);
  logger.info(`   notification_id: ${notification_id} (type: ${typeof notification_id})`);

  if (!user_id || !notification_id) {
    logger.warn(`❌ [${requestId}] Missing parameters`);
    logger.warn(`   user_id present: ${!!user_id}`);
    logger.warn(`   notification_id present: ${!!notification_id}`);
    return res.status(400).json({ error: "Missing user_id or notification_id" });
  }

  try {
    logger.info(`🔍 [${requestId}] Starting Firestore transaction...`);
    
    const notificationRef = db.collection("new_notification").doc(notification_id);
    
    // Pre-transaction read to log initial state
    logger.info(`🔍 [${requestId}] Pre-transaction: Reading notification document...`);
    const preReadSnap = await notificationRef.get();
    
    if (!preReadSnap.exists) {
      logger.warn(`❌ [${requestId}] Pre-transaction: Document does NOT exist`);
      return res.status(404).json({ error: "Notification not found" });
    }

    const preData = preReadSnap.data();

    // ─────────────────────────────────────────────────────
    // OWNERSHIP CHECK: user can only dismiss their own notifications
    // ─────────────────────────────────────────────────────
    if (!(preData.status || []).includes(user_id)) {
      logger.warn(`❌ [${requestId}] User ${user_id} not in status array — not authorized`);
      return res.status(403).json({ error: "Not authorized to dismiss this notification" });
    }

    logger.info(`✅ [${requestId}] Pre-transaction: Document EXISTS`);
    logger.info(`   Status array: [${(preData.status || []).join(', ')}]`);
    logger.info(`   User ${user_id} in status: true`);

    logger.info(`🔄 [${requestId}] Executing transaction...`);
    const transactionStartTime = Date.now();

    const result = await db.runTransaction(async (tx) => {
      logger.info(`🔄 [${requestId}] Inside transaction - reading document...`);
      
      const snap = await tx.get(notificationRef);

      if (!snap.exists) {
        logger.warn(`⚠️ [${requestId}] Transaction: Document not found`);
        throw new Error("not-found");
      }

      const data = snap.data();
      const statusBefore = data.status || [];
      
      logger.info(`📊 [${requestId}] Transaction: Document state`);
      logger.info(`   Status array: [${statusBefore.join(', ')}]`);
      logger.info(`   Status length: ${statusBefore.length}`);
      logger.info(`   User ${user_id} in array: ${statusBefore.includes(user_id)}`);
      logger.info(`   User position in array: ${statusBefore.indexOf(user_id)}`);

      // Check if user is in the array
      if (!statusBefore.includes(user_id)) {
        logger.warn(`⚠️ [${requestId}] User NOT in status array - already removed or never added`);
        logger.warn(`   Current status array: [${statusBefore.join(', ')}]`);
        logger.warn(`   Looking for user: ${user_id}`);
        
        // Log each element for debugging
        statusBefore.forEach((uid, idx) => {
          logger.warn(`   [${idx}] "${uid}" === "${user_id}" ? ${uid === user_id}`);
        });
        
        return { alreadyRemoved: true, statusBefore };
      }

      // Calculate what the array will look like after removal
      const statusAfterRemoval = statusBefore.filter(uid => uid !== user_id);
      
      logger.info(`🔄 [${requestId}] Calculating removal:`);
      logger.info(`   Before: [${statusBefore.join(', ')}] (${statusBefore.length} items)`);
      logger.info(`   After:  [${statusAfterRemoval.join(', ')}] (${statusAfterRemoval.length} items)`);
      logger.info(`   Removing: ${user_id}`);

      if (statusAfterRemoval.length === 0) {
        logger.info(`🗑️ [${requestId}] Status will be EMPTY → DELETING notification`);
        logger.info(`   User ${user_id} was the LAST user in status array`);
        tx.delete(notificationRef);
        return { deleted: true, statusBefore, statusAfter: [] };
      }

      logger.info(`💾 [${requestId}] Updating status array (removing ${user_id})`);
      logger.info(`   Using arrayRemove operation`);
      
      tx.update(notificationRef, {
        status: admin.firestore.FieldValue.arrayRemove(user_id)
      });

      return { deleted: false, statusBefore, statusAfter: statusAfterRemoval };
    });

    const transactionDuration = Date.now() - transactionStartTime;
    logger.info(`✅ [${requestId}] Transaction completed in ${transactionDuration}ms`);
    logger.info(`   Result: ${JSON.stringify(result)}`);

    // Verify the removal by reading the document again (only if not deleted)
    if (!result.deleted && !result.alreadyRemoved) {
      logger.info(`🔍 [${requestId}] Post-transaction verification...`);
      
      const verifySnap = await notificationRef.get();
      
      if (verifySnap.exists) {
        const verifyData = verifySnap.data();
        const verifyStatus = verifyData.status || [];
        
        logger.info(`✅ [${requestId}] Verification results:`);
        logger.info(`   Document still exists: true`);
        logger.info(`   Status array NOW: [${verifyStatus.join(', ')}]`);
        logger.info(`   Status length NOW: ${verifyStatus.length}`);
        logger.info(`   User ${user_id} still in array: ${verifyStatus.includes(user_id)}`);
        logger.info(`   Removal CONFIRMED: ${!verifyStatus.includes(user_id)}`);
        
        if (verifyStatus.includes(user_id)) {
          logger.error(`❌ [${requestId}] VERIFICATION FAILED - User still in array!`);
          logger.error(`   Expected: user NOT in array`);
          logger.error(`   Actual: user IS in array`);
        }
      } else {
        logger.warn(`⚠️ [${requestId}] Verification: Document no longer exists (was deleted)`);
      }
    }

    // Prepare response based on result
    if (result.alreadyRemoved) {
      logger.info(`📩 [${requestId}] Response: User was already removed`);
      logger.info(`📩 ========================================`);
      
      return res.status(200).json({
        success: true,
        message: "User was not in the notification status (already removed)",
        user_id,
        notification_id,
        statusBefore: result.statusBefore,
        requestId
      });
    }

    if (result.deleted) {
      logger.info(`📩 [${requestId}] Response: Notification DELETED`);
      logger.info(`   User ${user_id} was the last user in status array`);
      logger.info(`📩 ========================================`);
      
      return res.status(200).json({
        success: true,
        deleted: true,
        user_id,
        notification_id,
        message: `Notification deleted - ${user_id} was the last user`,
        requestId
      });
    }

    logger.info(`📩 [${requestId}] Response: User REMOVED from notification`);
    logger.info(`   Before: [${result.statusBefore.join(', ')}]`);
    logger.info(`   After:  [${result.statusAfter.join(', ')}]`);
    logger.info(`📩 ========================================`);

    return res.status(200).json({
      success: true,
      deleted: false,
      user_id,
      notification_id,
      message: `User ${user_id} removed from notification ${notification_id}`,
      statusBefore: result.statusBefore,
      statusAfter: result.statusAfter,
      requestId
    });

  } catch (error) {
    logger.error(`❌ ========================================`);
    logger.error(`❌ [${requestId}] ignoreNotification ERROR`);
    logger.error(`❌ [${requestId}] Error type: ${error.constructor.name}`);
    logger.error(`❌ [${requestId}] Error message: ${error.message}`);
    logger.error(`❌ [${requestId}] Error stack: ${error.stack}`);
    logger.error(`❌ ========================================`);

    if (error.message === "not-found") {
      logger.error(`❌ [${requestId}] Notification document not found in Firestore`);
      logger.error(`   Collection: new_notification`);
      logger.error(`   Document ID: ${notification_id}`);
      
      return res.status(404).json({ 
        error: "Notification not found",
        notification_id,
        requestId
      });
    }

    return res.status(500).json({
      error: "Internal Server Error",
      details: error.message,
      requestId
    });
  }
});

// Consolidated user management
// TODO: Implement rate limiting using Firebase App Check or
// in-memory counter with IP/UID tracking

exports.manageUsers = onRequest(async (req, res) => {
  // ─────────────────────────────────────────────────────
  // AUTH: Verify admin Firebase ID token
  // ─────────────────────────────────────────────────────
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Missing authorization header" });
  }

  try {
    const idToken = authHeader.split("Bearer ")[1];
    const decoded = await admin.auth().verifyIdToken(idToken);
    const isAdmin = await verifyIsAdmin(decoded.uid);
    if (!isAdmin) {
      return res.status(403).json({ error: "Only admins can manage users" });
    }
  } catch (authErr) {
    return res.status(401).json({ error: "Invalid or expired token" });
  }

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

/* =========================================================
   ⚡ CREATE APPOINTMENT (FULLY OPTIMIZED)
   ========================================================= */
   exports.createSingleAppointment = onCall(async (request) => {
    const startMs = Date.now();

    try {
      const callerUid = request.auth?.uid;
      if (!callerUid) {
        throw new HttpsError("unauthenticated", "Authentication required.");
      }

      const data = request.data || {};
      const {
        number,
        email,
        clientId,
        treatment_id_list,
        employeeIds,
        notes = "",
        duration,
        statusId,
        colorId,
        currentUserId,
        currentUserName,
        start,
        originalStart,
        originalEnd,
      } = data;

      // ─────────────────────────────────────────────────────
      // 1️⃣ VALIDATION (fail fast)
      // ─────────────────────────────────────────────────────
      if (!start || !originalStart || !originalEnd) {
        throw new HttpsError("invalid-argument", "Missing required start or end time.");
      }

      // ─────────────────────────────────────────────────────
      // 1.5️⃣ SERVER-SIDE ADMIN VERIFICATION
      // ─────────────────────────────────────────────────────
      const isAdmin = await verifyIsAdmin(callerUid);

      // ─────────────────────────────────────────────────────
      // 2️⃣ COMPUTE VALUES (sync - no await)
      // ─────────────────────────────────────────────────────
      const displayTime = extractTimeFromInput(start);
      const startTimestamp = toTimestampFromInput(start);
      const originalStartTimestamp = toTimestampFromInput(originalStart);
      const originalEndTimestamp = toTimestampFromInput(originalEnd);

      const durationMs = originalEndTimestamp.toMillis() - originalStartTimestamp.toMillis();
      const durationMinutes = Math.max(1, Math.round(durationMs / 60000));
      const endTimestamp = admin.firestore.Timestamp.fromMillis(
        startTimestamp.toMillis() + durationMs
      );

      // ─────────────────────────────────────────────────────
      // 3️⃣ RESOLVE PERMISSIONS (server-verified)
      // ─────────────────────────────────────────────────────
      const safeClientId = isAdmin ? clientId : callerUid;
      const safeTreatments = treatment_id_list || [];
      const safeEmployeeIds = isAdmin ? (employeeIds || []) : [];
      const safeNotes = isAdmin ? notes : "";
      const safeStatusId = isAdmin ? statusId : STATUS_CONFERMATO;
      const safeColorId = colorId || DEFAULT_COLOR_ID;

      // ─────────────────────────────────────────────────────
      // 4️⃣ BUILD TITLE (parallel with doc ref generation)
      // ─────────────────────────────────────────────────────
      const docRef = db.collection("appointments").doc();
      const title = await buildAppointmentTitle(safeClientId, safeTreatments);

      // ─────────────────────────────────────────────────────
      // 5️⃣ BUILD DOCUMENT (sync)
      // ─────────────────────────────────────────────────────
      const appointment = cleanUndefined({
        number,
        email,
        client_id: safeClientId,
        date: startTimestamp,
        start_time: startTimestamp,
        end_time: endTimestamp,
        date_timestamp: startTimestamp,
        appointment_time: startTimestamp,
        time: displayTime,
        title,
        treatment_id_list: safeTreatments,
        is_regular: true,
        total_duration: duration || durationMinutes,
        employee_id_list: safeEmployeeIds,
        room_id_list: [],
        notes: safeNotes,
        status_id: safeStatusId,
        color_id: safeColorId,
        from_google_calendar: false,
        from_calendar_appointment: true,
        created_at: getNowTimestamp(),
        created_by: currentUserId || callerUid,
        created_by_name: currentUserName || "Unknown",
        is_admin_created: isAdmin,
      });
  
      // ─────────────────────────────────────────────────────
      // 6️⃣ WRITE TO FIRESTORE (single write, no transaction)
      // ─────────────────────────────────────────────────────
      await docRef.set(appointment);
  
      console.log(`✅ Appointment ${docRef.id} created in ${Date.now() - startMs}ms`);
  
      // ─────────────────────────────────────────────────────
      // 7️⃣ RETURN IMMEDIATELY
      // ─────────────────────────────────────────────────────
      return {
        success: true,
        appointmentId: docRef.id,
        message: "Appointment created successfully"
      };
  
    } catch (err) {
      console.error("❌ createSingleAppointment error:", err.message);
      throw new Error(err.message);
    }
  });
  
/* =========================================================
   ⚡ UPDATE APPOINTMENT (FULLY OPTIMIZED)
   ========================================================= */
   exports.updateAppointment = onCall(async (request) => {
    const startMs = Date.now();

    try {
      const callerUid = request.auth?.uid;
      if (!callerUid) {
        throw new HttpsError("unauthenticated", "Authentication required.");
      }

      const data = request.data || {};
      const {
        appointmentId,
        number,
        email,
        clientId,
        treatment_id_list,
        employeeIds,
        notes,
        duration,
        statusId,
        colorId,
        currentUserId,
        currentUserName,
        start,
        originalStart,
        originalEnd,
      } = data;

      // ─────────────────────────────────────────────────────
      // 1️⃣ VALIDATION (fail fast)
      // ─────────────────────────────────────────────────────
      if (!appointmentId) {
        throw new HttpsError("invalid-argument", "Missing appointmentId.");
      }

      const docRef = db.collection("appointments").doc(appointmentId);

      // ─────────────────────────────────────────────────────
      // 1.5️⃣ SERVER-SIDE AUTH + FETCH (parallel)
      // ─────────────────────────────────────────────────────
      const [isAdmin, existingDoc] = await Promise.all([
        verifyIsAdmin(callerUid),
        docRef.get(),
        loadTreatmentCache() // Pre-warm cache while fetching doc
      ]);

      if (!existingDoc.exists) {
        throw new HttpsError("not-found", "Appointment not found.");
      }

      const existingData = existingDoc.data();

      // ─────────────────────────────────────────────────────
      // 1.6️⃣ OWNERSHIP + TIME RESTRICTION (non-admins)
      // ─────────────────────────────────────────────────────
      if (!isAdmin) {
        if (existingData.client_id !== callerUid) {
          throw new HttpsError("permission-denied", "You can only edit your own appointments.");
        }
        if (isWithinOneHour(existingData)) {
          throw new HttpsError("failed-precondition", "too_close_to_start");
        }
      }

      // ─────────────────────────────────────────────────────
      // 3️⃣ BUILD UPDATES OBJECT (only changed fields)
      // ─────────────────────────────────────────────────────
      const updates = {
        updated_at: getNowTimestamp(),
        updated_by: currentUserId || callerUid,
        updated_by_name: currentUserName || "Unknown",
        is_admin_updated: isAdmin,
        from_calendar_appointment: true,
        from_google_calendar: false,
      };

      // Only update fields that were provided
      if (number !== undefined) updates.number = number;
      if (email !== undefined) updates.email = email;
      if (clientId !== undefined) updates.client_id = isAdmin ? clientId : existingData.client_id;
      if (treatment_id_list !== undefined) updates.treatment_id_list = treatment_id_list;
      if (employeeIds !== undefined) updates.employee_id_list = isAdmin ? employeeIds : existingData.employee_id_list;
      if (notes !== undefined) updates.notes = isAdmin ? notes : existingData.notes;
      if (statusId !== undefined) updates.status_id = isAdmin ? statusId : existingData.status_id;
      if (colorId !== undefined) updates.color_id = colorId;
      if (duration !== undefined) updates.total_duration = duration;
  
      // ─────────────────────────────────────────────────────
      // 4️⃣ HANDLE TIME UPDATES (if provided)
      // ─────────────────────────────────────────────────────
      if (start && originalStart && originalEnd) {
        const displayTime = extractTimeFromInput(start);
        const startTimestamp = toTimestampFromInput(start);
        const originalStartTimestamp = toTimestampFromInput(originalStart);
        const originalEndTimestamp = toTimestampFromInput(originalEnd);
        
        const durationMs = originalEndTimestamp.toMillis() - originalStartTimestamp.toMillis();
        const endTimestamp = admin.firestore.Timestamp.fromMillis(
          startTimestamp.toMillis() + durationMs
        );
  
        updates.date = startTimestamp;
        updates.start_time = startTimestamp;
        updates.end_time = endTimestamp;
        updates.date_timestamp = startTimestamp;
        updates.appointment_time = startTimestamp;
        updates.time = displayTime;
        
        if (duration === undefined) {
          updates.total_duration = Math.max(1, Math.round(durationMs / 60000));
        }
      }
  
      // ─────────────────────────────────────────────────────
      // 5️⃣ REBUILD TITLE IF NEEDED (conditional)
      // ─────────────────────────────────────────────────────
      const clientChanged = clientId !== undefined;
      const treatmentsChanged = treatment_id_list !== undefined;
      
      if (clientChanged || treatmentsChanged) {
        const finalClientId = updates.client_id ?? existingData.client_id;
        const finalTreatments = updates.treatment_id_list ?? existingData.treatment_id_list;
        
        updates.title = await buildAppointmentTitle(finalClientId, finalTreatments);
        console.log("🏷️ Rebuilt title:", updates.title);
      }
  
      // ─────────────────────────────────────────────────────
      // 6️⃣ WRITE TO FIRESTORE (single update)
      // ─────────────────────────────────────────────────────
      await docRef.update(updates);
  
      console.log(`✅ Appointment ${appointmentId} updated in ${Date.now() - startMs}ms`);
  
      // ─────────────────────────────────────────────────────
      // 7️⃣ RETURN IMMEDIATELY
      // ─────────────────────────────────────────────────────
      return {
        success: true,
        appointmentId,
        message: "Appointment updated successfully"
      };
  
    } catch (err) {
      console.error("❌ updateAppointment error:", err.message);
      throw new Error(err.message);
    }
  });

  /* =========================================================
     📖 READ: Get single appointment
     ========================================================= */
  exports.getAppointment = onCall(async (request) => {
    try {
      const callerUid = request.auth?.uid;
      if (!callerUid) {
        throw new HttpsError("unauthenticated", "Authentication required.");
      }

      const { appointmentId } = request.data || {};
      if (!appointmentId) {
        throw new HttpsError("invalid-argument", "Missing appointmentId");
      }

      const [isAdmin, snap] = await Promise.all([
        verifyIsAdmin(callerUid),
        db.collection("appointments").doc(appointmentId).get(),
      ]);

      if (!snap.exists) return { success: false, message: "Appointment not found" };

      const appointmentData = snap.data();
      if (!isAdmin && appointmentData.client_id !== callerUid) {
        throw new HttpsError("permission-denied", "Cannot view other users' appointments.");
      }

      return { success: true, data: { id: snap.id, ...appointmentData } };
    } catch (err) {
      console.error("❌ getAppointment error:", err);
      if (err instanceof HttpsError) throw err;
      throw new HttpsError("internal", err.message);
    }
  });

/* =========================================================
   ⚡ DELETE Appointment (OPTIMIZED - INSTANT RESPONSE)
   ========================================================= */
   exports.deleteAppointment = onCall(async (request) => {
    try {
      const callerUid = request.auth?.uid;
      if (!callerUid) {
        throw new HttpsError("unauthenticated", "Authentication required.");
      }

      const {
        appointmentId,
        currentUserId,
        currentUserName,
      } = request.data || {};

      if (!appointmentId) {
        throw new HttpsError("invalid-argument", "Missing appointmentId");
      }

      // ─────────────────────────────────────────────────────
      // SERVER-SIDE AUTH + FETCH (parallel)
      // ─────────────────────────────────────────────────────
      const ref = db.collection("appointments").doc(appointmentId);
      const [isAdmin, snap] = await Promise.all([
        verifyIsAdmin(callerUid),
        ref.get(),
      ]);

      if (!snap.exists) {
        return { success: false, message: "Appointment not found" };
      }

      const appointmentData = snap.data();

      // ─────────────────────────────────────────────────────
      // OWNERSHIP + TIME RESTRICTION (non-admins)
      // ─────────────────────────────────────────────────────
      if (!isAdmin) {
        if (appointmentData.client_id !== callerUid) {
          throw new HttpsError("permission-denied", "You can only delete your own appointments.");
        }
        if (isWithinOneHour(appointmentData)) {
          throw new HttpsError("failed-precondition", "too_close_to_start");
        }
      }

      // ✅ Add metadata for background processing
      await ref.update({
        _markedForDeletion: true,
        _deletedBy: currentUserId || callerUid,
        _deletedByName: currentUserName || "Unknown",
        _isAdminDeleted: isAdmin,
        _deletedAt: getNowTimestamp(),
      });

      // ✅ Delete the appointment
      await ref.delete();

      logger.info(`✅ Appointment ${appointmentId} deleted by ${callerUid} (admin: ${isAdmin})`);

      return {
        success: true,
        message: "Appointment deleted successfully",
      };

    } catch (err) {
      console.error("❌ deleteAppointment error:", err);
      if (err instanceof HttpsError) throw err;
      throw new HttpsError("internal", err.message);
    }
  });

/* =========================================================
   📋 APPOINTMENT REQUEST SYSTEM
   Clients use this when they cannot directly edit/delete
   (within 1-hour window). Admins approve/reject requests.
   ========================================================= */

/**
 * createAppointmentRequest
 *
 * Called by clients when they want to edit or cancel an appointment
 * that is within the 1-hour restriction window.
 *
 * Creates a doc in "appointment_requests" and notifies all admins.
 *
 * @param {string} appointmentId - The appointment to request changes for
 * @param {string} type - "edit" or "cancel"
 * @param {string} reason - Free-text reason from the client
 */
exports.createAppointmentRequest = onCall(async (request) => {
  try {
    const callerUid = request.auth?.uid;
    if (!callerUid) {
      throw new HttpsError("unauthenticated", "Authentication required.");
    }

    const { appointmentId, type, reason = "" } = request.data || {};

    if (!appointmentId) {
      throw new HttpsError("invalid-argument", "Missing appointmentId.");
    }
    if (!type || !["edit", "cancel"].includes(type)) {
      throw new HttpsError("invalid-argument", "type must be 'edit' or 'cancel'.");
    }

    // ─────────────────────────────────────────────────────
    // VERIFY OWNERSHIP
    // ─────────────────────────────────────────────────────
    const appointmentRef = db.collection("appointments").doc(appointmentId);
    const appointmentSnap = await appointmentRef.get();

    if (!appointmentSnap.exists) {
      throw new HttpsError("not-found", "Appointment not found.");
    }

    const appointmentData = appointmentSnap.data();

    if (appointmentData.client_id !== callerUid) {
      throw new HttpsError("permission-denied", "You can only request changes to your own appointments.");
    }

    // ─────────────────────────────────────────────────────
    // CHECK FOR DUPLICATE PENDING REQUESTS
    // ─────────────────────────────────────────────────────
    const existingRequests = await db.collection("appointment_requests")
      .where("appointmentId", "==", appointmentId)
      .where("status", "==", "pending")
      .limit(1)
      .get();

    if (!existingRequests.empty) {
      return {
        success: false,
        message: "A pending request already exists for this appointment.",
      };
    }

    // ─────────────────────────────────────────────────────
    // CREATE REQUEST DOCUMENT
    // ─────────────────────────────────────────────────────
    const clientName = await resolveClientName(callerUid);
    const requestRef = db.collection("appointment_requests").doc();

    const requestDoc = cleanUndefined({
      id: requestRef.id,
      appointmentId,
      clientId: callerUid,
      clientName,
      type,
      reason,
      status: "pending",
      createdAt: getNowTimestamp(),
      createdAtFormatted: formatDateTimeRome(new Date()),
    });

    await requestRef.set(requestDoc);

    // ─────────────────────────────────────────────────────
    // NOTIFY ADMINS (DIRECT FCM)
    // ─────────────────────────────────────────────────────
    const adminIds = await getAdminUsers();

    if (adminIds.length > 0) {
      const typeLabel = type === "cancel" ? "cancellazione" : "modifica";
      const title = `${clientName} richiede ${typeLabel} appuntamento`;
      const body = reason
        ? `Motivo: ${reason}`
        : "Controlla i dettagli nell'app.";

      const notificationRef = db.collection("new_notification").doc();
      const tokens = await getFcmTokens(adminIds);

      let fcmSuccessCount = 0;
      let fcmFailureCount = 0;

      if (tokens.length > 0) {
        const fcmResult = await sendFcm(tokens, title, body, {
          type: "appointment_request",
          appointmentId,
          requestId: requestRef.id,
          notificationId: notificationRef.id,
        }, adminIds);
        fcmSuccessCount = fcmResult.successCount;
        fcmFailureCount = fcmResult.failureCount;
      }

      const notifDoc = buildNotification({
        id: notificationRef.id,
        title,
        body,
        type: "appointment_request",
        receiverIds: adminIds,
        appointmentId,
      });

      await notificationRef.set({
        ...notifDoc,
        requestId: requestRef.id,
        fcmSent: tokens.length > 0,
        fcmSentAt: admin.firestore.Timestamp.now(),
        fcmSuccessCount,
        fcmFailureCount,
      });
    }

    logger.info(`📋 Appointment request created: ${requestRef.id} (${type}) by ${callerUid}`);

    return {
      success: true,
      requestId: requestRef.id,
      message: "Request submitted successfully.",
    };

  } catch (err) {
    console.error("❌ createAppointmentRequest error:", err);
    if (err instanceof HttpsError) throw err;
    throw new HttpsError("internal", err.message);
  }
});

/**
 * handleAppointmentRequest
 *
 * Called by admins to approve or reject a client's request.
 *
 * If approved + type=cancel → deletes the appointment.
 * If approved + type=edit  → marks request approved (client can then edit).
 * Always notifies the client of the decision.
 *
 * @param {string} requestId - The appointment_requests document ID
 * @param {string} action - "approve" or "reject"
 * @param {string} adminNote - Optional note from admin
 */
exports.handleAppointmentRequest = onCall(async (request) => {
  try {
    const callerUid = request.auth?.uid;
    if (!callerUid) {
      throw new HttpsError("unauthenticated", "Authentication required.");
    }

    // ─────────────────────────────────────────────────────
    // VERIFY CALLER IS ADMIN
    // ─────────────────────────────────────────────────────
    const isAdmin = await verifyIsAdmin(callerUid);
    if (!isAdmin) {
      throw new HttpsError("permission-denied", "Only admins can handle requests.");
    }

    const { requestId, action, adminNote = "" } = request.data || {};

    if (!requestId) {
      throw new HttpsError("invalid-argument", "Missing requestId.");
    }
    if (!action || !["approve", "reject"].includes(action)) {
      throw new HttpsError("invalid-argument", "action must be 'approve' or 'reject'.");
    }

    // ─────────────────────────────────────────────────────
    // FETCH REQUEST
    // ─────────────────────────────────────────────────────
    const requestRef = db.collection("appointment_requests").doc(requestId);
    const requestSnap = await requestRef.get();

    if (!requestSnap.exists) {
      throw new HttpsError("not-found", "Request not found.");
    }

    const requestData = requestSnap.data();

    if (requestData.status !== "pending") {
      return {
        success: false,
        message: `Request already ${requestData.status}.`,
      };
    }

    // ─────────────────────────────────────────────────────
    // UPDATE REQUEST STATUS
    // ─────────────────────────────────────────────────────
    const newStatus = action === "approve" ? "approved" : "rejected";

    await requestRef.update(cleanUndefined({
      status: newStatus,
      handledBy: callerUid,
      handledAt: getNowTimestamp(),
      adminNote,
    }));

    // ─────────────────────────────────────────────────────
    // IF APPROVED CANCEL → DELETE APPOINTMENT
    // ─────────────────────────────────────────────────────
    if (action === "approve" && requestData.type === "cancel") {
      const appointmentRef = db.collection("appointments").doc(requestData.appointmentId);
      const appointmentSnap = await appointmentRef.get();

      if (appointmentSnap.exists) {
        // Mark for deletion (background trigger will handle calendar sync + cleanup)
        await appointmentRef.update({
          _markedForDeletion: true,
          _deletedBy: callerUid,
          _deletedByName: "Admin (via request)",
          _isAdminDeleted: true,
          _deletedAt: getNowTimestamp(),
        });
        await appointmentRef.delete();
        logger.info(`✅ Appointment ${requestData.appointmentId} deleted via approved request`);
      }
    }

    // ─────────────────────────────────────────────────────
    // NOTIFY CLIENT OF DECISION (DIRECT FCM)
    // ─────────────────────────────────────────────────────
    const clientId = requestData.clientId;
    if (clientId) {
      const typeLabel = requestData.type === "cancel" ? "cancellazione" : "modifica";
      const actionLabel = action === "approve" ? "approvata" : "rifiutata";
      const title = `Richiesta di ${typeLabel} ${actionLabel}`;
      const body = adminNote || (action === "approve"
        ? "La tua richiesta è stata approvata."
        : "La tua richiesta è stata rifiutata. Contatta il salone per maggiori informazioni.");

      const notificationRef = db.collection("new_notification").doc();
      const tokens = await getFcmTokens([clientId]);

      let fcmSuccessCount = 0;
      let fcmFailureCount = 0;

      if (tokens.length > 0) {
        const fcmResult = await sendFcm(tokens, title, body, {
          type: "appointment_request_response",
          appointmentId: requestData.appointmentId,
          requestId,
          action,
          notificationId: notificationRef.id,
        }, [clientId]);
        fcmSuccessCount = fcmResult.successCount;
        fcmFailureCount = fcmResult.failureCount;
      }

      const notifDoc = buildNotification({
        id: notificationRef.id,
        title,
        body,
        type: "appointment_request_response",
        receiverIds: [clientId],
        appointmentId: requestData.appointmentId,
      });

      await notificationRef.set({
        ...notifDoc,
        requestId,
        fcmSent: tokens.length > 0,
        fcmSentAt: admin.firestore.Timestamp.now(),
        fcmSuccessCount,
        fcmFailureCount,
      });
    }

    logger.info(`📋 Request ${requestId} ${newStatus} by admin ${callerUid}`);

    return {
      success: true,
      status: newStatus,
      message: `Request ${newStatus} successfully.`,
    };

  } catch (err) {
    console.error("❌ handleAppointmentRequest error:", err);
    if (err instanceof HttpsError) throw err;
    throw new HttpsError("internal", err.message);
  }
});

  exports.listAppointments = onCall(async (request) => {
    try {
      const callerUid = request.auth?.uid;
      if (!callerUid) {
        throw new HttpsError("unauthenticated", "Authentication required.");
      }

      const isAdmin = await verifyIsAdmin(callerUid);

      const {
        clientId,
        employeeId,
        dateFrom,
        dateTo,
        limit = 20,
        startAfter,
      } = request.data || {};

      // Non-admins can ONLY see their own appointments
      const effectiveClientId = isAdmin ? clientId : callerUid;

      let q = db.collection("appointments");

      if (effectiveClientId) q = q.where("client_id", "==", effectiveClientId);
      if (isAdmin && employeeId) q = q.where("employee_id_list", "array-contains", employeeId);
      
      if (dateFrom) {
        const fromDate = parseAsUTC(dateFrom);
        q = q.where("date", ">=", admin.firestore.Timestamp.fromDate(fromDate));
      }
      if (dateTo) {
        const toDate = parseAsUTC(dateTo);
        q = q.where("date", "<=", admin.firestore.Timestamp.fromDate(toDate));
      }
  
      q = q.orderBy("date", "desc");  // ✅ Most recent first
  
      // ✅ Pagination support
      if (startAfter) {
        const cursorDoc = await db.collection("appointments").doc(startAfter).get();
        if (cursorDoc.exists) {
          q = q.startAfter(cursorDoc);
        }
      }
  
      // ✅ Strict limit (max 50 instead of 200)
      q = q.limit(Math.min(limit, 50));
  
      const snap = await q.get();
      
      // ✅ Return only essential fields (smaller payload)
      const items = snap.docs.map((d) => {
        const data = d.data();
        return {
          id: d.id,
          client_id: data.client_id,
          start_time: data.start_time,
          end_time: data.end_time,
          date: data.date,
          time: data.time,
          status_id: data.status_id,
          total_duration: data.total_duration,
          treatment_id_list: data.treatment_id_list,
          employee_id_list: data.employee_id_list,
          notes: data.notes,
          color_id: data.color_id,
          // ✅ Don't return large fields like created_at, updated_at unless needed
        };
      });
  
      // ✅ Return pagination info
      const lastDoc = snap.docs[snap.docs.length - 1];
      
      return { 
        success: true, 
        count: items.length, 
        data: items,
        hasMore: snap.docs.length === Math.min(limit, 50),
        lastDocId: lastDoc?.id || null,
      };
    } catch (err) {
      console.error("❌ listAppointments error:", err);
      throw new Error(err.message);
    }
  });


// ========== FIRESTORE TRIGGERS ==========

exports.scheduleFollowUpNotification = onDocumentCreated("appointments/{appointmentId}", async (event) => {
  const appointment = event.data.data();
  const appointmentId = event.params.appointmentId;

  if (!appointment?.end_time || !appointment?.client_id) {
    logger.warn(`Missing fields for appointment ${appointmentId}`);
    return;
  }

  // ✅ SKIP if appointment is from Google Calendar (to avoid duplicates)
  if (appointment.from_google_calendar === true) {
    logger.info(`⏭️ Skipping follow-up for Google Calendar appointment ${appointmentId}`);
    return;
  }

  try {
    // ✅ Check if already scheduled to prevent duplicates
    const scheduledRef = db.collection("scheduled_appointment_notifications").doc(appointmentId);
    const existingScheduled = await scheduledRef.get();
    
    if (existingScheduled.exists) {
      logger.info(`⏭️ Follow-up already scheduled for ${appointmentId}, skipping`);
      return;
    }

    // Convert end_time to Rome timezone
    const endTimeValue = appointment.end_time?.toDate 
      ? appointment.end_time.toDate() 
      : new Date(appointment.end_time);
    const endTimeRome = moment.tz(endTimeValue, EUROPE_ROME);

    if (!endTimeRome.isValid()) {
      logger.error(`Invalid end_time for appointment ${appointmentId}: ${appointment.end_time}`);
      return;
    }

    // Add 2 minutes after appointment end
    const sendAtRome = endTimeRome.clone().add(2, "minutes").startOf("minute");

    // If in the past, schedule for now + 1 minute
    const nowRome = moment.tz(EUROPE_ROME);
    let finalSendTimeRome = sendAtRome;

    if (sendAtRome.isSameOrBefore(nowRome)) {
      logger.warn(`Follow-up sendTime was in the past for ${appointmentId}. Adjusting to now + 1 min.`);
      finalSendTimeRome = nowRome.clone().add(1, "minute").startOf("minute");
    }

    const sendTime = admin.firestore.Timestamp.fromDate(finalSendTimeRome.toDate());

    // Get client name and admin list
    const [client_name, adminIds] = await Promise.all([
      resolveClientName(appointment.client_id),
      getAdminUsers(),
    ]);

    if (!adminIds.length) {
      logger.warn("No admin users found — follow-up will NOT be scheduled");
      return;
    }

    // ✅ Create scheduled notification (use SET instead of merge to ensure clean creation)
    const payload = buildScheduledNotification({
      id: appointmentId,
      appointmentId,
      clientId: appointment.client_id,
      client_name,
      receiverIds: adminIds,
      sendTime,
      action: "appointment_followup",
      ttlDays: 30,
    });

    // ✅ Use set() without merge to ensure clean creation
    await scheduledRef.set(payload);

    logger.info(
      `⏰ Follow-up scheduled for ${client_name} at ${finalSendTimeRome.format("YYYY-MM-DD HH:mm:ss")} Rome (appointmentId: ${appointmentId})`
    );

  } catch (error) {
    logger.error(`❌ Error scheduling follow-up for ${appointmentId}:`, error);
  }
});


exports.deleteFollowUpNotification = onDocumentDeleted("appointments/{appointmentId}", async (event) => {
  const appointmentId = event.params.appointmentId;

  try {
    const scheduledRef = db.collection("scheduled_appointment_notifications").doc(appointmentId);
    const notifSnap = await scheduledRef.get();

    const followUpNotifs = await db.collection("new_notification")
      .where("appointmentId", "==", appointmentId)
      .where("type", "==", "appointment_followup")
      .get();

    const batch = db.batch();

    if (notifSnap.exists) batch.delete(scheduledRef);

    followUpNotifs.forEach((doc) => batch.delete(doc.ref));

    await batch.commit();

    logger.info(`🧹 Cleaned follow-up notifications for appointment ${appointmentId}`);
  } catch (error) {
    logger.error(`❌ Error cleaning follow-up notifications for ${appointmentId}:`, error);
  }
});


/**
 * SEND NOTIFICATION (FALLBACK)
 * 
 * This trigger is now a FALLBACK only.
 * Main FCM sending is done directly by:
 * - Background triggers (appointment create/update/delete)
 * - processScheduledNotifications (review prompts, follow-ups)
 * 
 * This only fires if a notification was created without fcmSent flag.
 */
exports.sendNotification = onDocumentCreated("new_notification/{docId}", async (event) => {
  const data = event.data.data();
  const docId = event.params.docId;

  // ✅ FIXED: Check type FIRST (most reliable skip condition)
  if (data.type === "appointment_followup") {
    logger.info(`⏭️ [${docId}] Follow-up handled elsewhere, skipping`);
    return;
  }

  // ✅ Skip if FCM was already sent
  if (data.fcmSent === true) {
    logger.info(`⏭️ [${docId}] FCM already sent, skipping`);
    return;
  }

  // Validate required fields
  if (!data.receiverIds?.length || !data.title || !data.body) {
    logger.warn(`⚠️ [${docId}] Missing required fields, skipping`);
    return;
  }

  logger.info(`📬 [${docId}] Fallback sendNotification for type: ${data.type}`);

  try {
    const tokens = await getFcmTokens(data.receiverIds);

    if (tokens.length === 0) {
      logger.warn(`⚠️ [${docId}] No FCM tokens found`);
      return;
    }

    const fcmData = {
      type: data.type || "general",
      notificationId: docId,
    };
    if (data.appointmentId) fcmData.appointmentId = data.appointmentId;
    if (data.link) fcmData.link = data.link;

    const result = await sendFcm(tokens, data.title, data.body, fcmData, data.receiverIds || []);

    await event.data.ref.update({
      fcmSent: true,
      fcmSentAt: admin.firestore.Timestamp.now(),
      fcmSuccessCount: result.successCount,
      fcmFailureCount: result.failureCount,
    });

    logger.info(`✅ [${docId}] Fallback FCM sent: ${result.successCount} success`);

  } catch (error) {
    logger.error(`❌ [${docId}] sendNotification error: ${error.message}`);
    await event.data.ref.update({
      fcmSent: false,
      fcmError: error.message,
    });
  }
});


/* =========================================================
   🔄 BACKGROUND: Process New Appointments
   This runs automatically when an appointment is created
   ========================================================= */
   exports.onAppointmentCreatedBackground = onDocumentCreated("appointments/{appointmentId}", async (event) => {
    const appointment = event.data.data();
    const appointmentId = event.params.appointmentId;
  
    logger.info(`🔄 [Background] Processing appointment ${appointmentId}`);
  
    // Skip if this appointment was synced from Google Calendar
    if (appointment.from_google_calendar === true) {
      logger.info(`⏭️ [Background] Skipping Google Calendar appointment ${appointmentId}`);
      return;
    }
  
    try {
      // ==========================================================
      // TASK 1: AWARD MEMBERSHIP POINTS (if applicable)
      // ==========================================================
      if (appointment.status_id === STATUS_ARCHIVIATO && appointment.client_id) {
        try {
          const startTime = appointment.start_time?.toDate?.() || new Date(appointment.start_time);
          await awardPointsForPastAppointment(appointment.client_id, startTime.toISOString());
          logger.info(`✅ [Background] Points awarded for ${appointmentId}`);
        } catch (error) {
          logger.error(`❌ [Background] Points award failed for ${appointmentId}:`, error.message);
        }
      }
  
      // ==========================================================
      // TASK 2: SYNC WITH GOOGLE CALENDAR
      // ==========================================================
      try {
        await syncWithGoogleCalendar("CREATE", appointmentId, appointment);
        logger.info(`✅ [Background] Google Calendar synced for ${appointmentId}`);
      } catch (error) {
        logger.error(`❌ [Background] Google Calendar sync failed for ${appointmentId}:`, error.message);
      }
  
      // ==========================================================
      // TASK 3: SEND NOTIFICATIONS (DIRECT FCM - NO RACE CONDITION)
      // ==========================================================
      try {
        const isAdmin = appointment.is_admin_created || false;
        const currentUserId = appointment.created_by || "system";
        const currentUserName = appointment.created_by_name || "Sistema";
        const clientId = appointment.client_id;
  
        const receivers = await getNotificationRecipients(isAdmin, currentUserId, clientId);
  
        if (receivers.length > 0) {
          const title = isAdmin
            ? "Abbiamo creato un appuntamento"
            : `${currentUserName} ha creato un appuntamento`;
          const body = "Controlla i dettagli nell'app.";
  
          // ✅ Generate notification ID first
          const notificationRef = db.collection("new_notification").doc();
  
          // ✅ Get tokens FIRST
          const tokens = await getFcmTokens(receivers);
          logger.info(`🔑 [Background] Found ${tokens.length} FCM tokens for ${receivers.length} receivers`);
  
          // ✅ Track FCM results
          let fcmSuccessCount = 0;
          let fcmFailureCount = 0;
  
          // ✅ Send FCM BEFORE creating document
          if (tokens.length > 0) {
            const fcmResult = await sendFcm(tokens, title, body, {
              type: "appointment_new",
              appointmentId: appointmentId,
              notificationId: notificationRef.id,
            }, receivers);
            fcmSuccessCount = fcmResult.successCount;
            fcmFailureCount = fcmResult.failureCount;
            logger.info(`✅ [Background] FCM sent: ${fcmSuccessCount} success, ${fcmFailureCount} failed`);
          }
  
          // ✅ Create notification with fcmSent ALREADY SET (prevents race condition)
          const notifDoc = buildNotification({
            id: notificationRef.id,
            title,
            body,
            type: "appointment_new",
            receiverIds: receivers,
            appointmentId: appointmentId,
          });
  
          await notificationRef.set({
            ...notifDoc,
            fcmSent: tokens.length > 0,
            fcmSentAt: admin.firestore.Timestamp.now(),
            fcmSuccessCount,
            fcmFailureCount,
          });
  
          logger.info(`✅ [Background] Notification created for ${appointmentId} → ${receivers.length} users`);
        }
      } catch (error) {
        logger.error(`❌ [Background] Notification failed for ${appointmentId}:`, error.message);
      }
  
      logger.info(`✅ [Background] All tasks completed for ${appointmentId}`);
  
    } catch (error) {
      logger.error(`❌ [Background] Processing failed for ${appointmentId}:`, error);
    }
  });

/* =========================================================
   🔄 BACKGROUND: Process Updated Appointments
   This runs automatically when an appointment is updated
   ========================================================= */
exports.onAppointmentUpdatedBackground = onDocumentUpdated("appointments/{appointmentId}", async (event) => {
  const appointmentId = event.params.appointmentId;
  const before = event.data?.before?.data();
  const after = event.data?.after?.data();
  
  // ========================================
  // ⚡ SKIP CONDITIONS (Prevent False Notifications)
  // ========================================
  
  // ✅ SKIP if from Google Calendar (to avoid sync loops)
  if (after.from_google_calendar === true) {
    logger.info(`⏭️ [Background] Skipping Google Calendar appointment update ${appointmentId}`);
    return;
  }

  // ✅ SKIP if document is marked for deletion (prevents "modified" on delete)
  if (after._markedForDeletion === true) {
    logger.info(`⏭️ [Background] Skipping notification - document marked for deletion`);
    return;
  }

  // ✅ SKIP if update was from notification action (Yes/No-Show button)
  if (after.followup_action_at && !before.followup_action_at) {
    logger.info(`⏭️ [Background] Skipping notification - update was from followup action`);
    return;
  }

  // ✅ SKIP if only status changed from followup action
  if (after.followup_action && before.followup_action !== after.followup_action) {
    logger.info(`⏭️ [Background] Skipping notification - followup action status change`);
    return;
  }

  // ✅ SKIP if document was created within last 60 seconds (prevents Calendar sync notification)
  const createdAt = after.created_at;
  if (createdAt?.toMillis) {
    const timeSinceCreation = Date.now() - createdAt.toMillis();
    if (timeSinceCreation < 60000) { // 60 seconds
      logger.info(`⏭️ [Background] Skipping notification - document created ${Math.round(timeSinceCreation/1000)}s ago`);
      return;
    }
  }

  // ✅ SKIP if this is a Calendar sync update (only calendar-related fields changed)
  const calendarSyncFields = ['google_calendar_event_id', 'last_calendar_sync', 'last_synced_at'];
  
  const beforeKeys = Object.keys(before || {});
  const afterKeys = Object.keys(after || {});
  
  // Find added fields
  const addedKeys = afterKeys.filter(k => !beforeKeys.includes(k));
  
  // Find changed fields (excluding added)
  const changedKeys = afterKeys.filter(k => {
    if (!beforeKeys.includes(k)) return false;
    const beforeVal = before[k];
    const afterVal = after[k];
    // Compare Timestamps by millis
    if (beforeVal?.toMillis && afterVal?.toMillis) {
      return beforeVal.toMillis() !== afterVal.toMillis();
    }
    // Compare arrays
    if (Array.isArray(beforeVal) && Array.isArray(afterVal)) {
      return JSON.stringify(beforeVal) !== JSON.stringify(afterVal);
    }
    // Compare primitives
    return beforeVal !== afterVal;
  });
  
  const allChanges = [...addedKeys, ...changedKeys];
  
  // If only calendar sync fields changed, skip notification
  if (allChanges.length > 0 && allChanges.every(k => calendarSyncFields.includes(k))) {
    logger.info(`⏭️ [Background] Skipping - only calendar sync fields changed: [${allChanges.join(', ')}]`);
    return;
  }

  // ✅ SKIP if no meaningful changes detected
  if (allChanges.length === 0) {
    logger.info(`⏭️ [Background] Skipping - no changes detected`);
    return;
  }

  // ========================================
  // 📝 PROCEED WITH UPDATE PROCESSING
  // ========================================
  
  logger.info(`🔄 [Background] Processing appointment update ${appointmentId}`);
  logger.info(`🔄 [Background] Changed fields: [${allChanges.join(', ')}]`);

  try {
    // ==========================================================
    // TASK 1: AWARD POINTS (if status changed to completed)
    // ==========================================================
    const wasCompleted = before.status_id === STATUS_ARCHIVIATO;
    const isCompleted = after.status_id === STATUS_ARCHIVIATO;

    if (!wasCompleted && isCompleted && after.client_id) {
      try {
        const startTime = after.start_time?.toDate?.() || new Date(after.start_time);
        await awardPointsForPastAppointment(after.client_id, startTime.toISOString());
        logger.info(`✅ [Background] Points awarded for ${appointmentId}`);
      } catch (error) {
        logger.error(`❌ [Background] Points award failed for ${appointmentId}:`, error.message);
      }
    }

    // ==========================================================
    // TASK 2: SYNC WITH GOOGLE CALENDAR (if has event ID)
    // ==========================================================
    if (after.google_calendar_event_id) {
      try {
        await syncWithGoogleCalendar("UPDATE", appointmentId, after);
        logger.info(`✅ [Background] Google Calendar updated for ${appointmentId}`);
      } catch (error) {
        logger.error(`❌ [Background] Google Calendar sync failed for ${appointmentId}:`, error.message);
      }
    }

    // ==========================================================
    // TASK 3: SEND NOTIFICATIONS (DIRECT FCM - NO RACE CONDITION)
    // ==========================================================
    try {
      const isAdmin = after.is_admin_updated || after.is_admin_created || false;
      const currentUserId = after.updated_by || after.created_by || "system";
      const currentUserName = after.updated_by_name || after.created_by_name || "Sistema";
      const clientId = after.client_id;

      logger.info(`📋 [Background] Notification context:`);
      logger.info(`   isAdmin: ${isAdmin}`);
      logger.info(`   currentUserId: ${currentUserId}`);
      logger.info(`   currentUserName: ${currentUserName}`);
      logger.info(`   clientId: ${clientId}`);

      const receivers = await getNotificationRecipients(isAdmin, currentUserId, clientId);
      
      logger.info(`📋 [Background] Receivers: [${receivers.join(', ')}]`);

      if (receivers.length > 0) {
        const title = isAdmin
          ? "Abbiamo modificato un appuntamento"
          : `${currentUserName} ha modificato un appuntamento`;
        const body = "Controlla i dettagli nell'app.";

        // ✅ Generate notification ID first
        const notificationRef = db.collection("new_notification").doc();

        // ✅ Get tokens FIRST
        const tokens = await getFcmTokens(receivers);
        logger.info(`🔑 [Background] Found ${tokens.length} FCM tokens for ${receivers.length} receivers`);

        // ✅ Track FCM results
        let fcmSuccessCount = 0;
        let fcmFailureCount = 0;

        // ✅ Send FCM BEFORE creating document
        if (tokens.length > 0) {
          const fcmResult = await sendFcm(tokens, title, body, {
            type: "appointment_update",
            appointmentId: appointmentId,
            notificationId: notificationRef.id,
          }, receivers);
          fcmSuccessCount = fcmResult.successCount;
          fcmFailureCount = fcmResult.failureCount;
          logger.info(`✅ [Background] FCM sent: ${fcmSuccessCount} success, ${fcmFailureCount} failed`);
        }

        // ✅ Create notification with fcmSent ALREADY SET
        const notifDoc = buildNotification({
          id: notificationRef.id,
          title,
          body,
          type: "appointment_update",
          receiverIds: receivers,
          appointmentId: appointmentId,
        });

        await notificationRef.set({
          ...notifDoc,
          fcmSent: tokens.length > 0,
          fcmSentAt: admin.firestore.Timestamp.now(),
          fcmSuccessCount,
          fcmFailureCount,
        });

        logger.info(`✅ [Background] Notification created for ${appointmentId} → ${receivers.length} users`);
      } else {
        logger.info(`⏭️ [Background] No receivers for notification`);
      }
    } catch (error) {
      logger.error(`❌ [Background] Notification failed for ${appointmentId}:`, error.message);
    }

    logger.info(`✅ [Background] All update tasks completed for ${appointmentId}`);

  } catch (error) {
    logger.error(`❌ [Background] Update processing failed for ${appointmentId}:`, error);
  }
});

/* =========================================================
   🔄 BACKGROUND: Process Deleted Appointments
   This runs automatically when an appointment is deleted
   ========================================================= */
   exports.onAppointmentDeletedBackground = onDocumentDeleted("appointments/{appointmentId}", async (event) => {
    const appointmentId = event.params.appointmentId;
    const appointmentData = event.data?.data() || {};
  
    // Skip if from Google Calendar
    if (appointmentData.from_google_calendar === true) {
      logger.info(`⏭️ [Background] Skipping Google Calendar appointment deletion ${appointmentId}`);
      return;
    }
  
    logger.info(`🔄 [Background] Processing appointment deletion ${appointmentId}`);
  
    try {
      // ==========================================================
      // TASK 1: SYNC WITH GOOGLE CALENDAR
      // ==========================================================
      if (appointmentData.google_calendar_event_id) {
        try {
          await syncWithGoogleCalendar("DELETE", appointmentId, appointmentData);
          logger.info(`✅ [Background] Google Calendar event deleted for ${appointmentId}`);
        } catch (error) {
          logger.error(`❌ [Background] Google Calendar deletion failed for ${appointmentId}:`, error.message);
        }
      }
  
      // ==========================================================
      // TASK 2: CLEAN UP SCHEDULED NOTIFICATIONS
      // ==========================================================
      try {
        const scheduledRef = db.collection("scheduled_appointment_notifications").doc(appointmentId);
        const scheduledSnap = await scheduledRef.get();
  
        if (scheduledSnap.exists) {
          await scheduledRef.delete();
          logger.info(`✅ [Background] Scheduled notification deleted for ${appointmentId}`);
        }
  
        // Also delete any follow-up notifications
        const followUpNotifs = await db.collection("new_notification")
          .where("appointmentId", "==", appointmentId)
          .where("type", "==", "appointment_followup")
          .get();
  
        if (!followUpNotifs.empty) {
          const batch = db.batch();
          followUpNotifs.forEach((doc) => batch.delete(doc.ref));
          await batch.commit();
          logger.info(`✅ [Background] ${followUpNotifs.size} follow-up notifications deleted for ${appointmentId}`);
        }
      } catch (error) {
        logger.error(`❌ [Background] Notification cleanup failed for ${appointmentId}:`, error.message);
      }
  
      // ==========================================================
      // TASK 3: SEND NOTIFICATIONS (DIRECT FCM - NO RACE CONDITION)
      // ==========================================================
      try {
        const isAdmin = appointmentData._isAdminDeleted || false;
        const currentUserId = appointmentData._deletedBy || "system";
        const currentUserName = appointmentData._deletedByName || "Sistema";
        const clientId = appointmentData.client_id;
  
        const receivers = await getNotificationRecipients(isAdmin, currentUserId, clientId);
  
        if (receivers.length > 0) {
          const title = isAdmin
            ? "Abbiamo eliminato un appuntamento"
            : `${currentUserName} ha eliminato un appuntamento`;
          const body = "Controlla i dettagli nell'app.";
  
          // ✅ Generate notification ID first
          const notificationRef = db.collection("new_notification").doc();
  
          // ✅ Get tokens FIRST
          const tokens = await getFcmTokens(receivers);
          logger.info(`🔑 [Background] Found ${tokens.length} FCM tokens for ${receivers.length} receivers`);
  
          // ✅ Track FCM results
          let fcmSuccessCount = 0;
          let fcmFailureCount = 0;
  
          // ✅ Send FCM BEFORE creating document
          if (tokens.length > 0) {
            const fcmResult = await sendFcm(tokens, title, body, {
              type: "appointment_delete",
              appointmentId: appointmentId,
              notificationId: notificationRef.id,
            }, receivers);
            fcmSuccessCount = fcmResult.successCount;
            fcmFailureCount = fcmResult.failureCount;
            logger.info(`✅ [Background] FCM sent: ${fcmSuccessCount} success, ${fcmFailureCount} failed`);
          }
  
          // ✅ Create notification with fcmSent ALREADY SET
          const notifDoc = buildNotification({
            id: notificationRef.id,
            title,
            body,
            type: "appointment_delete",
            receiverIds: receivers,
            appointmentId: appointmentId,
          });
  
          await notificationRef.set({
            ...notifDoc,
            fcmSent: tokens.length > 0,
            fcmSentAt: admin.firestore.Timestamp.now(),
            fcmSuccessCount,
            fcmFailureCount,
          });
  
          logger.info(`✅ [Background] Deletion notification created for ${appointmentId} → ${receivers.length} users`);
        }
      } catch (error) {
        logger.error(`❌ [Background] Notification failed for ${appointmentId}:`, error.message);
      }
  
      logger.info(`✅ [Background] All deletion tasks completed for ${appointmentId}`);
  
    } catch (error) {
      logger.error(`❌ [Background] Deletion processing failed for ${appointmentId}:`, error);
    }
  });

  /**
 * TEST NOTIFICATION
 * 
 * Tests all notification types in the system.
 * Call with: { clientId: "xxx", type: "appointment_new" }
 * 
 * Types:
 * - appointment_new
 * - appointment_update
 * - appointment_delete
 * - appointment_followup (with actions)
 * - review (with link)
 * - all (sends all types with 2s delay between each)
 */
exports.testNotification = onCall(async (request) => {
  try {
    const { clientId, type = "appointment_new" } = request.data || {};
    
    if (!clientId) {
      throw new Error("Missing clientId");
    }

    logger.info(`🧪 ========================================`);
    logger.info(`🧪 testNotification called`);
    logger.info(`🧪 clientId: ${clientId}`);
    logger.info(`🧪 type: ${type}`);
    logger.info(`🧪 ========================================`);

    // Get FCM tokens for the client
    const tokens = await getFcmTokens([clientId]);
    
    if (tokens.length === 0) {
      logger.error(`❌ No FCM tokens found for client ${clientId}`);
      return {
        success: false,
        error: "No FCM tokens found for this client",
        clientId,
      };
    }

    logger.info(`🔑 Found ${tokens.length} FCM token(s)`);

    const testAppointmentId = "TEST-" + Date.now();
    const results = [];

    // Define all notification templates
    const templates = {
      appointment_new: {
        title: "🟢 Nuovo appuntamento",
        body: "È stato creato un nuovo appuntamento per te.",
        type: "appointment_new",
        data: {
          type: "appointment_new",
          appointmentId: testAppointmentId,
        },
      },

      appointment_update: {
        title: "🟡 Appuntamento modificato",
        body: "Un appuntamento è stato aggiornato.",
        type: "appointment_update",
        data: {
          type: "appointment_update",
          appointmentId: testAppointmentId,
        },
      },

      appointment_delete: {
        title: "🔴 Appuntamento eliminato",
        body: "Un appuntamento è stato eliminato.",
        type: "appointment_delete",
        data: {
          type: "appointment_delete",
          appointmentId: testAppointmentId,
        },
      },

      appointment_followup: {
        title: "📋 Follow-up appuntamento",
        body: "Test Cliente si è presentato/a all'appuntamento?",
        type: "appointment_followup",
        data: {
          type: "appointment_followup",
          appointmentId: testAppointmentId,
          clientName: "Test Cliente",
          actions: JSON.stringify([
            { id: "appointment_attended", title: "Sì" },
            { id: "appointment_no_show", title: "No Show" },
          ]),
        },
      },

      review: {
        title: "⭐ Ti sei trovata bene oggi?",
        body: "Lasciaci una recensione 🌟",
        type: "review",
        data: {
          type: "review",
          appointmentId: testAppointmentId,
          link: "https://g.page/r/CQvE4blXs4esEAE/review",
        },
      },
    };

    // Function to send a single test notification
    const sendTestNotification = async (templateKey) => {
      const template = templates[templateKey];
      if (!template) {
        return { type: templateKey, success: false, error: "Unknown type" };
      }

      logger.info(`📤 Sending ${templateKey} notification...`);

      try {
        // Create notification document
        const notificationRef = db.collection("new_notification").doc();
        const notifDoc = buildNotification({
          id: notificationRef.id,
          title: template.title,
          body: template.body,
          type: template.type,
          receiverIds: [clientId],
          appointmentId: testAppointmentId,
          actions: templateKey === "appointment_followup" 
            ? [{ id: "appointment_attended", title: "Sì" }, { id: "appointment_no_show", title: "No Show" }]
            : undefined,
          link: templateKey === "review" 
            ? "https://g.page/r/CQvE4blXs4esEAE/review" 
            : undefined,
        });

        // Send FCM directly
        const fcmResult = await sendFcm(tokens, template.title, template.body, {
          ...template.data,
          notificationId: notificationRef.id,
        }, [clientId]);

        // Save notification with FCM status
        await notificationRef.set({
          ...notifDoc,
          fcmSent: true,
          fcmSentAt: admin.firestore.Timestamp.now(),
          fcmSuccessCount: fcmResult.successCount,
          fcmFailureCount: fcmResult.failureCount,
          isTest: true,
        });

        logger.info(`✅ ${templateKey}: ${fcmResult.successCount} success, ${fcmResult.failureCount} failed`);

        return {
          type: templateKey,
          success: fcmResult.successCount > 0,
          notificationId: notificationRef.id,
          fcmSuccessCount: fcmResult.successCount,
          fcmFailureCount: fcmResult.failureCount,
        };

      } catch (error) {
        logger.error(`❌ ${templateKey} failed: ${error.message}`);
        return {
          type: templateKey,
          success: false,
          error: error.message,
        };
      }
    };

    // Send notifications based on type
    if (type === "all") {
      // Send all types with delay between each
      for (const templateKey of Object.keys(templates)) {
        const result = await sendTestNotification(templateKey);
        results.push(result);
        
        // Wait 2 seconds between notifications
        if (templateKey !== "review") {
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    } else {
      // Send single type
      const result = await sendTestNotification(type);
      results.push(result);
    }

    logger.info(`🧪 ========================================`);
    logger.info(`🧪 testNotification completed`);
    logger.info(`🧪 Results: ${JSON.stringify(results)}`);
    logger.info(`🧪 ========================================`);

    return {
      success: true,
      clientId,
      type,
      tokensFound: tokens.length,
      results,
      testAppointmentId,
    };

  } catch (err) {
    logger.error("❌ testNotification error:", err);
    throw new Error(err.message);
  }
});

// ========== SCHEDULED FUNCTIONS ==========
exports.handleMembershipRenewals = onSchedule({
  schedule: "0 3 * * *", // every day at 3 AM Rome time
  timeZone: "Europe/Rome",
}, async () => {
  logger.info("🔁 Starting daily membership renewal job...");

  try {
    const today = moment.tz("Europe/Rome").startOf("day");

    const tierOrder = [
      "lYeZaxwl5qBGWSeuT3dN", // base
      "fvIfv20r5GeOrqmdYXYw", // primo
      "Z9oHbQkZTNDJY7C3tJk9", // fly
      "InKKF29ND27qPqMSv3UN", // corona
    ];

    const membershipsSnap = await db.collection("client_memberships").get();
    if (membershipsSnap.empty) {
      logger.info("No memberships found.");
      return null;
    }

    const batch = db.batch();
    let renewed = 0, upgraded = 0, downgraded = 0, frozen = 0, fixedRenewalDate = 0;

    for (const doc of membershipsSnap.docs) {
      const membership = doc.data();
      const {
        client_id,
        membership_type_id,
        start_date,
        renewal_date,
        points = 0,
        membership_freeze = false,
      } = membership;

      if (!client_id || !membership_type_id || !start_date) continue;

      let renewalMoment = null;

      // ✅ Primary: use existing renewal_date
      if (renewal_date) {
        renewalMoment = moment.tz(renewal_date, "YYYY-MM-DD", "Europe/Rome").startOf("day");
        if (!renewalMoment.isValid()) {
          logger.warn(`⚠️ Invalid renewal_date for ${client_id}: ${renewal_date}`);
          renewalMoment = null;
        }
      }

      // 🛠️ Fail-safe: recompute if missing or invalid
      if (!renewalMoment) {
        const [day, month, year] = String(start_date).split("/");
        if (!day || !month || !year) {
          logger.warn(`⚠️ Invalid start_date for client ${client_id}: ${start_date}`);
          continue;
        }

        const startMoment = moment.tz(`${year}-${month}-${day}`, "YYYY-MM-DD", "Europe/Rome");
        renewalMoment = startMoment.clone().add(1, "year").startOf("day");
        fixedRenewalDate++;

        batch.update(db.collection("client_memberships").doc(client_id), {
          renewal_date: renewalMoment.format("YYYY-MM-DD"),
        });
        logger.info(`🩹 Fixed missing renewal_date for ${client_id}`);
      }

      // Skip unless today == renewal_date
      if (!today.isSame(renewalMoment, "day")) continue;
      renewed++;

      // Prepare updates
      let newTierId = membership_type_id;
      let newFreeze = membership_freeze;
      let membership_standing = "maintained";
      const currentIndex = tierOrder.indexOf(membership_type_id);
      const baseId = tierOrder[0];

      // 🏆 UPGRADE condition
      if (points >= 300) {
        if (currentIndex >= 0 && currentIndex < tierOrder.length - 1) {
          newTierId = tierOrder[currentIndex + 1];
          upgraded++;
          membership_standing = "upgraded";
          logger.info(`🏆 ${client_id} upgraded ${membership_type_id} → ${newTierId}`);
        } else {
          membership_standing = "maintained";
          logger.info(`👑 ${client_id} already top tier (corona).`);
        }
        newFreeze = false;
      }

      // 🔻 DOWNGRADE condition
      else if (points < 275) {
        newTierId = baseId;
        downgraded++;
        newFreeze = false;
        membership_standing = "downgraded";
        logger.info(`🔻 ${client_id} downgraded to base (points ${points})`);
      }

      // ⏸️ FREEZE condition
      else if (points === 275) {
        if (!membership_freeze) {
          newFreeze = true;
          frozen++;
          membership_standing = "frozen";
          logger.info(`⏸️ ${client_id} stays same tier, freeze activated`);
        } else {
          newTierId = baseId;
          newFreeze = false;
          downgraded++;
          membership_standing = "downgraded";
          logger.info(`⚠️ ${client_id} freeze expired, downgraded to base`);
        }
      }

      // 🗓️ Compute next renewal date (always +1 year)
      const nextRenewal = renewalMoment.clone().add(1, "year").format("YYYY-MM-DD");

      // Skip redundant writes
      if (
        newTierId === membership_type_id &&
        newFreeze === membership_freeze &&
        points === 0
      ) continue;
      // 🔁 Update membership
      batch.update(db.collection("client_memberships").doc(client_id), {
        membership_type_id: newTierId,
        points: 0,
        start_date: today.format("DD/MM/YYYY"),
        renewal_date: nextRenewal,
        membership_freeze: newFreeze,
        membership_standing,
        last_renewed_at: admin.firestore.Timestamp.now(),
      });
    }

    if (renewed > 0 || fixedRenewalDate > 0) await batch.commit();

    logger.info(`✅ Membership renewal summary:
      Renewed today: ${renewed}
      Upgraded: ${upgraded}
      Frozen: ${frozen}
      Downgraded: ${downgraded}
      Renewal dates fixed: ${fixedRenewalDate}`);
  } catch (error) {
    logger.error("❌ handleMembershipRenewals error:", error);
  }
});


exports.scheduledCalendarSync = onSchedule("*/15 * * * *", async () => {
  const startTime = Date.now();
  logger.info("Starting optimized calendar sync...");

  try {
    const auth = await getAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const { startOfMonth, endOfNextMonth } = getDateRanges();

    // ---- FETCH EVENTS WITH RETRIES ----
    const fetchEvents = async (attempt = 1) => {
      try {
        return await calendar.events.list({
          calendarId: CALENDAR_ID,
          timeMin: startOfMonth,
          timeMax: endOfNextMonth,
          singleEvents: true,
          orderBy: "startTime",
          maxResults: MAX_EVENTS,
          fields:
            "items(id,summary,description,start,end,extendedProperties/private,updated,status)",
          timeZone: EUROPE_ROME,
          showDeleted: true,
        });
      } catch (error) {
        if (attempt < 3) {
          logger.warn(`Fetch attempt ${attempt} failed: ${error.message}. Retrying...`);
          await new Promise((r) => setTimeout(r, attempt * 1000));
          return fetchEvents(attempt + 1);
        } else {
          handleCalendarError(error, "Event fetch failed after retries");
        }
      }
    };

    const res = await fetchEvents();
    const events = res?.data?.items || [];
    logger.info(`Fetched ${events.length} events from Google Calendar`);

    if (events.length === 0) {
      return {
        success: true,
        totalEvents: 0,
        processedEvents: 0,
        updatedEvents: 0,
        skippedEvents: 0,
        deletedEvents: 0,
        executionTime: Date.now() - startTime,
      };
    }

    // ---- IDENTIFY EXISTING EVENTS ----
    const collectionRef = db.collection("appointments");

    const existingEventIds = new Set();
    const eventIds = events.map((e) => e.id).filter(Boolean);

    for (let i = 0; i < eventIds.length; i += 10) {
      const chunk = eventIds.slice(i, i + 10);
      const existingQuery = await collectionRef
        .where("google_calendar_event_id", "in", chunk)
        .get();

      existingQuery.docs.forEach((doc) =>
        existingEventIds.add(doc.data().google_calendar_event_id)
      );
    }

    logger.info(`Found ${existingEventIds.size} existing synced appointments.`);

    let processedCount = 0;
    let updatedCount = 0;
    let skippedCount = 0;
    let deletedCount = 0;
    let appCreatedCount = 0;
    let appEditedCount = 0; // ✅ NEW: Track appointments edited in app

    const calendarUpdates = [];

    // ---- PROCESS EVENTS IN BATCHES ----
    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const eventBatch = events.slice(i, i + BATCH_SIZE);
      let batchOps = 0;

      for (const event of eventBatch) {
        try {
          const eventId = event.id;
          if (!eventId) continue;

          // ---- SKIP events with no title → avoids empty Firestore rows ----
          if (!event.summary || event.summary.trim() === "") {
            logger.warn(`Skipping event ${event.id} because summary/title is empty`);
            skippedCount++;
            continue;
          }

          // ---- Handle deleted events ----
          if (event.status === "cancelled") {
            const delQuery = await collectionRef
              .where("google_calendar_event_id", "==", eventId)
              .get();

            delQuery.docs.forEach((doc) => batch.delete(doc.ref));
            deletedCount += delQuery.size;
            continue;
          }

          // ---- Handle app-created events (has appointment_id property) ----
          const props = event.extendedProperties?.private || {};
          if (props.appointment_id || props.calendar_event_id) {
            const appointmentId = props.appointment_id || props.calendar_event_id;
            
            logger.info(`Event ${eventId} has appointment_id: ${appointmentId}. Checking if it exists in Firestore...`);
            
            // Check if the appointment still exists in Firestore
            const appointmentDoc = await collectionRef.doc(appointmentId).get();
            
            if (appointmentDoc.exists) {
              // Appointment exists in Firestore, skip it
              logger.info(`Appointment ${appointmentId} exists in Firestore. Skipping sync.`);
              appCreatedCount++;
              continue;
            } else {
              // Appointment was deleted from Firestore but event still has the property
              logger.warn(`Appointment ${appointmentId} NOT found in Firestore. Removing property from calendar event ${eventId} and will sync to Firestore.`);
              
              // Remove the appointment_id property from the Google Calendar event
              try {
                await calendar.events.patch({
                  calendarId: CALENDAR_ID,
                  eventId: eventId,
                  requestBody: {
                    extendedProperties: {
                      private: {
                        appointment_id: null,
                        calendar_event_id: null
                      }
                    }
                  }
                });
                logger.info(`Successfully removed appointment_id from event ${eventId}. Proceeding with normal sync.`);
              } catch (err) {
                logger.error(`Failed to remove appointment_id from event ${eventId}: ${err.message}`);
                skippedCount++;
                continue;
              }
              
              // Fall through to normal sync logic below
            }
          }

          // ---- Validate dates ----
          const startDateStr = event.start?.dateTime || event.start?.date;
          const endDateStr = event.end?.dateTime || event.end?.date;

          if (!startDateStr || !endDateStr) {
            skippedCount++;
            continue;
          }

          // ✅ TIMEZONE-AGNOSTIC: Extract event timezone
          const eventTimeZone = event.start?.timeZone || event.end?.timeZone;

          // ✅ TIMEZONE-AGNOSTIC: Store local time (not UTC)
          const startTimestamp = toLocalTimestamp(startDateStr, eventTimeZone);
          const endTimestamp = toLocalTimestamp(endDateStr, eventTimeZone);
          const timeDisplay = formatTimeLocal(startDateStr, eventTimeZone);

          // ✅ Duration calculation (still works correctly)
          const startDate = new Date(startDateStr);
          const endDate = new Date(endDateStr);
          
          if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
            logger.warn(`Invalid dates on event ${eventId}`);
            skippedCount++;
            continue;
          }

          const durationMin = getDurationMinutes(startDate, endDate);

          // ✅ TIMEZONE-AGNOSTIC: Check if past using event's timezone
          const isPast = isPastLocal(endDateStr, eventTimeZone);

          const statusId = isPast
            ? STATUS_ARCHIVIATO
            : "88aa7cf3-c6b6-4cab-91eb-247aa6445a0a";

          // ✅ FIXED: Build title and notes
          // title = event.summary (even if empty)
          // notes = event.summary + "\n" + event.description (if description exists)
          // notes = event.summary (if no description)
          const eventTitle = event.summary || "";
          const eventDescription = event.description?.trim() || "";
          const notesContent = eventDescription 
            ? `${eventTitle}\n${eventDescription}`
            : eventTitle;

          // ---- Check if exists ----
          const existingQuery = await collectionRef
            .where("google_calendar_event_id", "==", eventId)
            .limit(1)
            .get();

          if (!existingQuery.empty) {
            const existingDoc = existingQuery.docs[0];
            const existingData = existingDoc.data();

            // ✅ NEW: Skip if appointment was edited in the app
            // This prevents overwriting admin changes when they edit a Google Calendar appointment
            if (existingData.from_calendar_appointment === true) {
              logger.info(`Skipping event ${eventId} - appointment was edited in app (from_calendar_appointment: true)`);
              appEditedCount++;
              continue;
            }

            // Only update if changed
            if (existingData.updated !== event.updated) {
              batch.update(existingDoc.ref, {
                title: eventTitle,
                notes: notesContent,
                updated: event.updated,
                // ✅ TIMEZONE-AGNOSTIC: Use local timestamps
                start_time: startTimestamp,
                end_time: endTimestamp,
                date: startTimestamp,
                date_timestamp: startTimestamp,
                appointment_time: startTimestamp,
                time: timeDisplay,
                total_duration: durationMin,
                status_id: statusId,
                last_synced_at: getNowTimestamp(),
              });

              updatedCount++;
              batchOps++;
            } else {
              skippedCount++;
            }

            continue;
          }

          // ---- CREATE new appointment from Google Calendar ----
          const newRef = collectionRef.doc();

          const newDoc = {
            google_calendar_event_id: event.id,
            from_calendar_appointment: false,
            title: eventTitle,
            notes: notesContent,
            
            // ✅ TIMEZONE-AGNOSTIC: Store local time from calendar
            start_time: startTimestamp,
            end_time: endTimestamp,
            date: startTimestamp,
            date_timestamp: startTimestamp,
            appointment_time: startTimestamp,
            
            // ✅ TIMEZONE-AGNOSTIC: Display time in event's timezone
            time: timeDisplay,
            
            total_duration: durationMin,
            color_id: "WHGQm6aJH4wdT8RIqbT1",
            client_id: "",
            email: "",
            employee_id_list: [],
            is_regular: false,
            from_google_calendar: true,
            number: "",
            room_id_list: [],
            status_id: statusId,
            treatment_id_list: [],
            updated: event.updated,
            created_at: getNowTimestamp(),
          };

          batch.set(newRef, newDoc);
          processedCount++;
          batchOps++;

          calendarUpdates.push({
            eventId: event.id,
            appointmentId: newRef.id,
            originalEvent: event,
          });
        } catch (err) {
          logger.error(`Error processing event ${event.id}: ${err.message}`);
          skippedCount++;
        }
      }

      // ---- Commit batch ----
      if (batchOps > 0) {
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            await batch.commit();
            logger.info(`Committed batch of ${batchOps} ops`);
            break;
          } catch (err) {
            logger.warn(`Batch commit failed (attempt ${attempt}): ${err.message}`);
            if (attempt === 3) throw err;
            await new Promise((r) => setTimeout(r, attempt * 1000));
          }
        }
      }
    }

    // ---- Sync back Firestore IDs into Calendar ----
    if (calendarUpdates.length > 0) {
      for (let i = 0; i < calendarUpdates.length; i += 10) {
        const chunk = calendarUpdates.slice(i, i + 10);
        await updateCalendarEventsBatch(calendar, chunk);
      }
    }

    const execTime = Date.now() - startTime;

    logger.info(
      `Sync completed in ${execTime}ms — Total: ${events.length}, 
      New: ${processedCount}, Updated: ${updatedCount}, 
      Skipped: ${skippedCount}, Deleted: ${deletedCount}, 
      AppCreated: ${appCreatedCount}, AppEdited: ${appEditedCount}`
    );

    return {
      success: true,
      totalEvents: events.length,
      processedEvents: processedCount,
      updatedEvents: updatedCount,
      skippedEvents: skippedCount,
      deletedEvents: deletedCount,
      appCreatedEvents: appCreatedCount,
      appEditedEvents: appEditedCount, // ✅ NEW: Include in return
      executionTime: execTime,
    };
  } catch (err) {
    const execTime = Date.now() - startTime;
    logger.error(`Calendar sync failed after ${execTime}ms: ${err.message}`);
    throw err;
  }
});

/**
 * SCHEDULED NOTIFICATION PROCESSOR
 * 
 * Runs every minute to process pending notifications:
 * - Fetches notifications where sendTime has passed and status is "pending"
 * - Uses transaction-based leasing to prevent duplicate processing
 * - Sends FCM push notifications to target users
 * - Supports two actions: "open_review_prompt" and "appointment_followup"
 */
exports.processScheduledNotifications = onSchedule({
  schedule: "*/1 * * * *",
  timeZone: EUROPE_ROME,
}, async () => {
  const nowTs = admin.firestore.Timestamp.now();
  const nowDate = nowTs.toDate();
  
  logger.info(`⏰ ========================================`);
  logger.info(`⏰ processScheduledNotifications STARTED`);
  logger.info(`⏰ Current time: ${nowDate.toISOString()}`);
  logger.info(`⏰ Current time (Rome): ${moment.tz(nowDate, EUROPE_ROME).format('YYYY-MM-DD HH:mm:ss')}`);
  logger.info(`⏰ ========================================`);

  // Fetch all pending notifications ready to send
  const snapshot = await db
    .collection("scheduled_appointment_notifications")
    .where("sendTime", "<=", nowTs)
    .where("notification_status", "==", "pending")
    .get();

  logger.info(`📊 Query results: ${snapshot.size} pending notifications found`);

  if (snapshot.empty) {
    // Check if there are ANY pending notifications (timing debug)
    const allPendingSnap = await db
      .collection("scheduled_appointment_notifications")
      .where("notification_status", "==", "pending")
      .limit(5)
      .get();
    
    if (!allPendingSnap.empty) {
      logger.info(`⏳ Found ${allPendingSnap.size} pending notifications NOT yet ready:`);
      allPendingSnap.docs.forEach((doc, idx) => {
        const data = doc.data();
        const sendTime = data.sendTime?.toDate?.() || new Date(data.sendTime);
        const diff = Math.round((sendTime.getTime() - nowDate.getTime()) / 1000);
        logger.info(`   [${idx + 1}] ID: ${doc.id}`);
        logger.info(`       Action: ${data.action}`);
        logger.info(`       SendTime: ${sendTime.toISOString()}`);
        logger.info(`       SendTime (Rome): ${moment.tz(sendTime, EUROPE_ROME).format('YYYY-MM-DD HH:mm:ss')}`);
        logger.info(`       Seconds until ready: ${diff}s`);
        logger.info(`       AppointmentId: ${data.appointmentId}`);
      });
    } else {
      logger.info(`📭 No pending notifications in queue at all`);
    }
    
    logger.info(`⏰ processScheduledNotifications FINISHED (nothing to process)`);
    return;
  }

  // Log all notifications being processed
  logger.info(`📋 Notifications to process:`);
  snapshot.docs.forEach((doc, idx) => {
    const data = doc.data();
    const sendTime = data.sendTime?.toDate?.() || new Date(data.sendTime);
    const delaySeconds = Math.round((nowDate.getTime() - sendTime.getTime()) / 1000);
    logger.info(`   [${idx + 1}] ID: ${doc.id}`);
    logger.info(`       Action: ${data.action}`);
    logger.info(`       AppointmentId: ${data.appointmentId}`);
    logger.info(`       ClientId: ${data.clientId}`);
    logger.info(`       ClientName: ${data.client_name}`);
    logger.info(`       SendTime: ${sendTime.toISOString()}`);
    logger.info(`       Processing delay: ${delaySeconds}s`);
    logger.info(`       ReceiverIds: ${JSON.stringify(data.receiverIds)}`);
  });

  let processed = 0;
  let skipped = 0;
  let failed = 0;

  for (const doc of snapshot.docs) {
    const docRef = doc.ref;
    const docId = doc.id;

    logger.info(`🔄 ----------------------------------------`);
    logger.info(`🔄 Processing notification: ${docId}`);

    // Acquire lease via transaction to prevent duplicate processing across instances
    const leased = await db.runTransaction(async (tx) => {
      const snap = await tx.get(docRef);
      if (!snap.exists) {
        logger.warn(`⚠️ [${docId}] Document no longer exists`);
        return false;
      }
      
      const currentStatus = snap.get("notification_status");
      if (currentStatus !== "pending") {
        logger.warn(`⚠️ [${docId}] Status is "${currentStatus}", not "pending" - already being processed?`);
        return false;
      }

      tx.update(docRef, {
        notification_status: "processing",
        processingAt: admin.firestore.Timestamp.now(),
      });
      logger.info(`🔒 [${docId}] Lease acquired, status set to "processing"`);
      return true;
    });

    if (!leased) {
      logger.warn(`⏭️ [${docId}] Could not acquire lease, skipping`);
      skipped++;
      continue;
    }

    const s = (await docRef.get()).data();
    
    logger.info(`📦 [${docId}] Notification data:`);
    logger.info(`   Action: ${s.action}`);
    logger.info(`   AppointmentId: ${s.appointmentId}`);
    logger.info(`   ClientId: ${s.clientId}`);
    logger.info(`   ClientName: ${s.client_name}`);
    logger.info(`   ReceiverIds: ${JSON.stringify(s.receiverIds)}`);
    logger.info(`   CreatedAt: ${s.createdAt?.toDate?.()?.toISOString() || 'N/A'}`);

    try {
      // ==================== REVIEW PROMPT ====================
      if (s.action === "open_review_prompt") {
        logger.info(`🌟 [${docId}] Processing REVIEW PROMPT`);
        
        const notificationRef = db.collection("new_notification").doc();
        const title = "Ti sei trovata bene oggi?";
        const body = "Lasciaci una recensione 🌟";

        logger.info(`📝 [${docId}] Creating notification document: ${notificationRef.id}`);
        logger.info(`🔍 [${docId}] Getting FCM tokens for client: ${s.clientId}`);

        // ✅ Get tokens FIRST (before creating document)
        const tokens = await getFcmTokens([s.clientId]);
        
        logger.info(`🔑 [${docId}] FCM tokens retrieved: ${tokens.length} token(s)`);

        // ✅ Track FCM status
        let fcmSuccessCount = 0;
        let fcmFailureCount = 0;

        // ✅ Send FCM BEFORE creating document
        if (tokens.length > 0) {
          logger.info(`📤 [${docId}] Sending FCM push notification...`);
          const fcmResult = await sendFcm(tokens, title, body, {
            type: "review",
            appointmentId: s.appointmentId,
            notificationId: notificationRef.id,
            link: "https://g.page/r/CQvE4blXs4esEAE/review",
          }, [s.clientId]);
          fcmSuccessCount = fcmResult.successCount;
          fcmFailureCount = fcmResult.failureCount;
          logger.info(`✅ [${docId}] FCM result: ${fcmSuccessCount} success, ${fcmFailureCount} failed`);
        } else {
          logger.warn(`⚠️ [${docId}] No FCM tokens - notification saved but not pushed`);
        }

        // ✅ Build notification document
        const notifDoc = buildNotification({
          id: notificationRef.id,
          title,
          body,
          type: "review",
          receiverIds: [s.clientId],
          appointmentId: s.appointmentId,
          link: "https://g.page/r/CQvE4blXs4esEAE/review",
        });

        // ✅ Create notification with fcmSent ALREADY SET (prevents race condition)
        await notificationRef.set({
          ...notifDoc,
          fcmSent: tokens.length > 0,
          fcmSentAt: admin.firestore.Timestamp.now(),
          fcmSuccessCount,
          fcmFailureCount,
        });

        // ✅ FIXED: Mark client as having received review prompt (prevents duplicate prompts)
        await db.collection("clients").doc(s.clientId).update({
          open_review_prompt: true,
          open_review_prompt_at: admin.firestore.Timestamp.now(),
        });

        await docRef.update({ 
          notification_status: "sent", 
          sentAt: admin.firestore.Timestamp.now(),
          notificationId: notificationRef.id,
          tokenCount: tokens.length,
        });
        
        logger.info(`✅ [${docId}] REVIEW PROMPT completed successfully and client flagged`);
        processed++;
        continue;
      }

      // ==================== FOLLOW-UP ====================
      if (s.action === "appointment_followup") {
        logger.info(`📋 [${docId}] Processing APPOINTMENT FOLLOW-UP`);
        logger.info(`   AppointmentId: ${s.appointmentId}`);
        logger.info(`   ClientName: ${s.client_name}`);
        logger.info(`   ReceiverIds: ${JSON.stringify(s.receiverIds)}`);

        // Check if follow-up already exists
        logger.info(`🔍 [${docId}] Checking for existing follow-up notification...`);
        const existing = await db.collection("new_notification")
          .where("appointmentId", "==", s.appointmentId)
          .where("type", "==", "appointment_followup")
          .limit(1)
          .get();

        if (!existing.empty) {
          logger.warn(`⚠️ [${docId}] Follow-up already exists for appointment ${s.appointmentId}`);
          logger.warn(`   Existing notification ID: ${existing.docs[0].id}`);
          await docRef.update({ 
            notification_status: "sent", 
            sentAt: admin.firestore.Timestamp.now(), 
            note: "Already existed",
            existingNotificationId: existing.docs[0].id,
          });
          skipped++;
          continue;
        }

        logger.info(`✅ [${docId}] No existing follow-up found, creating new one`);

        const notificationRef = db.collection("new_notification").doc();
        const title = "Follow-up appuntamento";
        const body = `${s.client_name} si è presentato/a all'appuntamento?`;
        const actions = [
          { id: "appointment_attended", title: "Sì" },
          { id: "appointment_no_show", title: "No Show" },
        ];

        logger.info(`📝 [${docId}] Creating notification document: ${notificationRef.id}`);
        logger.info(`   Title: ${title}`);
        logger.info(`   Body: ${body}`);
        logger.info(`   Actions: ${JSON.stringify(actions)}`);
        logger.info(`🔍 [${docId}] Getting FCM tokens for receivers: ${JSON.stringify(s.receiverIds)}`);

        // ✅ Get tokens FIRST (before creating document)
        const tokens = await getFcmTokens(s.receiverIds);

        logger.info(`🔑 [${docId}] FCM tokens retrieved: ${tokens.length} token(s)`);

        // ✅ Track FCM status
        let fcmSuccessCount = 0;
        let fcmFailureCount = 0;

        // ✅ Send FCM BEFORE creating document
        if (tokens.length > 0) {
          logger.info(`📤 [${docId}] Sending FCM push notification with actions...`);
          
          const fcmPayload = {
            type: "appointment_followup",
            appointmentId: s.appointmentId,
            notificationId: notificationRef.id,
            clientName: s.client_name,
            actions: JSON.stringify(actions),
          };
          logger.info(`   FCM payload: ${JSON.stringify(fcmPayload)}`);
          
          const fcmResult = await sendFcm(tokens, title, body, fcmPayload, s.receiverIds);
          fcmSuccessCount = fcmResult.successCount;
          fcmFailureCount = fcmResult.failureCount;
          
          logger.info(`✅ [${docId}] FCM result: ${fcmSuccessCount} success, ${fcmFailureCount} failed`);
          
          if (fcmFailureCount > 0) {
            fcmResult.responses.forEach((resp, idx) => {
              if (!resp.success) {
                logger.error(`   ❌ Token ${idx} failed: ${resp.error?.code} - ${resp.error?.message}`);
              }
            });
          }
        } else {
          logger.warn(`⚠️ [${docId}] No FCM tokens - notification saved to Firestore but NOT pushed`);
        }

        // ✅ Build notification document
        const notifDoc = buildNotification({
          id: notificationRef.id,
          title,
          body,
          type: "appointment_followup",
          receiverIds: s.receiverIds,
          appointmentId: s.appointmentId,
          actions,
          ttlDays: 7,
        });

        // ✅ Create notification with fcmSent ALREADY SET (prevents race condition)
        await notificationRef.set({
          ...notifDoc,
          fcmSent: tokens.length > 0,
          fcmSentAt: admin.firestore.Timestamp.now(),
          fcmSuccessCount,
          fcmFailureCount,
        });

        await docRef.update({ 
          notification_status: "sent", 
          sentAt: admin.firestore.Timestamp.now(), 
          notificationId: notificationRef.id,
          tokenCount: tokens.length,
        });
        
        logger.info(`✅ [${docId}] FOLLOW-UP completed successfully`);
        processed++;
        continue;
      }

      // Unknown action
      logger.error(`❗ [${docId}] Unknown action: "${s.action}"`);
      await docRef.update({ 
        notification_status: "failed", 
        error: `Unknown action: ${s.action}`,
      });
      failed++;

    } catch (err) {
      logger.error(`❌ [${docId}] EXCEPTION during processing:`);
      logger.error(`   Error: ${err.message}`);
      logger.error(`   Stack: ${err.stack}`);
      
      await docRef.update({ 
        notification_status: "failed", 
        error: err.message,
        errorStack: err.stack?.substring(0, 500),
      });
      failed++;
    }
  }

  logger.info(`⏰ ========================================`);
  logger.info(`⏰ processScheduledNotifications FINISHED`);
  logger.info(`   Total found: ${snapshot.size}`);
  logger.info(`   Processed: ${processed}`);
  logger.info(`   Skipped: ${skipped}`);
  logger.info(`   Failed: ${failed}`);
  logger.info(`⏰ ========================================`);
});



// ============================================================
// HELPERS CACHE
// ============================================================

/**
 * Load treatments and categories into cache
 * @param {boolean} force - Force reload even if cache exists
 */
async function loadTreatmentCache(force = false) {
  const now = Date.now();
  
  if (!force && treatmentCache && categoryCache && cacheLoadedAt) {
    if ((now - cacheLoadedAt) < CACHE_TTL_MS) {
      return { treatments: treatmentCache, categories: categoryCache };
    }
  }

  console.log("🔄 Loading treatment cache...");

  const [treatmentsSnap, categoriesSnap] = await Promise.all([
    db.collection("treatments").get(),
    db.collection("treatment_categories").get()
  ]);

  categoryCache = {};
  categoriesSnap.forEach(doc => {
    categoryCache[doc.id] = doc.data().name || "";
  });

  treatmentCache = {};
  treatmentsSnap.forEach(doc => {
    const data = doc.data();
    const categoryName = categoryCache[data.treatment_category_id] || "";
    const treatmentName = data.name || "";
    
    treatmentCache[doc.id] = {
      name: treatmentName,
      categoryName: categoryName,
      fullName: categoryName && treatmentName 
        ? `${categoryName} - ${treatmentName}`
        : treatmentName || categoryName || "Trattamento"
    };
  });

  cacheLoadedAt = now;
  console.log(`✅ Cache loaded: ${Object.keys(treatmentCache).length} treatments`);
  
  return { treatments: treatmentCache, categories: categoryCache };
}

/**
 * Get multiple treatment full names from cache
 */
async function getTreatmentFullNames(treatmentIds = []) {
  if (!treatmentIds?.length) return [];
  
  const { treatments } = await loadTreatmentCache();
  return treatmentIds.map(id => treatments[id]?.fullName).filter(Boolean);
}

/**
 * Invalidate cache
 */
function invalidateTreatmentCache() {
  treatmentCache = null;
  categoryCache = null;
  cacheLoadedAt = null;
}

async function getCachedUserName(userId) {
  if (!userId) return null;

  const cached = userCache.get(userId);
  if (cached && (Date.now() - cached.loadedAt) < USER_CACHE_TTL_MS) {
    return cached.name;
  }

  try {
    // ✅ FIXED: Use "clients" collection, not "users"
    const userDoc = await db.collection("clients").doc(userId).get();
    if (userDoc.exists) {
      // ✅ FIXED: Use first_name/last_name, not name/surname
      const { first_name = "", last_name = "" } = userDoc.data();
      const fullName = `${first_name} ${last_name}`.trim() || null;
      
      if (userCache.size >= MAX_USER_CACHE_SIZE) {
        userCache.delete(userCache.keys().next().value);
      }
      
      userCache.set(userId, { name: fullName, loadedAt: Date.now() });
      return fullName;
    }
  } catch (err) {
    console.error("Error fetching user:", err.message);
  }
  return null;
}


// ============================================================
// HELPERS
// ============================================================

/**
 * Fetches FCM tokens for given user IDs from Firestore
 * Handles both single token (fcmToken) and array (fcmTokens) fields
 */
async function getFcmTokens(userIds) {
  logger.info(`🔑 getFcmTokens called for ${userIds.length} users`);

  const docs = await Promise.all(
    userIds.map(async (id) => {
      const doc = await db.collection("clients").doc(id).get();

      if (!doc.exists) {
        logger.warn(`   User ${id}: NOT FOUND in clients collection`);
      } else {
        const data = doc.data();
        const hasToken = !!(data?.fcmToken || data?.fcmTokens?.length);
        logger.info(`   User ${id}: ${hasToken ? '✅ HAS TOKEN' : '❌ NO TOKEN'}`);
      }

      return doc;
    })
  );

  const tokens = docs.flatMap((doc) => {
    if (!doc.exists) return [];
    const data = doc.data();
    const t = data?.fcmTokens || data?.fcmToken;
    return Array.isArray(t) ? t : t ? [t] : [];
  });

  const uniqueTokens = [...new Set(tokens)];
  logger.info(`🔑 getFcmTokens result: ${uniqueTokens.length} unique tokens found`);

  return uniqueTokens;
}

/**
 * Returns a map of { token -> userId } for per-user FCM customization (e.g. badge).
 * @param {string[]} userIds
 * @returns {Promise<Map<string, string>>} token → userId
 */
async function getFcmTokenMap(userIds) {
  const tokenMap = new Map();

  await Promise.all(
    userIds.map(async (id) => {
      const doc = await db.collection("clients").doc(id).get();
      if (!doc.exists) return;
      const data = doc.data();
      const t = data?.fcmTokens || data?.fcmToken;
      const tokenList = Array.isArray(t) ? t : t ? [t] : [];
      for (const token of tokenList) {
        tokenMap.set(token, id);
      }
    })
  );

  return tokenMap;
}

/**
 * Queries actual unread notification count for a user.
 * @param {string} userId
 * @returns {Promise<number>}
 */
async function getUnreadCount(userId) {
  try {
    const snap = await db.collection("new_notification")
      .where("status", "array-contains", userId)
      .count()
      .get();
    return snap.data().count;
  } catch (err) {
    logger.warn(`⚠️ getUnreadCount failed for ${userId}: ${err.message}`);
    return 1; // Fallback to 1 so badge is visible
  }
}

// ============================================================
// FIXED sendFcm FUNCTION FOR index.js
// ============================================================
// Find and replace the entire sendFcm function with this:
// ============================================================

/**
 * Sends FCM push notification
 * 
 * BEHAVIOR BY PLATFORM AND STATE:
 * 
 * ANDROID:
 * - Foreground: onMessage called, system does NOT show → Flutter shows local notification
 * - Background: System shows notification (from android.notification) → Flutter skips local
 * - Killed: System shows notification (from android.notification)
 * 
 * iOS:
 * - Foreground: onMessage called → Flutter shows local notification  
 * - Background/Killed: System shows notification (from apns.payload.aps.alert)
 *
 * @param {string[]} tokens - FCM device tokens
 * @param {string} title - Notification title
 * @param {string} body - Notification body
 * @param {Object} data - Custom data payload (type, appointmentId, actions, etc.)
 * @returns {Promise<Object>} - FCM response with successCount, failureCount, responses
 */
async function sendFcm(tokens, title, body, data = {}, receiverIds = []) {
  if (!tokens || tokens.length === 0) {
    logger.warn("⚠️ sendFcm: No tokens provided");
    return { successCount: 0, failureCount: 0, responses: [] };
  }

  logger.info(`📤 sendFcm: ${tokens.length} tokens, title="${title}", type=${data.type}`);

  // FCM requires all data values to be strings
  const stringData = { title, body };
  for (const [k, v] of Object.entries(data)) {
    stringData[k] = typeof v === "string" ? v : JSON.stringify(v);
  }

  // ✅ KEY: Tell Flutter to only show local notification in foreground
  stringData.showLocalNotification = "foreground_only";

  const isFollowUp = data.type === "appointment_followup";
  const androidChannelId = isFollowUp
    ? "APPOINTMENT_FOLLOWUP_CHANNEL"
    : "high_importance_channel";

  // ─────────────────────────────────────────────────────
  // iOS BADGE: query actual unread count per user
  // ─────────────────────────────────────────────────────
  let tokenToUser = new Map();
  const badgeCache = new Map(); // userId → unread count

  if (receiverIds.length > 0) {
    tokenToUser = await getFcmTokenMap(receiverIds);
    // Query unread counts in parallel for all receivers
    await Promise.all(
      receiverIds.map(async (uid) => {
        badgeCache.set(uid, await getUnreadCount(uid));
      })
    );
  }

  // ─────────────────────────────────────────────────────
  // Build per-token messages (each gets correct badge)
  // ─────────────────────────────────────────────────────
  const messages = tokens.map((token) => {
    const userId = tokenToUser.get(token);
    // +1 because this notification hasn't been written to Firestore yet
    const badge = userId && badgeCache.has(userId)
      ? badgeCache.get(userId) + 1
      : 1;

    return {
      token,
      data: stringData,
      android: {
        priority: "high",
        ttl: 86400000,
        notification: {
          title,
          body,
          channelId: androidChannelId,
          priority: "high",
          defaultSound: true,
          defaultVibrateTimings: true,
          tag: data.appointmentId || data.notificationId || "aetherium",
        },
      },
      apns: {
        payload: {
          aps: {
            alert: { title, body },
            sound: "default",
            badge,
            "mutable-content": 1,
            ...(isFollowUp && { category: "APPOINTMENT_FOLLOWUP" }),
          },
          ...stringData,
        },
        headers: {
          "apns-priority": "10",
          "apns-push-type": "alert",
        },
      },
    };
  });

  try {
    const res = await admin.messaging().sendEach(messages);

    if (res.failureCount > 0) {
      logger.warn(`⚠️ FCM: ${res.successCount} sent, ${res.failureCount} failed`);
      res.responses.forEach((resp, idx) => {
        if (!resp.success) {
          logger.error(`❌ Token[${idx}]: ${resp.error?.code} - ${resp.error?.message}`);
        }
      });
    } else {
      logger.info(`✅ FCM: ${res.successCount} sent successfully`);
    }

    return res;
  } catch (error) {
    logger.error(`❌ sendFcm error: ${error.message}`);
    throw error;
  }
}


/* =========================================================
   🏷️ BUILD APPOINTMENT TITLE
   ========================================================= */
   async function buildAppointmentTitle(clientId, treatmentIdList = []) {
    const [clientName, treatmentNames] = await Promise.all([
      getCachedUserName(clientId),
      getTreatmentFullNames(treatmentIdList)
    ]);
  
    if (!clientName && !treatmentNames.length) return "Appuntamento";
    if (!clientName) return treatmentNames.join(", ");
    if (!treatmentNames.length) return clientName;
    
    return `${clientName} - ${treatmentNames.join(", ")}`;
  }
  

exports.expireOldPendingNotifications = onSchedule({
  schedule: "0 3 * * *",
  timeZone: EUROPE_ROME
}, async () => {
  // ✅ FIXED: Use new timestamp helpers
  const now = getNowTimestamp();
  const twoDaysAgo = getTimestampDaysAgo(2);

  try {
    const snapshot = await db
      .collection("new_notification")
      .where("notification_status", "==", "pending")
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
        notification_status: "failed",
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
