/* =========================================================
   📆 CALENDAR SYNC v2 — webhook + incremental sync core
   =========================================================

   One-way mirror: Google Calendar → Firestore.
     - Firestore is the source of truth for CONFIRMED appointments.
     - Google events land as provisional (red) "needs details" docs.

   Why this exists: the old `scheduledCalendarSync` re-polls a fixed
   two-month window every 15 minutes. It cannot see events outside that
   window, it burns quota, and it is not real-time. This path is poked by
   Google and asks only "what changed since my last token?".

   COEXISTENCE: the old scheduled sync stays running as a safety net.
   Both paths reconcile on `google_calendar_event_id` and reuse the SAME
   doc ref, so they converge on one doc and cannot duplicate each other.
   ⚠️ Do not switch this path to "google event id AS the Firestore doc id"
   while the old function lives — the old path allocates random doc ids,
   and the two schemes would mirror every event twice.

   Times: `toUTCTimestamp` stores the TRUE instant; `formatTimeRome`
   renders the salon-zone display string. Never reintroduce the
   wall-clock-as-UTC conversion this replaced.
   ========================================================= */

const { randomUUID } = require("crypto");

module.exports = function createCalendarSyncV2(deps) {
  const {
    db, google, logger, getAuth, CALENDAR_ID,
    toUTCTimestamp, formatTimeRome, isPastLocal, getDurationMinutes,
    getNowTimestamp, STATUS_ARCHIVIATO, STATUS_CONFERMATO,
  } = deps;

  // Red "needs details" colour — the provisional marker the agenda renders.
  const PROVISIONAL_COLOR_ID = "WHGQm6aJH4wdT8RIqbT1";

  /* --- Safety cap ---------------------------------------------------------
     December 2025 saw an unexplained mass-duplication in `appointments`.
     Root cause was never established, so this is a blast-radius limiter, not
     a fix: if one run would CREATE more than this many docs, we write NOTHING
     and alert. Normal traffic is single-digit creations per poke; the initial
     full sync is the only legitimate bulk write, and this calendar holds ~21
     events. If this ever trips, something is wrong — investigate, don't raise it.
     ---------------------------------------------------------------------- */
  const SAFETY_CAP = 200;

  const PAGE_SIZE = 250;

  /* --- Field ownership ----------------------------------------------------
     GOOGLE owns the event: when/what. We overwrite these on every change.
     THE APP owns the booking: who/which service/how much. We never touch
     these on update — staff enrichment must survive a Google-side edit.
     ---------------------------------------------------------------------- */
  const GOOGLE_OWNED = [
    "title", "notes", "start_time", "end_time", "date", "date_timestamp",
    "appointment_time", "time", "total_duration", "status_id", "updated",
  ];
  const APP_OWNED_PRESERVED = [
    "client_id", "email", "number", "employee_id_list", "room_id_list",
    "treatment_id_list", "color_id", "is_regular", "from_calendar_appointment",
    "created_at", "google_calendar_event_id",
  ];

  const stateRef = (calendarId) => db.collection("sync_state").doc(calendarId);

  /* ---------- events.list: full or incremental, fully paginated ---------- */
  async function listChangedEvents(calendar, calendarId, syncToken) {
    const events = [];
    let pageToken = null;
    let nextSyncToken = null;

    do {
      // With a syncToken, Google REJECTS timeMin/timeMax/orderBy/q/updatedMin
      // (400). singleEvents must match the original request that minted the
      // token — hence it is passed identically on both paths.
      const params = syncToken
        ? { calendarId, syncToken, singleEvents: true, pageToken, maxResults: PAGE_SIZE }
        : {
            calendarId,
            // NO timeMax: a booking 5 months out must be in scope.
            timeMin: new Date().toISOString(),
            singleEvents: true,
            showDeleted: true,
            pageToken,
            maxResults: PAGE_SIZE,
          };

      const res = await calendar.events.list(params);
      events.push(...(res.data.items || []));
      pageToken = res.data.nextPageToken || null;
      // Only the FINAL page carries nextSyncToken.
      nextSyncToken = res.data.nextSyncToken || nextSyncToken;
    } while (pageToken);

    return { events, nextSyncToken };
  }

  /* ---------- map a Google event → the calendar-owned field set ---------- */
  function googleFieldsFor(event) {
    const startStr = event.start?.dateTime || event.start?.date;
    const endStr = event.end?.dateTime || event.end?.date;
    if (!startStr || !endStr) return null;

    const startDate = new Date(startStr);
    const endDate = new Date(endStr);
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) return null;

    const startTimestamp = toUTCTimestamp(startStr);
    const title = event.summary || "";
    const description = event.description?.trim() || "";

    return {
      title,
      notes: description ? `${title}\n${description}` : title,
      start_time: startTimestamp,
      end_time: toUTCTimestamp(endStr),
      date: startTimestamp,
      date_timestamp: startTimestamp,
      appointment_time: startTimestamp,
      time: formatTimeRome(startStr),
      total_duration: getDurationMinutes(startDate, endDate),
      status_id: isPastLocal(endStr, event.start?.timeZone || event.end?.timeZone)
        ? STATUS_ARCHIVIATO
        : STATUS_CONFERMATO,
      updated: event.updated,
    };
  }

  /* ---------- look up existing docs by Google event id ---------- */
  async function findExisting(eventIds) {
    const byEventId = new Map();
    const col = db.collection("appointments");
    // `in` caps at 30; 10 mirrors the chunk size the old sync has proven.
    for (let i = 0; i < eventIds.length; i += 10) {
      const chunk = eventIds.slice(i, i + 10);
      const snap = await col.where("google_calendar_event_id", "in", chunk).get();
      snap.docs.forEach((d) => byEventId.set(d.data().google_calendar_event_id, d));
    }
    return byEventId;
  }

  /* =========================================================
     runIncrementalSync — the core
     ========================================================= */
  async function runIncrementalSync(calendarId = CALENDAR_ID, _isRetry = false) {
    const started = Date.now();
    const ref = stateRef(calendarId);
    const snap = await ref.get();
    if (!snap.exists) throw new Error(`sync_state/${calendarId} missing — run Step 1 first`);

    const syncToken = snap.data().syncToken || null;
    const mode = syncToken ? "incremental" : "full";
    logger.info(`[syncV2] ${mode} sync starting for ${calendarId}`);

    const calendar = google.calendar({ version: "v3", auth: await getAuth() });

    let events, nextSyncToken;
    try {
      ({ events, nextSyncToken } = await listChangedEvents(calendar, calendarId, syncToken));
    } catch (err) {
      // 410 GONE = our token aged out. Drop it and full-sync ONCE. The _isRetry
      // guard is what stops a poisoned token from looping forever.
      if (err?.code === 410 && !_isRetry) {
        logger.warn(`[syncV2] 410 fullSyncRequired — clearing token and re-running full sync`);
        await ref.update({ syncToken: null });
        return runIncrementalSync(calendarId, true);
      }
      throw err;
    }

    if (!nextSyncToken) {
      // Without a token the next run silently degrades to a full sync forever.
      logger.error(`[syncV2] no nextSyncToken returned — token NOT advanced`);
    }
    logger.info(`[syncV2] ${events.length} changed event(s) returned`);

    /* ---- PASS 1: classify. No writes happen in this pass. ---- */
    const withId = events.filter((e) => e.id);
    const existing = await findExisting(withId.map((e) => e.id));

    const toCreate = [], toUpdate = [], toArchive = [], skipped = [];

    for (const event of withId) {
      const doc = existing.get(event.id);

      if (event.status === "cancelled") {
        if (doc) toArchive.push({ event, doc });
        continue;
      }
      if (!event.summary || !event.summary.trim()) {
        skipped.push({ id: event.id, why: "empty title" });
        continue;
      }
      const fields = googleFieldsFor(event);
      if (!fields) {
        skipped.push({ id: event.id, why: "missing/invalid dates" });
        continue;
      }
      if (!doc) {
        toCreate.push({ event, fields });
        continue;
      }
      // Staff already converted this into a real appointment. Google no longer
      // drives it — leave it entirely alone.
      if (doc.data().from_calendar_appointment === true) {
        skipped.push({ id: event.id, why: "converted in app" });
        continue;
      }
      if (doc.data().updated === event.updated) {
        skipped.push({ id: event.id, why: "unchanged" });
        continue;
      }
      toUpdate.push({ event, fields, doc });
    }

    /* ---- SAFETY CAP: stop-and-alert BEFORE any write ---- */
    if (toCreate.length > SAFETY_CAP) {
      const msg = `[syncV2] ABORT: run would create ${toCreate.length} docs (cap ${SAFETY_CAP}). ` +
                  `Nothing written. Token NOT advanced. Investigate before re-running.`;
      logger.error(msg);
      return { aborted: true, reason: "safety_cap", wouldCreate: toCreate.length };
    }

    /* ---- PASS 2: write ---- */
    const batch = db.batch();
    const col = db.collection("appointments");

    for (const { event, fields } of toCreate) {
      batch.set(col.doc(), {
        ...fields,
        google_calendar_event_id: event.id,
        from_google_calendar: true,     // provisional — renders red
        from_calendar_appointment: false,
        is_regular: false,
        color_id: PROVISIONAL_COLOR_ID,
        client_id: "",
        email: "",
        number: "",
        employee_id_list: [],
        room_id_list: [],
        treatment_id_list: [],
        created_at: getNowTimestamp(),
        last_synced_at: getNowTimestamp(),
      });
    }

    for (const { fields, doc } of toUpdate) {
      // Only GOOGLE_OWNED keys are in `fields` — app-owned data is untouched
      // by construction, not by omission. An `update` merges, so client_id,
      // treatment_id_list &c. survive.
      batch.update(doc.ref, {
        ...fields,
        last_synced_at: getNowTimestamp(),
        // The event came back from the dead — un-archive it.
        google_event_deleted: false,
        from_google_calendar: true,
      });
    }

    for (const { doc } of toArchive) {
      // ARCHIVE, not delete: staff data is irreplaceable and a deletion here
      // is unrecoverable. Clearing from_google_calendar drops a pure
      // provisional row out of the agenda; a converted appointment stays
      // visible via from_calendar_appointment and is left for staff to judge.
      batch.update(doc.ref, {
        google_event_deleted: true,
        google_deleted_at: getNowTimestamp(),
        from_google_calendar: false,
        status_id: STATUS_ARCHIVIATO,
      });
    }

    await batch.commit();

    const patch = { lastSyncAt: getNowTimestamp() };
    if (nextSyncToken) patch.syncToken = nextSyncToken;
    if (mode === "full") patch.lastFullSyncAt = getNowTimestamp();
    await ref.update(patch);

    const result = {
      mode,
      created: toCreate.length,
      updated: toUpdate.length,
      archived: toArchive.length,
      skipped: skipped.length,
      tokenStored: Boolean(nextSyncToken),
      ms: Date.now() - started,
    };
    logger.info(`[syncV2] done: ${JSON.stringify(result)}`);
    return result;
  }

  /* =========================================================
     registerCalendarWatch — one-shot channel registration
     (Brief 2 automates renewal; this is manual for now)
     ========================================================= */
  async function registerCalendarWatch(calendarId, webhookUrl, token) {
    const calendar = google.calendar({ version: "v3", auth: await getAuth() });
    const ref = stateRef(calendarId);
    const prev = (await ref.get()).data() || {};

    // Stop any previous channel first, or it keeps poking us alongside the new
    // one. Best-effort: an already-dead channel 404s and that is fine.
    if (prev.channelId && prev.channelResourceId) {
      try {
        await calendar.channels.stop({
          requestBody: { id: prev.channelId, resourceId: prev.channelResourceId },
        });
        logger.info(`[syncV2] stopped previous channel ${prev.channelId}`);
      } catch (e) {
        logger.warn(`[syncV2] could not stop previous channel: ${e.message}`);
      }
    }

    // Google requires the channel id to match [A-Za-z0-9\-_\+/=]+ — so it must
    // NOT embed calendarId, which is an email and carries `@` and `.`.
    const channelId = `aetherium-${randomUUID()}`;
    let res;
    try {
      res = await calendar.events.watch({
        calendarId,
        requestBody: {
          id: channelId,
          type: "web_hook",
          address: webhookUrl,
          token,
          params: { ttl: "604800" }, // 7 days — Google's max for calendar
        },
      });
    } catch (err) {
      // gaxios attaches the full request body — including `token` — to the
      // error. Letting that reach the logger writes our secret to Cloud
      // Logging in plaintext. Re-throw a clean error instead.
      throw new Error(`events.watch failed: ${err.message}`);
    }

    await ref.update({
      channelId,
      channelResourceId: res.data.resourceId,
      channelExpiration: new Date(Number(res.data.expiration)),
    });

    logger.info(`[syncV2] watch registered: ${channelId} exp=${res.data.expiration}`);
    return { channelId, resourceId: res.data.resourceId, expiration: res.data.expiration };
  }

  return { runIncrementalSync, registerCalendarWatch, SAFETY_CAP };
};
