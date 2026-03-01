// timeUtils.js
// ============================================================
// TIMEZONE UTILITY FUNCTIONS FOR FIREBASE FUNCTIONS
// ============================================================
// 
// IMPORTANT: This module uses REAL UTC timestamps
// 
// Storage Strategy:
// - date, start_time, end_time → Firestore Timestamps (UTC)
// - date_timestamp → Firestore Timestamp (UTC)
// - time → Display only, derived from start_time
// - created_at, updated_at → Firestore Timestamp.now()
//
// Mobile App Integration:
// - Mobile app sends UTC ISO strings with 'Z' suffix
// - parseAsUTC() handles defensive parsing if 'Z' is missing
// - All times stored in UTC, converted to local for display
//
// Google Calendar Sync:
// - toLocalTimestamp() extracts local time from event's timezone
// - formatTimeLocal() formats time using event's timezone
// - Timezone-agnostic: works with any calendar timezone
//
// App Appointments (createSingleAppointment, updateAppointment):
// - extractTimeFromInput() extracts HH:mm directly from input string
// - toTimestampFromInput() creates timestamp without timezone shifting
// - What user selects is exactly what gets stored
//
// ============================================================

const moment = require('moment-timezone');
const { Timestamp } = require("firebase-admin/firestore");

// ============================================================
// CONSTANTS
// ============================================================

const EUROPE_ROME = "Europe/Rome";
const DEFAULT_TIMEZONE = "Europe/Rome"; // Fallback for calendar sync

// ============================================================
// VALIDATION HELPERS
// ============================================================

/**
 * Validates a moment object to ensure it's valid before timestamp creation
 * @param {moment.Moment} momentObj - The moment object to validate
 * @param {string} context - Context for error messages
 * @returns {boolean} - True if valid
 * @throws {Error} - If invalid
 */
function validateMoment(momentObj, context = '') {
  if (!momentObj || !moment.isMoment(momentObj)) {
    throw new Error(`Expected moment object in ${context}, got: ${typeof momentObj}`);
  }
  
  if (!momentObj.isValid()) {
    throw new Error(`Invalid moment object in ${context}: ${momentObj.format() || 'Invalid date'}`);
  }
  
  const date = momentObj.toDate();
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid date from moment in ${context}: ${momentObj.toString()}`);
  }
  
  // Check for reasonable date bounds (1970-2100)
  const year = momentObj.year();
  if (year < 1970 || year > 2100) {
    throw new Error(`Date out of reasonable bounds in ${context}: ${year}`);
  }
  
  return true;
}

/**
 * Validates a Date object
 * @param {Date} date - The date to validate
 * @param {string} context - Context for error messages
 * @returns {boolean} - True if valid
 * @throws {Error} - If invalid
 */
function validateDate(date, context = '') {
  if (!(date instanceof Date)) {
    throw new Error(`Expected Date object in ${context}, got: ${typeof date}`);
  }
  
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid Date object in ${context}: ${date}`);
  }
  
  return true;
}

// ============================================================
// CORE ROME TIMEZONE FUNCTIONS
// ============================================================

/**
 * Returns a Moment object for the current time in Europe/Rome
 * @returns {moment.Moment} - Current time in Rome timezone
 */
function getNowRome() {
  return moment().tz(EUROPE_ROME);
}

/**
 * Converts any input to a Moment object in Europe/Rome timezone
 * 
 * @param {Date|string|number|moment.Moment} input - The input to convert
 * @returns {moment.Moment} - Moment object in Rome timezone
 * @throws {Error} - If input is invalid
 * 
 * @example
 * toRomeMoment(new Date())                    // Current time in Rome
 * toRomeMoment("2025-11-25T16:05:00.000Z")   // UTC string → Rome moment
 * toRomeMoment("2025-11-25 17:05:00")        // Naive string → treated as Rome time
 */
function toRomeMoment(input) {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for toRomeMoment');
  }

  if (moment.isMoment(input)) {
    const cloned = input.clone().tz(EUROPE_ROME);
    validateMoment(cloned, 'toRomeMoment from moment');
    return cloned;
  }

  if (input instanceof Date) {
    validateDate(input, 'toRomeMoment from Date');
    const romeMoment = moment(input).tz(EUROPE_ROME);
    validateMoment(romeMoment, 'toRomeMoment from Date conversion');
    return romeMoment;
  }

  if (typeof input === 'string') {
    if (input.trim() === '') {
      throw new Error('Empty string provided to toRomeMoment');
    }

    let romeMoment;
    
    // If string has timezone info (Z or +/-offset) or ISO format, parse and convert
    if (/\dT\d/.test(input) || /[zZ]|[+\-]\d{2}:\d{2}/.test(input)) {
      romeMoment = moment(input).tz(EUROPE_ROME);
    } else {
      // Naive string → treat as Rome local time
      const formats = [
        'YYYY-MM-DD HH:mm:ss.SSS',
        'YYYY-MM-DD HH:mm:ss',
        'YYYY-MM-DD HH:mm',
        'YYYY-MM-DD'
      ];
      romeMoment = moment.tz(input, formats, true, EUROPE_ROME);
    }
    
    if (!romeMoment.isValid()) {
      throw new Error(`Unable to parse date string: "${input}"`);
    }
    
    validateMoment(romeMoment, 'toRomeMoment from string');
    return romeMoment;
  }

  if (typeof input === 'number') {
    if (!isFinite(input) || input < 0) {
      throw new Error(`Invalid timestamp number: ${input}`);
    }
    
    const romeMoment = moment.tz(input, EUROPE_ROME);
    validateMoment(romeMoment, 'toRomeMoment from number');
    return romeMoment;
  }

  throw new Error(`Invalid input type for toRomeMoment: ${typeof input}`);
}

// ============================================================
// ✅ TIMEZONE-AGNOSTIC FUNCTIONS (FOR APP APPOINTMENTS)
// ============================================================

/**
 * Extracts HH:mm directly from input string - NO timezone conversion
 * What user selected is what we store
 * 
 * USE FOR: createSingleAppointment, updateAppointment - storing the 'time' field
 * 
 * Handles:
 * - "2025-12-07T20:00:00"
 * - "2025-12-07T20:00:00Z"
 * - "2025-12-07T20:00:00+01:00"
 * - "2025-12-07T20:00:00.000Z"
 * 
 * @param {string} input - ISO datetime string from client
 * @returns {string} - Time in "HH:mm" format exactly as user selected
 * 
 * @example
 * extractTimeFromInput("2025-12-07T20:00:00Z")       // → "20:00"
 * extractTimeFromInput("2025-12-07T08:30:00+01:00") // → "08:30"
 * extractTimeFromInput("2025-12-07T14:45:00")       // → "14:45"
 */
function extractTimeFromInput(input) {
  if (!input || typeof input !== 'string') {
    console.warn(`⚠️ extractTimeFromInput: Invalid input: ${input}`);
    return "00:00";
  }
  
  const match = input.match(/T(\d{2}):(\d{2})/);
  if (match) {
    return `${match[1]}:${match[2]}`;
  }
  
  console.warn(`⚠️ extractTimeFromInput: Could not extract time from: ${input}`);
  return "00:00";
}

/**
 * Creates Firestore Timestamp from input string - NO timezone conversion
 * Extracts date/time components directly from the string
 * 
 * USE FOR: createSingleAppointment, updateAppointment - all date fields
 * 
 * This ensures that if user picks 20:00, the timestamp represents 20:00
 * regardless of server timezone or any timezone info in the string.
 * 
 * @param {string} input - ISO datetime string from client
 * @returns {Timestamp} - Firestore Timestamp representing exact time user selected
 * 
 * @example
 * // User picks 20:00 → stores 20:00 (no shifting)
 * toTimestampFromInput("2025-12-07T20:00:00Z")
 * toTimestampFromInput("2025-12-07T20:00:00+01:00")
 * toTimestampFromInput("2025-12-07T20:00:00")
 */
function toTimestampFromInput(input) {
  if (!input || typeof input !== 'string') {
    console.warn(`⚠️ toTimestampFromInput: Invalid input: ${input}, using current time`);
    return Timestamp.now();
  }

  // Extract date/time components directly from string
  const match = input.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?/);
  if (!match) {
    console.warn(`⚠️ toTimestampFromInput: Invalid date format: ${input}, using current time`);
    return Timestamp.now();
  }

  const [, year, month, day, hour, minute, second = "0"] = match;

  // Create Date using UTC constructor with exact components from input
  // This ensures 20:00 in input = 20:00 in stored timestamp
  const date = new Date(Date.UTC(
    parseInt(year),
    parseInt(month) - 1,
    parseInt(day),
    parseInt(hour),
    parseInt(minute),
    parseInt(second)
  ));

  if (isNaN(date.getTime())) {
    console.warn(`⚠️ toTimestampFromInput: Failed to create date from: ${input}`);
    return Timestamp.now();
  }

  return Timestamp.fromDate(date);
}

/**
 * Calculates duration in milliseconds between two input strings
 * 
 * @param {string} startInput - Start datetime string
 * @param {string} endInput - End datetime string
 * @returns {number} - Duration in milliseconds
 */
function getDurationMsFromInputs(startInput, endInput) {
  const startTs = toTimestampFromInput(startInput);
  const endTs = toTimestampFromInput(endInput);
  return endTs.toMillis() - startTs.toMillis();
}

/**
 * Calculates duration in minutes between two input strings
 * 
 * @param {string} startInput - Start datetime string
 * @param {string} endInput - End datetime string
 * @returns {number} - Duration in minutes (minimum 1)
 */
function getDurationMinutesFromInputs(startInput, endInput) {
  const durationMs = getDurationMsFromInputs(startInput, endInput);
  return Math.max(1, Math.round(durationMs / (1000 * 60)));
}

// ============================================================
// ✅ TIMEZONE-AGNOSTIC FUNCTIONS (FOR GOOGLE CALENDAR SYNC)
// ============================================================

/**
 * Extracts local time from a datetime string and stores it as Firestore Timestamp
 * 
 * This is TIMEZONE-AGNOSTIC: it reads the timezone from the event itself
 * (either from eventTimeZone parameter or from the offset in the datetime string)
 * and stores the local time as the user sees it in their calendar.
 * 
 * USE FOR: Google Calendar sync - storing times exactly as they appear in the calendar
 * 
 * @param {string} dateTimeStr - ISO datetime string (e.g., "2025-12-12T09:00:00+01:00")
 * @param {string|undefined} eventTimeZone - Timezone from event.start.timeZone (e.g., "Europe/Rome")
 * @returns {Timestamp} - Firestore Timestamp representing local time
 * @throws {Error} - If input is invalid
 * 
 * @example
 * // User in Rome sees 9:00 AM → stores 09:00
 * toLocalTimestamp("2025-12-12T09:00:00+01:00", "Europe/Rome")
 * 
 * // User in New York sees 9:00 AM → stores 09:00
 * toLocalTimestamp("2025-12-12T09:00:00-05:00", "America/New_York")
 * 
 * // No timezone provided, parses from offset → stores 09:00
 * toLocalTimestamp("2025-12-12T09:00:00+01:00")
 */
function toLocalTimestamp(dateTimeStr, eventTimeZone) {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for toLocalTimestamp");
  }

  if (typeof dateTimeStr !== 'string') {
    throw new Error(`dateTimeStr must be a string, got: ${typeof dateTimeStr}`);
  }

  let localMoment;

  // Priority 1: Use the event's timezone if provided and valid
  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  }
  // Priority 2: Parse the offset from the dateTime string itself
  else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    // moment.parseZone preserves the original offset from the string
    localMoment = moment.parseZone(dateTimeStr);
  }
  // Priority 3: Fallback to default timezone
  else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
    console.warn(`⚠️ No timezone info found in toLocalTimestamp, using default: ${DEFAULT_TIMEZONE}`);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for toLocalTimestamp: ${dateTimeStr}`);
  }

  // Extract LOCAL time components (as displayed in the calendar)
  const year = localMoment.year();
  const month = localMoment.month();
  const day = localMoment.date();
  const hour = localMoment.hour();
  const minute = localMoment.minute();
  const second = localMoment.second();

  // Store local time by using UTC constructor with local values
  // This "tricks" Firestore into storing the local time value
  const naiveDate = new Date(Date.UTC(year, month, day, hour, minute, second));

  validateDate(naiveDate, 'toLocalTimestamp result');
  return Timestamp.fromDate(naiveDate);
}

/**
 * Formats time for display (HH:mm) using event's timezone
 * 
 * This is TIMEZONE-AGNOSTIC: extracts the local time as it appears in the calendar
 * 
 * USE FOR: Google Calendar sync - getting display time from event
 * 
 * @param {string} dateTimeStr - ISO datetime string
 * @param {string|undefined} eventTimeZone - Timezone from event (optional)
 * @returns {string} - Time in "HH:mm" format
 * 
 * @example
 * formatTimeLocal("2025-12-12T09:00:00+01:00", "Europe/Rome") // → "09:00"
 * formatTimeLocal("2025-12-12T14:30:00-05:00", "America/New_York") // → "14:30"
 */
function formatTimeLocal(dateTimeStr, eventTimeZone) {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for formatTimeLocal");
  }

  let localMoment;

  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  } else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    localMoment = moment.parseZone(dateTimeStr);
  } else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for formatTimeLocal: ${dateTimeStr}`);
  }

  return localMoment.format("HH:mm");
}

/**
 * Formats date for display using event's timezone
 * 
 * @param {string} dateTimeStr - ISO datetime string
 * @param {string|undefined} eventTimeZone - Timezone from event (optional)
 * @param {string} format - Moment format string (default: "DD/MM/YYYY")
 * @returns {string} - Formatted date string
 */
function formatDateLocal(dateTimeStr, eventTimeZone, format = "DD/MM/YYYY") {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for formatDateLocal");
  }

  let localMoment;

  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  } else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    localMoment = moment.parseZone(dateTimeStr);
  } else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for formatDateLocal: ${dateTimeStr}`);
  }

  return localMoment.format(format);
}

/**
 * Checks if a date/time is in the past using event's timezone
 * 
 * @param {string} dateTimeStr - ISO datetime string
 * @param {string|undefined} eventTimeZone - Timezone from event (optional)
 * @returns {boolean} - True if in the past
 */
function isPastLocal(dateTimeStr, eventTimeZone) {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for isPastLocal");
  }

  let localMoment;

  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  } else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    localMoment = moment.parseZone(dateTimeStr);
  } else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for isPastLocal: ${dateTimeStr}`);
  }

  return localMoment.isBefore(moment());
}

/**
 * Checks if a date/time is in the future using event's timezone
 * 
 * @param {string} dateTimeStr - ISO datetime string
 * @param {string|undefined} eventTimeZone - Timezone from event (optional)
 * @returns {boolean} - True if in the future
 */
function isFutureLocal(dateTimeStr, eventTimeZone) {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for isFutureLocal");
  }

  let localMoment;

  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  } else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    localMoment = moment.parseZone(dateTimeStr);
  } else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for isFutureLocal: ${dateTimeStr}`);
  }

  return localMoment.isAfter(moment());
}

// ============================================================
// ✅ DEFENSIVE PARSING (handles client app bugs)
// ============================================================

/**
 * Parses a date string defensively, treating as UTC even if 'Z' is missing
 * 
 * USE FOR: Parsing dates from client apps in createSingleAppointment and updateAppointment
 * 
 * This is defensive against client app bugs where dates are sent
 * without timezone information (e.g., "2025-11-26T21:41:00.000")
 * 
 * When mobile app is fixed to always send 'Z' suffix, this function will
 * still work correctly (it detects and handles both cases).
 * 
 * @param {string} dateString - ISO date string (may or may not have 'Z')
 * @returns {Date} - JavaScript Date object in UTC
 * @throws {Error} - If input is invalid
 * 
 * @example
 * parseAsUTC("2025-11-26T16:41:00.000Z")   // ✅ Has Z - parses normally
 * parseAsUTC("2025-11-26T16:41:00.000")    // ⚠️ Missing Z - adds it and warns
 * parseAsUTC("2025-11-26T16:41:00+05:00")  // ✅ Has timezone - parses normally
 */
function parseAsUTC(dateString) {
  if (!dateString || typeof dateString !== 'string') {
    throw new Error(`Invalid date string: ${dateString}`);
  }
  
  const trimmed = dateString.trim();
  
  // If already has Z or timezone offset (+05:00, -06:00), parse normally
  if (trimmed.endsWith('Z') || /[+-]\d{2}:\d{2}$/.test(trimmed)) {
    const date = new Date(trimmed);
    if (isNaN(date.getTime())) {
      throw new Error(`Failed to parse date: ${trimmed}`);
    }
    return date;
  }
  
  // ⚠️ Missing timezone info - assume UTC (defensive against client bug)
  console.warn(`⚠️ Date string missing timezone suffix, treating as UTC: ${trimmed}`);
  console.warn(`⚠️ Please fix the client app to send proper ISO strings with 'Z' suffix!`);
  
  const utcString = trimmed + 'Z';
  const date = new Date(utcString);
  
  if (isNaN(date.getTime())) {
    throw new Error(`Failed to parse date as UTC: ${trimmed}`);
  }
  
  return date;
}

// ============================================================
// ✅ UTC CONVERSION FUNCTIONS (FOR APPOINTMENT FIELDS)
// ============================================================

/**
 * Converts any input to a REAL UTC ISO string
 * 
 * USE FOR: When you need to output UTC ISO strings
 * 
 * @param {Date|string|number|moment.Moment} input - The input to convert
 * @returns {string} - UTC ISO string like "2025-11-25T16:05:00.000Z"
 * @throws {Error} - If input is invalid
 * 
 * @example
 * // Rome time 17:05 → UTC 16:05 (winter) or 15:05 (summer)
 * toUTCISOString(new Date())
 * toUTCISOString("2025-11-25T17:05:00+01:00")
 */
function toUTCISOString(input) {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for toUTCISOString');
  }

  if (input instanceof Date) {
    validateDate(input, 'toUTCISOString Date');
    return input.toISOString();
  }

  if (moment.isMoment(input)) {
    validateMoment(input, 'toUTCISOString moment');
    return input.utc().toISOString();
  }

  if (typeof input === 'string') {
    // If already a valid UTC ISO string, validate and return
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/.test(input)) {
      const parsed = new Date(input);
      validateDate(parsed, 'toUTCISOString ISO string');
      return parsed.toISOString();
    }
    
    // Parse and convert to UTC
    const parsed = moment(input);
    if (!parsed.isValid()) {
      throw new Error(`Unable to parse date string for UTC conversion: "${input}"`);
    }
    return parsed.utc().toISOString();
  }

  if (typeof input === 'number') {
    if (!isFinite(input) || input < 0) {
      throw new Error(`Invalid timestamp number: ${input}`);
    }
    return new Date(input).toISOString();
  }

  throw new Error(`Invalid input type for toUTCISOString: ${typeof input}`);
}

/**
 * Converts any input to a Firestore Timestamp (real UTC)
 * 
 * USE FOR: Converting date/time to Firestore Timestamp
 * 
 * @param {Date|string|number|moment.Moment} input - The input to convert
 * @returns {Timestamp} - Firestore Timestamp
 * @throws {Error} - If input is invalid
 */
function toUTCTimestamp(input) {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for toUTCTimestamp');
  }

  if (input instanceof Date) {
    validateDate(input, 'toUTCTimestamp Date');
    return Timestamp.fromDate(input);
  }

  if (moment.isMoment(input)) {
    validateMoment(input, 'toUTCTimestamp moment');
    return Timestamp.fromDate(input.toDate());
  }

  if (typeof input === 'string') {
    const date = new Date(input);
    if (isNaN(date.getTime())) {
      throw new Error(`Unable to parse date string: "${input}"`);
    }
    return Timestamp.fromDate(date);
  }

  if (typeof input === 'number') {
    if (!isFinite(input) || input < 0) {
      throw new Error(`Invalid timestamp number: ${input}`);
    }
    return Timestamp.fromDate(new Date(input));
  }

  throw new Error(`Invalid input type for toUTCTimestamp: ${typeof input}`);
}

/**
 * Gets start of day (midnight UTC) as Firestore Timestamp
 * 
 * USE FOR: date_timestamp field in appointments
 * 
 * @param {Date|string|number|moment.Moment} input - The input date
 * @returns {Timestamp} - Firestore Timestamp at 00:00:00.000 UTC of that day
 * @throws {Error} - If input is invalid
 * 
 * @example
 * // Any time on Nov 25 UTC → Timestamp for 2025-11-25T00:00:00.000Z
 * getStartOfDayTimestamp("2025-11-25T16:05:00.000Z")
 */
function getStartOfDayTimestamp(input) {
  let date;
  
  if (input instanceof Date) {
    validateDate(input, 'getStartOfDayTimestamp Date');
    date = input;
  } else if (moment.isMoment(input)) {
    validateMoment(input, 'getStartOfDayTimestamp moment');
    date = input.toDate();
  } else if (typeof input === 'string') {
    date = new Date(input);
    if (isNaN(date.getTime())) {
      throw new Error(`Unable to parse date string: "${input}"`);
    }
  } else if (typeof input === 'number') {
    if (!isFinite(input) || input < 0) {
      throw new Error(`Invalid timestamp number: ${input}`);
    }
    date = new Date(input);
  } else {
    throw new Error(`Invalid input type for getStartOfDayTimestamp: ${typeof input}`);
  }

  validateDate(date, 'getStartOfDayTimestamp parsed');

  // Create start of day in UTC (00:00:00.000 UTC)
  const startOfDayUTC = new Date(Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    0, 0, 0, 0
  ));

  validateDate(startOfDayUTC, 'getStartOfDayTimestamp result');
  return Timestamp.fromDate(startOfDayUTC);
}

/**
 * Gets start of day in Rome timezone as Firestore Timestamp
 * 
 * USE FOR: date_timestamp when you want midnight in Rome (not UTC)
 * 
 * @param {Date|string|number|moment.Moment} input - The input date
 * @returns {Timestamp} - Firestore Timestamp at midnight Rome time
 */
function getStartOfDayRomeTimestamp(input) {
  const romeMoment = toRomeMoment(input);
  const startOfDayRome = romeMoment.clone().startOf('day');
  validateMoment(startOfDayRome, 'getStartOfDayRomeTimestamp');
  return Timestamp.fromDate(startOfDayRome.toDate());
}

/**
 * Gets start of day using event's timezone as Firestore Timestamp
 * 
 * USE FOR: date_timestamp in calendar sync (timezone-agnostic)
 * 
 * @param {string} dateTimeStr - ISO datetime string
 * @param {string|undefined} eventTimeZone - Timezone from event (optional)
 * @returns {Timestamp} - Firestore Timestamp at midnight in event's timezone
 */
function getStartOfDayLocalTimestamp(dateTimeStr, eventTimeZone) {
  if (!dateTimeStr) {
    throw new Error("dateTimeStr is required for getStartOfDayLocalTimestamp");
  }

  let localMoment;

  if (eventTimeZone && moment.tz.zone(eventTimeZone)) {
    localMoment = moment.tz(dateTimeStr, eventTimeZone);
  } else if (dateTimeStr.includes('+') || /T.*-\d{2}:?\d{2}$/.test(dateTimeStr) || dateTimeStr.endsWith('Z')) {
    localMoment = moment.parseZone(dateTimeStr);
  } else {
    localMoment = moment.tz(dateTimeStr, DEFAULT_TIMEZONE);
  }

  if (!localMoment.isValid()) {
    throw new Error(`Invalid date string for getStartOfDayLocalTimestamp: ${dateTimeStr}`);
  }

  // Get start of day in local timezone
  const startOfDay = localMoment.clone().startOf('day');

  // Extract local time components
  const year = startOfDay.year();
  const month = startOfDay.month();
  const day = startOfDay.date();

  // Store as naive timestamp (local time)
  const naiveDate = new Date(Date.UTC(year, month, day, 0, 0, 0, 0));

  return Timestamp.fromDate(naiveDate);
}

// ============================================================
// ✅ DISPLAY FORMATTING FUNCTIONS
// ============================================================

/**
 * Formats time in Rome timezone for display
 * 
 * USE FOR: Displaying time to users (e.g., "17:05")
 * NOTE: Don't store this — derive it from start_time when displaying
 * 
 * @param {Date|string|number|moment.Moment} input - The input time
 * @returns {string} - Time in "HH:mm" format (Rome timezone)
 * 
 * @example
 * formatTimeRome("2025-11-25T16:05:00.000Z") // → "17:05" (UTC+1 winter)
 */
function formatTimeRome(input) {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for formatTimeRome');
  }

  const romeMoment = toRomeMoment(input);
  validateMoment(romeMoment, 'formatTimeRome');
  return romeMoment.format("HH:mm");
}

/**
 * Formats date in Rome timezone for display
 * 
 * @param {Date|string|number|moment.Moment} input - The input date
 * @param {string} format - Moment format string (default: "DD/MM/YYYY")
 * @returns {string} - Formatted date string
 */
function formatDateRome(input, format = "DD/MM/YYYY") {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for formatDateRome');
  }

  const romeMoment = toRomeMoment(input);
  validateMoment(romeMoment, 'formatDateRome');
  return romeMoment.format(format);
}

/**
 * Formats datetime in Rome timezone for display
 * 
 * @param {Date|string|number|moment.Moment} input - The input datetime
 * @param {string} format - Moment format string (default: "YYYY-MM-DD HH:mm:ss")
 * @returns {string} - Formatted datetime string
 */
function formatDateTimeRome(input, format = "YYYY-MM-DD HH:mm:ss") {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for formatDateTimeRome');
  }

  const romeMoment = toRomeMoment(input);
  validateMoment(romeMoment, 'formatDateTimeRome');
  return romeMoment.format(format);
}

// ============================================================
// ✅ FIRESTORE TIMESTAMP HELPERS
// ============================================================

/**
 * Returns current timestamp using Timestamp.now()
 * 
 * USE FOR: created_at, updated_at, sentAt, etc.
 * This is the preferred method for "current time" timestamps
 * 
 * @returns {Timestamp} - Current Firestore Timestamp
 */
function getNowTimestamp() {
  return Timestamp.now();
}

/**
 * Creates a TTL timestamp (N days from now)
 * 
 * USE FOR: ttl field in notifications
 * 
 * @param {number} days - Number of days from now (default: 30)
 * @returns {Timestamp} - Future Firestore Timestamp
 */
function createTTL(days = 30) {
  if (typeof days !== 'number' || !isFinite(days) || days < 0) {
    throw new Error(`Invalid days value: ${days}`);
  }
  
  const futureDate = new Date(Date.now() + (days * 24 * 60 * 60 * 1000));
  validateDate(futureDate, 'createTTL future date');
  return Timestamp.fromDate(futureDate);
}

/**
 * Creates a timestamp for N minutes from now
 * 
 * USE FOR: Scheduling notifications, retry times, etc.
 * 
 * @param {number} minutes - Number of minutes from now
 * @returns {Timestamp} - Future Firestore Timestamp
 */
function getTimestampInMinutes(minutes) {
  if (typeof minutes !== 'number' || !isFinite(minutes)) {
    throw new Error(`Invalid minutes value: ${minutes}`);
  }
  
  const futureDate = new Date(Date.now() + (minutes * 60 * 1000));
  validateDate(futureDate, 'getTimestampInMinutes');
  return Timestamp.fromDate(futureDate);
}

/**
 * Creates a timestamp for N days ago
 * 
 * USE FOR: Expiring old notifications, cleanup queries
 * 
 * @param {number} days - Number of days ago
 * @returns {Timestamp} - Past Firestore Timestamp
 */
function getTimestampDaysAgo(days) {
  if (typeof days !== 'number' || !isFinite(days) || days < 0) {
    throw new Error(`Invalid days value: ${days}`);
  }
  
  const pastDate = new Date(Date.now() - (days * 24 * 60 * 60 * 1000));
  validateDate(pastDate, 'getTimestampDaysAgo');
  return Timestamp.fromDate(pastDate);
}

/**
 * Creates a timestamp from a specific date
 * 
 * @param {Date} date - The date to convert
 * @returns {Timestamp} - Firestore Timestamp
 */
function dateToTimestamp(date) {
  validateDate(date, 'dateToTimestamp');
  return Timestamp.fromDate(date);
}

/**
 * Creates a Firestore Timestamp from milliseconds
 * 
 * @param {number} milliseconds - Unix timestamp in milliseconds
 * @returns {Timestamp} - Firestore Timestamp
 */
function timestampFromMillis(milliseconds) {
  if (typeof milliseconds !== 'number' || !isFinite(milliseconds) || milliseconds < 0) {
    throw new Error(`Invalid milliseconds value: ${milliseconds}`);
  }
  
  const date = new Date(milliseconds);
  validateDate(date, 'timestampFromMillis');
  return Timestamp.fromDate(date);
}

// ============================================================
// ✅ LEGACY COMPATIBILITY (Rome Timestamps)
// ============================================================

/**
 * Returns Firestore Timestamp of current time
 * @deprecated Use getNowTimestamp() instead
 */
function getTimestampNowRome() {
  return Timestamp.now();
}

/**
 * Converts input to Firestore Timestamp
 * @deprecated Use toUTCTimestamp() instead
 */
function toTimestampRome(input) {
  return toUTCTimestamp(input);
}

/**
 * Converts a Moment to Firestore Timestamp
 */
function momentToTimestampRome(momentObj) {
  validateMoment(momentObj, 'momentToTimestampRome input');
  return Timestamp.fromDate(momentObj.toDate());
}

/**
 * Creates a Firestore Timestamp for a specific Rome date/time
 * 
 * @param {number} year - Year (1970-2100)
 * @param {number} month - Month (1-12)
 * @param {number} day - Day (1-31)
 * @param {number} hour - Hour (0-23)
 * @param {number} minute - Minute (0-59)
 * @param {number} second - Second (0-59)
 * @returns {Timestamp} - Firestore Timestamp
 */
function getTimestampRome(year, month, day, hour = 0, minute = 0, second = 0) {
  // Validate inputs
  if (year < 1970 || year > 2100) {
    throw new Error(`Year out of reasonable bounds: ${year}`);
  }
  if (month < 1 || month > 12) {
    throw new Error(`Month must be 1-12, got: ${month}`);
  }
  if (day < 1 || day > 31) {
    throw new Error(`Day must be 1-31, got: ${day}`);
  }
  if (hour < 0 || hour > 23) {
    throw new Error(`Hour must be 0-23, got: ${hour}`);
  }
  if (minute < 0 || minute > 59) {
    throw new Error(`Minute must be 0-59, got: ${minute}`);
  }
  if (second < 0 || second > 59) {
    throw new Error(`Second must be 0-59, got: ${second}`);
  }
  
  const romeMoment = moment.tz([year, month - 1, day, hour, minute, second], EUROPE_ROME);
  validateMoment(romeMoment, 'getTimestampRome');
  return Timestamp.fromDate(romeMoment.toDate());
}

/**
 * Converts any input to Firestore Timestamp
 */
function anyToTimestampRome(input) {
  return toUTCTimestamp(input);
}

// ============================================================
// ✅ TTL HELPERS
// ============================================================

/**
 * Creates a TTL timestamp from a specific date
 * 
 * @param {Date} date - Base date
 * @param {number} days - Days to add (default: 7)
 * @returns {Timestamp} - Future Firestore Timestamp
 */
function createTTLFromDate(date, days = 7) {
  validateDate(date, 'createTTLFromDate');
  
  if (typeof days !== 'number' || !isFinite(days) || days < 0) {
    throw new Error(`Invalid days value: ${days}`);
  }
  
  const futureDate = new Date(date.getTime() + (days * 24 * 60 * 60 * 1000));
  validateDate(futureDate, 'createTTLFromDate future date');
  return Timestamp.fromDate(futureDate);
}

/**
 * Creates a TTL timestamp from a Rome moment
 * 
 * @param {moment.Moment} romeMoment - Base moment
 * @param {number} days - Days to add (default: 7)
 * @returns {Timestamp} - Future Firestore Timestamp
 */
function createTTLFromRomeMoment(romeMoment, days = 7) {
  validateMoment(romeMoment, 'createTTLFromRomeMoment input');
  
  if (typeof days !== 'number' || !isFinite(days) || days < 0) {
    throw new Error(`Invalid days value: ${days}`);
  }
  
  const futureMoment = romeMoment.clone().add(days, 'days');
  validateMoment(futureMoment, 'createTTLFromRomeMoment result');
  return Timestamp.fromDate(futureMoment.toDate());
}

// ============================================================
// ✅ SAFETY & UTILITY FUNCTIONS
// ============================================================

/**
 * Safe wrapper for Timestamp.fromDate with validation
 */
function safeTimestampFromDate(date) {
  validateDate(date, 'safeTimestampFromDate');
  
  try {
    return Timestamp.fromDate(date);
  } catch (error) {
    throw new Error(`Failed to create Firestore timestamp from date ${date}: ${error.message}`);
  }
}

/**
 * Checks if a value can be converted to a valid timestamp
 * 
 * @param {any} input - Value to check
 * @returns {boolean} - True if valid
 */
function isValidTimestampInput(input) {
  try {
    toRomeMoment(input);
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Calculates duration in minutes between two dates
 * 
 * @param {Date|string|moment.Moment} start - Start time
 * @param {Date|string|moment.Moment} end - End time
 * @returns {number} - Duration in minutes
 */
function getDurationMinutes(start, end) {
  const startMoment = moment.isMoment(start) ? start : moment(start);
  const endMoment = moment.isMoment(end) ? end : moment(end);
  
  validateMoment(startMoment, 'getDurationMinutes start');
  validateMoment(endMoment, 'getDurationMinutes end');
  
  return Math.max(1, Math.round(endMoment.diff(startMoment, 'minutes')));
}

/**
 * Checks if a date/time is in the past (Rome timezone)
 * 
 * @param {Date|string|moment.Moment} input - Time to check
 * @returns {boolean} - True if in the past
 */
function isPastRome(input) {
  const romeMoment = toRomeMoment(input);
  const nowRome = getNowRome();
  return romeMoment.isBefore(nowRome);
}

/**
 * Checks if a date/time is in the future (Rome timezone)
 * 
 * @param {Date|string|moment.Moment} input - Time to check
 * @returns {boolean} - True if in the future
 */
function isFutureRome(input) {
  const romeMoment = toRomeMoment(input);
  const nowRome = getNowRome();
  return romeMoment.isAfter(nowRome);
}

// ============================================================
// EXPORTS
// ============================================================

module.exports = {
  // ─────────────────────────────────────────────────────────
  // CONSTANTS
  // ─────────────────────────────────────────────────────────
  EUROPE_ROME,
  DEFAULT_TIMEZONE,

  // ─────────────────────────────────────────────────────────
  // CORE ROME FUNCTIONS
  // ─────────────────────────────────────────────────────────
  getNowRome,                   // Get current moment in Rome
  toRomeMoment,                 // Convert any input to Rome moment

  // ─────────────────────────────────────────────────────────
  // ✅ TIMEZONE-AGNOSTIC (FOR APP APPOINTMENTS)
  // ─────────────────────────────────────────────────────────
  extractTimeFromInput,         // → "HH:mm" exactly as user selected
  toTimestampFromInput,         // → Firestore Timestamp (no timezone shift)
  getDurationMsFromInputs,      // → Duration in milliseconds
  getDurationMinutesFromInputs, // → Duration in minutes

  // ─────────────────────────────────────────────────────────
  // ✅ TIMEZONE-AGNOSTIC (FOR GOOGLE CALENDAR SYNC)
  // ─────────────────────────────────────────────────────────
  toLocalTimestamp,             // → Firestore Timestamp (local time from event)
  formatTimeLocal,              // → "HH:mm" in event's timezone
  formatDateLocal,              // → "DD/MM/YYYY" in event's timezone
  isPastLocal,                  // Check if time is in the past (event's timezone)
  isFutureLocal,                // Check if time is in the future (event's timezone)
  getStartOfDayLocalTimestamp,  // → date_timestamp (midnight in event's timezone)

  // ─────────────────────────────────────────────────────────
  // ✅ DEFENSIVE PARSING (handles client app bugs)
  // ─────────────────────────────────────────────────────────
  parseAsUTC,                   // Parse dates defensively (handles missing 'Z')

  // ─────────────────────────────────────────────────────────
  // ✅ UTC CONVERSION (for appointment fields)
  // ─────────────────────────────────────────────────────────
  toUTCISOString,               // → date, start_time, end_time (ISO string)
  toUTCTimestamp,               // → Firestore Timestamp (real UTC)
  getStartOfDayTimestamp,       // → date_timestamp (midnight UTC)
  getStartOfDayRomeTimestamp,   // → date_timestamp (midnight Rome)

  // ─────────────────────────────────────────────────────────
  // ✅ DISPLAY FORMATTING
  // ─────────────────────────────────────────────────────────
  formatTimeRome,               // → "HH:mm" in Rome
  formatDateRome,               // → "DD/MM/YYYY" in Rome
  formatDateTimeRome,           // → "YYYY-MM-DD HH:mm:ss" in Rome

  // ─────────────────────────────────────────────────────────
  // ✅ TIMESTAMP HELPERS
  // ─────────────────────────────────────────────────────────
  getNowTimestamp,              // → created_at, updated_at, sentAt
  createTTL,                    // → ttl field (N days from now)
  getTimestampInMinutes,        // → scheduling (N minutes from now)
  getTimestampDaysAgo,          // → cleanup queries (N days ago)
  dateToTimestamp,              // → Convert Date to Timestamp
  timestampFromMillis,          // → Convert millis to Timestamp

  // ─────────────────────────────────────────────────────────
  // TTL HELPERS
  // ─────────────────────────────────────────────────────────
  createTTLFromDate,
  createTTLFromRomeMoment,

  // ─────────────────────────────────────────────────────────
  // LEGACY COMPATIBILITY (still work, but prefer new functions)
  // ─────────────────────────────────────────────────────────
  getTimestampNowRome,          // @deprecated → use getNowTimestamp()
  toTimestampRome,              // @deprecated → use toUTCTimestamp()
  momentToTimestampRome,
  getTimestampRome,
  anyToTimestampRome,

  // ─────────────────────────────────────────────────────────
  // UTILITY FUNCTIONS
  // ─────────────────────────────────────────────────────────
  getDurationMinutes,           // Calculate duration between times
  isPastRome,                   // Check if time is in the past
  isFutureRome,                 // Check if time is in the future

  // ─────────────────────────────────────────────────────────
  // SAFETY & VALIDATION
  // ─────────────────────────────────────────────────────────
  safeTimestampFromDate,
  isValidTimestampInput,
  validateMoment,
  validateDate,
};