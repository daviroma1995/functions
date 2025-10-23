// timeUtils.js
const moment = require('moment-timezone');
const { Timestamp } = require("firebase-admin/firestore");

// Constant for timezone
const EUROPE_ROME = "Europe/Rome";

/**
 * Validates a moment object to ensure it's valid before timestamp creation
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

/**
 * Returns a Moment object for the current time in Europe/Rome
 */
function getNowRome() {
  return moment().tz(EUROPE_ROME);
}

/**
 * Converts a JS Date, ISO string, or naive string to a Moment object in Europe/Rome
 * - If it's a naive string like "2025-08-08 12:10:00.000", parse it as local Rome time (not UTC)
 * - If it's a Date, convert it to Rome timezone
 * - If it's already a Moment, clone & convert to Rome
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
    
    // If string has timezone info or "T" format, parse normally and then convert
    if (/\dT\d/.test(input) || /[zZ]|[+\-]\d{2}:\d{2}/.test(input)) {
      romeMoment = moment(input).tz(EUROPE_ROME);
    } else {
      // Otherwise treat it as a naive Rome time
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

/**
 * Returns Firestore Timestamp of current time in Europe/Rome
 */
function getTimestampNowRome() {
  const nowMoment = getNowRome();
  validateMoment(nowMoment, 'getTimestampNowRome');
  return Timestamp.fromDate(nowMoment.toDate());
}

/**
 * Converts a JS Date or ISO string into a Firestore Timestamp in Europe/Rome
 * If input is already a Moment object, preserves its timezone info
 */
function toTimestampRome(input) {
  try {
    if (moment.isMoment(input)) {
      validateMoment(input, 'toTimestampRome direct moment');
      const date = input.toDate();
      validateDate(date, 'toTimestampRome moment to date');
      return Timestamp.fromDate(date);
    }
    
    const romeMoment = toRomeMoment(input);
    validateMoment(romeMoment, 'toTimestampRome conversion');
    const date = romeMoment.toDate();
    validateDate(date, 'toTimestampRome converted to date');
    return Timestamp.fromDate(date);
  } catch (error) {
    throw new Error(`Failed to create timestamp from input "${input}": ${error.message}`);
  }
}

/**
 * Converts a JS Date to Firestore Timestamp (preserves the actual time)
 */
function dateToTimestamp(date) {
  validateDate(date, 'dateToTimestamp');
  return Timestamp.fromDate(date);
}

/**
 * Creates a Firestore Timestamp from milliseconds (in Rome timezone context)
 */
function timestampFromMillis(milliseconds) {
  if (typeof milliseconds !== 'number' || !isFinite(milliseconds) || milliseconds < 0) {
    throw new Error(`Invalid milliseconds value: ${milliseconds}`);
  }
  
  const romeMoment = moment(milliseconds).tz(EUROPE_ROME);
  validateMoment(romeMoment, 'timestampFromMillis');
  return Timestamp.fromDate(romeMoment.toDate());
}

/**
 * Creates a TTL timestamp (7 days from a given date)
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
 * Creates a TTL timestamp (7 days from a Rome moment)
 */
function createTTLFromRomeMoment(romeMoment, days = 7) {
  validateMoment(romeMoment, 'createTTLFromRomeMoment input');
  
  if (typeof days !== 'number' || !isFinite(days) || days < 0) {
    throw new Error(`Invalid days value: ${days}`);
  }
  
  const futureRomeMoment = romeMoment.clone().tz(EUROPE_ROME).add(days, 'days');
  validateMoment(futureRomeMoment, 'createTTLFromRomeMoment result');
  
  const futureDate = futureRomeMoment.toDate();
  validateDate(futureDate, 'createTTLFromRomeMoment future date');
  return Timestamp.fromDate(futureDate);
}

/**
 * Converts a Moment object to Firestore Timestamp (ensures Rome timezone)
 */
function momentToTimestampRome(momentObj) {
  validateMoment(momentObj, 'momentToTimestampRome input');
  
  const romeMoment = momentObj.clone().tz(EUROPE_ROME);
  validateMoment(romeMoment, 'momentToTimestampRome conversion');
  
  const date = romeMoment.toDate();
  validateDate(date, 'momentToTimestampRome date');
  return Timestamp.fromDate(date);
}

/**
 * Gets a Rome timezone timestamp for a specific date/time
 */
function getTimestampRome(year, month, day, hour = 0, minute = 0, second = 0) {
  // Validate inputs
  const inputs = [year, month, day, hour, minute, second];
  inputs.forEach((value, index) => {
    if (typeof value !== 'number' || !isFinite(value)) {
      const names = ['year', 'month', 'day', 'hour', 'minute', 'second'];
      throw new Error(`Invalid ${names[index]} value: ${value}`);
    }
  });
  
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
  
  const date = romeMoment.toDate();
  validateDate(date, 'getTimestampRome date');
  return Timestamp.fromDate(date);
}

/**
 * Converts any input to Rome timezone Moment, then to Timestamp
 */
function anyToTimestampRome(input) {
  if (input === null || input === undefined) {
    throw new Error('Input cannot be null or undefined for anyToTimestampRome');
  }

  try {
    let romeMoment;

    if (moment.isMoment(input)) {
      romeMoment = input.clone().tz(EUROPE_ROME);
    } else if (input instanceof Date) {
      validateDate(input, 'anyToTimestampRome Date');
      romeMoment = moment(input).tz(EUROPE_ROME);
    } else if (typeof input === 'string') {
      romeMoment = toRomeMoment(input);
    } else if (typeof input === 'number') {
      if (!isFinite(input) || input < 0) {
        throw new Error(`Invalid timestamp number: ${input}`);
      }
      romeMoment = moment(input).tz(EUROPE_ROME);
    } else {
      throw new Error(`Invalid input type for timestamp conversion: ${typeof input}`);
    }

    validateMoment(romeMoment, 'anyToTimestampRome conversion');
    const date = romeMoment.toDate();
    validateDate(date, 'anyToTimestampRome final date');
    return Timestamp.fromDate(date);
  } catch (error) {
    throw new Error(`Failed to convert input "${input}" to timestamp: ${error.message}`);
  }
}

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
 * Utility to check if a value can be converted to a valid timestamp
 */
function isValidTimestampInput(input) {
  try {
    toRomeMoment(input);
    return true;
  } catch (error) {
    return false;
  }
}

module.exports = {
  // Core functions
  getNowRome,
  toRomeMoment,
  getTimestampNowRome,
  toTimestampRome,
  
  // Utility functions
  dateToTimestamp,
  timestampFromMillis,
  createTTLFromDate,
  createTTLFromRomeMoment,
  momentToTimestampRome,
  getTimestampRome,
  anyToTimestampRome,
  
  // Safety functions
  safeTimestampFromDate,
  isValidTimestampInput,
  
  // Validation functions (for debugging)
  validateMoment,
  validateDate,
  
  // Constants
  EUROPE_ROME
};