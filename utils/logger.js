/**
 * 🔐 SECURITY-FOCUSED LOGGER UTILITY
 * Prevents sensitive data leakage while maintaining audit trail
 * 
 * Levels: INFO, WARN, SECURITY, ERROR
 * Environment: Development (verbose) vs Production (minimal)
 */

const isDevelopment = process.env.NODE_ENV !== 'production';

// Mask sensitive fields
const maskSensitiveData = (str) => {
  if (!str || typeof str !== 'string') return str;
  
  // Mask tokens (show first 10 and last 4 chars)
  str = str.replace(/Bearer\s+[a-zA-Z0-9\-_.]+/g, (match) => {
    const parts = match.split(' ');
    const token = parts[1];
    return token ? `Bearer ${token.substring(0, 10)}...${token.substring(token.length - 4)}` : match;
  });
  
  // Mask PINs (never show)
  str = str.replace(/"pin"\s*:\s*"[^"]*"/gi, '"pin":"***"');
  
  // Mask passwords (never show)
  str = str.replace(/"password"\s*:\s*"[^"]*"/gi, '"password":"***"');
  
  // Mask OTP (never show)
  str = str.replace(/"otp"\s*:\s*"[^"]*"/gi, '"otp":"***"');
  str = str.replace(/"activation_code"\s*:\s*"[^"]*"/gi, '"activation_code":"***"');
  
  // Mask email (show first char + *** + domain)
  str = str.replace(/"email"\s*:\s*"([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"/gi, 
    (match, local, domain) => `"email":"${local.charAt(0)}***@${domain}"`);
  
  // Mask balance (never show actual amount)
  str = str.replace(/"balance"\s*:\s*\d+/gi, '"balance":"[MASKED]"');
  str = str.replace(/"credits"\s*:\s*\d+/gi, '"credits":"[MASKED]"');
  
  // Mask device IDs (show first 8 chars only)
  str = str.replace(/[a-f0-9]{24}/gi, (match) => `${match.substring(0, 8)}...`);
  
  // Mask full authorization headers
  str = str.replace(/authorization["\s:]*[^,}]*/gi, 'authorization: [MASKED]');
  
  return str;
};

const formatLog = (level, message, data = {}) => {
  const timestamp = new Date().toISOString();
  const maskedData = typeof data === 'object' ? JSON.stringify(maskSensitiveData(JSON.stringify(data))) : maskSensitiveData(String(data));
  
  return {
    timestamp,
    level,
    message,
    data: maskedData && maskedData !== '{}' ? maskedData : undefined
  };
};

const logger = {
  // INFO: Normal operation status
  info: (message, data = {}) => {
    if (isDevelopment) {
      const log = formatLog('INFO', message, data);
      console.log(`[${log.timestamp}] ✓ ${message}`, log.data ? JSON.parse(log.data) : '');
    }
  },

  // WARN: Validation failures, suspicious but not blocked
  warn: (message, data = {}) => {
    const log = formatLog('WARN', message, data);
    console.warn(`[${log.timestamp}] ⚠️  ${message}`, log.data ? JSON.parse(log.data) : '');
  },

  // SECURITY: Rate limits, device mismatches, abuse attempts
  security: (message, data = {}) => {
    const log = formatLog('SECURITY', message, data);
    console.warn(`[${log.timestamp}] 🚨 SECURITY: ${message}`, log.data ? JSON.parse(log.data) : '');
  },

  // ERROR: Internal errors (dev logs, production logs only message)
  error: (message, error = null, data = {}) => {
    const log = formatLog('ERROR', message, data);
    if (isDevelopment && error) {
      console.error(`[${log.timestamp}] ❌ ${message}`, error.message, log.data ? JSON.parse(log.data) : '');
    } else if (!isDevelopment) {
      // Production: log only safe info
      console.error(`[${log.timestamp}] ❌ ERROR: ${message}`);
    }
  },

  // Mask helper for use in other functions
  mask: maskSensitiveData
};

module.exports = logger;
