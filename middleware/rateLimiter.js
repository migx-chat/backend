const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');

const createRedisStore = () => {
  try {
    const redis = getRedisClient();
    if (redis) {
      return new RedisStore({
        sendCommand: (...args) => redis.call(...args),
        prefix: 'rl:'
      });
    }
  } catch (err) {
    logger.warn('RATE_LIMIT: Redis not available, using memory store');
  }
  return undefined;
};

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: {
    success: false,
    error: 'Too many login attempts. Please try again after 15 minutes.',
    code: 'RATE_LIMIT_EXCEEDED'
  },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || 'unknown';
  },
  handler: (req, res, next, options) => {
    logger.warn('RATE_LIMIT_EXCEEDED', { 
      ip: req.ip, 
      endpoint: req.path,
      type: 'auth'
    });
    res.status(429).json(options.message);
  }
});

const registerLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 5,
  message: {
    success: false,
    error: 'Too many registration attempts. Please try again after 1 hour.',
    code: 'RATE_LIMIT_EXCEEDED'
  },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || 'unknown';
  },
  handler: (req, res, next, options) => {
    logger.warn('RATE_LIMIT_EXCEEDED', { 
      ip: req.ip, 
      endpoint: req.path,
      type: 'register'
    });
    res.status(429).json(options.message);
  }
});

const otpLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: {
    success: false,
    error: 'Too many OTP requests. Please try again after 10 minutes.',
    code: 'RATE_LIMIT_EXCEEDED'
  },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || 'unknown';
  }
});

const apiLimiter = rateLimit({
  windowMs: 1 * 60 * 1000,
  max: 100,
  message: {
    success: false,
    error: 'Too many requests. Please slow down.',
    code: 'RATE_LIMIT_EXCEEDED'
  },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    return req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || 'unknown';
  }
});

module.exports = {
  authLimiter,
  registerLimiter,
  otpLimiter,
  apiLimiter
};
