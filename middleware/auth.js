const jwt = require('jsonwebtoken');
const db = require('../db/db');
const logger = require('../utils/logger');
const sessionService = require('../services/sessionService');

const getJwtSecret = () => {
  const secret = process.env.JWT_SECRET;
  if (!secret || secret.length < 32) {
    logger.error('SECURITY_CRITICAL: JWT_SECRET not set or too short (min 32 chars)');
    throw new Error('JWT_SECRET environment variable must be set with at least 32 characters');
  }
  return secret;
};

async function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  
  if (!authHeader) {
    logger.warn('AUTH_FAILED: Missing authorization header', { endpoint: req.path });
    return res.status(401).json({ 
      success: false,
      error: 'Authentication token missing. Please login again.',
      code: 'NO_TOKEN'
    });
  }

  if (!authHeader.startsWith('Bearer ')) {
    logger.warn('AUTH_FAILED: Invalid Bearer format', { endpoint: req.path });
    return res.status(401).json({ 
      success: false,
      error: 'Invalid authorization format. Please login again.',
      code: 'INVALID_BEARER_FORMAT'
    });
  }

  const token = authHeader.split(' ')[1];
  
  if (!token || token.trim() === '') {
    logger.warn('AUTH_FAILED: Empty token', { endpoint: req.path });
    return res.status(401).json({ 
      success: false,
      error: 'Empty token. Please login again.',
      code: 'EMPTY_TOKEN'
    });
  }

  const tokenParts = token.split('.');
  if (tokenParts.length !== 3) {
    logger.warn('AUTH_FAILED: Malformed token format', { endpoint: req.path });
    return res.status(401).json({ 
      success: false,
      error: 'Invalid token format. Please login again.',
      code: 'INVALID_TOKEN_FORMAT'
    });
  }

  try {
    const decoded = jwt.verify(token, getJwtSecret());
    
    // 🔐 SID-based session lookup
    if (decoded.sid) {
      const session = await sessionService.getSession(decoded.sid);
      
      if (!session) {
        if (!req.path.includes('/feed')) {
          logger.warn('AUTH_FAILED: Session not found or expired', { 
            sid: decoded.sid.substring(0, 8) + '...',
            endpoint: req.path 
          });
        }
        return res.status(401).json({ 
          success: false,
          error: 'Session expired. Please login again.',
          code: 'SESSION_EXPIRED'
        });
      }

      // Attach full user data from session to request
      req.user = {
        id: session.userId,
        userId: session.userId,
        username: session.username,
        role: session.role,
        email: session.email,
        sid: decoded.sid,
        deviceId: session.deviceId,
        ip: session.ip
      };
    } else {
      req.user = decoded;
    }
    
    next();
  } catch (err) {
    logger.warn('AUTH_FAILED: Token verification error', { 
      error: err.message,
      endpoint: req.path 
    });
    return res.status(401).json({ 
      success: false,
      error: 'Invalid or expired token. Please login again.',
      code: 'TOKEN_VERIFICATION_FAILED'
    });
  }
}

async function superAdminMiddleware(req, res, next) {
  try {
    const authHeader = req.headers['authorization'];
    
    if (!authHeader) {
      return res.status(401).json({ 
        success: false,
        error: 'Authentication token missing.' 
      });
    }

    const token = authHeader.split(' ')[1];
    
    if (!token) {
      return res.status(401).json({ 
        success: false,
        error: 'Invalid token format.' 
      });
    }

    const decoded = jwt.verify(token, getJwtSecret());
    
    let userId, userRole;
    
    if (decoded.sid) {
      const session = await sessionService.getSession(decoded.sid);
      if (!session) {
        return res.status(401).json({ 
          success: false,
          error: 'Session expired. Please login again.' 
        });
      }
      userId = session.userId;
      userRole = session.role;
      req.user = {
        id: session.userId,
        userId: session.userId,
        username: session.username,
        role: session.role,
        sid: decoded.sid
      };
    } else {
      // Legacy support
      userId = decoded.id || decoded.userId;
      const user = await db.query('SELECT role FROM users WHERE id = $1', [userId]);
      if (user.rows.length === 0) {
        return res.status(401).json({ 
          success: false,
          error: 'User not found.' 
        });
      }
      userRole = user.rows[0].role;
      req.user = decoded;
    }

    if (userRole !== 'super_admin') {
      logger.warn('SUPER_ADMIN_ACCESS_DENIED', { userId, role: userRole });
      return res.status(403).json({ 
        success: false,
        error: 'Admin access denied. Super admin role required.' 
      });
    }

    next();
  } catch (err) {
    logger.error('SUPER_ADMIN_MIDDLEWARE_ERROR', err);
    return res.status(401).json({ 
      success: false,
      error: 'Authentication failed.' 
    });
  }
}

module.exports = authMiddleware;
module.exports.superAdminMiddleware = superAdminMiddleware;
