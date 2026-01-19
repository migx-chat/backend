const { query, getClient } = require('../db/db');
const { generateTransactionId } = require('../utils/idGenerator');
const { checkTransferLimit } = require('../utils/floodControl');
const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');
const { getUserLevel } = require('../utils/xpLeveling');

const transferCredits = async (fromUserId, toUserId, amount, description = null, requestId = null) => {
  const client = await getClient();
  
  try {
    await client.query('BEGIN');
    
    // 🔐 STEP 5: Check for duplicate request_id (idempotency)
    if (requestId) {
      const existingTransfer = await client.query(
        'SELECT id FROM credit_logs WHERE request_id = $1',
        [requestId]
      );
      if (existingTransfer.rows.length > 0) {
        await client.query('ROLLBACK');
        return { success: false, error: 'Duplicate transfer request. Transaction already processed.' };
      }
    }
    
    // 🔐 STEP 10: Log to immutable audit log with "pending" status at start
    const senderResult = await client.query(
      'SELECT username FROM users WHERE id = $1',
      [fromUserId]
    );
    const recipientResult = await client.query(
      'SELECT username FROM users WHERE id = $1',
      [toUserId]
    );
    const fromUsername = senderResult.rows[0]?.username || 'unknown';
    const toUsername = recipientResult.rows[0]?.username || 'unknown';
    
    if (requestId) {
      await client.query(
        `INSERT INTO audit_logs (request_id, from_user_id, from_username, to_user_id, to_username, amount, status)
         VALUES ($1, $2, $3, $4, $5, $6, 'pending')`,
        [requestId, fromUserId, fromUsername, toUserId, toUsername, amount]
      );
    }
    
    const fromResult = await client.query(
      'SELECT id, username, credits FROM users WHERE id = $1 FOR UPDATE',
      [fromUserId]
    );
    
    if (fromResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return { success: false, error: 'Sender not found' };
    }
    
    const sender = fromResult.rows[0];
    
    if (sender.credits < amount) {
      await client.query('ROLLBACK');
      return { success: false, error: 'Insufficient credits' };
    }
    
    const toResult = await client.query(
      'SELECT id, username, credits FROM users WHERE id = $1 FOR UPDATE',
      [toUserId]
    );
    
    if (toResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return { success: false, error: 'Recipient not found' };
    }
    
    const recipient = toResult.rows[0];
    
    const deductResult = await client.query(
      'UPDATE users SET credits = credits - $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING credits',
      [amount, fromUserId]
    );
    
    if (deductResult.rowCount === 0) {
      await client.query('ROLLBACK');
      logger.error('TRANSFER_FAILED: Failed to deduct credits from sender', null, { fromUserId, amount });
      return { success: false, error: 'Failed to deduct credits from sender' };
    }
    
    const addResult = await client.query(
      'UPDATE users SET credits = credits + $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING credits',
      [amount, toUserId]
    );
    
    if (addResult.rowCount === 0) {
      await client.query('ROLLBACK');
      logger.error('TRANSFER_FAILED: Failed to add credits to recipient', null, { toUserId, amount });
      return { success: false, error: 'Failed to add credits to recipient' };
    }
    
    logger.info('TRANSFER_VERIFIED', { 
      fromUserId, 
      toUserId, 
      amount, 
      senderNewBalance: deductResult.rows[0].credits,
      recipientNewBalance: addResult.rows[0].credits
    });
    
    await client.query(
      `INSERT INTO credit_logs (from_user_id, to_user_id, from_username, to_username, amount, transaction_type, description, request_id)
       VALUES ($1, $2, $3, $4, $5, 'transfer', $6, $7)`,
      [fromUserId, toUserId, sender.username, recipient.username, amount, description, requestId]
    );

    // Update merchant leaderboard if sender is merchant
    if (sender.role === 'merchant') {
      const currentMonth = new Date().toISOString().slice(0, 7);
      await client.query(
        `INSERT INTO merchant_leaderboard (user_id, username, total_spent, month_year)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (user_id, month_year) 
         DO UPDATE SET total_spent = merchant_leaderboard.total_spent + $3, updated_at = CURRENT_TIMESTAMP`,
        [fromUserId, sender.username, amount, currentMonth]
      );
    }

    // ➕ Mentor Payment Tracking: Record payment if sender is mentor and receiver is their merchant
    if (sender.role === 'mentor' && recipient.role === 'merchant' && recipient.mentor_id === parseInt(fromUserId)) {
      await client.query(
        'INSERT INTO mentor_payments (mentor_id, merchant_id, amount) VALUES ($1, $2, $3)',
        [fromUserId, toUserId, amount]
      );
    }
    
    // 🔐 STEP 10: Update audit log to "completed" on success (immutable after this)
    if (requestId) {
      await client.query(
        `UPDATE audit_logs SET status = 'completed' WHERE request_id = $1`,
        [requestId]
      );
    }
    
    await client.query('COMMIT');
    
    // 🔄 Invalidate Redis credit cache for both users (used by lowcardService)
    try {
      const redis = getRedisClient();
      await redis.del(`credits:${fromUserId}`);
      await redis.del(`credits:${toUserId}`);
      // Also delete the specific key used by giftQueueService
      await redis.del(`user:${fromUserId}:credits`);
      await redis.del(`user:${toUserId}:credits`);
    } catch (cacheError) {
      logger.error('CACHE_INVALIDATION_ERROR', cacheError);
    }
    
    // 🔔 Send notification to recipient
    try {
      const notificationMessage = `You have received credit of ${amount.toLocaleString()} COINS from ${sender.username}`;
      await client.query(
        `INSERT INTO notifications (user_id, type, message, data)
         VALUES ($1, 'credit', $2, $3)`,
        [toUserId, notificationMessage, JSON.stringify({ from: sender.username, amount })]
      );
    } catch (notificationError) {
      logger.error('NOTIFICATION_ERROR: Failed to create transfer notification', notificationError);
    }
    
    const newFromCredits = Number(sender.credits) - Number(amount);
    const newToCredits = Number(recipient.credits) + Number(amount);
    
    logger.info('TRANSFER_COMPLETED: Credit transfer successful', { 
      fromUser: sender.username,
      toUser: recipient.username,
      amount: amount,
      requestId: requestId ? requestId.substring(0, 8) : 'N/A'
    });
    
    return {
      success: true,
      transactionId: generateTransactionId(),
      from: {
        userId: fromUserId,
        username: sender.username,
        newBalance: newFromCredits
      },
      to: {
        userId: toUserId,
        username: recipient.username,
        newBalance: newToCredits
      },
      amount
    };
  } catch (error) {
    // 🔐 STEP 10: Update audit log to "failed" with error reason
    if (requestId) {
      try {
        await client.query(
          `UPDATE audit_logs SET status = 'failed', error_reason = $1 WHERE request_id = $2`,
          [error.message, requestId]
        );
      } catch (auditError) {
        logger.error('AUDIT_LOG_ERROR: Failed to update audit log', auditError, { requestId: requestId?.substring(0, 8) });
      }
    }
    
    await client.query('ROLLBACK');
    logger.error('TRANSFER_FAILED: Credit transfer error', error, { 
      requestId: requestId ? requestId.substring(0, 8) : 'N/A'
    });
    return { success: false, error: 'Transfer failed' };
  } finally {
    client.release();
  }
};

const addCredits = async (userId, amount, transactionType = 'topup', description = null) => {
  try {
    const result = await query(
      `UPDATE users SET credits = credits + $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, credits`,
      [amount, userId]
    );
    
    if (result.rows.length === 0) {
      return { success: false, error: 'User not found' };
    }
    
    await query(
      `INSERT INTO credit_logs (to_user_id, to_username, amount, transaction_type, description)
       VALUES ($1, $2, $3, $4, $5)`,
      [userId, result.rows[0].username, amount, transactionType, description]
    );
    
    // 🔄 Invalidate Redis credit cache
    try {
      const redis = getRedisClient();
      await redis.del(`credits:${userId}`);
      // Also delete the specific key used by giftQueueService
      await redis.del(`user:${userId}:credits`);
    } catch (cacheError) {
      logger.error('CACHE_INVALIDATION_ERROR', cacheError);
    }
    
    return {
      success: true,
      userId,
      username: result.rows[0].username,
      newBalance: result.rows[0].credits
    };
  } catch (error) {
    logger.error('ADD_CREDITS_ERROR: Failed to add credits', error, { userId });
    return { success: false, error: 'Failed to add credits' };
  }
};

const deductCredits = async (userId, amount, transactionType = 'game_spend', description = null) => {
  const client = await getClient();
  
  try {
    await client.query('BEGIN');
    
    const result = await client.query(
      'SELECT id, username, credits FROM users WHERE id = $1 FOR UPDATE',
      [userId]
    );
    
    if (result.rows.length === 0) {
      await client.query('ROLLBACK');
      return { success: false, error: 'User not found' };
    }
    
    const user = result.rows[0];
    
    if (user.credits < amount) {
      await client.query('ROLLBACK');
      return { success: false, error: 'Insufficient credits' };
    }
    
    await client.query(
      'UPDATE users SET credits = credits - $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
      [amount, userId]
    );
    
    await client.query(
      `INSERT INTO credit_logs (from_user_id, from_username, amount, transaction_type, description)
       VALUES ($1, $2, $3, $4, $5)`,
      [userId, user.username, -amount, transactionType, description]
    );
    
    await client.query('COMMIT');
    
    // 🔄 Invalidate Redis credit cache
    try {
      const redis = getRedisClient();
      await redis.del(`credits:${userId}`);
      // Also delete the specific key used by giftQueueService
      await redis.del(`user:${userId}:credits`);
    } catch (cacheError) {
      logger.error('CACHE_INVALIDATION_ERROR', cacheError);
    }
    
    return {
      success: true,
      userId,
      username: user.username,
      newBalance: user.credits - amount
    };
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('DEDUCT_CREDITS_ERROR: Failed to deduct credits', error, { userId });
    return { success: false, error: 'Failed to deduct credits' };
  } finally {
    client.release();
  }
};

const getBalance = async (userId) => {
  try {
    const result = await query(
      'SELECT credits FROM users WHERE id = $1',
      [userId]
    );
    return result.rows[0]?.credits || 0;
  } catch (error) {
    logger.error('GET_BALANCE_ERROR: Failed to get balance', error, { userId });
    return 0;
  }
};

const getTransactionHistory = async (userId, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT * FROM credit_logs
       WHERE from_user_id = $1 OR to_user_id = $1
       ORDER BY created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    logger.error('GET_HISTORY_ERROR: Failed to get transaction history', error, { userId });
    return [];
  }
};

const getTransferHistory = async (userId, limit = 50) => {
  try {
    const result = await query(
      `SELECT * FROM credit_logs
       WHERE (from_user_id = $1 OR to_user_id = $1)
       AND transaction_type = 'transfer'
       ORDER BY created_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    return result.rows;
  } catch (error) {
    logger.error('GET_TRANSFER_HISTORY_ERROR: Failed to get transfer history', error, { userId });
    return [];
  }
};

const validateTransfer = async (fromUserId, toUserId, amount) => {
  // Transfer limits
  const MIN_AMOUNT = 1000;
  const MAX_AMOUNT = 1000000;
  const MIN_LEVEL = 10;
  
  // Self-transfer check (prevent user from sending to themselves)
  if (String(fromUserId) === String(toUserId)) {
    return { valid: false, error: 'Cannot transfer to yourself' };
  }
  
  // Minimum level check (must be level 10 or higher to transfer)
  const senderLevelData = await getUserLevel(fromUserId);
  if (senderLevelData.level < MIN_LEVEL) {
    return { valid: false, error: `Minimum level ${MIN_LEVEL} required to transfer credits. Your level: ${senderLevelData.level}` };
  }
  
  // Amount must be positive
  if (amount <= 0) {
    return { valid: false, error: 'Amount must be positive' };
  }

  // Minimum amount check
  if (amount < MIN_AMOUNT) {
    return { valid: false, error: `Minimum transfer is ${MIN_AMOUNT} credits` };
  }
  
  // Maximum amount check (prevent large transfers)
  if (amount > MAX_AMOUNT) {
    return { valid: false, error: `Maximum transfer is ${MAX_AMOUNT} credits per transaction` };
  }
  
  // Rate limiting check (prevent spam transfers)
  const rateCheck = await checkTransferLimit(fromUserId);
  if (!rateCheck.allowed) {
    return { valid: false, error: rateCheck.message };
  }
  
  // Sufficient balance check
  const balance = await getBalance(fromUserId);
  if (balance < amount) {
    return { valid: false, error: 'Insufficient credits' };
  }
  
  return { valid: true };
};

// 🔐 STEP 6: PIN Attempt Limiting with 10-minute cooldown
const validatePIN = async (userId, providedPin) => {
  const redis = getRedisClient();
  const MAX_ATTEMPTS = 3;
  const COOLDOWN_MINUTES = 10;
  const COOLDOWN_SECONDS = COOLDOWN_MINUTES * 60;
  
  const attemptKey = `pin:attempts:${userId}`;
  const cooldownKey = `pin:cooldown:${userId}`;
  
  try {
    // 1️⃣ Check if user is in cooldown
    const cooldownExists = await redis.exists(cooldownKey);
    if (cooldownExists) {
      return { valid: false, error: `Too many failed PIN attempts. Try again in ${COOLDOWN_MINUTES} minutes.`, cooldown: true };
    }
    
    // 2️⃣ Get user's stored PIN
    const userResult = await query('SELECT pin FROM users WHERE id = $1', [userId]);
    if (userResult.rows.length === 0) {
      return { valid: false, error: 'User not found' };
    }
    
    const storedPin = userResult.rows[0].pin;
    
    // 3️⃣ Validate PIN matches
    if (!storedPin || storedPin !== providedPin) {
      // Increment failed attempts
      const attempts = await redis.incr(attemptKey);
      
      // Set TTL on attempts counter (expires after 1 hour)
      if (attempts === 1) {
        await redis.expire(attemptKey, 3600);
      }
      
      // Check if max attempts reached
      if (attempts >= MAX_ATTEMPTS) {
        // Set cooldown (node-redis v4 uses set with EX option)
        await redis.set(cooldownKey, '1', { EX: COOLDOWN_SECONDS });
        // Clear attempts counter
        await redis.del(attemptKey);
        return { valid: false, error: `Too many failed PIN attempts. Try again in ${COOLDOWN_MINUTES} minutes.`, cooldown: true };
      }
      
      const attemptsLeft = MAX_ATTEMPTS - attempts;
      return { valid: false, error: `Invalid PIN. ${attemptsLeft} attempt${attemptsLeft > 1 ? 's' : ''} remaining.` };
    }
    
    // 4️⃣ PIN is valid - clear attempts
    await redis.del(attemptKey);
    return { valid: true };
    
  } catch (error) {
    console.error('❌ PIN validation error:', error);
    return { valid: false, error: 'PIN validation failed' };
  }
};

// 🔐 STEP 7: Enhanced Error Messages - Sanitize sensitive data from client, log details server-side
const sanitizeErrorForClient = (errorType, originalError, userId = null, context = {}) => {
  const timestamp = new Date().toISOString();
  
  // Log detailed error server-side with full context
  const detailedLog = {
    timestamp,
    errorType,
    userId,
    context,
    originalError: originalError?.message || originalError?.toString(),
    stack: originalError?.stack
  };
  
  console.error(`🔴 [${timestamp}] Credit Transfer Error - Type: ${errorType}`, detailedLog);
  
  // Return generic, safe message to client (never expose sensitive details)
  const safeMessages = {
    'DATABASE_ERROR': 'A system error occurred. Please try again later.',
    'VALIDATION_ERROR': 'Invalid request parameters.',
    'PIN_ERROR': 'PIN validation failed.',
    'TRANSFER_ERROR': 'Transfer could not be completed.',
    'INSUFFICIENT_BALANCE': 'Insufficient credits for this transfer.',
    'RATE_LIMIT_ERROR': 'Too many transfer attempts. Please wait a moment.',
    'UNKNOWN_ERROR': 'An unexpected error occurred. Please try again.'
  };
  
  return safeMessages[errorType] || safeMessages['UNKNOWN_ERROR'];
};

const getFullHistory = async (userId, limit = 100) => {
  try {
    const transfers = await query(
      `SELECT 
        id,
        from_user_id,
        to_user_id,
        from_username,
        to_username,
        amount,
        'transfer' as history_type,
        created_at
       FROM credit_logs
       WHERE (from_user_id = $1 OR to_user_id = $1)
       AND transaction_type = 'transfer'`,
      [userId]
    );

    const games = await query(
      `SELECT 
        id,
        from_user_id,
        to_user_id,
        from_username,
        to_username,
        amount,
        transaction_type,
        description,
        'game' as history_type,
        created_at
       FROM credit_logs
       WHERE (from_user_id = $1 OR to_user_id = $1)
       AND transaction_type IN ('game_bet', 'game_win', 'game_refund', 'flagbot_bet', 'flagbot_win', 'flagbot_refund')`,
      [userId]
    );

    const gifts = await query(
      `SELECT 
        g.id,
        g.sender_id,
        g.receiver_id,
        s.username as sender_username,
        r.username as receiver_username,
        g.gift_name,
        g.gift_cost as amount,
        'gift' as history_type,
        g.created_at
       FROM user_gifts g
       JOIN users s ON g.sender_id = s.id
       JOIN users r ON g.receiver_id = r.id
       WHERE g.sender_id = $1 OR g.receiver_id = $1`,
      [userId]
    );

    const allHistory = [
      ...transfers.rows.map(t => ({
        ...t,
        history_type: 'transfer',
        display_type: t.from_user_id == userId ? 'sent' : 'received',
        display_label: t.from_user_id == userId 
          ? `To: ${t.to_username}` 
          : `From: ${t.from_username}`,
        display_amount: t.from_user_id == userId ? -t.amount : t.amount
      })),
      ...games.rows.map(g => {
        const isFlagbot = g.transaction_type.startsWith('flagbot_');
        const isWin = g.transaction_type === 'game_win' || g.transaction_type === 'flagbot_win';
        const isRefund = g.transaction_type === 'game_refund' || g.transaction_type === 'flagbot_refund';
        const isDiceBot = g.description && g.description.includes('DiceBot');
        
        let gameName = 'LowCard';
        if (isFlagbot) gameName = 'FlagBot';
        else if (isDiceBot) gameName = 'DiceBot';
        
        return {
          ...g,
          history_type: 'game',
          display_type: isWin ? 'win' : isRefund ? 'refund' : 'bet',
          display_label: g.description || (
            isWin ? `${gameName} Win` 
            : isRefund ? `${gameName} Refund` 
            : `${gameName} Bet`
          ),
          display_amount: g.amount
        };
      }),
      ...gifts.rows.map(g => ({
        ...g,
        history_type: 'gift',
        display_type: g.sender_id == userId ? 'sent' : 'received',
        display_label: g.sender_id == userId 
          ? `Send Gift [${g.gift_name}] to ${g.receiver_username}` 
          : `Received Gift [${g.gift_name}] from ${g.sender_username}`,
        display_amount: g.sender_id == userId ? -g.amount : g.amount
      }))
    ];

    allHistory.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

    return allHistory.slice(0, limit);
  } catch (error) {
    logger.error('GET_FULL_HISTORY_ERROR: Failed to get full history', error, { userId });
    return [];
  }
};

module.exports = {
  transferCredits,
  addCredits,
  deductCredits,
  getBalance,
  getTransactionHistory,
  getTransferHistory,
  getFullHistory,
  validateTransfer,
  validatePIN,
  sanitizeErrorForClient
};
