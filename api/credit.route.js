const express = require('express');
const router = express.Router();
const creditService = require('../services/creditService');
const userService = require('../services/userService');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { addNotification } = require('../services/notificationService');
const authMiddleware = require('../middleware/auth');

router.post('/transfer', authMiddleware, async (req, res) => {
  // 🔐 Use authenticated user ID from JWT session, not from body
  const fromUserId = req.user.id;
  const { toUserId, amount, message, pin } = req.body;
  
  // 🔐 STEP 10: Generate unique request_id for immutable audit logging
  const requestId = crypto.randomBytes(16).toString('hex');
  
  try {
    if (!fromUserId || !toUserId || amount === undefined || amount === null) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    
    // 🔐 STEP 8: Server-side amount authority (don't trust client amount)
    // Normalize and clamp amount to valid range
    let normalizedAmount = parseInt(amount, 10);
    
    // Check if amount is a valid number
    if (isNaN(normalizedAmount) || normalizedAmount <= 0) {
      return res.status(400).json({ error: 'Amount must be a positive integer' });
    }
    
    // Define server-side limits (source of truth - client cannot override)
    const MIN_AMOUNT = 1000;
    const MAX_AMOUNT = 1000000;
    
    // Clamp amount to valid range (strict server-side authority)
    if (normalizedAmount < MIN_AMOUNT) {
      return res.status(400).json({ error: `Minimum transfer is ${MIN_AMOUNT} credits` });
    }
    
    if (normalizedAmount > MAX_AMOUNT) {
      // NEVER allow more than MAX - reject instead of clamping
      logger.security('FRAUD_ATTEMPT: Transfer amount exceeds maximum', { 
        userId: fromUserId,
        attemptedAmount: '[MASKED]',
        maxAmount: MAX_AMOUNT
      });
      return res.status(400).json({ error: `Maximum transfer is ${MAX_AMOUNT} credits per transaction` });
    }
    
    // 🔐 STEP 6: Validate PIN before transfer
    if (!pin) {
      return res.status(400).json({ error: 'PIN required for transfer' });
    }
    
    const pinValidation = await creditService.validatePIN(fromUserId, pin);
    if (!pinValidation.valid) {
      // Return 429 for cooldown (too many attempts), 400 for invalid PIN
      const statusCode = pinValidation.cooldown ? 429 : 400;
      return res.status(statusCode).json({ error: pinValidation.error, cooldown: pinValidation.cooldown });
    }
    
    // Use normalized amount for validation
    const validation = await creditService.validateTransfer(fromUserId, toUserId, normalizedAmount);
    if (!validation.valid) {
      // 🔐 STEP 7: Log detailed error server-side but return generic message to client
      logger.warn('TRANSFER_VALIDATION_FAILED', { 
        userId: fromUserId,
        reason: validation.error
      });
      return res.status(400).json({ error: validation.error });
    }
    
    // Use normalized amount throughout transfer process + pass requestId for audit logging
    const result = await creditService.transferCredits(fromUserId, toUserId, normalizedAmount, message, requestId);
    
    if (!result.success) {
      // 🔐 STEP 7: Sanitize error details from client, log server-side
      const sanitizedError = creditService.sanitizeErrorForClient('TRANSFER_ERROR', result.error, fromUserId, {
        toUserId,
        amount: normalizedAmount,
        reason: result.error
      });
      return res.status(400).json({ error: sanitizedError });
    }
    
    logger.info(`✅ Transfer completed: ${fromUserId} → ${toUserId} (${normalizedAmount} credits)`);
    
    // Send notification to recipient via Redis (for NotificationModal)
    try {
      const fromUser = await userService.getUserById(fromUserId);
      const toUser = await userService.getUserById(toUserId);
      if (fromUser && toUser) {
        await addNotification(toUser.username, {
          id: crypto.randomBytes(8).toString('hex'),
          type: 'credit',
          from: fromUser.username,
          fromUserId: fromUserId,
          amount: normalizedAmount,
          message: `${fromUser.username} sent you ${normalizedAmount.toLocaleString()} credits`
        });
        logger.info(`📬 Credit notification sent to ${toUser.username} from ${fromUser.username}`);
      }
    } catch (notifError) {
      console.error('⚠️ Error sending credit notification:', notifError.message);
    }
    
    res.json({
      success: true,
      transactionId: result.transactionId,
      from: result.from,
      to: result.to,
      amount: normalizedAmount
    });
    
  } catch (error) {
    // 🔐 STEP 7: Log detailed error server-side, return generic message to client
    const sanitizedError = creditService.sanitizeErrorForClient('UNKNOWN_ERROR', error, fromUserId, {
      toUserId,
      amount: normalizedAmount,
      endpoint: '/api/credit/transfer'
    });
    console.error('❌ Transfer endpoint error:', { userId: fromUserId, error: error.message });
    res.status(500).json({ error: sanitizedError });
  }
});

router.get('/balance/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const balance = await creditService.getBalance(userId);
    
    res.json({
      userId,
      balance
    });
    
  } catch (error) {
    console.error('Get balance error:', error);
    res.status(500).json({ error: 'Failed to get balance' });
  }
});

router.get('/history/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const history = await creditService.getTransactionHistory(userId, parseInt(limit), parseInt(offset));
    
    res.json({
      userId,
      transactions: history,
      count: history.length,
      hasMore: history.length === parseInt(limit)
    });
    
  } catch (error) {
    console.error('Get history error:', error);
    res.status(500).json({ error: 'Failed to get transaction history' });
  }
});

router.get('/transfers/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50 } = req.query;
    
    const transfers = await creditService.getTransferHistory(userId, parseInt(limit));
    
    res.json({
      userId,
      transfers,
      count: transfers.length
    });
    
  } catch (error) {
    console.error('Get transfers error:', error);
    res.status(500).json({ error: 'Failed to get transfer history' });
  }
});

router.get('/full-history/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 100 } = req.query;
    
    const fullHistory = await creditService.getFullHistory(userId, parseInt(limit));
    
    res.json({
      userId,
      history: fullHistory,
      count: fullHistory.length
    });
    
  } catch (error) {
    console.error('Get full history error:', error);
    res.status(500).json({ error: 'Failed to get full history' });
  }
});

router.post('/topup', async (req, res) => {
  try {
    const { userId, amount, adminId } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    const isAdmin = await userService.isAdmin(adminId);
    if (!isAdmin) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }
    
    if (!userId || !amount || amount <= 0) {
      return res.status(400).json({ error: 'Valid user ID and amount required' });
    }
    
    const result = await creditService.addCredits(userId, amount, 'topup', 'Admin top-up');
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      userId,
      amount,
      newBalance: result.newBalance
    });
    
  } catch (error) {
    console.error('Top-up error:', error);
    res.status(500).json({ error: 'Top-up failed' });
  }
});

module.exports = router;
