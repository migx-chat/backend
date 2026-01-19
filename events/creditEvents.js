const logger = require('../utils/logger');
const creditService = require('../services/creditService');
const messageService = require('../services/messageService');
const userService = require('../services/userService');
const notificationService = require('../services/notificationService');
const merchantService = require('../services/merchantService');
const { getUserSocket } = require('../utils/presence');
const { addXp, XP_REWARDS } = require('../utils/xpLeveling');
const { getRedisClient } = require('../redis');
const crypto = require('crypto');

module.exports = (io, socket) => {
  const transferCredits = async (data) => {
    let lockAcquired = false;
    const lockKey = `lock:transfer:${data.fromUserId}`;
    const lockTTL = 5; // 5 seconds lock timeout
    
    try {
      const { fromUserId, toUserId, toUsername, amount, message } = data;

      // 🔒 STEP 1: Strict validation - reject early if invalid
      // Missing fields check
      if (!fromUserId || !toUserId || amount === undefined || amount === null) {
        console.warn(`[TRANSFER] ❌ Missing required fields: fromUserId=${fromUserId}, toUserId=${toUserId}, amount=${amount}`);
        socket.emit('credit:transfer:error', { message: 'Missing required fields' });
        return;
      }

      // Type conversion to ensure numeric comparison
      const numAmount = Number(amount);
      
      // Amount must be positive integer
      if (!Number.isInteger(numAmount) || numAmount <= 0) {
        console.warn(`[TRANSFER] ❌ Invalid amount: ${amount} (not a positive integer)`);
        socket.emit('credit:transfer:error', { message: 'Amount must be a positive integer' });
        return;
      }

      // 🔐 STEP 2: Server-side validation (amount, self-transfer, rate limit, balance)
      const validation = await creditService.validateTransfer(fromUserId, toUserId, numAmount);
      if (!validation.valid) {
        console.warn(`[TRANSFER] ❌ Validation failed: ${validation.error}`);
        socket.emit('credit:transfer:error', { message: validation.error });
        return;
      }

      // 🎯 STEP 2.5: Level check for regular users only
      const senderUser = await userService.getUserById(fromUserId);
      if (senderUser && senderUser.role === 'user') {
        const { query } = require('../db/db');
        const levelResult = await query('SELECT level FROM user_levels WHERE user_id = $1', [fromUserId]);
        const userLevel = levelResult.rows[0]?.level || 1;
        
        if (userLevel < 10) {
          console.warn(`[TRANSFER] ❌ Level too low: User ${fromUserId} is level ${userLevel}, needs level 10`);
          socket.emit('credit:transfer:error', { message: `You need to be at least Level 10 to transfer credits. Your current level is ${userLevel}.` });
          return;
        }
      }

      // 🔒 STEP 4: Redis Distributed Lock (prevent double-send)
      const redis = getRedisClient();
      if (redis) {
        const lockSet = await redis.set(
          lockKey,
          '1',
          'NX', // Only set if not exists
          'EX', // Set expiry
          lockTTL
        );
        
        if (!lockSet) {
          console.warn(`[TRANSFER] ❌ Double-send detected: Transfer already in progress for user ${fromUserId}`);
          socket.emit('credit:transfer:error', { message: 'Transfer already in progress. Please wait.' });
          return;
        }
        lockAcquired = true;
        logger.info(`[TRANSFER] 🔐 Lock acquired for user ${fromUserId}`);
      }

      let recipientUsername = toUsername;
      if (!recipientUsername) {
        const recipient = await userService.getUserById(toUserId);
        if (!recipient) {
          console.warn(`[TRANSFER] Recipient not found: ${toUserId}`);
          socket.emit('credit:transfer:error', { message: 'Recipient not found' });
          return;
        }
        recipientUsername = recipient.username;
      }

      logger.info(`[TRANSFER] 🔄 Processing: ${fromUserId} → ${toUserId} (${numAmount} credits)`);

      // 🆔 STEP 5: Generate request_id for idempotency
      const requestId = crypto.randomBytes(16).toString('hex');
      logger.info(`[TRANSFER] 🆔 Generated request_id: ${requestId}`);

      // 💳 STEP 3: Execute database transaction with request_id for idempotency
      const result = await creditService.transferCredits(
        fromUserId,
        toUserId,
        numAmount,
        message || 'Credit transfer',
        requestId
      );

      // All database failures MUST emit error
      if (!result.success) {
        console.error(`[TRANSFER] ❌ Transaction failed: ${result.error}`);
        socket.emit('credit:transfer:error', { message: result.error });
        return;
      }

      logger.info(`[TRANSFER] ✅ Success: ${fromUserId} → ${toUserId} (${numAmount} credits)`);

      await addXp(fromUserId, XP_REWARDS.TRANSFER_CREDIT, 'transfer_credit', io);

      // Fetch user data for notification
      const fromUserData = await userService.getUserById(fromUserId);
      const toUserData = await userService.getUserById(toUserId);
      
      // Track mentor-to-merchant transfers for subscription renewal
      if (fromUserData.role === 'mentor' && toUserData.role === 'merchant') {
        const trackResult = await merchantService.recordMentorTransfer(fromUserId, toUserId, numAmount);
        if (trackResult.success) {
          if (trackResult.renewed) {
            logger.info(`[MERCHANT] ✅ Subscription renewed for ${toUserData.username} until ${trackResult.newExpiredAt}`);
          } else {
            logger.info(`[MERCHANT] 📊 Transfer tracked for ${toUserData.username}: ${trackResult.monthlyTotal}/${merchantService.MONTHLY_MINIMUM_TRANSFER} (${trackResult.remaining} remaining)`);
          }
        }
      }

      socket.emit('credit:transfer:success', {
        fromUser: fromUserData.username,
        toUser: toUserData.username,
        amount,
        newBalance: result.from.newBalance
      });

      // Send notification to receiver
      const notification = {
        type: 'credit',
        from: fromUserData.username,
        amount,
        message: `${fromUserData.username} sent you ${amount} credits`
      };

      await notificationService.addNotification(toUserData.username, notification);

      // Emit real-time notification if user is online
      const toUserSocketId = await getUserSocket(toUserId);
      if (toUserSocketId) {
        io.to(toUserSocketId).emit('notif:credit', notification);
      }

      const recipientSocketId = await getUserSocket(toUserId);
      if (recipientSocketId) {
        io.to(recipientSocketId).emit('credit:received', {
          transactionId: result.transactionId,
          amount,
          fromUsername: result.from.username,
          newBalance: result.to.newBalance,
          message: message || null
        });

        io.to(recipientSocketId).emit('pm:receive', {
          from: {
            userId: fromUserId,
            username: result.from.username
          },
          to: {
            userId: toUserId,
            username: result.to.username
          },
          message: `💰 ${result.from.username} sent you ${amount} credits${message ? `: "${message}"` : ''}`,
          timestamp: new Date().toISOString(),
          isSystem: true
        });
      }

      await messageService.savePrivateMessage(
        fromUserId,
        toUserId,
        result.from.username,
        result.to.username,
        `💰 Sent ${amount} credits${message ? `: "${message}"` : ''}`
      );

    } catch (error) {
      console.error('[TRANSFER] Unexpected error:', error);
      socket.emit('credit:transfer:error', { message: 'Transfer failed: ' + (error.message || 'Unknown error') });
    } finally {
      // 🔐 Release lock in finally block to ensure cleanup
      if (lockAcquired) {
        const redis = getRedisClient();
        if (redis) {
          await redis.del(lockKey);
          logger.info(`[TRANSFER] 🔓 Lock released for user ${data.fromUserId}`);
        }
      }
    }
  };

  const getBalance = async (data) => {
    try {
      const { userId } = data;

      if (!userId) {
        socket.emit('error', { message: 'User ID required' });
        return;
      }

      const balance = await creditService.getBalance(userId);

      socket.emit('credit:balance', {
        userId,
        balance
      });

    } catch (error) {
      console.error('Error getting balance:', error);
      socket.emit('error', { message: 'Failed to get balance' });
    }
  };

  const getHistory = async (data) => {
    try {
      const { userId, limit = 50, offset = 0 } = data;

      if (!userId) {
        socket.emit('error', { message: 'User ID required' });
        return;
      }

      const history = await creditService.getTransactionHistory(userId, limit, offset);

      socket.emit('credit:history', {
        userId,
        transactions: history,
        hasMore: history.length === limit
      });

    } catch (error) {
      console.error('Error getting credit history:', error);
      socket.emit('error', { message: 'Failed to get credit history' });
    }
  };

  const topUp = async (data) => {
    try {
      const { userId, amount, adminId } = data;

      if (!adminId) {
        socket.emit('error', { message: 'Admin authentication required' });
        return;
      }

      const isAdmin = await userService.isAdmin(adminId);
      if (!isAdmin) {
        socket.emit('error', { message: 'Admin privileges required' });
        return;
      }

      const result = await creditService.addCredits(userId, amount, 'topup', 'Admin top-up');

      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }

      socket.emit('credit:topup:success', {
        userId,
        amount,
        newBalance: result.newBalance
      });

      const userSocketId = await getUserSocket(userId);
      if (userSocketId) {
        io.to(userSocketId).emit('credit:received', {
          amount,
          fromUsername: 'System',
          newBalance: result.newBalance,
          message: 'Credit top-up'
        });
      }

    } catch (error) {
      console.error('Error topping up credits:', error);
      socket.emit('error', { message: 'Top-up failed' });
    }
  };

  socket.on('credit:transfer', transferCredits);
  socket.on('credit:balance:get', getBalance);
  socket.on('credit:history:get', getHistory);
  socket.on('credit:topup', topUp);
};