const logger = require('../utils/logger');
const { query, getClient } = require('../db/db');
const { getRedisClient } = require('../redis');
const creditService = require('./creditService');
const { v4: uuidv4 } = require('uuid');

const TAG_AMOUNT = 50;
const MERCHANT_COMMISSION_RATE = 0.02;  // 2% to merchant
const USER_COMMISSION_RATE = 0.02;      // 2% to tagged user
const COMMISSION_MATURE_HOURS = 24;

const tagUser = async (merchantUserId, targetUsername) => {
  const dbClient = await getClient();
  
  try {
    await dbClient.query('BEGIN');
    
    const merchantResult = await dbClient.query(
      'SELECT m.*, u.username as merchant_username FROM merchants m JOIN users u ON m.user_id = u.id WHERE m.user_id = $1 AND m.active = TRUE',
      [merchantUserId]
    );
    
    if (merchantResult.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'You are not an active merchant' };
    }
    
    const merchant = merchantResult.rows[0];
    
    const targetResult = await dbClient.query(
      'SELECT id, username, credits FROM users WHERE LOWER(username) = LOWER($1)',
      [targetUsername]
    );
    
    if (targetResult.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'User not found' };
    }
    
    const targetUser = targetResult.rows[0];
    
    if (targetUser.id === merchantUserId) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'You cannot tag yourself' };
    }
    
    const existingTagResult = await dbClient.query(
      'SELECT id FROM merchant_tags WHERE merchant_id = $1 AND tagged_user_id = $2 AND status = $3',
      [merchant.id, targetUser.id, 'active']
    );
    
    if (existingTagResult.rows.length > 0) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'This user is already tagged by you' };
    }
    
    const merchantBalance = await creditService.getBalance(merchantUserId);
    if (merchantBalance < TAG_AMOUNT) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: `Insufficient credits. You need ${TAG_AMOUNT} COINS` };
    }
    
    const slotResult = await dbClient.query(
      'SELECT COALESCE(MAX(tag_slot), 0) + 1 as next_slot FROM merchant_tags WHERE merchant_id = $1',
      [merchant.id]
    );
    const nextSlot = slotResult.rows[0].next_slot;
    
    const deductResult = await creditService.deductCredits(
      merchantUserId,
      TAG_AMOUNT,
      'merchant_tag',
      `Tagged user ${targetUser.username} with ${TAG_AMOUNT} COINS`
    );
    
    if (!deductResult.success) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: deductResult.error };
    }
    
    const expiredAt = new Date();
    expiredAt.setMonth(expiredAt.getMonth() + 1);
    
    const tagResult = await dbClient.query(
      `INSERT INTO merchant_tags (merchant_id, merchant_user_id, tagged_user_id, tagged_username, tag_slot, amount, remaining_balance, status, expired_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8)
       RETURNING *`,
      [merchant.id, merchantUserId, targetUser.id, targetUser.username, nextSlot, TAG_AMOUNT, TAG_AMOUNT, expiredAt]
    );
    
    const newTag = tagResult.rows[0];
    
    const redis = getRedisClient();
    await redis.hSet(`merchant:${merchant.id}:tags`, `slot:${nextSlot}`, JSON.stringify({
      tagId: newTag.id,
      userId: targetUser.id,
      username: targetUser.username,
      balance: TAG_AMOUNT
    }));
    await redis.set(`user:${targetUser.id}:tagged_balance`, TAG_AMOUNT.toString());
    await redis.set(`user:${targetUser.id}:merchant_tag_id`, newTag.id.toString());
    
    await dbClient.query('COMMIT');
    
    return {
      success: true,
      tag: {
        id: newTag.id,
        slot: nextSlot,
        username: targetUser.username,
        amount: TAG_AMOUNT,
        remainingBalance: TAG_AMOUNT
      }
    };
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Error tagging user:', error);
    return { success: false, error: 'Failed to tag user' };
  } finally {
    dbClient.release();
  }
};

const getTaggedUsers = async (merchantUserId) => {
  try {
    const merchantResult = await query(
      'SELECT id FROM merchants WHERE user_id = $1',
      [merchantUserId]
    );
    
    if (merchantResult.rows.length === 0) {
      return { success: false, error: 'Merchant not found' };
    }
    
    const merchantId = merchantResult.rows[0].id;
    
    const tagsResult = await query(
      `SELECT mt.*, u.avatar as user_avatar
       FROM merchant_tags mt
       LEFT JOIN users u ON mt.tagged_user_id = u.id
       WHERE mt.merchant_id = $1
       ORDER BY mt.tag_slot ASC`,
      [merchantId]
    );
    
    const tags = tagsResult.rows.map(tag => ({
      id: tag.id,
      slot: tag.tag_slot,
      username: tag.tagged_username,
      userId: tag.tagged_user_id,
      avatar: tag.user_avatar,
      amount: parseInt(tag.amount),
      remainingBalance: parseInt(tag.remaining_balance),
      totalSpent: parseInt(tag.total_spent),
      status: tag.status,
      taggedAt: tag.tagged_at,
      expiredAt: tag.expired_at
    }));
    
    return { success: true, tags };
  } catch (error) {
    console.error('Error getting tagged users:', error);
    return { success: false, error: 'Failed to get tagged users' };
  }
};

const getTaggedBalance = async (userId) => {
  try {
    const redis = getRedisClient();
    const cachedBalance = await redis.get(`user:${userId}:tagged_balance`);
    
    if (cachedBalance !== null) {
      return parseInt(cachedBalance);
    }
    
    const result = await query(
      `SELECT COALESCE(SUM(remaining_balance), 0) as total_balance
       FROM merchant_tags
       WHERE tagged_user_id = $1 AND status = 'active'`,
      [userId]
    );
    
    const balance = parseInt(result.rows[0]?.total_balance || 0);
    await redis.set(`user:${userId}:tagged_balance`, balance.toString(), { EX: 300 });
    
    return balance;
  } catch (error) {
    console.error('Error getting tagged balance:', error);
    return 0;
  }
};

const consumeForGame = async (userId, gameType, amount, gameSessionId = null) => {
  const dbClient = await getClient();
  
  try {
    await dbClient.query('BEGIN');
    
    const tagsResult = await dbClient.query(
      `SELECT mt.*, m.user_id as merchant_user_id
       FROM merchant_tags mt
       JOIN merchants m ON mt.merchant_id = m.id
       WHERE mt.tagged_user_id = $1 AND mt.status = 'active' AND mt.remaining_balance > 0
       ORDER BY mt.tagged_at ASC`,
      [userId]
    );
    
    if (tagsResult.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return { success: true, usedTaggedCredits: 0, remainingAmount: amount };
    }
    
    let totalConsumed = 0;
    let remainingToConsume = amount;
    
    for (const tag of tagsResult.rows) {
      if (remainingToConsume <= 0) break;
      
      const availableBalance = parseInt(tag.remaining_balance);
      const consumeAmount = Math.min(remainingToConsume, availableBalance);
      
      if (consumeAmount <= 0) continue;
      
      const newBalance = availableBalance - consumeAmount;
      const newStatus = newBalance <= 0 ? 'exhausted' : 'active';
      
      await dbClient.query(
        `UPDATE merchant_tags 
         SET remaining_balance = $1, total_spent = total_spent + $2, status = $3, updated_at = CURRENT_TIMESTAMP
         WHERE id = $4`,
        [newBalance, consumeAmount, newStatus, tag.id]
      );
      
      const spendResult = await dbClient.query(
        `INSERT INTO merchant_tag_spends (merchant_tag_id, merchant_id, tagged_user_id, game_type, spend_amount, game_session_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id`,
        [tag.id, tag.merchant_id, userId, gameType, consumeAmount, gameSessionId]
      );
      
      const spendId = spendResult.rows[0].id;
      
      const merchantCommission = Math.floor(consumeAmount * MERCHANT_COMMISSION_RATE);
      const userCommission = Math.floor(consumeAmount * USER_COMMISSION_RATE);
      const matureAt = new Date(Date.now() + COMMISSION_MATURE_HOURS * 60 * 60 * 1000);
      
      await dbClient.query(
        `INSERT INTO merchant_tag_commissions 
         (merchant_tag_spend_id, merchant_tag_id, merchant_id, merchant_user_id, tagged_user_id, spend_amount, merchant_commission, user_commission, status, mature_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9)`,
        [spendId, tag.id, tag.merchant_id, tag.merchant_user_id, userId, consumeAmount, merchantCommission, userCommission, matureAt]
      );
      
      totalConsumed += consumeAmount;
      remainingToConsume -= consumeAmount;
      
      logger.info(`[MERCHANT TAG] User ${userId} spent ${consumeAmount} tagged credits on ${gameType}. Commission: ${merchantCommission + userCommission} (mature at ${matureAt.toISOString()})`);
    }
    
    const redis = getRedisClient();
    const newTotalBalance = await getTaggedBalance(userId);
    await redis.set(`user:${userId}:tagged_balance`, newTotalBalance.toString());
    
    await dbClient.query('COMMIT');
    
    return {
      success: true,
      usedTaggedCredits: totalConsumed,
      remainingAmount: remainingToConsume
    };
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Error consuming tagged credits:', error);
    return { success: true, usedTaggedCredits: 0, remainingAmount: amount };
  } finally {
    dbClient.release();
  }
};

const getTagSpendHistory = async (tagId) => {
  try {
    const result = await query(
      `SELECT mts.*, mtc.merchant_commission, mtc.user_commission, mtc.status as commission_status, mtc.mature_at, mtc.paid_at
       FROM merchant_tag_spends mts
       LEFT JOIN merchant_tag_commissions mtc ON mts.id = mtc.merchant_tag_spend_id
       WHERE mts.merchant_tag_id = $1
       ORDER BY mts.spent_at DESC`,
      [tagId]
    );
    
    return { success: true, spends: result.rows };
  } catch (error) {
    console.error('Error getting tag spend history:', error);
    return { success: false, error: 'Failed to get spend history' };
  }
};

const getPendingCommissions = async (merchantUserId) => {
  try {
    const result = await query(
      `SELECT mtc.*, u.username as tagged_username, mts.game_type
       FROM merchant_tag_commissions mtc
       JOIN merchant_tags mt ON mtc.merchant_tag_id = mt.id
       JOIN users u ON mtc.tagged_user_id = u.id
       JOIN merchant_tag_spends mts ON mtc.merchant_tag_spend_id = mts.id
       WHERE mt.merchant_user_id = $1 AND mtc.status = 'pending'
       ORDER BY mtc.mature_at ASC`,
      [merchantUserId]
    );
    
    const totalPending = result.rows.reduce((sum, c) => sum + parseInt(c.merchant_commission), 0);
    
    return {
      success: true,
      commissions: result.rows,
      totalPending,
      count: result.rows.length
    };
  } catch (error) {
    console.error('Error getting pending commissions:', error);
    return { success: false, error: 'Failed to get pending commissions' };
  }
};

const getCommissionHistory = async (merchantUserId, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT mtc.*, u.username as tagged_username, mts.game_type
       FROM merchant_tag_commissions mtc
       JOIN merchant_tags mt ON mtc.merchant_tag_id = mt.id
       JOIN users u ON mtc.tagged_user_id = u.id
       JOIN merchant_tag_spends mts ON mtc.merchant_tag_spend_id = mts.id
       WHERE mt.merchant_user_id = $1
       ORDER BY mtc.created_at DESC
       LIMIT $2 OFFSET $3`,
      [merchantUserId, limit, offset]
    );
    
    const totalResult = await query(
      `SELECT 
         COALESCE(SUM(CASE WHEN mtc.status = 'paid' THEN mtc.merchant_commission ELSE 0 END), 0) as total_paid,
         COALESCE(SUM(CASE WHEN mtc.status = 'pending' THEN mtc.merchant_commission ELSE 0 END), 0) as total_pending,
         COALESCE(SUM(mtc.merchant_commission), 0) as total_all
       FROM merchant_tag_commissions mtc
       JOIN merchant_tags mt ON mtc.merchant_tag_id = mt.id
       WHERE mt.merchant_user_id = $1`,
      [merchantUserId]
    );
    
    const commissions = result.rows.map(c => ({
      id: c.id,
      username: c.tagged_username,
      gameType: c.game_type,
      spendAmount: parseInt(c.spend_amount),
      merchantCommission: parseInt(c.merchant_commission),
      status: c.status,
      matureAt: c.mature_at,
      paidAt: c.paid_at,
      createdAt: c.created_at
    }));
    
    return {
      success: true,
      commissions,
      totalPaid: parseInt(totalResult.rows[0]?.total_paid || 0),
      totalPending: parseInt(totalResult.rows[0]?.total_pending || 0),
      totalCommission: parseInt(totalResult.rows[0]?.total_all || 0),
      count: result.rows.length
    };
  } catch (error) {
    console.error('Error getting commission history:', error);
    return { success: false, error: 'Failed to get commission history' };
  }
};

const processMaturedCommissions = async () => {
  const dbClient = await getClient();
  
  try {
    await dbClient.query('BEGIN');
    
    const maturedResult = await dbClient.query(
      `SELECT mtc.*, mt.merchant_user_id, u1.username as merchant_username, u2.username as tagged_username
       FROM merchant_tag_commissions mtc
       JOIN merchant_tags mt ON mtc.merchant_tag_id = mt.id
       JOIN users u1 ON mt.merchant_user_id = u1.id
       JOIN users u2 ON mtc.tagged_user_id = u2.id
       WHERE mtc.status = 'pending' AND mtc.mature_at <= CURRENT_TIMESTAMP
       ORDER BY mtc.mature_at ASC
       LIMIT 100`
    );
    
    if (maturedResult.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return { success: true, processed: 0 };
    }
    
    const batchId = uuidv4();
    let totalMerchantPayout = 0;
    let totalUserPayout = 0;
    let processedCount = 0;
    
    for (const commission of maturedResult.rows) {
      try {
        await creditService.addCredits(
          commission.merchant_user_id,
          parseInt(commission.merchant_commission),
          'tag_commission',
          `Commission from tagged user ${commission.tagged_username} game spend`
        );
        
        await creditService.addCredits(
          commission.tagged_user_id,
          parseInt(commission.user_commission),
          'tag_commission',
          `Commission from game spend (tagged by ${commission.merchant_username})`
        );
        
        await dbClient.query(
          `UPDATE merchant_tag_commissions 
           SET status = 'paid', paid_at = CURRENT_TIMESTAMP, payout_batch_id = $1
           WHERE id = $2`,
          [batchId, commission.id]
        );
        
        totalMerchantPayout += parseInt(commission.merchant_commission);
        totalUserPayout += parseInt(commission.user_commission);
        processedCount++;
        
        logger.info(`[COMMISSION PAYOUT] Paid ${commission.merchant_commission} to merchant ${commission.merchant_username}, ${commission.user_commission} to user ${commission.tagged_username}`);
      } catch (payErr) {
        console.error(`Failed to process commission ${commission.id}:`, payErr);
      }
    }
    
    if (processedCount > 0) {
      await dbClient.query(
        `INSERT INTO merchant_commission_payouts (batch_id, merchant_id, total_merchant_payout, total_user_payout, commissions_count, note)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [batchId, maturedResult.rows[0].merchant_id, totalMerchantPayout, totalUserPayout, processedCount, 'Automated payout']
      );
    }
    
    await dbClient.query('COMMIT');
    
    return {
      success: true,
      processed: processedCount,
      totalMerchantPayout,
      totalUserPayout,
      batchId
    };
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Error processing matured commissions:', error);
    return { success: false, error: 'Failed to process commissions' };
  } finally {
    dbClient.release();
  }
};

const removeTag = async (merchantUserId, tagId) => {
  const dbClient = await getClient();
  
  try {
    await dbClient.query('BEGIN');
    
    const tagResult = await dbClient.query(
      `SELECT mt.*, m.user_id as merchant_owner_id
       FROM merchant_tags mt
       JOIN merchants m ON mt.merchant_id = m.id
       WHERE mt.id = $1`,
      [tagId]
    );
    
    if (tagResult.rows.length === 0) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'Tag not found' };
    }
    
    const tag = tagResult.rows[0];
    
    if (tag.merchant_owner_id !== merchantUserId) {
      await dbClient.query('ROLLBACK');
      return { success: false, error: 'You do not own this tag' };
    }
    
    const remainingBalance = parseInt(tag.remaining_balance);
    
    if (remainingBalance > 0) {
      await creditService.addCredits(
        merchantUserId,
        remainingBalance,
        'tag_refund',
        `Refund for removed tag (user: ${tag.tagged_username})`
      );
    }
    
    await dbClient.query(
      `UPDATE merchant_tags SET status = 'inactive', remaining_balance = 0, updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
      [tagId]
    );
    
    const redis = getRedisClient();
    await redis.del(`user:${tag.tagged_user_id}:tagged_balance`);
    await redis.del(`user:${tag.tagged_user_id}:merchant_tag_id`);
    await redis.del(`merchant:${tag.merchant_id}:tags`);
    
    await dbClient.query('COMMIT');
    
    return {
      success: true,
      refundedAmount: remainingBalance
    };
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Error removing tag:', error);
    return { success: false, error: 'Failed to remove tag' };
  } finally {
    dbClient.release();
  }
};

const getUserPendingCommissions = async (userId) => {
  try {
    const result = await query(
      `SELECT mtc.*, mts.game_type, u.username as merchant_username
       FROM merchant_tag_commissions mtc
       JOIN merchant_tags mt ON mtc.merchant_tag_id = mt.id
       JOIN merchant_tag_spends mts ON mtc.merchant_tag_spend_id = mts.id
       JOIN users u ON mt.merchant_user_id = u.id
       WHERE mtc.tagged_user_id = $1 AND mtc.status = 'pending'
       ORDER BY mtc.mature_at ASC`,
      [userId]
    );
    
    const totalPending = result.rows.reduce((sum, c) => sum + parseInt(c.user_commission), 0);
    
    return {
      success: true,
      commissions: result.rows,
      totalPending,
      count: result.rows.length
    };
  } catch (error) {
    console.error('Error getting user pending commissions:', error);
    return { success: false, error: 'Failed to get pending commissions' };
  }
};

// Track ALL spending from tagged user (not limited to tag balance)
const trackTaggedUserSpending = async (userId, gameType, spendAmount, gameSessionId = null) => {
  const dbClient = await getClient();
  
  try {
    // Find active tag for this user
    const tagResult = await dbClient.query(
      `SELECT mt.*, m.user_id as merchant_user_id
       FROM merchant_tags mt
       JOIN merchants m ON mt.merchant_id = m.id
       WHERE mt.tagged_user_id = $1 AND mt.status IN ('active', 'exhausted')
       ORDER BY mt.tagged_at DESC
       LIMIT 1`,
      [userId]
    );
    
    if (tagResult.rows.length === 0) {
      return { success: false, tracked: false, reason: 'User not tagged' };
    }
    
    const tag = tagResult.rows[0];
    
    await dbClient.query('BEGIN');
    
    // Update total_spent on tag
    await dbClient.query(
      `UPDATE merchant_tags 
       SET total_spent = total_spent + $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2`,
      [spendAmount, tag.id]
    );
    
    // Record the spend
    const spendResult = await dbClient.query(
      `INSERT INTO merchant_tag_spends (merchant_tag_id, merchant_id, tagged_user_id, game_type, spend_amount, game_session_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id`,
      [tag.id, tag.merchant_id, userId, gameType, spendAmount, gameSessionId]
    );
    
    const spendId = spendResult.rows[0].id;
    
    // Calculate commission on FULL spend amount
    const merchantCommission = Math.floor(spendAmount * MERCHANT_COMMISSION_RATE);
    const userCommission = Math.floor(spendAmount * USER_COMMISSION_RATE);
    const matureAt = new Date(Date.now() + COMMISSION_MATURE_HOURS * 60 * 60 * 1000);
    
    await dbClient.query(
      `INSERT INTO merchant_tag_commissions 
       (merchant_tag_spend_id, merchant_tag_id, merchant_id, merchant_user_id, tagged_user_id, spend_amount, merchant_commission, user_commission, status, mature_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9)`,
      [spendId, tag.id, tag.merchant_id, tag.merchant_user_id, userId, spendAmount, merchantCommission, userCommission, matureAt]
    );
    
    await dbClient.query('COMMIT');
    
    logger.info(`[MERCHANT TAG] Tracked spending: User ${userId} spent ${spendAmount} on ${gameType}. Commission: merchant=${merchantCommission}, user=${userCommission}`);
    
    return {
      success: true,
      tracked: true,
      spendAmount,
      merchantCommission,
      userCommission,
      matureAt
    };
  } catch (error) {
    await dbClient.query('ROLLBACK');
    console.error('Error tracking tagged user spending:', error);
    return { success: false, tracked: false, error: 'Failed to track spending' };
  } finally {
    dbClient.release();
  }
};

// Check if user is tagged (active or exhausted)
const isUserTagged = async (userId) => {
  try {
    const result = await query(
      `SELECT mt.id, mt.merchant_id, m.user_id as merchant_user_id, u.username as merchant_username
       FROM merchant_tags mt
       JOIN merchants m ON mt.merchant_id = m.id
       JOIN users u ON m.user_id = u.id
       WHERE mt.tagged_user_id = $1 AND mt.status IN ('active', 'exhausted')
       ORDER BY mt.tagged_at DESC
       LIMIT 1`,
      [userId]
    );
    
    if (result.rows.length === 0) {
      return { tagged: false };
    }
    
    return {
      tagged: true,
      tagId: result.rows[0].id,
      merchantId: result.rows[0].merchant_id,
      merchantUserId: result.rows[0].merchant_user_id,
      merchantUsername: result.rows[0].merchant_username
    };
  } catch (error) {
    console.error('Error checking if user is tagged:', error);
    return { tagged: false };
  }
};

module.exports = {
  TAG_AMOUNT,
  MERCHANT_COMMISSION_RATE,
  USER_COMMISSION_RATE,
  COMMISSION_MATURE_HOURS,
  tagUser,
  getTaggedUsers,
  getTaggedBalance,
  consumeForGame,
  trackTaggedUserSpending,
  isUserTagged,
  getTagSpendHistory,
  getPendingCommissions,
  getCommissionHistory,
  processMaturedCommissions,
  removeTag,
  getUserPendingCommissions
};
