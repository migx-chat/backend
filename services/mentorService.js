const logger = require('../utils/logger');
const { query } = require('../db/db');

/**
 * Service to handle mentor-merchant management and auto-demotion
 */
const mentorService = {
  /**
   * Check all merchants and demote those who haven't received 1,600.000 COINS 
   * from their mentor in the last 30 days or whose expiry has passed.
   */
  async checkMerchantStatus() {
    try {
      logger.info('Running merchant status check...');
      
      // 1. Find all active merchants
      const merchants = await query(
        "SELECT id, username, mentor_id, merchant_expired_at FROM users WHERE role = 'merchant' AND mentor_id IS NOT NULL"
      );

      for (const merchant of merchants.rows) {
        const now = new Date();
        const expiry = new Date(merchant.merchant_expired_at);

        // Check 1: Expiry date
        if (now > expiry) {
          await this.demoteMerchant(merchant.id, 'Subscription expired');
          continue;
        }

        // Check 2: Monthly transfer from mentor (1,000,000 IDR)
        // Look at mentor_payments table for the last 30 days
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

        const paymentRes = await query(
          `SELECT SUM(amount) as total 
           FROM mentor_payments 
           WHERE merchant_id = $1 AND mentor_id = $2 AND payment_date > $3`,
          [merchant.id, merchant.mentor_id, thirtyDaysAgo]
        );

        const totalPaid = parseInt(paymentRes.rows[0].total || '0');
        if (totalPaid < 1600) {
          await this.demoteMerchant(merchant.id, `Insufficient monthly payment (${totalPaid}/1,600)`);
        }
      }
    } catch (error) {
      console.error('Error in checkMerchantStatus:', error);
    }
  },

  async demoteMerchant(userId, reason) {
    logger.info(`Demoting user ${userId} to regular user. Reason: ${reason}`);
    await query(
      "UPDATE users SET role = 'user', mentor_id = NULL, merchant_expired_at = NULL WHERE id = $1",
      [userId]
    );
    
    // Log demotion in audit_logs if available
    await query(
      "INSERT INTO audit_logs (request_id, from_user_id, from_username, to_user_id, to_username, amount, status, error_reason) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
      ['DEMOTE_' + Date.now(), 0, 'SYSTEM', userId, 'merchant', 0, 'completed', reason]
    ).catch(err => logger.info('Audit log failed (expected if table differs):', err.message));
  }
};

// Run check every hour
setInterval(() => mentorService.checkMerchantStatus(), 3600000);

module.exports = mentorService;