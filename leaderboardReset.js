
const { query } = require('./db/db');
const logger = require('./utils/logger');

const checkAndResetLeaderboard = async () => {
  try {
    const now = new Date();
    const day = now.getDay(); // 0 = Sunday, 1 = Monday
    const hours = now.getHours();
    
    // Check if it's Monday 00:00-01:00
    if (day === 1 && hours === 0) {
      // 1. TOP LEVEL RESET & REWARD
      const lastLevelReset = await query(
        "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_level' ORDER BY reset_at DESC LIMIT 1"
      );
      
      const levelResetNeeded = !lastLevelReset.rows.length || 
        (new Date() - new Date(lastLevelReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;
        
      if (levelResetNeeded) {
        logger.info('LEADERBOARD_RESET_START', { category: 'top_level' });
        
        // Get top 1 user by level
        const topUser = await query(
          `SELECT u.id FROM users u 
           LEFT JOIN user_levels ul ON u.id = ul.user_id 
           WHERE u.is_active = true 
           ORDER BY ul.level DESC, ul.xp DESC LIMIT 1`
        );
        
        if (topUser.rows.length) {
          const userId = topUser.rows[0].id;
          const expiry = new Date();
          expiry.setDate(expiry.getDate() + 3); // 3 day expiry
          
          await query(
            "UPDATE users SET username_color = '#FF69B4', username_color_expiry = $1 WHERE id = $2",
            [expiry, userId]
          );
          logger.info('TOP1_LEVEL_REWARD_GRANTED', { userId, expiry });
        }
        
        await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_level')");
        logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_level' });
      }

      // 2. TOP GIFT SENDER RESET & REWARD
      const lastGiftReset = await query(
        "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_gift_sender' ORDER BY reset_at DESC LIMIT 1"
      );

      const giftResetNeeded = !lastGiftReset.rows.length || 
        (new Date() - new Date(lastGiftReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;

      if (giftResetNeeded) {
        logger.info('LEADERBOARD_RESET_START', { category: 'top_gift_sender' });

        // Get Top 1 Gift Sender
        const topGiftUser = await query(
          `SELECT u.id, COUNT(ug.id) as total_gifts
           FROM users u
           LEFT JOIN user_gifts ug ON u.id = ug.sender_id
           WHERE u.is_active = true
           GROUP BY u.id
           HAVING COUNT(ug.id) > 0
           ORDER BY total_gifts DESC LIMIT 1`
        );

        if (topGiftUser.rows.length) {
          const userId = topGiftUser.rows[0].id;
          const expiry = new Date();
          expiry.setDate(expiry.getDate() + 3);

          await query(
            "UPDATE users SET username_color = '#FF69B4', username_color_expiry = $1 WHERE id = $2",
            [expiry, userId]
          );
          logger.info('TOP1_GIFT_REWARD_GRANTED', { userId, expiry });
        }

        // Reset gift records for the new week (move to history or clear)
        // For simplicity in this logic, we just log the reset. 
        // The API should ideally filter by week if we wanted historical data, 
        // but user asked for "reset list", so we clear the current week's activity.
        await query("DELETE FROM user_gifts WHERE created_at < NOW()"); 

        await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_gift_sender')");
        logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_gift_sender' });
      }

      // 3. TOP GIFT RECEIVER RESET & REWARD
      const lastReceiverReset = await query(
        "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_gift_receiver' ORDER BY reset_at DESC LIMIT 1"
      );

      const receiverResetNeeded = !lastReceiverReset.rows.length || 
        (new Date() - new Date(lastReceiverReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;

      if (receiverResetNeeded) {
        logger.info('LEADERBOARD_RESET_START', { category: 'top_gift_receiver' });

        // Get Top 1 Gift Receiver
        const topReceiverUser = await query(
          `SELECT u.id, COUNT(ug.id) as total_gifts
           FROM users u
           LEFT JOIN user_gifts ug ON u.id = ug.receiver_id
           WHERE u.is_active = true
           GROUP BY u.id
           HAVING COUNT(ug.id) > 0
           ORDER BY total_gifts DESC LIMIT 1`
        );

        if (topReceiverUser.rows.length) {
          const userId = topReceiverUser.rows[0].id;
          const expiry = new Date();
          expiry.setDate(expiry.getDate() + 3);

          await query(
            "UPDATE users SET username_color = '#FF69B4', username_color_expiry = $1 WHERE id = $2",
            [expiry, userId]
          );
          logger.info('TOP1_RECEIVER_REWARD_GRANTED', { userId, expiry });
        }

        // Note: user_gifts is cleared in the sender reset above, 
        // which covers both sender and receiver lists for the new week.

        await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_gift_receiver')");
        logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_gift_receiver' });
      }

      // 4. TOP FOOTPRINT RESET
      const lastFootprintReset = await query(
        "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_footprint' ORDER BY reset_at DESC LIMIT 1"
      );

      const footprintResetNeeded = !lastFootprintReset.rows.length || 
        (new Date() - new Date(lastFootprintReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;

      if (footprintResetNeeded) {
        logger.info('LEADERBOARD_RESET_START', { category: 'top_footprint' });
        
        // Get Top 1 Footprint
        const topFootprintUser = await query(
          `SELECT u.id, COUNT(pf.id) as total_footprints
           FROM users u
           LEFT JOIN profile_footprints pf ON u.id = pf.profile_id
           WHERE u.is_active = true
           GROUP BY u.id
           HAVING COUNT(pf.id) > 0
           ORDER BY total_footprints DESC LIMIT 1`
        );

        if (topFootprintUser.rows.length) {
          const userId = topFootprintUser.rows[0].id;
          const expiry = new Date();
          expiry.setDate(expiry.getDate() + 3);

          await query(
            "UPDATE users SET username_color = '#FF69B4', username_color_expiry = $1 WHERE id = $2",
            [expiry, userId]
          );
          logger.info('TOP1_FOOTPRINT_REWARD_GRANTED', { userId, expiry });
        }

        // Clear footprints for the new week
        await query("DELETE FROM profile_footprints WHERE viewed_at < NOW()");

        await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_footprint')");
        logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_footprint' });
      }

      // 5. TOP MERCHANT MONTHLY RESET & REWARD
      // Run on the 1st of every month
      if (now.getDate() === 1 && hours === 0) {
        const lastMerchantReset = await query(
          "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_merchant_monthly' ORDER BY reset_at DESC LIMIT 1"
        );
        
        const merchantResetNeeded = !lastMerchantReset.rows.length || 
          (new Date() - new Date(lastMerchantReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;

        if (merchantResetNeeded) {
          logger.info('LEADERBOARD_RESET_START', { category: 'top_merchant_monthly' });
          
          const topMerchant = await query(
            `SELECT u.id FROM users u
             LEFT JOIN merchant_leaderboard ml ON u.id = ml.user_id
             WHERE u.role = 'merchant' AND ml.month_year = TO_CHAR(NOW() - INTERVAL '1 month', 'YYYY-MM')
             ORDER BY ml.total_spent DESC LIMIT 1`
          );

          if (topMerchant.rows.length) {
            const userId = topMerchant.rows[0].id;
            const expiry = new Date();
            expiry.setDate(expiry.getDate() + 15);

            await query(
              "UPDATE users SET has_top_merchant_badge = true, top_merchant_badge_expiry = $1 WHERE id = $2",
              [expiry, userId]
            );
            logger.info('TOP1_MERCHANT_REWARD_GRANTED', { userId, expiry });
          }

          await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_merchant_monthly')");
          logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_merchant_monthly' });
        }
      }
      
      // 6. TOP LIKE WEEKLY RESET & REWARD
      // Run every Monday at 00:00
      if (day === 1 && hours === 0) {
        const lastLikeReset = await query(
          "SELECT reset_at FROM leaderboard_reset_log WHERE category = 'top_like_weekly' ORDER BY reset_at DESC LIMIT 1"
        );
        
        const likeResetNeeded = !lastLikeReset.rows.length || 
          (new Date() - new Date(lastLikeReset.rows[0].reset_at)) > 24 * 60 * 60 * 1000;

        if (likeResetNeeded) {
          logger.info('LEADERBOARD_RESET_START', { category: 'top_like_weekly' });
          
          // Find top users from last week
          const lastWeek = new Date();
          lastWeek.setDate(lastWeek.getDate() - 7);
          const lastWeekNum = Math.ceil(lastWeek.getDate() / 7);
          const lastWeekYear = `${lastWeek.getFullYear()}-W${lastWeekNum}`;

          const topLikers = await query(
            `SELECT user_id FROM user_likes_leaderboard 
             WHERE week_year = $1
             ORDER BY likes_count DESC LIMIT 5`,
            [lastWeekYear]
          );

          if (topLikers.rows.length) {
            const userIds = topLikers.rows.map(r => r.user_id);
            const expiry = new Date();
            expiry.setDate(expiry.getDate() + 3);

            await query(
              "UPDATE users SET has_top_like_reward = true, top_like_reward_expiry = $1 WHERE id = ANY($2)",
              [expiry, userIds]
            );
            logger.info('TOP_LIKE_REWARD_GRANTED', { userIds, expiry });
          }

          await query("INSERT INTO leaderboard_reset_log (category) VALUES ('top_like_weekly')");
          logger.info('LEADERBOARD_RESET_COMPLETE', { category: 'top_like_weekly' });
        }
      }
    }
  } catch (error) {
    logger.error('LEADERBOARD_RESET_ERROR', error);
  }
};

// Run every hour
setInterval(checkAndResetLeaderboard, 60 * 60 * 1000);

module.exports = { checkAndResetLeaderboard };
