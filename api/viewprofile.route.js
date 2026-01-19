
const express = require('express');
const router = express.Router();
const { query } = require('../db/db');
const profileService = require('../services/profileService');
const logger = require('../utils/logger');

// Get user profile by ID
router.get('/:userId', async (req, res) => {
  const { userId } = req.params;
  const { viewerId } = req.query; // User yang melihat profile
  
  try {
    // Get user basic info
    const userResult = await query(
      `SELECT u.id, u.username, u.avatar, u.role, u.status, u.status_message, 
              u.gender, u.country, u.created_at, u.username_color, ul.level, ul.xp,
              u.has_top_like_reward, u.top_like_reward_expiry
       FROM users u
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.id = $1`,
      [userId]
    );
    
    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    const user = userResult.rows[0];
    
    // Check badge expiry
    const hasBadge = user.has_top_merchant_badge && user.top_merchant_badge_expiry > new Date();
    const hasLikeReward = user.has_top_like_reward && user.top_like_reward_expiry > new Date();
    
    let usernameColor = user.username_color;
    if (hasLikeReward && user.role !== 'merchant') {
      usernameColor = '#FF69B4'; // Pink
    }
    
    // Get stats
    const [postCount, giftCount, followersCount, followingCount, footprintCount] = await Promise.all([
      profileService.getPostCount(userId),
      profileService.getGiftCount(userId),
      profileService.getFollowersCount(userId),
      profileService.getFollowingCount(userId),
      profileService.getFootprintCount(userId)
    ]);
    
    // Check if viewer is following this user
    let isFollowing = false;
    if (viewerId) {
      isFollowing = await profileService.isFollowing(viewerId, userId);
      
      // 👣 Track footprint (Profile View)
      if (viewerId !== userId) {
        try {
          await query(
            `INSERT INTO profile_footprints (profile_id, viewer_id) 
             VALUES ($1, $2) 
             ON CONFLICT (profile_id, viewer_id) 
             DO UPDATE SET viewed_at = CURRENT_TIMESTAMP`,
            [userId, viewerId]
          );
        } catch (err) {
          logger.error('FOOTPRINT_TRACKING_ERROR', err);
        }
      }
    }
    
    res.json({
      user: {
        id: user.id,
        username: user.username,
        usernameColor: usernameColor,
        avatar: user.avatar,
        role: user.role,
        status: user.status,
        statusMessage: user.status_message,
        gender: user.gender,
        country: user.country,
        level: user.level || 1,
        xp: user.xp || 0,
        created_at: user.created_at,
        hasTopMerchantBadge: hasBadge,
        hasTopLikeReward: hasLikeReward,
        topLikeRewardExpiry: user.top_like_reward_expiry
      },
      stats: {
        postCount,
        giftCount,
        followersCount,
        followingCount,
        footprintCount
      },
      isFollowing
    });
    
  } catch (error) {
    logger.error('VIEWPROFILE_ERROR: Failed to get user profile', error, { userId });
    res.status(500).json({ error: 'Failed to get user profile' });
  }
});

module.exports = router;
