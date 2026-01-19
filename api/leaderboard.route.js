
const express = require('express');
const router = express.Router();
const { query } = require('../db/db');

// Get top level users
router.get('/top-level', async (req, res) => {
  try {
    const { limit = 5 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country, u.username_color,
              COALESCE(ul.level, 1) as level, COALESCE(ul.xp, 0) as xp
       FROM users u
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       ORDER BY level DESC, xp DESC
       LIMIT $1`,
      [Math.min(parseInt(limit), 5)]
    );

    res.json({
      category: 'top_level',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top level error:', error);
    res.status(500).json({ error: 'Failed to get top level users' });
  }
});

// Get top gift senders
router.get('/top-gift-sender', async (req, res) => {
  try {
    const { limit = 5 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COUNT(ug.id) as total_gifts_sent,
              COALESCE(SUM(ug.gift_cost), 0) as total_cost
       FROM users u
       LEFT JOIN user_gifts ug ON u.id = ug.sender_id
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level
       HAVING COUNT(ug.id) > 0
       ORDER BY total_gifts_sent DESC, total_cost DESC
       LIMIT $1`,
      [parseInt(limit)]
    );

    res.json({
      category: 'top_gift_sender',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top gift sender error:', error);
    res.status(500).json({ error: 'Failed to get top gift senders' });
  }
});

// Get top gift receivers
router.get('/top-gift-receiver', async (req, res) => {
  try {
    const { limit = 5 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COUNT(ug.id) as total_gifts_received,
              COALESCE(SUM(ug.gift_cost), 0) as total_value
       FROM users u
       LEFT JOIN user_gifts ug ON u.id = ug.receiver_id
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level
       HAVING COUNT(ug.id) > 0
       ORDER BY total_gifts_received DESC, total_value DESC
       LIMIT $1`,
      [parseInt(limit)]
    );

    res.json({
      category: 'top_gift_receiver',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top gift receiver error:', error);
    res.status(500).json({ error: 'Failed to get top gift receivers' });
  }
});

// Get top footprint (most profile views)
router.get('/top-footprint', async (req, res) => {
  try {
    const { limit = 5 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COUNT(pf.id) as total_footprints
       FROM users u
       LEFT JOIN profile_footprints pf ON u.id = pf.profile_id
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level
       HAVING COUNT(pf.id) > 0
       ORDER BY total_footprints DESC
       LIMIT $1`,
      [parseInt(limit)]
    );

    res.json({
      category: 'top_footprint',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top footprint error:', error);
    res.status(500).json({ error: 'Failed to get top footprint users' });
  }
});

// Get top gamers (weekly)
router.get('/top-gamer', async (req, res) => {
  try {
    const { limit = 10 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COUNT(gh.id) as total_games,
              SUM(CASE WHEN gh.result = 'win' THEN 1 ELSE 0 END) as wins,
              SUM(gh.bet_amount) as total_bet
       FROM users u
       LEFT JOIN game_history gh ON u.id = gh.user_id
         AND gh.created_at >= NOW() - INTERVAL '7 days'
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level
       HAVING COUNT(gh.id) > 0
       ORDER BY total_games DESC, wins DESC
       LIMIT $1`,
      [parseInt(limit)]
    );

    res.json({
      category: 'top_gamer',
      period: 'weekly',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top gamer error:', error);
    res.status(500).json({ error: 'Failed to get top gamers' });
  }
});

// Get top game winners (weekly)
router.get('/top-get', async (req, res) => {
  try {
    const { limit = 10 } = req.query;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              SUM(gh.reward_amount) as total_winnings,
              SUM(CASE WHEN gh.result = 'win' THEN 1 ELSE 0 END) as wins,
              COUNT(gh.id) as total_games
       FROM users u
       LEFT JOIN game_history gh ON u.id = gh.user_id
         AND gh.created_at >= NOW() - INTERVAL '7 days'
         AND gh.result = 'win'
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level
       HAVING SUM(gh.reward_amount) > 0
       ORDER BY total_winnings DESC, wins DESC
       LIMIT $1`,
      [parseInt(limit)]
    );

    res.json({
      category: 'top_get',
      period: 'weekly',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top game winners error:', error);
    res.status(500).json({ error: 'Failed to get top game winners' });
  }
});

// Get top merchants (monthly)
router.get('/top-merchant', async (req, res) => {
  try {
    const { limit = 5 } = req.query;
    const currentMonth = new Date().toISOString().slice(0, 7);

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COALESCE(ml.total_spent, 0) as total_spent
       FROM users u
       LEFT JOIN merchant_leaderboard ml ON u.id = ml.user_id AND ml.month_year = $1
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true AND u.role = 'merchant'
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, ml.total_spent
       HAVING COALESCE(ml.total_spent, 0) > 0
       ORDER BY total_spent DESC
       LIMIT $2`,
      [currentMonth, Math.min(parseInt(limit), 5)]
    );

    res.json({
      category: 'top_merchant_monthly',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top merchant error:', error);
    res.status(500).json({ error: 'Failed to get top merchant users' });
  }
});

// Get top likes (weekly)
router.get('/top-likes', async (req, res) => {
  try {
    const { limit = 5 } = req.query;
    const now = new Date();
    const weekNum = Math.ceil(now.getDate() / 7);
    const weekYear = `${now.getFullYear()}-W${weekNum}`;

    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
              ul.level,
              COALESCE(ull.likes_count, 0) as likes_count
       FROM users u
       LEFT JOIN user_likes_leaderboard ull ON u.id = ull.user_id AND ull.week_year = $1
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.is_active = true
       GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, ull.likes_count
       HAVING COALESCE(ull.likes_count, 0) > 0
       ORDER BY likes_count DESC
       LIMIT $2`,
      [weekYear, Math.min(parseInt(limit), 5)]
    );

    res.json({
      category: 'top_likes_weekly',
      users: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get top likes error:', error);
    res.status(500).json({ error: 'Failed to get top liked users' });
  }
});

// Get all leaderboards at once
router.get('/all', async (req, res) => {
  try {
    const { limit = 10 } = req.query;
    const limitInt = parseInt(limit);
    const currentMonth = new Date().toISOString().slice(0, 7);
    const now = new Date();
    const weekNum = Math.ceil(now.getDate() / 7);
    const weekYear = `${now.getFullYear()}-W${weekNum}`;

    const [topLevel, topGiftSender, topGiftReceiver, topFootprint, topGamer, topGet, topMerchant, topLikes] = await Promise.all([
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country, u.username_color,
                COALESCE(ul.level, 1) as level, COALESCE(ul.xp, 0) as xp,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         ORDER BY level DESC, xp DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COUNT(ug.id) as total_gifts_sent,
                COALESCE(SUM(ug.gift_cost), 0) as total_cost,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN user_gifts ug ON u.id = ug.sender_id
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COUNT(ug.id) > 0
         ORDER BY total_gifts_sent DESC, total_cost DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COUNT(ug.id) as total_gifts_received,
                COALESCE(SUM(ug.gift_cost), 0) as total_value,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN user_gifts ug ON u.id = ug.receiver_id
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COUNT(ug.id) > 0
         ORDER BY total_gifts_received DESC, total_value DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COUNT(pf.id) as total_footprints,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN profile_footprints pf ON u.id = pf.profile_id
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COUNT(pf.id) > 0
         ORDER BY total_footprints DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COUNT(gh.id) as total_games,
                SUM(CASE WHEN gh.result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(gh.bet_amount) as total_bet,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN game_history gh ON u.id = gh.user_id
           AND gh.created_at >= NOW() - INTERVAL '7 days'
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COUNT(gh.id) > 0
         ORDER BY total_games DESC, wins DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                SUM(gh.reward_amount) as total_winnings,
                SUM(CASE WHEN gh.result = 'win' THEN 1 ELSE 0 END) as wins,
                COUNT(gh.id) as total_games,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN game_history gh ON u.id = gh.user_id
           AND gh.created_at >= NOW() - INTERVAL '7 days'
           AND gh.result = 'win'
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING SUM(gh.reward_amount) > 0
         ORDER BY total_winnings DESC, wins DESC
         LIMIT 5`,
        []
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COALESCE(ml.total_spent, 0) as total_spent,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN merchant_leaderboard ml ON u.id = ml.user_id AND ml.month_year = $1
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true AND u.role = 'merchant'
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, ml.total_spent, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COALESCE(ml.total_spent, 0) > 0
         ORDER BY total_spent DESC
         LIMIT 5`,
        [currentMonth]
      ),
      query(
        `SELECT u.id, u.username, u.avatar, u.gender, u.role, u.country,
                ul.level,
                COALESCE(ull.likes_count, 0) as likes_count,
                u.has_top_like_reward, u.top_like_reward_expiry
         FROM users u
         LEFT JOIN user_likes_leaderboard ull ON u.id = ull.user_id AND ull.week_year = $1
         LEFT JOIN user_levels ul ON u.id = ul.user_id
         WHERE u.is_active = true
         GROUP BY u.id, u.username, u.avatar, u.gender, u.role, u.country, ul.level, ull.likes_count, u.has_top_like_reward, u.top_like_reward_expiry
         HAVING COALESCE(ull.likes_count, 0) > 0
         ORDER BY likes_count DESC
         LIMIT 5`,
        [weekYear]
      )
    ]);

    const formatUser = (u, i, category) => {
      let color = u.username_color;
      
      // Top 1 in ALL categories get pink color
      if (i === 0) {
        color = '#FF69B4'; // Pink for all top 1
      } else {
        // Pink Reward Logic from likes leaderboard for non-top-1 users
        const hasActiveLikeReward = u.has_top_like_reward && new Date(u.top_like_reward_expiry) > new Date();
        if (hasActiveLikeReward && u.role !== 'merchant') {
          color = '#FF69B4'; // Pink
        }
      }

      return { ...u, username_color: color, is_top_1: i === 0 };
    };

    res.json({
      top_level: topLevel.rows.map((u, i) => formatUser(u, i, 'top_level')),
      top_gift_sender: topGiftSender.rows.map((u, i) => formatUser(u, i, 'top_gift_sender')),
      top_gift_receiver: topGiftReceiver.rows.map((u, i) => formatUser(u, i, 'top_gift_receiver')),
      top_footprint: topFootprint.rows.map((u, i) => formatUser(u, i, 'top_footprint')),
      top_gamer: topGamer.rows.map((u, i) => formatUser(u, i, 'top_gamer')),
      top_get: topGet.rows.map((u, i) => formatUser(u, i, 'top_get')),
      top_merchant: topMerchant.rows.map((u, i) => formatUser(u, i, 'top_merchant')),
      top_likes: topLikes.rows.map((u, i) => formatUser(u, i, 'top_likes'))
    });

  } catch (error) {
    console.error('Get all leaderboards error:', error);
    res.status(500).json({ error: 'Failed to get leaderboards' });
  }
});

// Get Top 1 user IDs for all categories (for chat room username colors)
router.get('/top1-users', async (req, res) => {
  try {
    const currentMonth = new Date().toISOString().slice(0, 7);
    const now = new Date();
    const weekNum = Math.ceil(now.getDate() / 7);
    const weekYear = `${now.getFullYear()}-W${weekNum}`;

    const [topLevel, topGiftSender, topGiftReceiver, topFootprint, topGamer, topGet, topMerchant, topLikes] = await Promise.all([
      query(`SELECT u.id FROM users u LEFT JOIN user_levels ul ON u.id = ul.user_id WHERE u.is_active = true ORDER BY COALESCE(ul.level, 1) DESC, COALESCE(ul.xp, 0) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN user_gifts ug ON u.id = ug.sender_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(ug.id) > 0 ORDER BY COUNT(ug.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN user_gifts ug ON u.id = ug.receiver_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(ug.id) > 0 ORDER BY COUNT(ug.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN profile_footprints pf ON u.id = pf.profile_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(pf.id) > 0 ORDER BY COUNT(pf.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN game_history gh ON u.id = gh.user_id AND gh.created_at >= NOW() - INTERVAL '7 days' WHERE u.is_active = true GROUP BY u.id HAVING COUNT(gh.id) > 0 ORDER BY COUNT(gh.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN game_history gh ON u.id = gh.user_id AND gh.created_at >= NOW() - INTERVAL '7 days' AND gh.result = 'win' WHERE u.is_active = true GROUP BY u.id HAVING SUM(gh.reward_amount) > 0 ORDER BY SUM(gh.reward_amount) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN merchant_leaderboard ml ON u.id = ml.user_id AND ml.month_year = $1 WHERE u.is_active = true AND u.role = 'merchant' GROUP BY u.id, ml.total_spent HAVING COALESCE(ml.total_spent, 0) > 0 ORDER BY ml.total_spent DESC LIMIT 1`, [currentMonth]),
      query(`SELECT u.id FROM users u LEFT JOIN user_likes_leaderboard ull ON u.id = ull.user_id AND ull.week_year = $1 WHERE u.is_active = true GROUP BY u.id, ull.likes_count HAVING COALESCE(ull.likes_count, 0) > 0 ORDER BY ull.likes_count DESC LIMIT 1`, [weekYear])
    ]);

    // Collect all top 1 user IDs
    const top1UserIds = new Set();
    const top1Categories = {};
    
    if (topLevel.rows[0]) { top1UserIds.add(topLevel.rows[0].id); top1Categories[topLevel.rows[0].id] = [...(top1Categories[topLevel.rows[0].id] || []), 'top_level']; }
    if (topGiftSender.rows[0]) { top1UserIds.add(topGiftSender.rows[0].id); top1Categories[topGiftSender.rows[0].id] = [...(top1Categories[topGiftSender.rows[0].id] || []), 'top_gift_sender']; }
    if (topGiftReceiver.rows[0]) { top1UserIds.add(topGiftReceiver.rows[0].id); top1Categories[topGiftReceiver.rows[0].id] = [...(top1Categories[topGiftReceiver.rows[0].id] || []), 'top_gift_receiver']; }
    if (topFootprint.rows[0]) { top1UserIds.add(topFootprint.rows[0].id); top1Categories[topFootprint.rows[0].id] = [...(top1Categories[topFootprint.rows[0].id] || []), 'top_footprint']; }
    if (topGamer.rows[0]) { top1UserIds.add(topGamer.rows[0].id); top1Categories[topGamer.rows[0].id] = [...(top1Categories[topGamer.rows[0].id] || []), 'top_gamer']; }
    if (topGet.rows[0]) { top1UserIds.add(topGet.rows[0].id); top1Categories[topGet.rows[0].id] = [...(top1Categories[topGet.rows[0].id] || []), 'top_get']; }
    if (topMerchant.rows[0]) { top1UserIds.add(topMerchant.rows[0].id); top1Categories[topMerchant.rows[0].id] = [...(top1Categories[topMerchant.rows[0].id] || []), 'top_merchant']; }
    if (topLikes.rows[0]) { top1UserIds.add(topLikes.rows[0].id); top1Categories[topLikes.rows[0].id] = [...(top1Categories[topLikes.rows[0].id] || []), 'top_likes']; }

    res.json({
      top1_user_ids: Array.from(top1UserIds),
      top1_categories: top1Categories,
      top_merchant_id: topMerchant.rows[0]?.id || null
    });

  } catch (error) {
    console.error('Get top1 users error:', error);
    res.status(500).json({ error: 'Failed to get top1 users' });
  }
});

module.exports = router;
