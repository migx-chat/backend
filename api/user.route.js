const express = require('express');
const router = express.Router();
const userService = require('../services/userService');
const { getUserLevel, getLeaderboard } = require('../utils/xpLeveling');
const logger = require('../utils/logger');
const { query, getPool } = require('../db/db');

// Search users (must come BEFORE /:id route)
router.get('/search', async (req, res) => {
  try {
    const { q, limit = 20 } = req.query;

    if (!q || q.length < 1) {
      return res.json([]);
    }

    const users = await userService.searchUsers(q, parseInt(limit));
    res.json(users);

  } catch (error) {
    logger.error('SEARCH_USERS_ERROR: Failed to search users', error, {});
    res.json([]);
  }
});

// Online users (must come BEFORE /:id route)
router.get('/online', async (req, res) => {
  try {
    const { limit = 50 } = req.query;
    const users = await userService.getOnlineUsers(parseInt(limit));

    res.json({
      users,
      count: users.length
    });

  } catch (error) {
    logger.error('GET_ONLINE_USERS_ERROR: Failed to get online users', error, {});
    res.status(500).json({ error: 'Failed to get online users' });
  }
});

// Leaderboard (must come BEFORE /:id route)
router.get('/leaderboard', async (req, res) => {
  try {
    const { limit = 10 } = req.query;
    const leaderboard = await getLeaderboard(parseInt(limit));

    res.json({
      leaderboard
    });

  } catch (error) {
    logger.error('GET_LEADERBOARD_ERROR: Failed to get leaderboard', error, {});
    res.status(500).json({ error: 'Failed to get leaderboard' });
  }
});

// Get user by username
router.get('/username/:username', async (req, res) => {
  try {
    const { username } = req.params;
    const user = await userService.getUserByUsername(username);

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    const levelData = await getUserLevel(user.id);

    res.json({
      id: user.id,
      username: user.username,
      avatar: user.avatar,
      role: user.role,
      status: user.status,
      level: levelData.level,
      xp: levelData.xp,
      createdAt: user.created_at
    });

  } catch (error) {
    console.error('Get user by username error:', error);
    res.status(500).json({ error: 'Failed to get user' });
  }
});

// Get user by ID (must come LAST)
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const user = await userService.getUserById(id);

    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    const levelData = await getUserLevel(id);

    res.json({
      id: user.id,
      username: user.username,
      avatar: user.avatar,
      role: user.role,
      status: user.status,
      credits: user.credits,
      level: levelData.level,
      xp: levelData.xp,
      progress: levelData.progress,
      nextLevelXp: levelData.nextLevelXp,
      createdAt: user.created_at
    });

  } catch (error) {
    console.error('Get user error:', error);
    res.status(500).json({ error: 'Failed to get user' });
  }
});

router.put('/:id/role', async (req, res) => {
  try {
    const { id } = req.params;
    const { role, adminId } = req.body;

    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }

    const isAdmin = await userService.isAdmin(adminId);
    if (!isAdmin) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }

    const result = await userService.updateUserRole(id, role);

    if (!result || result.error) {
      return res.status(400).json({ error: result?.error || 'Failed to update role' });
    }

    res.json({
      success: true,
      user: result
    });

  } catch (error) {
    console.error('Update role error:', error);
    res.status(500).json({ error: 'Failed to update role' });
  }
});

router.put('/:id/status-message', async (req, res) => {
  try {
    const { id } = req.params;
    const { statusMessage } = req.body;

    if (statusMessage && statusMessage.length > 100) {
      return res.status(400).json({ error: 'Status message too long (max 100 characters)' });
    }

    const result = await userService.updateStatusMessage(id, statusMessage || '');

    if (!result) {
      return res.status(400).json({ error: 'Failed to update status message' });
    }

    res.json({
      success: true,
      user: result
    });

  } catch (error) {
    console.error('Update status message error:', error);
    res.status(500).json({ error: 'Failed to update status message' });
  }
});

// Update user status message
router.put('/:userId/status-message', async (req, res) => {
  try {
    const { userId } = req.params;
    const { statusMessage } = req.body;

    const pool = getPool();
    const result = await pool.query(
      'UPDATE users SET status_message = $1 WHERE id = $2 RETURNING *',
      [statusMessage, userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json({ success: true, user: result.rows[0] });
  } catch (error) {
    console.error('Error updating status message:', error);
    res.status(500).json({ error: 'Failed to update status message' });
  }
});

// Update user status (alternative endpoint)
router.put('/:userId/status', async (req, res) => {
  try {
    const { userId } = req.params;
    const { status_message } = req.body;

    const pool = getPool();
    const result = await pool.query(
      'UPDATE users SET status_message = $1 WHERE id = $2 RETURNING *',
      [status_message, userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.json({ success: true, user: result.rows[0] });
  } catch (error) {
    console.error('Error updating status:', error);
    res.status(500).json({ error: 'Failed to update status' });
  }
});

// Notifications routes
router.get('/:username/notifications', async (req, res) => {
  try {
    const { username } = req.params;
    const user = await userService.getUserByUsername(username);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const notifications = await query(
      `SELECT id, type, message, data, is_read, 
              EXTRACT(EPOCH FROM created_at) * 1000 as timestamp
       FROM notifications 
       WHERE user_id = $1 
       ORDER BY created_at DESC 
       LIMIT 50`,
      [user.id]
    );

    res.json({ notifications: notifications.rows });
  } catch (error) {
    logger.error('GET_NOTIFICATIONS_ERROR', error);
    res.status(500).json({ error: 'Failed to get notifications' });
  }
});

router.delete('/:username/notifications', async (req, res) => {
  try {
    const { username } = req.params;
    const user = await userService.getUserByUsername(username);
    if (!user) return res.status(404).json({ error: 'User not found' });

    await query('DELETE FROM notifications WHERE user_id = $1', [user.id]);
    res.json({ success: true });
  } catch (error) {
    logger.error('DELETE_NOTIFICATIONS_ERROR', error);
    res.status(500).json({ error: 'Failed to clear notifications' });
  }
});

module.exports = router;
