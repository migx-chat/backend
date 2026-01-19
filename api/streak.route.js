const express = require('express');
const router = express.Router();
const streakService = require('../services/streakService');
const authMiddleware = require('../middleware/auth');

// Update user streak on login (protected - requires authentication)
router.post('/check', authMiddleware, async (req, res) => {
  try {
    // 🔐 Use authenticated user ID from JWT session
    const userId = req.user.id;

    const result = await streakService.updateStreak(userId);
    res.json(result);
  } catch (error) {
    console.error('Error checking streak:', error);
    res.status(500).json({ error: 'Failed to check streak' });
  }
});

// Get streak info (protected - requires authentication)
router.get('/info', authMiddleware, async (req, res) => {
  try {
    // 🔐 Use authenticated user ID from JWT session
    const userId = req.user.id;
    const info = await streakService.getStreakInfo(userId);
    res.json(info);
  } catch (error) {
    console.error('Error getting streak info:', error);
    res.status(500).json({ error: 'Failed to get streak info' });
  }
});

module.exports = router;
