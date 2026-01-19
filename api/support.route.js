const logger = require('../utils/logger');
const express = require('express');
const router = express.Router();
const authMiddleware = require('../middleware/auth');
const { query } = require('../db/db');

router.post('/report-bug', authMiddleware, async (req, res) => {
  try {
    const { title, description } = req.body;
    const userId = req.user.id;

    if (!title || !description) {
      return res.status(400).json({ success: false, error: 'Title and description are required' });
    }

    // You can add an audit log or a dedicated bug_reports table here
    // For now, let's log it and return success
    logger.info(`[Bug Report] User ID ${userId} reported: ${title} - ${description}`);
    
    // In a real app, you'd insert this into a database table
    // await query('INSERT INTO bug_reports (user_id, title, description) VALUES ($1, $2, $3)', [userId, title, description]);

    res.json({ success: true, message: 'Bug report received' });
  } catch (error) {
    console.error('Bug report error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

module.exports = router;
