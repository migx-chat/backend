const express = require('express');
const router = express.Router();
const { query } = require('../db/db');
const authMiddleware = require('../middleware/auth');

// Get active announcement for login alert
router.get('/active', async (req, res) => {
  try {
    const result = await query(
      `SELECT id, title, content 
       FROM announcements 
       WHERE is_active = true 
       ORDER BY created_at DESC 
       LIMIT 1`,
      []
    );

    if (result.rows.length === 0) {
      return res.json({ announcement: null });
    }

    res.json({
      announcement: result.rows[0]
    });

  } catch (error) {
    console.error('Get active announcement error:', error);
    res.status(500).json({ error: 'Failed to get active announcement' });
  }
});

// Get all announcements
router.get('/', async (req, res) => {
  try {
    const result = await query(
      `SELECT id, title, content, image_url, scheduled_at, created_at, updated_at
       FROM announcements
       WHERE scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP
       ORDER BY created_at DESC`,
      []
    );

    res.json({
      announcements: result.rows,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Get announcements error:', error);
    res.status(500).json({ error: 'Failed to get announcements' });
  }
});

// Get single announcement
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const result = await query(
      `SELECT id, title, content, created_at, updated_at
       FROM announcements
       WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Announcement not found' });
    }

    res.json({
      announcement: result.rows[0]
    });

  } catch (error) {
    console.error('Get announcement error:', error);
    res.status(500).json({ error: 'Failed to get announcement' });
  }
});

// Create announcement (admin only)
router.post('/create', authMiddleware, async (req, res) => {
  try {
    const { title, content, image_url, scheduled_at } = req.body;

    if (!title || !content) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // 🔐 Verify admin from JWT session
    if (!['admin', 'super_admin'].includes(req.user.role)) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }

    const result = await query(
      `INSERT INTO announcements (title, content, image_url, scheduled_at)
       VALUES ($1, $2, $3, $4)
       RETURNING id, title, content, image_url, scheduled_at, created_at`,
      [title, content, image_url, scheduled_at]
    );

    res.json({
      success: true,
      announcement: result.rows[0]
    });

  } catch (error) {
    console.error('Create announcement error:', error);
    res.status(500).json({ error: 'Failed to create announcement' });
  }
});

// Update announcement (admin only)
router.put('/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const { title, content } = req.body;

    if (!title || !content) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // 🔐 Verify admin from JWT session
    if (!['admin', 'super_admin'].includes(req.user.role)) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }

    const result = await query(
      `UPDATE announcements
       SET title = $1, content = $2, updated_at = CURRENT_TIMESTAMP
       WHERE id = $3
       RETURNING id, title, content, created_at, updated_at`,
      [title, content, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Announcement not found' });
    }

    res.json({
      success: true,
      announcement: result.rows[0]
    });

  } catch (error) {
    console.error('Update announcement error:', error);
    res.status(500).json({ error: 'Failed to update announcement' });
  }
});

// Delete announcement (admin only)
router.delete('/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;

    // 🔐 Verify admin from JWT session
    if (!['admin', 'super_admin'].includes(req.user.role)) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }

    await query('DELETE FROM announcements WHERE id = $1', [id]);

    res.json({
      success: true,
      message: 'Announcement deleted'
    });

  } catch (error) {
    console.error('Delete announcement error:', error);
    res.status(500).json({ error: 'Failed to delete announcement' });
  }
});

module.exports = router;
