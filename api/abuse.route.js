const express = require('express');
const router = express.Router();
const db = require('../db/db');

// POST /api/abuse/report - Submit abuse report (public endpoint)
router.post('/report', async (req, res) => {
  try {
    const { reporter, target, roomId, reason, messageText, timestamp } = req.body;

    // Validate required fields
    if (!target || !roomId || !reason) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Valid reasons
    const validReasons = ['spam', 'harassment', 'porn', 'scam'];
    if (!validReasons.includes(reason)) {
      return res.status(400).json({ error: 'Invalid reason' });
    }

    // Insert into abuse_reports table
    const result = await db.query(
      `INSERT INTO abuse_reports (reporter, target, room_id, room_name, message_text, reason, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending')
       RETURNING id, created_at`,
      [reporter || 'anonymous', target, roomId, null, messageText || null, reason]
    );

    res.status(201).json({
      success: true,
      reportId: result.rows[0].id,
      createdAt: result.rows[0].created_at,
      message: 'Report submitted successfully'
    });
  } catch (error) {
    console.error('Error submitting abuse report:', error);
    res.status(500).json({ error: 'Failed to submit report' });
  }
});

// GET /api/abuse/reports - Get all abuse reports (admin only)
router.get('/reports', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT * FROM abuse_reports ORDER BY created_at DESC`
    );

    res.json({
      success: true,
      reports: result.rows
    });
  } catch (error) {
    console.error('Error fetching abuse reports:', error);
    res.status(500).json({ error: 'Failed to fetch reports' });
  }
});

// PATCH /api/abuse/reports/:id - Update report status (admin only)
router.patch('/reports/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    if (!status || !['pending', 'reviewed', 'actioned'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const result = await db.query(
      `UPDATE abuse_reports SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *`,
      [status, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Report not found' });
    }

    res.json({
      success: true,
      report: result.rows[0]
    });
  } catch (error) {
    console.error('Error updating abuse report:', error);
    res.status(500).json({ error: 'Failed to update report' });
  }
});

module.exports = router;
