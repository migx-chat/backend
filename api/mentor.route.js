const express = require('express');
const router = express.Router();
const { query } = require('../db/db');
const auth = require('../middleware/auth');

// Get mentor's merchants
router.get('/merchants', auth, async (req, res) => {
  try {
    if (req.user.role !== 'mentor') {
      return res.status(403).json({ success: false, error: 'Access denied' });
    }

    const result = await query(
      'SELECT id, username, merchant_expired_at FROM users WHERE mentor_id = $1 AND role = \'merchant\'',
      [req.user.id]
    );

    res.json({ success: true, merchants: result.rows });
  } catch (error) {
    console.error('Error fetching merchants:', error);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

// Add a merchant
router.post('/add-merchant', auth, async (req, res) => {
  try {
    const { username } = req.body;
    if (req.user.role !== 'mentor') {
      return res.status(403).json({ success: false, error: 'Access denied' });
    }

    // Find user by username
    const userRes = await query('SELECT id, role FROM users WHERE username = $1', [username]);
    if (userRes.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'User not found' });
    }

    const targetUser = userRes.rows[0];
    const expiryDate = new Date();
    expiryDate.setMonth(expiryDate.getMonth() + 1);

    await query(
      'UPDATE users SET role = \'merchant\', mentor_id = $1, merchant_expired_at = $2 WHERE id = $3',
      [req.user.id, expiryDate, targetUser.id]
    );

    res.json({ success: true });
  } catch (error) {
    console.error('Error adding merchant:', error);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

// Delete a merchant (revert to user role)
router.delete('/merchant/:id', auth, async (req, res) => {
  try {
    const { id } = req.params;
    if (req.user.role !== 'mentor') {
      return res.status(403).json({ success: false, error: 'Access denied' });
    }

    // Check if the merchant belongs to this mentor
    const merchantRes = await query(
      'SELECT id FROM users WHERE id = $1 AND mentor_id = $2 AND role = \'merchant\'',
      [id, req.user.id]
    );

    if (merchantRes.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Merchant not found' });
    }

    // Revert to user role
    await query(
      'UPDATE users SET role = \'user\', mentor_id = NULL, merchant_expired_at = NULL WHERE id = $1',
      [id]
    );

    res.json({ success: true, message: 'Merchant removed successfully' });
  } catch (error) {
    console.error('Error deleting merchant:', error);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

module.exports = router;