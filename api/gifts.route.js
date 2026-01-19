const express = require('express');
const router = express.Router();
const pool = require('../db/db');
const { superAdminMiddleware } = require('../middleware/auth');

// Get all available gifts
router.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM gifts ORDER BY created_at DESC');
    res.json({
      success: true,
      gifts: result.rows
    });
  } catch (error) {
    console.error('Get gifts error:', error);
    res.status(500).json({ error: 'Failed to get gifts' });
  }
});

// Create gift (admin only)
router.post('/create', superAdminMiddleware, async (req, res) => {
  try {
    const { name, price, image_url } = req.body;
    
    if (!name || !price) {
      return res.status(400).json({ 
        success: false,
        error: 'Gift name and price are required' 
      });
    }
    
    if (typeof price !== 'number' || price < 0) {
      return res.status(400).json({ 
        success: false,
        error: 'Price must be a non-negative number' 
      });
    }
    
    // Check if gift already exists
    const existingGift = await pool.query(
      'SELECT id FROM gifts WHERE LOWER(name) = LOWER($1)',
      [name.trim()]
    );
    
    if (existingGift.rows.length > 0) {
      return res.status(400).json({ 
        success: false,
        error: 'Gift with this name already exists' 
      });
    }
    
    const result = await pool.query(
      'INSERT INTO gifts (name, price, image_url) VALUES ($1, $2, $3) RETURNING *',
      [name.trim(), price, image_url || null]
    );
    
    res.status(201).json({
      success: true,
      message: 'Gift created successfully',
      gift: result.rows[0]
    });
    
  } catch (error) {
    console.error('Create gift error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to create gift' 
    });
  }
});

// Update gift (admin only)
router.put('/:id', superAdminMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, price, image_url } = req.body;
    
    if (!name || price === undefined) {
      return res.status(400).json({ 
        success: false,
        error: 'Gift name and price are required' 
      });
    }
    
    if (typeof price !== 'number' || price < 0) {
      return res.status(400).json({ 
        success: false,
        error: 'Price must be a non-negative number' 
      });
    }
    
    // Check if gift exists
    const giftExists = await pool.query('SELECT id FROM gifts WHERE id = $1', [id]);
    if (giftExists.rows.length === 0) {
      return res.status(404).json({ 
        success: false,
        error: 'Gift not found' 
      });
    }
    
    // Check if new name is unique (excluding current gift)
    const duplicateName = await pool.query(
      'SELECT id FROM gifts WHERE LOWER(name) = LOWER($1) AND id != $2',
      [name.trim(), id]
    );
    
    if (duplicateName.rows.length > 0) {
      return res.status(400).json({ 
        success: false,
        error: 'Gift with this name already exists' 
      });
    }
    
    const result = await pool.query(
      'UPDATE gifts SET name = $1, price = $2, image_url = $3, updated_at = CURRENT_TIMESTAMP WHERE id = $4 RETURNING *',
      [name.trim(), price, image_url || null, id]
    );
    
    res.json({
      success: true,
      message: 'Gift updated successfully',
      gift: result.rows[0]
    });
    
  } catch (error) {
    console.error('Update gift error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to update gift' 
    });
  }
});

// Delete gift (admin only)
router.delete('/:id', superAdminMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    
    // Check if gift exists
    const giftExists = await pool.query('SELECT id FROM gifts WHERE id = $1', [id]);
    if (giftExists.rows.length === 0) {
      return res.status(404).json({ 
        success: false,
        error: 'Gift not found' 
      });
    }
    
    await pool.query('DELETE FROM gifts WHERE id = $1', [id]);
    
    res.json({
      success: true,
      message: 'Gift deleted successfully'
    });
    
  } catch (error) {
    console.error('Delete gift error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to delete gift' 
    });
  }
});

module.exports = router;
