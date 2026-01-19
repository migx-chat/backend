
const express = require('express');
const router = express.Router();
const { query } = require('../db/db');
const { getRoomParticipants } = require('../utils/redisUtils');

router.get('/:roomId/info', async (req, res) => {
  try {
    const { roomId } = req.params;
    
    if (!roomId) {
      return res.status(400).json({ 
        success: false, 
        error: 'Room ID is required' 
      });
    }
    
    // Ambil data static dari PostgreSQL
    const roomResult = await query(
      `SELECT 
        r.id,
        r.name,
        r.description,
        r.owner_id as "owner_id",
        COALESCE(u.username, 'Unknown') as "ownerName",
        r.created_at as "createdAt",
        r.updated_at as "updatedAt",
        r.room_code as "roomCode",
        r.max_users as "maxUsers",
        r.is_private as "isPrivate",
        r.background_image as "background_image"
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE r.id = $1`,
      [roomId]
    );
    
    if (roomResult.rows.length === 0) {
      return res.status(404).json({ 
        success: false, 
        error: 'Room not found' 
      });
    }
    
    const room = roomResult.rows[0];
    
    // Ambil data real-time dari Redis untuk participant count
    const participants = await getRoomParticipants(roomId);
    const currentUsers = participants.length;
    
    // Ambil moderators dari database (kecuali owner)
    const moderatorsResult = await query(
      `SELECT u.username FROM room_admins ra
       JOIN users u ON ra.user_id = u.id
       WHERE ra.room_id = $1 AND ra.user_id != $2
       ORDER BY ra.created_at ASC`,
      [roomId, room.owner_id]
    );
    const moderators = moderatorsResult.rows.map(m => m.username);
    
    // Format response
    const roomInfo = {
      id: room.id,
      name: room.name,
      description: room.description || 'No description',
      owner_id: room.owner_id,
      ownerName: room.ownerName,
      createdAt: room.createdAt,
      updatedAt: room.updatedAt,
      roomCode: room.roomCode,
      maxUsers: room.maxUsers || 25,
      isPrivate: room.isPrivate || false,
      background_image: room.background_image,
      currentUsers,
      participants: moderators
    };
    
    res.json({
      success: true,
      roomInfo
    });
    
  } catch (error) {
    console.error('Get room info error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get room info' 
    });
  }
});

module.exports = router;
