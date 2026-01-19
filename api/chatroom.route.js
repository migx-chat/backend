const logger = require('../utils/logger');
const express = require('express');
const router = express.Router();
const roomService = require('../services/roomService');
const userService = require('../services/userService');
const { getRoomUserCount } = require('../utils/redisPresence');
const { getRoomParticipants } = require('../utils/redisUtils');

router.post('/:roomId/join', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { userId, username } = req.body;
    
    logger.info(`[Chatroom API] Join request - roomId: ${roomId}, userId: ${userId}, username: ${username}`);
    
    if (!roomId || !userId || !username) {
      return res.status(400).json({ 
        success: false, 
        error: 'roomId, userId, and username are required' 
      });
    }
    
    const result = await roomService.joinRoom(roomId, userId, username);
    
    if (!result.success) {
      return res.status(400).json(result);
    }
    
    const userCount = await getRoomUserCount(roomId);
    
    // Check if user has top merchant badge or like reward
    const user = await userService.getUserById(userId);
    const now = new Date();
    const hasMerchantBadge = user && user.has_top_merchant_badge && user.top_merchant_badge_expiry > now;
    const hasLikeReward = user && user.has_top_like_reward && user.top_like_reward_expiry > now;
    
    logger.info(`[Chatroom API] User ${username} joined room ${roomId}. User count: ${userCount}`);
    
    res.json({
      success: true,
      room: result.room,
      userCount,
      hasBadge: hasMerchantBadge,
      hasLikeReward,
      topLikeRewardExpiry: user?.top_like_reward_expiry
    });
    
  } catch (error) {
    console.error('[Chatroom API] Join room error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to join room' 
    });
  }
});

router.post('/:roomId/leave', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { userId, username } = req.body;
    
    logger.info(`[Chatroom API] Leave request - roomId: ${roomId}, userId: ${userId}, username: ${username}`);
    
    if (!roomId || !userId || !username) {
      return res.status(400).json({ 
        success: false, 
        error: 'roomId, userId, and username are required' 
      });
    }
    
    await roomService.leaveRoom(roomId, userId, username);
    
    const userCount = await getRoomUserCount(roomId);
    
    logger.info(`[Chatroom API] User ${username} left room ${roomId}. User count: ${userCount}`);
    
    res.json({
      success: true,
      userCount
    });
    
  } catch (error) {
    console.error('[Chatroom API] Leave room error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to leave room' 
    });
  }
});

router.get('/:roomId/participants', async (req, res) => {
  try {
    const { roomId } = req.params;
    
    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }
    
    // Get participants from Redis Set (returns usernames directly)
    const participantUsernames = await getRoomParticipants(roomId);
    const userCount = await getRoomUserCount(roomId);
    
    // Get full user details for rewards/badges and check room_admins for moderator status
    const { query } = require('../db/db');
    const userDetailsResult = await query(
      `SELECT u.id, u.username, u.role, u.username_color,
              u.has_top_merchant_badge, u.top_merchant_badge_expiry,
              u.has_top_like_reward, u.top_like_reward_expiry,
              EXISTS(SELECT 1 FROM room_admins ra WHERE ra.user_id = u.id AND ra.room_id = $2) as is_mod
       FROM users u
       WHERE u.username = ANY($1)`,
      [participantUsernames, roomId]
    );

    const userMap = new Map();
    userDetailsResult.rows.forEach(u => userMap.set(u.username, u));

    const room = await roomService.getRoomById(roomId);
    const now = new Date();
    const participants = participantUsernames.map(username => {
      const u = userMap.get(username);
      
      let color = u?.username_color;
      const hasLikeReward = u?.has_top_like_reward && new Date(u.top_like_reward_expiry) > now;
      
      if (hasLikeReward && u?.role !== 'merchant') {
        color = '#FF69B4'; // Pink
      } else if (!color) {
        // Apply role colors
        if (u?.id == room?.owner_id) {
          color = '#FFD700'; // Yellow for Creator
        } else if (u?.is_mod) {
          color = '#FFD700'; // Yellow for Moderator
        } else if (u?.role === 'admin') {
          color = '#FF9800'; // Orange
        }
      }

      return {
        username,
        role: u?.role || 'user',
        isModerator: u?.is_mod || false,
        isCreator: u?.id == room?.owner_id,
        usernameColor: color,
        hasTopMerchantBadge: u?.has_top_merchant_badge && new Date(u.top_merchant_badge_expiry) > now,
        hasTopLikeReward: hasLikeReward,
        topLikeRewardExpiry: u?.top_like_reward_expiry
      };
    });
    
    res.json({
      success: true,
      roomId,
      participants,
      count: userCount
    });
    
  } catch (error) {
    console.error('[Chatroom API] Get participants error:', error);
    res.status(500).json({ error: 'Failed to get participants' });
  }
});

router.get('/:roomId/status', async (req, res) => {
  try {
    const { roomId } = req.params;
    
    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }
    
    const room = await roomService.getRoomById(roomId);
    if (!room) {
      return res.status(404).json({
        success: false,
        error: 'Room not found'
      });
    }
    
    const userCount = await getRoomUserCount(roomId);
    const participants = await getRoomParticipants(roomId);
    
    res.json({
      success: true,
      roomId,
      name: room.name,
      userCount,
      maxUsers: room.max_users,
      isFull: userCount >= room.max_users,
      participants
    });
    
  } catch (error) {
    console.error('[Chatroom API] Get room status error:', error);
    res.status(500).json({ error: 'Failed to get room status' });
  }
});

router.get('/:roomId/info', async (req, res) => {
  try {
    const { roomId } = req.params;
    const room = await roomService.getRoomById(roomId);
    if (!room) return res.status(404).json({ success: false, error: 'Room not found' });

    const participants = await getRoomParticipants(roomId);
    const userCount = await getRoomUserCount(roomId);

    res.json({
      success: true,
      roomInfo: {
        id: room.id,
        name: room.name,
        description: room.description,
        ownerName: room.owner_name,
        ownerId: room.owner_id,
        createdAt: room.created_at,
        updatedAt: room.updated_at,
        roomCode: room.room_code,
        maxUsers: room.max_users,
        isPrivate: room.is_private,
        minLevel: room.min_level || 1,
        currentUsers: userCount,
        participants
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: 'Failed to get room info' });
  }
});

router.post('/:roomId/min-level', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { minLevel, userId } = req.body;

    if (isNaN(minLevel) || minLevel < 1 || minLevel > 100) {
      return res.status(400).json({ success: false, error: 'Level must be 1-100' });
    }

    const isAdmin = await roomService.isRoomAdmin(roomId, userId);
    if (!isAdmin) {
      return res.status(403).json({ success: false, error: 'Only owner or moderator can set level' });
    }

    const updated = await roomService.updateRoom(roomId, { minLevel });
    if (updated) {
      res.json({ success: true, minLevel: updated.min_level });
    } else {
      res.status(500).json({ success: false, error: 'Failed to update level' });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

router.put('/:roomId/description', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { description, userId } = req.body;

    if (!description || description.trim().length === 0) {
      return res.status(400).json({ success: false, error: 'Description cannot be empty' });
    }

    if (description.length > 500) {
      return res.status(400).json({ success: false, error: 'Description too long (max 500 chars)' });
    }

    const room = await roomService.getRoomById(roomId);
    if (!room) {
      return res.status(404).json({ success: false, error: 'Room not found' });
    }

    if (room.owner_id !== userId) {
      return res.status(403).json({ success: false, error: 'Only the room owner can edit description' });
    }

    const updated = await roomService.updateRoom(roomId, { description: description.trim() });
    if (updated) {
      res.json({ success: true, description: updated.description });
    } else {
      res.status(500).json({ success: false, error: 'Failed to update description' });
    }
  } catch (error) {
    console.error('[Chatroom API] Update description error:', error);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

router.post('/:roomId/mod', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { targetUserId, adminId } = req.body;

    const isAdmin = await roomService.isRoomAdmin(roomId, adminId);
    if (!isAdmin) {
      return res.status(403).json({ success: false, error: 'Only owner or moderator can promote others' });
    }

    const success = await roomService.addRoomAdmin(roomId, targetUserId);
    if (success) {
      res.json({ success: true });
    } else {
      res.status(500).json({ success: false, error: 'Failed to promote user' });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

router.post('/:roomId/unmod', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { targetUserId, adminId } = req.body;

    const isAdmin = await roomService.isRoomAdmin(roomId, adminId);
    if (!isAdmin) {
      return res.status(403).json({ success: false, error: 'Only owner or moderator can remove others' });
    }

    const success = await roomService.removeRoomAdmin(roomId, targetUserId);
    if (success) {
      res.json({ success: true });
    } else {
      res.status(500).json({ success: false, error: 'Failed to remove user' });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

module.exports = router;
