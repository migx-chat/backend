const logger = require('../utils/logger');
const express = require('express');
const router = express.Router();
const roomService = require('../services/roomService');
const banService = require('../services/banService');
const { getRecentRooms, addRecentRoom, getFavoriteRooms, addFavoriteRoom, removeFavoriteRoom, getHotRooms } = require('../utils/redisUtils');
const presence = require('../utils/presence');

router.get('/', async (req, res) => {
  try {
    const { limit = 50, offset = 0 } = req.query;
    const rooms = await roomService.getAllRooms(parseInt(limit), parseInt(offset));
    
    const formattedRooms = rooms.map(room => ({
      ...room,
      roomId: room.room_code || room.id,
      roomCode: room.room_code
    }));
    
    res.json({
      rooms: formattedRooms,
      count: formattedRooms.length
    });
    
  } catch (error) {
    console.error('Get rooms error:', error);
    res.status(500).json({ error: 'Failed to get rooms' });
  }
});

router.get('/favorites/:username', async (req, res) => {
  try {
    const { username } = req.params;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }
    
    const favoriteRooms = await getFavoriteRooms(username);
    const roomsWithDetails = await Promise.all(
      favoriteRooms.map(async (roomId) => {
        const room = await roomService.getRoomById(roomId);
        if (!room) return null;
        const userCount = await presence.getRoomUserCount(roomId);
        return {
          id: room.id,
          name: room.name,
          description: room.description,
          maxUsers: room.max_users,
          userCount,
          ownerId: room.owner_id,
          ownerName: room.owner_name
        };
      })
    );
    
    const validRooms = roomsWithDetails.filter(r => r !== null);
    
    res.json({
      success: true,
      rooms: validRooms,
      count: validRooms.length
    });
    
  } catch (error) {
    console.error('Get favorite rooms error:', error);
    res.status(500).json({ error: 'Failed to get favorite rooms' });
  }
});

router.post('/favorites/add', async (req, res) => {
  try {
    const { username, roomId } = req.body;
    
    if (!username || !roomId) {
      return res.status(400).json({ 
        success: false,
        message: 'Username and roomId are required' 
      });
    }
    
    const success = await addFavoriteRoom(username, roomId);
    
    if (success) {
      res.json({
        success: true,
        message: 'Room added to favorites'
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Failed to add room to favorites'
      });
    }
    
  } catch (error) {
    console.error('Add favorite room error:', error);
    res.status(500).json({ 
      success: false,
      message: 'Failed to add room to favorites' 
    });
  }
});

router.post('/favorites/remove', async (req, res) => {
  try {
    const { username, roomId } = req.body;
    
    if (!username || !roomId) {
      return res.status(400).json({ 
        success: false,
        message: 'Username and roomId are required' 
      });
    }
    
    const success = await removeFavoriteRoom(username, roomId);
    
    if (success) {
      res.json({
        success: true,
        message: 'Room removed from favorites'
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Failed to remove room from favorites'
      });
    }
    
  } catch (error) {
    console.error('Remove favorite room error:', error);
    res.status(500).json({ 
      success: false,
      message: 'Failed to remove room from favorites' 
    });
  }
});

router.get('/recent/:username', async (req, res) => {
  try {
    const { username } = req.params;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }
    
    // Get user by username to get ID for history
    const userService = require('../services/userService');
    const user = await userService.getUserByUsername(username);
    
    // 1. Get recent rooms from Redis (active sessions)
    const redisRecent = await getRecentRooms(username);
    
    // 2. Get history rooms from Database
    let dbHistory = [];
    if (user) {
      dbHistory = await roomService.getUserRoomHistory(user.id, 20);
    }
    
    // Merge and unique by roomId
    const combinedMap = new Map();
    
    // Add Redis recent first
    redisRecent.forEach(r => {
      combinedMap.set(r.roomId.toString(), {
        id: r.roomId.toString(),
        name: r.roomName,
        timestamp: r.timestamp || Date.now()
      });
    });
    
    // Add DB history
    dbHistory.forEach(r => {
      if (!combinedMap.has(r.id.toString())) {
        combinedMap.set(r.id.toString(), {
          id: r.id.toString(),
          name: r.name,
          timestamp: r.last_joined_at
        });
      }
    });
    
    const combinedRooms = Array.from(combinedMap.values())
      .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
      .slice(0, 15);

    const roomsWithDetails = await Promise.all(
      combinedRooms.map(async (r) => {
        const room = await roomService.getRoomById(r.id);
        if (!room) return null;
        const userCount = await presence.getRoomUserCount(r.id);
        return {
          id: room.id,
          roomId: r.id,
          name: room.name,
          description: room.description || '',
          maxUsers: room.max_users || 50,
          userCount,
          lastVisit: r.timestamp,
          ownerId: room.owner_id,
          ownerName: room.owner_name
        };
      })
    );
    
    const validRooms = roomsWithDetails.filter(r => r !== null);
    
    res.json({
      success: true,
      rooms: validRooms,
      count: validRooms.length
    });
    
  } catch (error) {
    console.error('Get recent rooms error:', error);
    res.status(500).json({ error: 'Failed to get recent rooms' });
  }
});

router.get('/hot', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    const hotRooms = await getHotRooms(parseInt(limit));
    
    const roomsWithDetails = await Promise.all(
      hotRooms.map(async (item) => {
        const room = await roomService.getRoomById(item.roomId);
        if (!room) return null;
        const userCount = await presence.getRoomUserCount(item.roomId);
        return {
          id: room.id,
          name: room.name,
          description: room.description,
          maxUsers: room.max_users,
          userCount,
          activeUsers: item.score,
          ownerId: room.owner_id,
          ownerName: room.owner_name,
          isPrivate: room.is_private
        };
      })
    );
    
    const validRooms = roomsWithDetails.filter(r => r !== null);
    
    res.json({
      success: true,
      rooms: validRooms,
      count: validRooms.length
    });
    
  } catch (error) {
    console.error('Get hot rooms error:', error);
    res.status(500).json({ error: 'Failed to get hot rooms' });
  }
});

router.get('/more', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    const rooms = await roomService.getRandomRooms(parseInt(limit));
    
    const roomsWithDetails = await Promise.all(
      rooms.map(async (room) => {
        const userCount = await presence.getRoomUserCount(room.id);
        return {
          id: room.id,
          roomId: room.room_code || room.id,
          roomCode: room.room_code,
          name: room.name,
          description: room.description,
          maxUsers: room.max_users,
          userCount,
          ownerId: room.owner_id,
          ownerName: room.owner_name,
          isPrivate: room.is_private
        };
      })
    );
    
    res.json({
      success: true,
      rooms: roomsWithDetails,
      count: roomsWithDetails.length
    });
    
  } catch (error) {
    console.error('Get more rooms error:', error);
    res.status(500).json({ error: 'Failed to get more rooms' });
  }
});

router.get('/search', async (req, res) => {
  try {
    const { q, limit = 20 } = req.query;
    
    if (!q || q.trim().length === 0) {
      return res.json({ success: true, rooms: [] });
    }

    const { query: dbQuery } = require('../db/db');
    const searchTerm = `%${q}%`;
    
    const result = await dbQuery(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE LOWER(r.name) LIKE LOWER($1)
       ORDER BY r.created_at DESC
       LIMIT $2`,
      [searchTerm, parseInt(limit)]
    );

    const roomsWithDetails = await Promise.all(
      result.rows.map(async (room) => {
        const userCount = await presence.getRoomUserCount(room.id);
        return {
          id: room.id,
          roomId: room.room_code || room.id,
          roomCode: room.room_code,
          name: room.name,
          description: room.description,
          maxUsers: room.max_users,
          userCount,
          ownerId: room.owner_id,
          ownerName: room.owner_name,
          category: room.category
        };
      })
    );

    res.json({
      success: true,
      rooms: roomsWithDetails,
      count: roomsWithDetails.length
    });

  } catch (error) {
    console.error('Search rooms error:', error);
    res.status(500).json({ error: 'Failed to search rooms' });
  }
});

router.get('/official', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    const rooms = await roomService.getOfficialRooms(parseInt(limit));
    
    const roomsWithDetails = rooms.map((room) => ({
      id: room.id,
      roomId: room.room_code || room.id,
      roomCode: room.room_code,
      name: room.name,
      description: room.description,
      maxUsers: room.max_users,
      userCount: room.userCount,
      ownerId: room.owner_id,
      ownerName: room.owner_name,
      category: room.category
    }));
    
    res.json({
      success: true,
      rooms: roomsWithDetails,
      count: roomsWithDetails.length
    });
    
  } catch (error) {
    console.error('Get official rooms error:', error);
    res.status(500).json({ error: 'Failed to get official rooms' });
  }
});

router.get('/game', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    const rooms = await roomService.getGameRooms(parseInt(limit));
    
    const roomsWithDetails = rooms.map((room) => ({
      id: room.id,
      roomId: room.room_code || room.id,
      roomCode: room.room_code,
      name: room.name,
      description: room.description,
      maxUsers: room.max_users,
      userCount: room.userCount,
      ownerId: room.owner_id,
      ownerName: room.owner_name,
      category: room.category
    }));
    
    res.json({
      success: true,
      rooms: roomsWithDetails,
      count: roomsWithDetails.length
    });
    
  } catch (error) {
    console.error('Get game rooms error:', error);
    res.status(500).json({ error: 'Failed to get game rooms' });
  }
});

router.post('/create', async (req, res) => {
  try {
    const { name, ownerId, creatorName, description, category } = req.body;
    
    logger.info('Create room request:', { name, ownerId, creatorName, description, category });
    
    // Validasi required fields
    if (!name) {
      return res.status(400).json({ 
        success: false,
        error: 'Room name is required' 
      });
    }
    
    // Check user level - minimum level 10 required
    if (ownerId) {
      const { query: dbQuery } = require('../db/db');
      const levelResult = await dbQuery(
        'SELECT level FROM user_levels WHERE user_id = $1',
        [ownerId]
      );
      const userLevel = levelResult.rows[0]?.level || 1;
      
      if (userLevel < 10) {
        return res.status(400).json({
          success: false,
          error: `You need to be at least Level 10 to create a room. Your current level is ${userLevel}.`
        });
      }
    }
    
    // Validate description - minimum 1 character, maximum 100 characters
    if (!description || description.trim().length < 1) {
      return res.status(400).json({
        success: false,
        error: `Description is required.`
      });
    }
    
    if (description.trim().length > 100) {
      return res.status(400).json({
        success: false,
        error: `Description must be 100 characters or less. Current: ${description.trim().length} characters.`
      });
    }
    
    // Owner is only required for official category
    if (category === 'official' && !ownerId) {
      return res.status(400).json({ 
        success: false,
        error: 'ownerId is required for official rooms' 
      });
    }
    
    if (category === 'official' && !creatorName) {
      return res.status(400).json({ 
        success: false,
        error: 'creatorName is required for official rooms' 
      });
    }
    
    // Validasi panjang name
    if (name.trim().length < 3) {
      return res.status(400).json({ 
        success: false,
        error: 'Room name must be at least 3 characters' 
      });
    }
    
    if (name.trim().length > 50) {
      return res.status(400).json({ 
        success: false,
        error: 'Room name must not exceed 50 characters' 
      });
    }
    
    // Cek apakah room name sudah ada
    const existingRoom = await roomService.getRoomByName(name.trim());
    if (existingRoom) {
      return res.status(400).json({ 
        success: false,
        error: 'Room name already exists' 
      });
    }
    
    // Check user role - only super_admin can add game category
    if (category === 'game') {
      const userService = require('../services/userService');
      const user = await userService.getUserById(ownerId);
      if (!user || user.role !== 'super_admin') {
        return res.status(403).json({
          success: false,
          error: 'Only Super Admin can add game to a room'
        });
      }
    }
    
    // Create room dengan maxUsers fixed 25
    const room = await roomService.createRoom(
      name.trim(), 
      ownerId || null,
      creatorName ? creatorName.trim() : null,
      description ? description.trim() : '',
      category || 'global'
    );
    
    if (!room) {
      return res.status(500).json({ 
        success: false,
        error: 'Failed to create room' 
      });
    }
    
    // Response dengan format yang benar
    res.status(200).json({
      success: true,
      message: 'Room created successfully',
      room: {
        id: room.id,
        roomId: room.id,
        name: room.name,
        description: room.description,
        ownerId: room.owner_id,
        creatorName: room.creator_name,
        maxUsers: room.max_users
      }
    });
    
  } catch (error) {
    console.error('Create room error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to create room' 
    });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const room = await roomService.getRoomById(id);
    
    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }
    
    const users = await roomService.getRoomUsers(id);
    const admins = await roomService.getRoomAdmins(id);
    
    res.json({
      room,
      users,
      admins,
      userCount: users.length
    });
    
  } catch (error) {
    console.error('Get room error:', error);
    res.status(500).json({ error: 'Failed to get room' });
  }
});

router.get('/:id/users', async (req, res) => {
  try {
    const { id } = req.params;
    const users = await roomService.getRoomUsers(id);
    
    res.json({
      roomId: id,
      users,
      count: users.length
    });
    
  } catch (error) {
    console.error('Get room users error:', error);
    res.status(500).json({ error: 'Failed to get room users' });
  }
});

router.get('/:id/banned', async (req, res) => {
  try {
    const { id } = req.params;
    const bannedUsers = await banService.getRoomBannedUsers(id);
    
    res.json({
      roomId: id,
      bannedUsers,
      count: bannedUsers.length
    });
    
  } catch (error) {
    console.error('Get banned users error:', error);
    res.status(500).json({ error: 'Failed to get banned users' });
  }
});



router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { adminId, ...updates } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    const isAdmin = await roomService.isRoomAdmin(id, adminId);
    if (!isAdmin) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }
    
    const room = await roomService.updateRoom(id, updates);
    
    res.json({
      success: true,
      room
    });
    
  } catch (error) {
    console.error('Update room error:', error);
    res.status(500).json({ error: 'Failed to update room' });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { adminId } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    const room = await roomService.getRoomById(id);
    if (!room || room.owner_id != adminId) {
      return res.status(403).json({ error: 'Only room owner can delete the room' });
    }
    
    await roomService.deleteRoom(id);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Delete room error:', error);
    res.status(500).json({ error: 'Failed to delete room' });
  }
});

router.post('/:id/admins', async (req, res) => {
  try {
    const { id } = req.params;
    const { userId, adminId } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    const isAdmin = await roomService.isRoomAdmin(id, adminId);
    if (!isAdmin) {
      return res.status(403).json({ error: 'Admin privileges required' });
    }
    
    await roomService.addRoomAdmin(id, userId);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Add room admin error:', error);
    res.status(500).json({ error: 'Failed to add room admin' });
  }
});

router.delete('/:id/admins/:userId', async (req, res) => {
  try {
    const { id, userId } = req.params;
    const { adminId } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    const room = await roomService.getRoomById(id);
    if (!room || room.owner_id != adminId) {
      return res.status(403).json({ error: 'Only room owner can remove admins' });
    }
    
    await roomService.removeRoomAdmin(id, userId);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Remove room admin error:', error);
    res.status(500).json({ error: 'Failed to remove room admin' });
  }
});

router.post('/join', async (req, res) => {
  console.warn('[DEPRECATED] POST /api/rooms/join - Use POST /api/chatroom/:roomId/join instead');
  try {
    const { roomId, userId, username } = req.body;
    
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
    
    // Check if user has top merchant badge
    const userService = require('../services/userService');
    const user = await userService.getUserById(userId);
    const hasBadge = user && user.has_top_merchant_badge && user.top_merchant_badge_expiry > new Date();
    
    res.json({
      success: true,
      room: result.room,
      hasBadge
    });
    
  } catch (error) {
    console.error('Join room error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to join room' 
    });
  }
});

router.post('/leave', async (req, res) => {
  console.warn('[DEPRECATED] POST /api/rooms/leave - Use POST /api/chatroom/:roomId/leave instead');
  try {
    const { roomId, userId, username } = req.body;
    
    if (!roomId || !userId || !username) {
      return res.status(400).json({ 
        success: false, 
        error: 'roomId, userId, and username are required' 
      });
    }
    
    const result = await roomService.leaveRoom(roomId, userId, username);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Leave room error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to leave room' 
    });
  }
});

router.get('/joined/:username', async (req, res) => {
  try {
    const { username } = req.params;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }
    
    const presence = require('../utils/presence');
    const userRooms = await presence.getUserRooms(username);
    
    const roomsWithDetails = await Promise.all(
      userRooms.map(async (roomId) => {
        const room = await roomService.getRoomById(roomId);
        if (!room) return null;
        const userCount = await presence.getRoomUserCount(roomId);
        return {
          id: room.id,
          name: room.name,
          description: room.description,
          maxUsers: room.max_users,
          userCount,
          ownerId: room.owner_id,
          ownerName: room.owner_name
        };
      })
    );
    
    const validRooms = roomsWithDetails.filter(r => r !== null);
    
    res.json({
      success: true,
      rooms: validRooms,
      count: validRooms.length
    });
    
  } catch (error) {
    console.error('Get joined rooms error:', error);
    res.status(500).json({ error: 'Failed to get joined rooms' });
  }
});

router.get('/:roomId/participants', async (req, res) => {
  try {
    const { roomId } = req.params;
    
    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }
    
    const { getRoomParticipants } = require('../utils/redisUtils');
    const participants = await getRoomParticipants(roomId);
    
    res.json({
      success: true,
      roomId,
      participants,
      count: participants.length
    });
    
  } catch (error) {
    console.error('Get room participants error:', error);
    res.status(500).json({ error: 'Failed to get room participants' });
  }
});

const auth = require('../middleware/auth');
const { query } = require('../db/db');

router.delete('/:roomId/background', auth, async (req, res) => {
  try {
    const { roomId } = req.params;
    const userId = req.user.id;
    const userRole = req.user.role;

    const room = await roomService.getRoomById(roomId);
    
    if (!room) {
      return res.status(404).json({
        success: false,
        error: 'Room not found'
      });
    }

    const isAdmin = ['admin', 'super_admin'].includes(userRole);
    const isRoomOwner = room.owner_id == userId;
    
    if (!isAdmin && !isRoomOwner) {
      return res.status(403).json({
        success: false,
        error: 'You do not have permission to remove this room background'
      });
    }

    await query(
      'UPDATE rooms SET background_image = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = $1',
      [roomId]
    );

    res.json({
      success: true,
      message: 'Background removed successfully'
    });

  } catch (error) {
    console.error('Remove background error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message || 'Failed to remove background' 
    });
  }
});

module.exports = router;
