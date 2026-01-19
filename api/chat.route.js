const logger = require('../utils/logger');
const express = require('express');
const router = express.Router();
const { getRedisClient } = require('../redis');
const roomService = require('../services/roomService');
const { getPool } = require('../db/db');

// REDIS-ONLY chatlist - NO DATABASE QUERIES for real-time performance
router.get('/list/:username', async (req, res) => {
  try {
    const { username } = req.params;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: 'Username is required'
      });
    }

    const redis = getRedisClient();
    
    // ONLY use Redis for active rooms - NO DATABASE QUERY
    const redisRoomsRaw = await redis.sMembers(`user:rooms:${username}`);
    
    // Parse Redis room data (even if empty, we still need to fetch DMs)
    const activeRooms = [];
    const seenIds = new Set();
    
    for (const r of redisRoomsRaw) {
      try {
        const parsed = JSON.parse(r);
        const id = parsed.id || parsed.roomId;
        if (id && !seenIds.has(id.toString())) {
          activeRooms.push({
            id: id.toString(),
            name: parsed.name || parsed.roomName,
            lastJoinedAt: parsed.joinedAt || Date.now()
          });
          seenIds.add(id.toString());
        }
      } catch (e) {
        // If not JSON, treat as room ID string
        if (r && !seenIds.has(r.toString())) {
          activeRooms.push({
            id: r.toString(),
            name: null,
            lastJoinedAt: Date.now()
          });
          seenIds.add(r.toString());
        }
      }
    }

    // Enrich with Redis data (viewer count, last message) + room name from cache
    const enrichedRooms = await Promise.all(
      activeRooms.map(async (room) => {
        try {
          // Get viewer count from Redis
          let viewerCount = 0;
          try {
            const count = await redis.sCard(`room:${room.id}:participants`);
            viewerCount = count || 0;
          } catch (err) {}

          // Get room name from cache or DB only if missing
          let roomName = room.name;
          if (!roomName) {
            try {
              const roomInfo = await roomService.getRoomById(room.id);
              roomName = roomInfo?.name || `Room ${room.id}`;
            } catch (err) {
              roomName = `Room ${room.id}`;
            }
          }

          // Get last message from Redis
          let lastMessage = 'Active now';
          let lastUsername = roomName;
          let timestamp = room.lastJoinedAt;
          
          try {
            const msgData = await redis.get(`room:lastmsg:${room.id}`);
            if (msgData) {
              const parsed = JSON.parse(msgData);
              lastMessage = parsed.message || lastMessage;
              lastUsername = parsed.username || roomName;
              timestamp = parsed.timestamp || room.lastJoinedAt;
            }
          } catch (err) {}

          return {
            id: room.id,
            name: roomName,
            lastMessage,
            lastUsername,
            timestamp,
            viewerCount,
            lastJoinedAt: room.lastJoinedAt,
            isActive: true
          };
        } catch (err) {
          return null;
        }
      })
    );

    const validRooms = enrichedRooms.filter(r => r !== null);

    // FETCH PRIVATE MESSAGES (DMs) - Use correct Redis format with deduplication
    const dmsMap = new Map(); // Use Map to deduplicate by username
    try {
      // Get all DM conversations from user:dm:${username} set
      const dmDataSet = await redis.sMembers(`user:dm:${username}`);
      logger.info(`📩 DM set for ${username}:`, dmDataSet);
      
      for (const dmData of dmDataSet) {
        try {
          const parsed = JSON.parse(dmData);
          const targetUsername = parsed.username;
          
          if (!targetUsername || dmsMap.has(targetUsername)) continue;
          
          // Get last message from dm:lastmsg:${[username, target].sort().join(':')}
          const sortedKey = [username, targetUsername].sort().join(':');
          const lastMsgData = await redis.get(`dm:lastmsg:${sortedKey}`);
          
          if (lastMsgData) {
            const lastMsg = JSON.parse(lastMsgData);
            
            // Fetch user id and avatar from database
            let avatarUrl = null;
            let numericUserId = null;
            try {
              const pool = getPool();
              const result = await pool.query(
                'SELECT id, avatar FROM users WHERE username = $1',
                [targetUsername]
              );
              if (result.rows.length > 0) {
                numericUserId = result.rows[0].id?.toString();
                if (result.rows[0].avatar) {
                  let avatar = result.rows[0].avatar;
                  // Construct full URL if it's a relative path
                  if (avatar && !avatar.startsWith('http')) {
                    const baseUrl = `${req.protocol}://${req.get('host')}`;
                    avatarUrl = `${baseUrl}${avatar}`;
                  } else {
                    avatarUrl = avatar;
                  }
                }
              }
            } catch (dbErr) {
              // Skip fetch error
              console.warn(`⚠️ Failed to fetch user data for ${targetUsername}:`, dbErr.message);
            }
            
            dmsMap.set(targetUsername, {
              userId: numericUserId || targetUsername,
              username: targetUsername,
              avatar: avatarUrl,
              lastMessage: {
                message: lastMsg.message,
                timestamp: lastMsg.timestamp || Date.now()
              },
              isOnline: true
            });
          }
        } catch (err) {
          // Skip malformed DM data
        }
      }
    } catch (err) {
      console.warn('⚠️ Error fetching DMs:', err.message);
    }
    const dms = Array.from(dmsMap.values());

    logger.info(`✅ Returning ${validRooms.length} rooms and ${dms.length} DMs for ${username} from REDIS`);

    res.json({
      success: true,
      rooms: validRooms,
      dms: dms
    });

  } catch (error) {
    console.error('Error getting chat list:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get chat list'
    });
  }
});

router.get('/joined/:username', async (req, res) => {
  try {
    const { username } = req.params;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: 'Username required'
      });
    }

    const redis = getRedisClient();
    const roomIds = await redis.sMembers(`user:rooms:${username}`);

    const roomsWithInfo = await Promise.all(
      roomIds.map(async (roomId) => {
        const roomInfo = await roomService.getRoomById(roomId);
        if (!roomInfo) return null;

        return {
          id: roomId,
          name: roomInfo.name,
          type: 'room'
        };
      })
    );

    const validRooms = roomsWithInfo.filter(r => r !== null);

    res.json({
      success: true,
      rooms: validRooms
    });

  } catch (error) {
    console.error('Get joined rooms error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get joined rooms'
    });
  }
});

module.exports = router;