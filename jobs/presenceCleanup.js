/**
 * Step 3️⃣: Server-side presence cleanup job
 * Runs every 60 seconds to detect expired TTL keys
 * Notifies clients when their presence has expired
 */

const { getRedisClient } = require('../redis');
const { getRoomUsersFromTTL, getActiveRooms } = require('../utils/roomPresenceTTL');

let cleanupInterval = null;

/**
 * Start the cleanup job
 */
const startPresenceCleanup = (io) => {
  if (cleanupInterval) {
    console.log('⚠️  Presence cleanup already running');
    return;
  }

  cleanupInterval = setInterval(async () => {
    try {
      const client = getRedisClient();
      const activeRooms = await getActiveRooms();

      for (const roomId of activeRooms) {
        // Get all presence keys for this room
        const pattern = `room:${roomId}:user:*`;
        const keys = await client.keys(pattern);

        for (const key of keys) {
          // Check if key still exists (TTL not expired)
          const exists = await client.exists(key);
          
          if (exists === 0) {
            // Key expired naturally - user should be removed
            const match = key.match(/room:(\d+):user:(\d+)/);
            if (match) {
              const expiredRoomId = match[1];
              const expiredUserId = match[2];
              
              console.log(`⏱️  Detected expired presence: room ${expiredRoomId}, userId ${expiredUserId}`);
              
              // Find socket and emit force-leave event
              const socketsInRoom = await io.in(`room:${expiredRoomId}`).fetchSockets();
              for (const socket of socketsInRoom) {
                if (socket.userId === parseInt(expiredUserId)) {
                  // Emit force-leave event to this socket
                  socket.emit('room:force-leave', {
                    message: 'You have been logged out due to inactivity (6+ hours without activity)',
                    reason: 'inactivity_timeout',
                    timestamp: Date.now()
                  });
                  
                  console.log(`📤 Sent force-leave to socket ${socket.id} (userId: ${expiredUserId})`);
                  
                  // Force disconnect after 1 second
                  setTimeout(() => {
                    socket.disconnect(true);
                  }, 1000);
                }
              }
            }
          }
        }
      }

    } catch (error) {
      console.error('❌ Error in presence cleanup job:', error.message);
    }
  }, 60000); // Run every 60 seconds

  console.log('✅ Started presence cleanup job (interval: 60s)');
};

/**
 * Stop the cleanup job
 */
const stopPresenceCleanup = () => {
  if (cleanupInterval) {
    clearInterval(cleanupInterval);
    cleanupInterval = null;
    console.log('🛑 Stopped presence cleanup job');
  }
};

module.exports = {
  startPresenceCleanup,
  stopPresenceCleanup
};
