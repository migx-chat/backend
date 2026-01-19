require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path'); // Import path module

const { connectRedis } = require('./redis');
const { initDatabase } = require('./db/db');
const { startPresenceCleanup } = require('./jobs/presenceCleanup');
const { startCommissionPayoutJob } = require('./jobs/commissionPayoutJob');
const { setSession, setPresence } = require('./utils/redisUtils');

const authRoutes = require('./api/auth.route');
const userRoutes = require('./api/user.route');
const roomRoutes = require('./api/room.route');
const roomInfoRoutes = require('./api/roomInfo.route');
const messageRoutes = require('./api/message.route');
const creditRoutes = require('./api/credit.route');
const mentorRoutes = require('./api/mentor.route');
const merchantRoutes = require('./api/merchant.route');
const notificationRoutes = require('./api/notification.route');
const profileRouter = require('./api/profile.route');
const viewProfileRouter = require('./api/viewprofile.route');
const messageRouter = require('./api/message.route');
const notificationRouter = require('./api/notification.route');
const abuseRoutes = require('./api/abuse.route');
const streakRoutes = require('./api/streak.route');
const supportRoutes = require('./api/support.route');
const giftsRoute = require('./api/gifts.route');
const uploadRoute = require('./api/upload.route');

const roomEvents = require('./events/roomEvents');
const chatEvents = require('./events/chatEvents');
const pmEvents = require('./events/pmEvents');
const systemEvents = require('./events/systemEvents');
const creditEvents = require('./events/creditEvents');
const merchantEvents = require('./events/merchantEvents');
const gameEvents = require('./events/gameEvents');
const notificationEvents = require('./events/notificationEvents');
const chatListEvents = require('./events/chatListEvents');
const voucherService = require('./services/voucherService');
const mentorService = require('./services/mentorService');
const { initPubSub } = require('./pubsub');

const app = express();
const server = http.createServer(app);

const getAllowedOrigins = () => {
  const origins = process.env.ALLOWED_ORIGINS;
  if (origins) {
    return origins.split(',').map(o => o.trim());
  }
  if (process.env.NODE_ENV === 'production') {
    return ['https://migx.app', 'https://www.migx.app'];
  }
  return '*';
};

const io = new Server(server, {
  cors: {
    origin: getAllowedOrigins(),
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['polling', 'websocket'],
  pingTimeout: 28800000,
  pingInterval: 25000,
  allowUpgrades: true,
  upgradeTimeout: 30000
});

app.use(cors({
  origin: getAllowedOrigins(),
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  if (process.env.NODE_ENV !== 'production') {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  }
  next();
});

app.use((req, res, next) => {
  // Skip content-type override for static files and HTML pages
  const htmlRoutes = ['/', '/status', '/admin'];
  const isHtmlRoute = htmlRoutes.some(route => req.url === route || req.url.startsWith('/admin'));
  const isStaticFile = req.url.startsWith('/uploads');
  
  if (!isStaticFile && !isHtmlRoute) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.setHeader('Content-Type', 'application/json');
  }
  next();
});

app.get('/', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>MigX Community API</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          background: linear-gradient(135deg, #082919 0%, #00936A 100%);
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
          color: #fff;
          text-align: center;
        }
        .container {
          padding: 40px;
          max-width: 600px;
        }
        h1 {
          font-size: 3rem;
          margin-bottom: 10px;
          text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        .tagline {
          font-size: 1.2rem;
          margin-bottom: 30px;
          opacity: 0.9;
        }
        .status {
          background: rgba(255,255,255,0.2);
          backdrop-filter: blur(10px);
          border-radius: 15px;
          padding: 20px 30px;
          margin-top: 20px;
        }
        .status-item {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 10px;
          margin: 10px 0;
        }
        .dot {
          width: 12px;
          height: 12px;
          background: #4ade80;
          border-radius: 50%;
          animation: pulse 2s infinite;
        }
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
        .endpoints {
          margin-top: 20px;
          font-size: 0.9rem;
          opacity: 0.8;
        }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>Welcome to MigX</h1>
        <p class="tagline">The World Chat Community</p>
        <div class="status">
          <div class="status-item">
            <span class="dot"></span>
            <span>API Server Online</span>
          </div>
          <div class="status-item">
            <span class="dot"></span>
            <span>WebSocket Ready</span>
          </div>
          <div class="status-item">
            <span class="dot"></span>
            <span>Database Connected</span>
          </div>
        </div>
        <div class="endpoints">
          API Endpoints: /api/auth | /api/users | /api/rooms | /api/credits
        </div>
      </div>
    </body>
    </html>
  `);
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    server: 'MigX Backend',
    port: PORT,
    environment: process.env.NODE_ENV || 'development'
  });
});

// Health check with visual HTML response
app.get('/status', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>MigX Server Status</title>
      <meta charset="UTF-8">
      <style>
        body {
          font-family: Arial, sans-serif;
          display: flex;
          justify-content: center;
          align-items: center;
          min-height: 100vh;
          margin: 0;
          background: linear-gradient(135deg, #082919 0%, #00936A 100%);
        }
        .status-card {
          background: white;
          padding: 40px;
          border-radius: 20px;
          box-shadow: 0 10px 40px rgba(0,0,0,0.2);
          text-align: center;
        }
        .status-ok {
          color: #00936A;
          font-size: 2em;
          margin-bottom: 20px;
        }
        .info {
          color: #666;
          margin: 10px 0;
        }
      </style>
    </head>
    <body>
      <div class="status-card">
        <div class="status-ok"> Welcome Migx Chat Comunity</div>
        <div class="info"><strong></strong> MigX Community</div>
        <div class="info"><strong></strong> </div>
        <div class="info"><strong></strong> </div>
        <div class="info"><strong></strong> </div>
      </div>
    </body>
    </html>
  `);
});

app.get('/api', (req, res) => {
  res.json({
    name: 'MIG33 Clone API',
    version: '1.0.0',
    endpoints: {
      auth: '/api/auth',
      users: '/api/users',
      rooms: '/api/rooms',
      messages: '/api/messages',
      credits: '/api/credits',
      merchants: '/api/merchants'
    }
  });
});

app.get('/api/stats/global', async (req, res) => {
  try {
    const db = require('./db/db');
    const totalUsersResult = await db.query('SELECT COUNT(*) as count FROM users');
    const totalRoomsResult = await db.query('SELECT COUNT(*) as count FROM rooms');
    
    res.json({
      success: true,
      stats: {
        totalUsers: parseInt(totalUsersResult.rows[0].count),
        totalRooms: parseInt(totalRoomsResult.rows[0].count)
      }
    });
  } catch (error) {
    console.error('Error fetching global stats:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch stats' });
  }
});

const chatRoutes = require('./api/chat.route');
const chatroomRoutes = require('./api/chatroom.route');

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/users', userRoutes); // Register user routes first
app.use('/api/profile', profileRouter);
app.use('/api/viewprofile', viewProfileRouter);
app.use('/api/rooms', roomRoutes);
app.use('/api/rooms', roomInfoRoutes);
app.use('/api/chatroom', chatroomRoutes);
app.use('/api/rooms', chatroomRoutes); // Add this line to handle /api/rooms/:roomId/min-level
app.use('/api/messages', messageRoutes);
app.use('/api/credit', creditRoutes);
app.use('/api/mentor', mentorRoutes);
app.use('/api/merchant', merchantRoutes);
app.use('/api/notifications', notificationRoutes);
app.use('/api/support', supportRoutes);
app.use('/api/chat', chatRoutes);

const announcementRoute = require('./api/announcement.route');
app.use('/api/announcements', announcementRoute);

const peopleRoute = require('./api/people.route');
app.use('/api/people', peopleRoute);

const leaderboardRoute = require('./api/leaderboard.route');
app.use('/api/leaderboard', leaderboardRoute);

const feedRoute = require('./api/feed.route');
app.use('/api/feed', feedRoute);

const adminRoute = require('./api/admin.route');
app.use('/api/admin', adminRoute);
app.use('/api/abuse', abuseRoutes);
app.use('/api/streak', streakRoutes);
app.use('/api/gifts', giftsRoute);
app.use('/api/upload', uploadRoute);

// Admin unban endpoint
const { clearGlobalBan, getAdminKickCount } = require('./utils/adminKick');
const { clearAdminCooldown, clearVoteCooldown, getCooldownStatus } = require('./utils/roomCooldown');
const jwt = require('jsonwebtoken');

const verifyAdminAuth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'No token provided' });
    }

    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret || jwtSecret.length < 32) {
      return res.status(500).json({ success: false, error: 'Server configuration error' });
    }

    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, jwtSecret);

    const userService = require('./services/userService');
    const user = await userService.getUserById(decoded.id || decoded.userId);

    if (!user) {
      return res.status(401).json({ success: false, error: 'User not found' });
    }

    if (user.role !== 'admin' && user.role !== 'super_admin' && user.role !== 'creator') {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }

    req.user = user;
    next();
  } catch (error) {
    return res.status(401).json({ success: false, error: 'Invalid token' });
  }
};

app.post('/api/admin/unban', verifyAdminAuth, async (req, res) => {
  try {
    const { username } = req.body;

    if (!username) {
      return res.status(400).json({ success: false, error: 'Username is required' });
    }

    await clearGlobalBan(username);

    res.json({
      success: true,
      message: `${username} has been unbanned globally.`
    });
  } catch (error) {
    console.error('Error unbanning user:', error);
    res.status(500).json({ success: false, error: 'Failed to unban user' });
  }
});

app.post('/api/admin/clear-cooldown', verifyAdminAuth, async (req, res) => {
  try {
    const { username, roomId, type } = req.body;

    if (!username || !roomId) {
      return res.status(400).json({ success: false, error: 'Username and roomId are required' });
    }

    if (type === 'admin') {
      await clearAdminCooldown(username, roomId);
    } else if (type === 'vote') {
      await clearVoteCooldown(username, roomId);
    } else {
      await clearAdminCooldown(username, roomId);
      await clearVoteCooldown(username, roomId);
    }

    res.json({
      success: true,
      message: `Cooldown cleared for ${username} in room ${roomId}`
    });
  } catch (error) {
    console.error('Error clearing cooldown:', error);
    res.status(500).json({ success: false, error: 'Failed to clear cooldown' });
  }
});

app.get('/api/admin/ban-status/:username', verifyAdminAuth, async (req, res) => {
  try {
    const { username } = req.params;
    const { roomId } = req.query;

    const kickCount = await getAdminKickCount(username);
    const cooldownStatus = roomId ? await getCooldownStatus(username, roomId) : null;

    res.json({
      success: true,
      username,
      adminKickCount: kickCount,
      isGlobalBanned: cooldownStatus?.isGlobalBanned || kickCount >= 3,
      cooldownStatus
    });
  } catch (error) {
    console.error('Error getting ban status:', error);
    res.status(500).json({ success: false, error: 'Failed to get ban status' });
  }
});

// Serve uploaded files
app.use('/uploads', express.static(path.join(__dirname, '../uploads')));

// Serve admin panel static files
app.use('/admin', express.static(path.join(__dirname, 'public/admin')));

// Admin panel SPA catch-all (must be after API routes)
app.get('/admin/*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public/admin/index.html'));
});

app.use((req, res, next) => {
  res.status(404).json({
    success: false,
    error: 'Route not found',
    path: req.url
  });
});

app.use((err, req, res, next) => {
  const logger = require('./utils/logger');
  logger.error('GLOBAL_ERROR', err);

  if (!res.headersSent) {
    res.status(err.status || 500).json({
      success: false,
      error: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message,
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
    });
  }
});

const chatNamespace = io.of('/chat');

chatNamespace.on('connection', (socket) => {
  const username = socket.handshake.auth?.username || 'Anonymous';
  const userId = socket.handshake.auth?.userId || 'Unknown';
  
  // ⚠️ WARNING: Reject anonymous connections to prevent resource waste
  if (username === 'Anonymous' || userId === 'Unknown') {
    console.warn(`⚠️  REJECTED anonymous connection attempt: ${socket.id}`);
    socket.emit('error', { 
      message: 'Authentication required. Please login first.',
      code: 'AUTH_REQUIRED'
    });
    socket.disconnect(true);
    return;
  }
  
  console.log(`✅ Client connected: ${socket.id} | User: ${username} (ID: ${userId})`);

  // 🔑 JOIN USER CHANNEL - For multi-tab PM delivery
  socket.join(`user:${userId}`);
  console.log(`🔑 User ${username} joined channel: user:${userId}`);

  // Store socket session in Redis for PM delivery
  setSession(username, socket.id).catch(err => {
    console.warn(`⚠️ Could not set session for ${username}:`, err.message);
  });
  setPresence(username, 'online').catch(err => {
    console.warn(`⚠️ Could not set presence for ${username}:`, err.message);
  });

  roomEvents(io.of('/chat'), socket);
  chatEvents(io.of('/chat'), socket);
  pmEvents(io.of('/chat'), socket);
  systemEvents(io.of('/chat'), socket);
  creditEvents(io.of('/chat'), socket);
  merchantEvents(io.of('/chat'), socket);
  gameEvents(io.of('/chat'), socket);
  notificationEvents(io.of('/chat'), socket);
  chatListEvents(io.of('/chat'), socket);

  socket.on('ping', () => {
    socket.emit('pong', { timestamp: Date.now() });
  });

  socket.on('error', (error) => {
    console.error(`Socket error for ${socket.id}:`, error);
  });
  
  socket.on('disconnect', (reason) => {
    console.log(`🔌 Client disconnected: ${socket.id} | User: ${username} | Reason: ${reason}`);
  });
});

// Main namespace handler removed - use /chat namespace only
// This prevents duplicate socket connections per client

const PORT = process.env.PORT || 5000;
const HOST = '0.0.0.0';

const startServer = async () => {
  try {
    console.log('Initializing MIG33 Clone Backend...');

    console.log('Connecting to Redis Cloud...');
    await connectRedis();
    console.log('Redis Cloud connected successfully');

    console.log('Initializing database...');
    await initDatabase();
    console.log('Database initialized successfully');

    // CRITICAL: Clear legacy Redis keys on startup for clean state
    try {
      const redis = require('./redis').getRedisClient();
      const legacyPatterns = ['room:users:*', 'room:participants:*', 'room:userRoom:*', 'room:*:participants', 'room:*:user:*'];
      for (const pattern of legacyPatterns) {
        const keys = await redis.keys(pattern);
        if (keys.length > 0) {
          await redis.del(...keys);
          console.log(`🧹 Cleared ${keys.length} legacy keys: ${pattern}`);
        }
      }
    } catch (err) {
      console.warn('⚠️  Could not clear legacy Redis keys:', err.message);
    }

    await initPubSub(io.of('/chat'));
    
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`Server running on 0.0.0.0:${PORT}`);

      const { checkAndResetLeaderboard } = require('./leaderboardReset');
      checkAndResetLeaderboard();

      // Start presence cleanup job (Step 3️⃣)
      startPresenceCleanup(io);

      voucherService.startVoucherGenerator(io.of('/chat'));
      
      // Start merchant expiry check job (every hour)
      const merchantService = require('./services/merchantService');
      setInterval(async () => {
        try {
          const result = await merchantService.checkAndExpireMerchants();
          if (result.success && result.expiredCount > 0) {
            console.log(`🏪 Merchant expiry check: ${result.expiredCount} merchants expired`);
          }
        } catch (error) {
          console.error('Merchant expiry check error:', error.message);
        }
      }, 60 * 60 * 1000); // Check every hour
      console.log('🏪 Merchant expiry check job started (interval: 1 hour)');
      
      // Start gift queue processor for async DB persistence
      const giftQueueService = require('./services/giftQueueService');
      giftQueueService.startGiftQueueProcessor();
      
      // Start merchant tag commission payout job (every hour)
      startCommissionPayoutJob();
      console.log('💰 Commission payout job started (interval: 1 hour, 24h maturity)');
      
      // Start Top 1 leaderboard cache refresh job (every 5 minutes)
      const { startScheduledRefresh: startTop1CacheRefresh } = require('./utils/top1Cache');
      startTop1CacheRefresh();
      console.log('🏆 Top 1 leaderboard cache job started (interval: 5 minutes)');
      console.log(`
╔═══════════════════════════════════════════════════════╗
║           MIG33 Clone Backend Server                  ║
╠═══════════════════════════════════════════════════════╣
║  HTTP Server:  http://0.0.0.0:5000                    ║
║  Socket.IO:    ws://0.0.0.0:5000                      ║
║  Namespace:    /chat                                  ║
╠═══════════════════════════════════════════════════════╣
║  API Endpoints:                                       ║
║    - POST /api/auth/login                             ║
║    - GET  /api/users/:id                              ║
║    - GET  /api/rooms                                  ║
║    - GET  /api/messages/:roomId                       ║
║    - POST /api/credits/transfer                       ║
║    - POST /api/merchants/create                       ║
║    - POST /api/profile/upload-avatar                  ║
║    - POST /api/profile/post                           ║
║    - POST /api/profile/gift                           ║
║    - GET  /api/profile/followers/:userId             ║
║    - GET  /api/profile/following/:userId              ║
║    - GET  /api/viewprofile/:userId                    ║
║    - POST /api/announcements/create                   ║
║    - GET  /api/announcements                          ║
║    - GET  /api/people/all                             ║
║    - GET  /api/people/role/:role                      ║
╚═══════════════════════════════════════════════════════╝
      `);
    });

  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
};

// Graceful shutdown handler - notify all clients before restart
const gracefulShutdown = async (signal) => {
  console.log(`${signal} received. Notifying all clients and shutting down gracefully...`);
  
  try {
    const chatNamespace = io.of('/chat');
    
    // Broadcast restart notification to all currently connected sockets
    chatNamespace.emit('server:restarting', {
      message: 'Server is restarting. You will be disconnected.',
      timestamp: Date.now()
    });
    
    console.log(`📢 Sent restart notification to all connected clients`);
    
    // Give clients 2 seconds to receive the message before closing
    setTimeout(async () => {
      try {
        // Re-fetch sockets right before disconnect to catch any new connections
        const socketsToDisconnect = await chatNamespace.fetchSockets();
        console.log(`🔌 Disconnecting ${socketsToDisconnect.length} sockets...`);
        
        socketsToDisconnect.forEach(socket => {
          socket.disconnect(true);
        });
      } catch (err) {
        console.error('Error disconnecting sockets:', err.message);
      }
      
      server.close(() => {
        console.log('HTTP server closed');
        process.exit(0);
      });
      
      // Force exit after 5 seconds if server hasn't closed
      setTimeout(() => {
        console.log('Forcing exit...');
        process.exit(0);
      }, 5000);
    }, 2000);
  } catch (err) {
    console.error('Error during graceful shutdown:', err.message);
    process.exit(1);
  }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

startServer();

module.exports = { app, server, io };
