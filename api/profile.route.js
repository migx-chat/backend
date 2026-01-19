const logger = require('../utils/logger');

const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const profileService = require('../services/profileService');
const authMiddleware = require('../middleware/auth');
const cloudinary = require('cloudinary').v2;
const { getRedisClient } = require('../redis');

// Helper to get actual presence status from Redis
const getActualPresence = async (username) => {
  try {
    const redis = getRedisClient();
    const presence = await redis.get(`presence:${username}`);
    if (presence) {
      // Return invisible as offline to other users
      return presence === 'invisible' ? 'offline' : presence;
    }
    return 'offline';
  } catch (error) {
    console.error('Error getting presence:', error);
    return 'offline';
  }
};

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET
});

// Configure multer for memory storage (for Cloudinary upload)
const memoryStorage = multer.memoryStorage();
const cloudinaryUpload = multer({
  storage: memoryStorage,
  limits: {
    fileSize: 5 * 1024 * 1024 // 5MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = /jpeg|jpg|png|gif/;
    const extname = allowedTypes.test(path.extname(file.originalname).toLowerCase());
    const mimetype = allowedTypes.test(file.mimetype);
    
    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error('Only image files are allowed!'));
    }
  }
});

// Configure multer for local disk storage (legacy)
const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    const uploadDir = path.join(__dirname, '../../uploads/avatars');
    try {
      await fs.mkdir(uploadDir, { recursive: true });
      cb(null, uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, 'avatar-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({
  storage: storage,
  limits: {
    fileSize: 5 * 1024 * 1024 // 5MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = /jpeg|jpg|png|gif/;
    const extname = allowedTypes.test(path.extname(file.originalname).toLowerCase());
    const mimetype = allowedTypes.test(file.mimetype);
    
    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error('Only image files are allowed!'));
    }
  }
});

// ==================== BACKGROUND ====================

router.post('/background/upload', authMiddleware, cloudinaryUpload.single('background'), async (req, res) => {
  try {
    logger.info('📥 Background upload request received');
    logger.info('📋 Authenticated user:', req.user);
    
    if (!req.file) {
      logger.info('❌ No file uploaded');
      return res.status(400).json({ 
        success: false,
        error: 'No file uploaded' 
      });
    }
    
    // Use authenticated user ID from token
    const userId = req.user.id || req.user.userId || req.body.userId;
    
    if (!userId) {
      logger.info('❌ No userId in token or body');
      return res.status(400).json({ 
        success: false,
        error: 'User ID is required' 
      });
    }
    
    logger.info('✅ Uploading background for user:', userId);
    
    // Upload to Cloudinary for persistent storage
    const cloudinaryResult = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        {
          folder: 'migx/backgrounds',
          resource_type: 'image',
          public_id: `bg_${userId}_${Date.now()}`
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );
      stream.end(req.file.buffer);
    });

    const backgroundUrl = cloudinaryResult.secure_url;
    logger.info('☁️ Cloudinary upload successful:', backgroundUrl);
    
    // Update user background in database
    const result = await profileService.updateBackground(userId, backgroundUrl);
    
    if (!result) {
      logger.info('❌ Failed to update background in database');
      return res.status(500).json({ 
        success: false,
        error: 'Failed to update background' 
      });
    }
    
    logger.info('✅ Background updated successfully:', backgroundUrl);
    
    res.json({
      success: true,
      backgroundUrl: backgroundUrl,
      background: backgroundUrl,
      user: result
    });
    
  } catch (error) {
    console.error('❌ Background upload error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to upload background',
      message: error.message 
    });
  }
});

// ==================== AVATAR ====================

router.post('/avatar/upload', authMiddleware, cloudinaryUpload.single('avatar'), async (req, res) => {
  try {
    logger.info('📥 Avatar upload request received');
    logger.info('📋 Authenticated user:', req.user);
    
    if (!req.file) {
      logger.info('❌ No file uploaded');
      return res.status(400).json({ 
        success: false,
        error: 'No file uploaded' 
      });
    }
    
    // Use authenticated user ID from token
    const userId = req.user.id || req.user.userId || req.body.userId;
    
    if (!userId) {
      logger.info('❌ No userId in token or body');
      return res.status(400).json({ 
        success: false,
        error: 'User ID is required' 
      });
    }
    
    logger.info('✅ Uploading avatar for user:', userId);
    
    // Upload to Cloudinary for persistent storage
    const cloudinaryResult = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        {
          folder: 'migx/avatars',
          resource_type: 'image',
          public_id: `avatar_${userId}_${Date.now()}`,
          transformation: [
            { width: 400, height: 400, crop: 'fill', gravity: 'face' },
            { quality: 'auto' }
          ]
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );
      stream.end(req.file.buffer);
    });

    const avatarUrl = cloudinaryResult.secure_url;
    logger.info('☁️ Cloudinary upload successful:', avatarUrl);
    
    // Update user avatar in database
    const result = await profileService.updateAvatar(userId, avatarUrl);
    
    if (!result) {
      logger.info('❌ Failed to update avatar in database');
      return res.status(500).json({ 
        success: false,
        error: 'Failed to update avatar' 
      });
    }
    
    logger.info('✅ Avatar updated successfully:', avatarUrl);
    
    res.json({
      success: true,
      avatarUrl: avatarUrl,
      avatar: avatarUrl,
      user: result
    });
    
  } catch (error) {
    console.error('❌ Avatar upload error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to upload avatar',
      message: error.message 
    });
  }
});

router.delete('/avatar/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const result = await profileService.deleteAvatar(userId);
    
    if (!result) {
      return res.status(500).json({ error: 'Failed to delete avatar' });
    }
    
    res.json({
      success: true,
      user: result
    });
    
  } catch (error) {
    console.error('Avatar delete error:', error);
    res.status(500).json({ error: 'Failed to delete avatar' });
  }
});

// ==================== POSTS ====================

router.post('/posts', async (req, res) => {
  try {
    const { userId, content, imageUrl } = req.body;
    
    if (!userId || !content) {
      return res.status(400).json({ error: 'User ID and content are required' });
    }
    
    const post = await profileService.createPost(userId, content, imageUrl);
    
    if (!post) {
      return res.status(500).json({ error: 'Failed to create post' });
    }
    
    res.json({
      success: true,
      post
    });
    
  } catch (error) {
    console.error('Create post error:', error);
    res.status(500).json({ error: 'Failed to create post' });
  }
});

router.get('/posts/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 20, offset = 0 } = req.query;
    
    const posts = await profileService.getUserPosts(userId, parseInt(limit), parseInt(offset));
    const count = await profileService.getPostCount(userId);
    
    res.json({
      posts,
      count,
      hasMore: (parseInt(offset) + posts.length) < count
    });
    
  } catch (error) {
    console.error('Get posts error:', error);
    res.status(500).json({ error: 'Failed to get posts' });
  }
});

router.delete('/posts/:postId', async (req, res) => {
  try {
    const { postId } = req.params;
    const { userId } = req.body;
    
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const result = await profileService.deletePost(postId, userId);
    
    if (!result) {
      return res.status(404).json({ error: 'Post not found or unauthorized' });
    }
    
    res.json({
      success: true,
      post: result
    });
    
  } catch (error) {
    console.error('Delete post error:', error);
    res.status(500).json({ error: 'Failed to delete post' });
  }
});

// ==================== GIFTS ====================

router.post('/gifts/send', async (req, res) => {
  try {
    const { senderId, receiverId, giftName, giftIcon, giftCost, senderUsername } = req.body;
    
    if (!senderId || !receiverId || !giftName) {
      return res.status(400).json({ error: 'Sender ID, receiver ID, and gift name are required' });
    }
    
    const gift = await profileService.sendGift(senderId, receiverId, giftName, giftIcon, giftCost);
    
    if (!gift) {
      return res.status(500).json({ error: 'Failed to send gift' });
    }
    
    // Send real-time notification to receiver
    try {
      const notificationService = require('../services/notificationService');
      const { getRedisClient } = require('../redis');
      const crypto = require('crypto');
      const redis = getRedisClient();
      
      // Get receiver username
      const { query } = require('../db/db');
      const receiverResult = await query('SELECT username FROM users WHERE id = $1', [receiverId]);
      const receiverUsername = receiverResult.rows[0]?.username;
      
      if (receiverUsername) {
        const giftNotification = {
          id: crypto.randomBytes(8).toString('hex'),
          type: 'gift',
          from: senderUsername || 'Someone',
          fromUserId: senderId,
          message: `${senderUsername || 'Someone'} sent you a gift [${giftName}]`,
          giftName: giftName,
          giftImage: giftIcon
        };
        
        // Save to Redis for persistence
        await notificationService.addNotification(receiverUsername, giftNotification);
        
        // Emit real-time notification for sound
        const receiverSocketId = await redis.get(`socket:${receiverUsername}`);
        if (receiverSocketId) {
          const { io } = require('../server');
          if (io) {
            io.to(receiverSocketId).emit('notif:gift', {
              ...giftNotification,
              timestamp: Date.now()
            });
          }
        }
        
        logger.info(`🎁 Gift notification sent to ${receiverUsername} from ${senderUsername}`);
      }
    } catch (notifError) {
      console.error('⚠️ Error sending gift notification:', notifError.message);
      // Don't fail the gift request if notification fails
    }
    
    res.json({
      success: true,
      gift
    });
    
  } catch (error) {
    console.error('Send gift error:', error);
    res.status(500).json({ error: 'Failed to send gift' });
  }
});

router.get('/gifts/received/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 20, offset = 0 } = req.query;
    
    const gifts = await profileService.getReceivedGifts(userId, parseInt(limit), parseInt(offset));
    const count = await profileService.getGiftCount(userId);
    
    res.json({
      gifts,
      count,
      hasMore: (parseInt(offset) + gifts.length) < count
    });
    
  } catch (error) {
    console.error('Get received gifts error:', error);
    res.status(500).json({ error: 'Failed to get received gifts' });
  }
});

router.get('/gifts/sent/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 20, offset = 0 } = req.query;
    
    const gifts = await profileService.getSentGifts(userId, parseInt(limit), parseInt(offset));
    
    res.json({
      gifts,
      count: gifts.length
    });
    
  } catch (error) {
    console.error('Get sent gifts error:', error);
    res.status(500).json({ error: 'Failed to get sent gifts' });
  }
});

// ==================== BLOCKS ====================

router.post('/block', authMiddleware, async (req, res) => {
  try {
    const { blockedUsername, targetId } = req.body;
    const userId = req.user.id;
    
    // Accept either username or user ID
    if (!blockedUsername && !targetId) {
      return res.status(400).json({ success: false, message: 'Username or targetId required' });
    }
    
    let result;
    if (targetId) {
      // Block by user ID
      result = await profileService.blockUserById(userId, targetId);
    } else {
      // Block by username
      result = await profileService.blockUser(userId, blockedUsername);
    }
    
    // Invalidate Redis cache when block list changes
    if (result.success) {
      const { getRedisClient } = require('../redis');
      const redis = getRedisClient();
      await redis.del(`user:blocks:${userId}`);
    }
    
    return res.json(result);
  } catch (error) {
    console.error('Error blocking user:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

router.post('/unblock', authMiddleware, async (req, res) => {
  try {
    const { blockedUsername } = req.body;
    const userId = req.user.id;
    if (!blockedUsername) return res.status(400).json({ success: false, message: 'Username required' });
    const result = await profileService.unblockUser(userId, blockedUsername);
    
    // Invalidate Redis cache when block list changes
    if (result.success) {
      const { getRedisClient } = require('../redis');
      const redis = getRedisClient();
      await redis.del(`user:blocks:${userId}`);
    }
    
    return res.json(result);
  } catch (error) {
    console.error('Error unblocking user:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ==================== FOLLOWS ====================

router.post('/follow', authMiddleware, async (req, res) => {
  try {
    const { followingId } = req.body;
    const followerId = req.user.id; // Use authenticated user as follower
    
    if (!followingId) {
      return res.status(400).json({ error: 'Following ID is required' });
    }
    
    const result = await profileService.followUser(followerId, followingId);
    
    if (result.error) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      follow: result
    });
    
  } catch (error) {
    console.error('Follow user error:', error);
    res.status(500).json({ error: 'Failed to follow user' });
  }
});

router.delete('/follow', authMiddleware, async (req, res) => {
  try {
    const { followingId } = req.body;
    const followerId = req.user.id; // Use authenticated user as follower
    
    if (!followingId) {
      return res.status(400).json({ error: 'Following ID is required' });
    }
    
    const result = await profileService.unfollowUser(followerId, followingId);
    
    if (!result) {
      return res.status(404).json({ error: 'Follow relationship not found' });
    }
    
    res.json({
      success: true,
      follow: result
    });
    
  } catch (error) {
    console.error('Unfollow user error:', error);
    res.status(500).json({ error: 'Failed to unfollow user' });
  }
});

router.get('/followers/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const followers = await profileService.getFollowers(userId, parseInt(limit), parseInt(offset));
    const count = await profileService.getFollowersCount(userId);
    
    // Enrich with real-time presence from Redis
    const followersWithPresence = await Promise.all(
      followers.map(async (user) => {
        const presenceStatus = await getActualPresence(user.username);
        return {
          ...user,
          presence_status: presenceStatus
        };
      })
    );
    
    res.json({
      followers: followersWithPresence,
      count,
      hasMore: (parseInt(offset) + followers.length) < count
    });
    
  } catch (error) {
    console.error('Get followers error:', error);
    res.status(500).json({ error: 'Failed to get followers' });
  }
});

router.get('/following/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const following = await profileService.getFollowing(userId, parseInt(limit), parseInt(offset));
    const count = await profileService.getFollowingCount(userId);
    
    // Enrich with real-time presence from Redis
    const followingWithPresence = await Promise.all(
      following.map(async (user) => {
        const presenceStatus = await getActualPresence(user.username);
        return {
          ...user,
          presence_status: presenceStatus
        };
      })
    );
    
    res.json({
      following: followingWithPresence,
      count,
      hasMore: (parseInt(offset) + following.length) < count
    });
    
  } catch (error) {
    console.error('Get following error:', error);
    res.status(500).json({ error: 'Failed to get following' });
  }
});

router.get('/follow/status', async (req, res) => {
  try {
    const { followerId, followingId } = req.query;
    
    if (!followerId || !followingId) {
      return res.status(400).json({ error: 'Follower ID and following ID are required' });
    }
    
    // Get detailed status (null, 'pending', 'accepted', 'rejected')
    const status = await profileService.getFollowStatus(followerId, followingId);
    const isFollowing = status === 'accepted';
    
    res.json({
      isFollowing,
      status // null = not following, 'pending' = request sent, 'accepted' = following
    });
    
  } catch (error) {
    console.error('Check follow status error:', error);
    res.status(500).json({ error: 'Failed to check follow status' });
  }
});

// Get pending follow requests for authenticated user
router.get('/follow/pending', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id; // Only get pending requests for authenticated user
    const { limit = 50, offset = 0 } = req.query;
    
    const requests = await profileService.getPendingFollowRequests(userId, parseInt(limit), parseInt(offset));
    const count = await profileService.getPendingFollowRequestsCount(userId);
    
    res.json({
      success: true,
      requests,
      count,
      hasMore: (parseInt(offset) + requests.length) < count
    });
    
  } catch (error) {
    console.error('Get pending follow requests error:', error);
    res.status(500).json({ error: 'Failed to get pending follow requests' });
  }
});

router.post('/follow/accept', authMiddleware, async (req, res) => {
  try {
    const { followerId } = req.body;
    const acceptingUserId = req.user.id; // Use authenticated user
    const acceptingUsername = req.user.username;
    const notificationService = require('../services/notificationService');
    
    if (!followerId) {
      return res.status(400).json({ 
        success: false,
        error: 'Follower ID is required' 
      });
    }
    
    // Accept the pending follow request in database
    // followerId = person who sent the request, acceptingUserId = person accepting (authenticated)
    const result = await profileService.acceptFollowRequest(acceptingUserId, followerId);
    
    if (result.error) {
      return res.status(400).json({ 
        success: false,
        error: result.error 
      });
    }
    
    // Remove the notification
    await notificationService.removeNotification(acceptingUsername, followerId);
    
    logger.info(`✅ ${acceptingUsername} (ID:${acceptingUserId}) accepted follow request from user ${followerId}`);
    
    res.json({
      success: true,
      message: 'Follow request accepted'
    });
    
  } catch (error) {
    console.error('Accept follow error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to accept follow request' 
    });
  }
});

router.post('/follow/reject', authMiddleware, async (req, res) => {
  try {
    const { followerId } = req.body;
    const rejectingUserId = req.user.id; // Use authenticated user
    const rejectingUsername = req.user.username;
    const notificationService = require('../services/notificationService');
    
    if (!followerId) {
      return res.status(400).json({ 
        success: false,
        error: 'Follower ID is required' 
      });
    }
    
    // Reject the pending follow request in database
    await profileService.rejectFollowRequest(rejectingUserId, followerId);
    
    // Remove the notification
    await notificationService.removeNotification(rejectingUsername, followerId);
    
    logger.info(`❌ ${rejectingUsername} (ID:${rejectingUserId}) rejected follow request from user ${followerId}`);
    
    res.json({
      success: true,
      message: 'Follow request rejected'
    });
    
  } catch (error) {
    console.error('Reject follow error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to reject follow request' 
    });
  }
});

// ==================== PRIVACY SETTINGS ====================

router.get('/privacy/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const settings = await profileService.getPrivacySettings(userId);
    res.json(settings);
  } catch (error) {
    console.error('Get privacy settings error:', error);
    res.status(500).json({ error: 'Failed to get privacy settings' });
  }
});

router.put('/privacy/:userId', authMiddleware, async (req, res) => {
  try {
    const { userId } = req.params;
    const { allowPrivateChat, profilePrivacy, allowShareLocation } = req.body;
    
    if (req.user.id !== parseInt(userId)) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const result = await profileService.updatePrivacySettings(userId, { 
      allowPrivateChat, 
      profilePrivacy, 
      allowShareLocation 
    });
    res.json(result);
  } catch (error) {
    console.error('Update privacy settings error:', error);
    res.status(500).json({ error: 'Failed to update privacy settings' });
  }
});

router.get('/blocks', authMiddleware, async (req, res) => {
  try {
    const blockedUsers = await profileService.getBlockedUsers(req.user.id);
    res.json(blockedUsers);
  } catch (error) {
    console.error('Get blocks error:', error);
    res.status(500).json({ error: 'Failed to get blocked users' });
  }
});

router.post('/unblock', authMiddleware, async (req, res) => {
  try {
    const { targetId } = req.body;
    if (!targetId) return res.status(400).json({ error: 'Target ID required' });
    
    await profileService.unblockUser(req.user.id, targetId);
    res.json({ success: true });
  } catch (error) {
    console.error('Unblock error:', error);
    res.status(500).json({ error: 'Failed to unblock user' });
  }
});

// ==================== STATS ====================

router.get('/stats/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const [postCount, giftCount, followersCount, followingCount] = await Promise.all([
      profileService.getPostCount(userId),
      profileService.getGiftCount(userId),
      profileService.getFollowersCount(userId),
      profileService.getFollowingCount(userId)
    ]);
    
    res.json({
      postCount,
      giftCount,
      followersCount,
      followingCount
    });
    
  } catch (error) {
    console.error('Get stats error:', error);
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

module.exports = router;
