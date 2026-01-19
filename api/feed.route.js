const logger = require('../utils/logger');
const express = require('express');
const router = express.Router();
const authMiddleware = require('../middleware/auth');
const multer = require('multer');
const cloudinary = require('cloudinary').v2;
const { getRedisClient } = require('../redis');
const crypto = require('crypto');
const { addNotification } = require('../services/notificationService');

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET
});

// Setup multer with memory storage (temporary)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB limit for video
});

// Helper to handle both image and video fields
const handleUpload = (req, res, next) => {
  upload.any()(req, res, (err) => {
    if (err) {
      console.error('❌ Multer error:', err.message);
      return res.status(400).json({ success: false, error: err.message });
    }
    // Make file available as req.file from either 'image' or 'video' field
    if (req.files && req.files.length > 0) {
      req.file = req.files[0];
    }
    next();
  });
};

// Normalize feed item with proper defaults
const normalizeFeedItem = async (feedData, feedId, redis, currentUserId = null) => {
  if (!feedData) return null;

  try {
    const feed = typeof feedData === 'string' ? JSON.parse(feedData) : feedData;

    // Get user details from PostgreSQL to ensure fresh avatar/role/level
    const { query } = require('../db/db');
    const userResult = await query(
      'SELECT avatar, role, username_color FROM users WHERE id = $1',
      [feed.userId || feed.user_id]
    );
    const user = userResult.rows[0];

    // Get level from user_levels
    const levelResult = await query(
      'SELECT level FROM user_levels WHERE user_id = $1',
      [feed.userId || feed.user_id]
    );
    const level = levelResult.rows[0]?.level || 1;

    // Get like count from PostgreSQL (persistent)
    const likesResult = await query(
      'SELECT COUNT(*) as count FROM redis_feed_likes WHERE post_id = $1',
      [feedId]
    );
    const likesCount = parseInt(likesResult.rows[0]?.count || 0);
    
    // Check if current user liked this post
    let isLiked = false;
    if (currentUserId) {
      const userLikeResult = await query(
        'SELECT id FROM redis_feed_likes WHERE post_id = $1 AND user_id = $2',
        [feedId, currentUserId]
      );
      isLiked = userLikeResult.rows.length > 0;
    }

    // Get comments count from Redis
    const commentsKey = `feed:${feedId}:comments`;
    const commentsData = await redis.get(commentsKey);
    const commentsArray = commentsData ? JSON.parse(commentsData) : [];

    const baseUrl = (process.env.BASE_URL || `https://${process.env.REPLIT_DEV_DOMAIN}`).replace(/\/$/, '');
    
    // Normalize avatar URL - ensure it is absolute and has no double slashes
    let avatarUrl = 'https://via.placeholder.com/40';
    if (user?.avatar) {
      if (user.avatar.startsWith('http')) {
        avatarUrl = user.avatar;
      } else {
        const cleanPath = user.avatar.startsWith('/') ? user.avatar : `/${user.avatar}`;
        avatarUrl = `${baseUrl}${cleanPath}`;
      }
    }
    
    // Log for debugging
    logger.info(`[Feed Debug] User: ${feed.username}, Avatar Path: ${user?.avatar}, Final URL: ${avatarUrl}`);
    
    return {
      id: feed.id ?? feedId ?? '',
      username: feed.username ?? null,
      content: feed.content ?? '',
      mediaType: feed.mediaType ?? null,
      mediaUrl: feed.image_url ?? null,
      image_url: feed.image_url ?? null,
      likes_count: likesCount,
      comments_count: commentsArray.length ?? 0,
      is_liked: isLiked,
      created_at: feed.created_at ?? feed.createdAt ?? new Date().toISOString(),
      avatarUrl: avatarUrl,
      avatar_url: avatarUrl,
      avatar: avatarUrl,
      userId: feed.userId ?? feed.user_id,
      user_id: feed.userId ?? feed.user_id,
      level: level,
      role: user?.role || feed.role || 'user',
      username_color: user?.username_color || feed.username_color || null,
      usernameColor: user?.username_color || feed.username_color || null
    };
  } catch (e) {
    console.error(`❌ Error normalizing feed item:`, e.message);
    return null;
  }
};

// Get feed posts from Redis (Ephemeral storage) with PostgreSQL likes
router.get('/', authMiddleware, async (req, res) => {
  try {
    const redis = getRedisClient();
    const feedKey = 'feed:global';
    const currentUserId = req.user.id;

    // Get all items from Redis list
    const feedItems = await redis.lRange(feedKey, 0, 49);

    if (!feedItems || feedItems.length === 0) {
      return res.json({
        success: true,
        posts: [],
        hasMore: false,
        currentPage: 1,
        totalPages: 0
      });
    }

    const posts = feedItems.map(item => JSON.parse(item));
    
    // Refresh role, level, and likes data from database for each post
    const { query } = require('../db/db');
    const postsWithFreshData = await Promise.all(posts.map(async (post) => {
      try {
        // Get user role and avatar
        const userResult = await query(
          'SELECT role, avatar, username_color FROM users WHERE id = $1',
          [post.userId || post.user_id]
        );
        const user = userResult.rows[0];
        
        // Get user level from user_levels table
        const levelResult = await query(
          'SELECT level FROM user_levels WHERE user_id = $1',
          [post.userId || post.user_id]
        );
        const userLevel = levelResult.rows[0]?.level || 1;
        
        // Get likes count from PostgreSQL
        const likesResult = await query(
          'SELECT COUNT(*) as count FROM redis_feed_likes WHERE post_id = $1',
          [post.id]
        );
        const likesCount = parseInt(likesResult.rows[0]?.count || 0);
        
        // Check if current user liked this post
        const userLikeResult = await query(
          'SELECT id FROM redis_feed_likes WHERE post_id = $1 AND user_id = $2',
          [post.id, currentUserId]
        );
        const isLiked = userLikeResult.rows.length > 0;
        
        if (user) {
          post.role = user.role || 'user';
          post.level = userLevel;
          if (user.username_color) {
            post.username_color = user.username_color;
            post.usernameColor = user.username_color;
          }
        } else {
          post.level = userLevel;
        }
        
        // Update likes data from PostgreSQL
        post.likes_count = likesCount;
        post.is_liked = isLiked;
        
        // Get comments count from Redis
        const commentsKey = `feed:${post.id}:comments`;
        const commentsData = await redis.get(commentsKey);
        const commentsArray = commentsData ? JSON.parse(commentsData) : [];
        post.comments_count = commentsArray.length;
        
        return post;
      } catch (e) {
        console.error(`Error refreshing user data for post:`, e.message);
        return post; // Return original if refresh fails
      }
    }));

    res.json({
      success: true,
      posts: postsWithFreshData,
      hasMore: false,
      currentPage: 1,
      totalPages: 1
    });
  } catch (error) {
    console.error('❌ Error fetching feed:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch feed' });
  }
});

// Create new post in Redis (In-memory only)
router.post('/create', authMiddleware, handleUpload, async (req, res) => {
  try {
    const { content } = req.body;
    const userId = req.user.id;
    const username = req.user.username;
    let mediaUrl = null;
    let mediaType = null;

    if (req.file) {
      try {
        logger.info(`📤 Uploading file to Cloudinary: ${req.file.originalname} (${req.file.mimetype})`);
        let resourceType = 'auto';
        if (req.file.mimetype.startsWith('video/')) {
          resourceType = 'video';
          mediaType = 'video';
        } else if (req.file.mimetype.startsWith('image/')) {
          resourceType = 'image';
          mediaType = 'image';
        }

        const cloudinaryResult = await new Promise((resolve, reject) => {
          const stream = cloudinary.uploader.upload_stream(
            {
              folder: 'migx/posts',
              resource_type: resourceType,
            },
            (error, result) => {
              if (error) reject(error);
              else resolve(result);
            }
          );
          stream.end(req.file.buffer);
        });

        mediaUrl = cloudinaryResult.secure_url;
      } catch (uploadError) {
        console.error('❌ Cloudinary upload error:', uploadError.message);
        return res.status(500).json({ success: false, error: 'Failed to upload media' });
      }
    }

    if (!content && !mediaUrl) {
      return res.status(400).json({ success: false, error: 'Content or media required' });
    }

    // Prepare feed data
    const feedId = crypto.randomBytes(8).toString('hex');
    const createdAt = new Date().toISOString();
    
    // Get user details for profile consistency
    const { query } = require('../db/db');
    const userResult = await query(
      'SELECT avatar, role, username_color FROM users WHERE id = $1',
      [userId]
    );
    const user = userResult.rows[0];
    
    const baseUrl = (process.env.BASE_URL || `https://${process.env.REPLIT_DEV_DOMAIN}`).replace(/\/$/, '');
    let avatarUrl = 'https://via.placeholder.com/40';
    if (user?.avatar) {
      if (user.avatar.startsWith('http')) {
        avatarUrl = user.avatar;
      } else {
        const cleanPath = user.avatar.startsWith('/') ? user.avatar : `/${user.avatar}`;
        avatarUrl = `${baseUrl}${cleanPath}`;
      }
    }

    const feedData = {
      id: feedId,
      userId,
      username,
      content: content || '',
      image_url: mediaUrl,
      mediaUrl: mediaUrl,
      mediaType,
      created_at: createdAt,
      avatarUrl,
      role: user?.role || 'user',
      username_color: user?.username_color,
      likes_count: 0,
      comments_count: 0,
      is_liked: false
    };

    // Save to Redis Global List
    const redis = getRedisClient();
    const feedKey = 'feed:global';
    
    await redis.lPush(feedKey, JSON.stringify(feedData));
    await redis.lTrim(feedKey, 0, 49); // Keep only 50 latest
    await redis.expire(feedKey, 86400); // 24 hours TTL

    res.json({
      success: true,
      post: feedData
    });
  } catch (error) {
    console.error('❌ Error creating post:', error);
    res.status(500).json({ success: false, error: 'Failed to create post' });
  }
});

// Like/Unlike post (PostgreSQL for persistence)
router.post('/:feedId/like', authMiddleware, async (req, res) => {
  try {
    const { feedId } = req.params;
    const userId = req.user.id;
    const { query } = require('../db/db');
    
    // Check if user already liked this post
    const existingLike = await query(
      'SELECT id FROM redis_feed_likes WHERE post_id = $1 AND user_id = $2',
      [feedId, userId]
    );

    if (existingLike.rows.length > 0) {
      // Unlike - remove from database
      await query(
        'DELETE FROM redis_feed_likes WHERE post_id = $1 AND user_id = $2',
        [feedId, userId]
      );
      
      // Get updated count
      const countResult = await query(
        'SELECT COUNT(*) as count FROM redis_feed_likes WHERE post_id = $1',
        [feedId]
      );
      
      res.json({ 
        success: true, 
        action: 'unliked',
        likes_count: parseInt(countResult.rows[0].count)
      });
    } else {
      // Like - add to database
      await query(
        'INSERT INTO redis_feed_likes (post_id, user_id) VALUES ($1, $2) ON CONFLICT (post_id, user_id) DO NOTHING',
        [feedId, userId]
      );
      
      // Get updated count
      const countResult = await query(
        'SELECT COUNT(*) as count FROM redis_feed_likes WHERE post_id = $1',
        [feedId]
      );
      
      res.json({ 
        success: true, 
        action: 'liked',
        likes_count: parseInt(countResult.rows[0].count)
      });
    }
  } catch (error) {
    console.error('❌ Error toggling like:', error);
    res.status(500).json({ success: false, error: 'Failed to toggle like' });
  }
});

// Get comments for a post (Redis only)
router.get('/:feedId/comments', authMiddleware, async (req, res) => {
  try {
    const { feedId } = req.params;
    const redis = getRedisClient();
    const commentsKey = `feed:${feedId}:comments`;
    
    const commentsData = await redis.get(commentsKey);
    const comments = commentsData ? JSON.parse(commentsData) : [];

    res.json({
      success: true,
      comments
    });
  } catch (error) {
    console.error('❌ Error fetching comments:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch comments' });
  }
});

// Add comment to post (Redis only)
router.post('/:feedId/comment', authMiddleware, async (req, res) => {
  try {
    const { feedId } = req.params;
    const { content } = req.body;
    const userId = req.user.id;
    const username = req.user.username;
    const redis = getRedisClient();

    if (!content || !content.trim()) {
      return res.status(400).json({ success: false, error: 'Comment content required' });
    }

    const commentsKey = `feed:${feedId}:comments`;
    const commentsData = await redis.get(commentsKey);
    const comments = commentsData ? JSON.parse(commentsData) : [];

    const newComment = {
      id: crypto.randomBytes(4).toString('hex'),
      userId,
      username,
      content: content.trim(),
      created_at: new Date().toISOString()
    };

    comments.push(newComment);
    await redis.set(commentsKey, JSON.stringify(comments.slice(-20)), { EX: 86400 });

    // Send notification to post owner (if commenter is not the owner)
    try {
      const feedKey = 'feed:global';
      const items = await redis.lRange(feedKey, 0, -1);
      const post = items.find(item => {
        const parsed = JSON.parse(item);
        return parsed.id === feedId;
      });
      
      if (post) {
        const postData = JSON.parse(post);
        const postOwnerUsername = postData.username;
        const postOwnerId = postData.userId;
        
        // Only send notification if commenter is not the post owner
        if (postOwnerId !== userId && postOwnerUsername !== username) {
          await addNotification(postOwnerUsername, {
            id: crypto.randomBytes(8).toString('hex'),
            type: 'comment',
            from: username,
            fromUserId: userId,
            message: `${username} commented on your post. Check feed to see`,
            postId: feedId
          });
          
          // Emit socket notification for sound
          const ownerSocketId = await redis.get(`socket:${postOwnerUsername}`);
          if (ownerSocketId) {
            const { io } = require('../server');
            if (io) {
              io.to(ownerSocketId).emit('notif:comment', {
                type: 'comment',
                from: username,
                message: `${username} commented on your post`,
                postId: feedId,
                timestamp: Date.now()
              });
            }
          }
          
          logger.info(`📬 Comment notification sent to ${postOwnerUsername} from ${username}`);
        }
      }
    } catch (notifError) {
      console.error('⚠️ Error sending comment notification:', notifError.message);
      // Don't fail the comment request if notification fails
    }

    res.json({
      success: true,
      comment: newComment
    });
  } catch (error) {
    console.error('❌ Error adding comment:', error);
    res.status(500).json({ success: false, error: 'Failed to add comment' });
  }
});

// Delete post (Redis + PostgreSQL cleanup)
router.delete('/:feedId', authMiddleware, async (req, res) => {
  try {
    const { feedId } = req.params;
    const userId = req.user.id;
    const redis = getRedisClient();
    const feedKey = 'feed:global';
    const { query } = require('../db/db');

    // Get list, find item, check ownership, and remove
    const items = await redis.lRange(feedKey, 0, -1);
    const itemToRemove = items.find(item => {
      const parsed = JSON.parse(item);
      return parsed.id === feedId && parsed.userId === userId;
    });

    if (itemToRemove) {
      await redis.lRem(feedKey, 1, itemToRemove);
      await redis.del(`feed:${feedId}:likes`);
      await redis.del(`feed:${feedId}:comments`);
      
      // Also delete likes from PostgreSQL
      await query('DELETE FROM redis_feed_likes WHERE post_id = $1', [feedId]);
      
      return res.json({ success: true, message: 'Post deleted' });
    }

    res.status(404).json({ success: false, error: 'Post not found or unauthorized' });
  } catch (error) {
    console.error('❌ Error deleting post:', error);
    res.status(500).json({ success: false, error: 'Failed to delete post' });
  }
});

// Optional: Admin endpoint to cleanup old Cloudinary media (manual only)
router.post('/admin/cleanup-cloudinary', authMiddleware, async (req, res) => {
  try {
    const { publicIds } = req.body;

    if (!Array.isArray(publicIds) || publicIds.length === 0) {
      return res.status(400).json({ success: false, error: 'publicIds array required' });
    }

    const results = [];
    for (const publicId of publicIds) {
      try {
        await cloudinary.uploader.destroy(publicId, { resource_type: 'auto' });
        results.push({ publicId, status: 'deleted' });
        logger.info(`🗑️  Cloudinary media deleted: ${publicId}`);
      } catch (error) {
        results.push({ publicId, status: 'failed', error: error.message });
        console.error(`❌ Failed to delete ${publicId}:`, error.message);
      }
    }

    res.json({
      success: true,
      message: 'Cloudinary cleanup completed',
      results
    });
  } catch (error) {
    console.error('❌ Error in cleanup endpoint:', error);
    res.status(500).json({ success: false, error: 'Failed to cleanup Cloudinary' });
  }
});

module.exports = router;