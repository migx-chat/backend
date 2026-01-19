const express = require('express');
const router = express.Router();
const cloudinary = require('cloudinary').v2;
const multer = require('multer');
const auth = require('../middleware/auth');
const { superAdminMiddleware } = require('../middleware/auth');

// Configure Cloudinary with backend secrets
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET
});

// Configure multer for in-memory uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB max
  fileFilter: (req, file, cb) => {
    if (!['image/png', 'image/jpeg'].includes(file.mimetype)) {
      cb(new Error('Only PNG and JPG images are allowed'));
    } else {
      cb(null, true);
    }
  }
});

// Upload endpoint for gifts
router.post('/gifts', superAdminMiddleware, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ 
        success: false,
        error: 'No file provided' 
      });
    }

    // Upload to Cloudinary
    const result = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          folder: 'mig33/gifts',
          resource_type: 'auto'
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );

      uploadStream.end(req.file.buffer);
    });

    res.json({
      success: true,
      url: result.secure_url
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message || 'Failed to upload image' 
    });
  }
});

// Upload endpoint for room background
router.post('/room-background', auth, upload.single('image'), async (req, res) => {
  try {
    const { roomId } = req.body;
    const userId = req.user.id;
    const userRole = req.user.role;

    if (!roomId) {
      return res.status(400).json({ 
        success: false,
        error: 'Room ID is required' 
      });
    }

    if (!req.file) {
      return res.status(400).json({ 
        success: false,
        error: 'No image provided' 
      });
    }

    const roomService = require('../services/roomService');
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
        error: 'You do not have permission to change this room background'
      });
    }

    const result = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          folder: 'mig33/room-backgrounds',
          resource_type: 'image',
          transformation: [
            { width: 1080, height: 1920, crop: 'limit' },
            { quality: 'auto' }
          ]
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );

      uploadStream.end(req.file.buffer);
    });

    const { query } = require('../db/db');
    await query(
      'UPDATE rooms SET background_image = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
      [result.secure_url, roomId]
    );

    res.json({
      success: true,
      backgroundUrl: result.secure_url
    });

  } catch (error) {
    console.error('Room background upload error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message || 'Failed to upload room background' 
    });
  }
});

// Upload endpoint for chat images (private chat)
router.post('/chat-image', auth, upload.single('image'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ 
        success: false,
        error: 'No image provided' 
      });
    }

    // Upload to Cloudinary
    const result = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          folder: 'mig33/chat-images',
          resource_type: 'image',
          transformation: [
            { width: 800, height: 800, crop: 'limit' },
            { quality: 'auto' }
          ]
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );

      uploadStream.end(req.file.buffer);
    });

    res.json({
      success: true,
      imageUrl: result.secure_url
    });

  } catch (error) {
    console.error('Chat image upload error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message || 'Failed to upload image' 
    });
  }
});

module.exports = router;
