const express = require('express');
const router = express.Router();
const messageService = require('../services/messageService');

router.get('/:roomId', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const messages = await messageService.getMessages(roomId, parseInt(limit), parseInt(offset));
    
    res.json({
      roomId,
      messages,
      count: messages.length,
      hasMore: messages.length === parseInt(limit)
    });
    
  } catch (error) {
    console.error('Get messages error:', error);
    res.status(500).json({ error: 'Failed to get messages' });
  }
});

router.get('/pm/:userId/:otherUserId', async (req, res) => {
  try {
    const { userId, otherUserId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const messages = await messageService.getPrivateMessages(
      userId, 
      otherUserId, 
      parseInt(limit), 
      parseInt(offset)
    );
    
    res.json({
      messages,
      count: messages.length,
      hasMore: messages.length === parseInt(limit)
    });
    
  } catch (error) {
    console.error('Get private messages error:', error);
    res.status(500).json({ error: 'Failed to get private messages' });
  }
});

router.get('/pm/unread/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const unread = await messageService.getUnreadMessages(userId);
    
    res.json({
      messages: unread,
      count: unread.length
    });
    
  } catch (error) {
    console.error('Get unread messages error:', error);
    res.status(500).json({ error: 'Failed to get unread messages' });
  }
});

router.get('/pm/conversations/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 20 } = req.query;
    
    const conversations = await messageService.getRecentConversations(userId, parseInt(limit));
    
    res.json({
      conversations
    });
    
  } catch (error) {
    console.error('Get conversations error:', error);
    res.status(500).json({ error: 'Failed to get conversations' });
  }
});

router.post('/pm/read', async (req, res) => {
  try {
    const { userId, fromUserId } = req.body;
    
    if (!userId || !fromUserId) {
      return res.status(400).json({ error: 'User IDs required' });
    }
    
    await messageService.markMessagesAsRead(userId, fromUserId);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Mark as read error:', error);
    res.status(500).json({ error: 'Failed to mark messages as read' });
  }
});

router.delete('/:roomId', async (req, res) => {
  try {
    const { roomId } = req.params;
    const { adminId } = req.body;
    
    if (!adminId) {
      return res.status(401).json({ error: 'Admin authentication required' });
    }
    
    await messageService.clearRoomMessages(roomId);
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Clear messages error:', error);
    res.status(500).json({ error: 'Failed to clear messages' });
  }
});

module.exports = router;
