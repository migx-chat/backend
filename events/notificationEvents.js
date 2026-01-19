
const notificationService = require('../services/notificationService');
const { getUserSocket } = require('../utils/presence');

module.exports = (io, socket) => {
  
  const sendNotificationToUser = async (username, notification) => {
    try {
      const userSocketId = await getUserSocket(username);
      if (userSocketId) {
        io.to(userSocketId).emit(`notif:${notification.type}`, notification);
      }
    } catch (error) {
      console.error('Error sending notification to user:', error);
    }
  };

  socket.on('notif:get', async (data) => {
    try {
      const { username } = data;
      
      if (!username) {
        socket.emit('error', { message: 'Username required' });
        return;
      }
      
      const notifications = await notificationService.getNotifications(username);
      const unreadCount = await notificationService.getUnreadCount(username);
      
      socket.emit('notif:list', {
        notifications,
        unreadCount
      });
      
    } catch (error) {
      console.error('Error getting notifications:', error);
      socket.emit('error', { message: 'Failed to get notifications' });
    }
  });

  socket.on('notif:clear', async (data) => {
    try {
      const { username } = data;
      
      if (!username) {
        socket.emit('error', { message: 'Username required' });
        return;
      }
      
      await notificationService.clearNotifications(username);
      
      socket.emit('notif:cleared', {
        success: true
      });
      
    } catch (error) {
      console.error('Error clearing notifications:', error);
      socket.emit('error', { message: 'Failed to clear notifications' });
    }
  });

  socket.on('notif:count', async (data) => {
    try {
      const { username } = data;
      
      if (!username) {
        socket.emit('error', { message: 'Username required' });
        return;
      }
      
      const count = await notificationService.getUnreadCount(username);
      
      socket.emit('notif:count:result', {
        count
      });
      
    } catch (error) {
      console.error('Error getting notification count:', error);
      socket.emit('error', { message: 'Failed to get notification count' });
    }
  });

  socket.on('notif:send', async (data) => {
    try {
      const { username, notification } = data;
      
      if (!username || !notification) {
        socket.emit('error', { message: 'Username and notification required' });
        return;
      }
      
      // Add notification to user's list
      await notificationService.addNotification(username, notification);
      
      // Send real-time notification to user if online
      await sendNotificationToUser(username, notification);
      
      socket.emit('notif:sent', {
        success: true
      });
      
    } catch (error) {
      console.error('Error sending notification:', error);
      socket.emit('error', { message: 'Failed to send notification' });
    }
  });

  return {
    sendNotificationToUser
  };
};
