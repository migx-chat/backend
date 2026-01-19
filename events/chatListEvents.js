
const {
  getUserRooms,
  getUserDMs,
  getRoomLastMessage,
  getDMLastMessage
} = require('../utils/redisUtils');
const roomService = require('../services/roomService');
const userService = require('../services/userService');

module.exports = (io, socket) => {
  const getChatList = async (data) => {
    try {
      const { username } = data;
      
      if (!username) {
        socket.emit('error', { message: 'Username required' });
        return;
      }

      const userRooms = await getUserRooms(username);
      const userDMs = await getUserDMs(username);

      const roomsWithLastMsg = await Promise.all(
        userRooms.map(async (room) => {
          const roomInfo = await roomService.getRoomById(room.roomId);
          const lastMsg = await getRoomLastMessage(room.roomId);
          const userCount = await roomService.getRoomUsers(room.roomId);
          
          return {
            type: 'room',
            id: room.roomId,
            name: room.roomName,
            userCount: userCount.length,
            lastMessage: lastMsg,
            joinedAt: room.joinedAt,
            isPrivate: roomInfo?.is_private || false
          };
        })
      );

      const dmsWithLastMsg = await Promise.all(
        userDMs.map(async (dm) => {
          const lastMsg = await getDMLastMessage(username, dm.username);
          const targetUser = await userService.getUserByUsername(dm.username);
          
          return {
            type: 'dm',
            username: dm.username,
            userId: targetUser?.id,
            avatar: targetUser?.avatar,
            lastMessage: lastMsg,
            addedAt: dm.addedAt
          };
        })
      );

      socket.emit('chatlist:list', {
        rooms: roomsWithLastMsg,
        dms: dmsWithLastMsg
      });

    } catch (error) {
      console.error('Error getting chat list:', error);
      socket.emit('error', { message: 'Failed to get chat list' });
    }
  };

  const notifyRoomUpdate = async (roomId, roomName, lastMessage) => {
    try {
      const users = await roomService.getRoomUsers(roomId);
      
      users.forEach(user => {
        io.to(`user:${user.username}`).emit('chatlist:update', {
          type: 'room',
          roomId,
          roomName,
          lastMessage
        });
      });
    } catch (error) {
      console.error('Error notifying room update:', error);
    }
  };

  const notifyDMUpdate = (fromUsername, toUsername, lastMessage) => {
    io.to(`user:${toUsername}`).emit('chatlist:update', {
      type: 'dm',
      username: fromUsername,
      lastMessage
    });
    
    io.to(`user:${fromUsername}`).emit('chatlist:update', {
      type: 'dm',
      username: toUsername,
      lastMessage
    });
  };

  socket.on('chatlist:get', getChatList);
  socket.on('chatlist:room:update', notifyRoomUpdate);
  socket.on('chatlist:dm:update', notifyDMUpdate);
};
