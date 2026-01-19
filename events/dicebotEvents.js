const dicebotService = require('../services/dicebotService');
const { generateMessageId } = require('../utils/idGenerator');
const logger = require('../utils/logger');

const activeTimers = new Map();

const sendBotMessage = (io, roomId, message, type = 'dicebot') => {
  io.to(`room:${roomId}`).emit('chat:message', {
    id: generateMessageId(),
    roomId,
    username: 'DiceBot',
    message: message,
    messageType: type,
    type: 'bot',
    botType: 'dicebot',
    userType: 'bot',
    usernameColor: '#e67e22',
    messageColor: '#2ecc71',
    timestamp: new Date().toISOString()
  });
};

const clearGameTimers = (roomId) => {
  const joinKey = `dicebot:join:${roomId}`;
  const rollKey = `dicebot:roll:${roomId}`;
  
  if (activeTimers.has(joinKey)) {
    clearTimeout(activeTimers.get(joinKey));
    activeTimers.delete(joinKey);
  }
  if (activeTimers.has(rollKey)) {
    clearTimeout(activeTimers.get(rollKey));
    activeTimers.delete(rollKey);
  }
};

const startJoinTimer = (io, roomId) => {
  clearGameTimers(roomId);
  
  const joinKey = `dicebot:join:${roomId}`;
  
  const timer = setTimeout(async () => {
    activeTimers.delete(joinKey);
    
    const result = await dicebotService.beginGame(roomId);
    
    if (!result) return;
    
    if (result.cancelled) {
      sendBotMessage(io, roomId, result.message);
      return;
    }
    
    if (result.started) {
      sendBotMessage(io, roomId, result.message);
      
      setTimeout(async () => {
        await startNextRoundFlow(io, roomId);
      }, dicebotService.COUNTDOWN_DELAY);
    }
  }, dicebotService.JOIN_TIMEOUT);
  
  activeTimers.set(joinKey, timer);
};

const startNextRoundFlow = async (io, roomId) => {
  const roundResult = await dicebotService.startNextRound(roomId);
  
  if (!roundResult) return;
  
  sendBotMessage(io, roomId, roundResult.message);
  sendBotMessage(io, roomId, roundResult.targetMessage);
  
  startRollTimer(io, roomId);
};

const startRollTimer = (io, roomId) => {
  const rollKey = `dicebot:roll:${roomId}`;
  
  if (activeTimers.has(rollKey)) {
    clearTimeout(activeTimers.get(rollKey));
  }
  
  const timer = setTimeout(async () => {
    activeTimers.delete(rollKey);
    await processRoundEnd(io, roomId);
  }, dicebotService.ROLL_TIMEOUT);
  
  activeTimers.set(rollKey, timer);
};

const processRoundEnd = async (io, roomId) => {
  const autoRolled = await dicebotService.autoRollForTimeout(roomId);
  for (const roll of autoRolled) {
    sendBotMessage(io, roomId, roll.message);
  }
  
  await new Promise(resolve => setTimeout(resolve, 500));
  
  sendBotMessage(io, roomId, 'Looks like everyone has rolled. Tallying roll.');
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  const result = await dicebotService.tallyRound(roomId);
  
  if (!result) return;
  
  if (result.error) {
    sendBotMessage(io, roomId, result.message);
    return;
  }
  
  if (result.allFailed) {
    sendBotMessage(io, roomId, result.message);
    
    setTimeout(async () => {
      await startNextRoundFlow(io, roomId);
    }, dicebotService.COUNTDOWN_DELAY);
    return;
  }
  
  if (result.gameOver) {
    sendBotMessage(io, roomId, result.message);
    
    if (result.playAgain) {
      sendBotMessage(io, roomId, result.playAgain);
    }
    
    clearGameTimers(roomId);
    
    if (result.winnerId) {
      io.to(`room:${roomId}`).emit('credits:updated', { 
        userId: result.winnerId,
        balance: result.newBalance 
      });
    }
    return;
  }
  
  if (result.roundComplete) {
    sendBotMessage(io, roomId, result.message);
    sendBotMessage(io, roomId, result.followUp);
    
    setTimeout(async () => {
      await startNextRoundFlow(io, roomId);
    }, dicebotService.COUNTDOWN_DELAY);
  }
};

const handleDicebotCommand = async (io, socket, data) => {
  const { roomId, userId, username, message } = data;
  
  if (message.startsWith('/bot ')) {
    const parts = message.slice(5).split(' ');
    const subCmd = parts[0]?.toLowerCase();
    const action = parts[1]?.toLowerCase();
    
    if (subCmd === 'dicebot' || subCmd === 'dice') {
      const userService = require('../services/userService');
      const user = await userService.getUserById(userId);
      if (!user || (user.role !== 'super_admin' && user.role !== 'admin')) {
        socket.emit('chat:message', {
          id: generateMessageId(),
          roomId,
          message: 'Error: Only admin can perform this action.',
          messageType: 'error',
          type: 'error',
          timestamp: new Date().toISOString()
        });
        return true;
      }
      
      if (action === 'add') {
        const result = await dicebotService.addBotToRoom(roomId);
        sendBotMessage(io, roomId, result.message);
        return true;
      }
      
      if (action === 'remove') {
        clearGameTimers(roomId);
        const result = await dicebotService.removeBotFromRoom(roomId);
        sendBotMessage(io, roomId, result.message);
        return true;
      }
      
      socket.emit('system:message', {
        roomId,
        message: 'Usage: /bot dice add|remove',
        timestamp: new Date().toISOString(),
        type: 'info'
      });
      return true;
    }
    
    return false;
  }
  
  const isBotActive = await dicebotService.isBotActive(roomId);
  if (!isBotActive) return false;
  
  const lowerMessage = message.toLowerCase().trim();
  
  if (lowerMessage.startsWith('!start')) {
    const parts = message.split(' ');
    const amount = parts[1] || dicebotService.MIN_ENTRY;
    
    const result = await dicebotService.startGame(roomId, userId, username, amount);
    
    if (result.success) {
      sendBotMessage(io, roomId, result.message);
      startJoinTimer(io, roomId);
      
      socket.emit('credits:updated', {
        userId,
        balance: result.newBalance
      });
    } else {
      socket.emit('system:message', {
        roomId,
        message: result.message,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
    }
    
    return true;
  }
  
  if (lowerMessage === '!j' || lowerMessage === '!join') {
    const result = await dicebotService.joinGame(roomId, userId, username);
    
    if (result.success) {
      sendBotMessage(io, roomId, result.message);
      
      socket.emit('credits:updated', {
        userId,
        balance: result.newBalance
      });
    } else {
      socket.emit('system:message', {
        roomId,
        message: result.message,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
    }
    
    return true;
  }
  
  if (lowerMessage === '!r' || lowerMessage === '!roll') {
    const result = await dicebotService.rollPlayerDice(roomId, userId, username);
    
    if (result.success) {
      sendBotMessage(io, roomId, result.message);
      
      if (result.allRolled) {
        const rollKey = `dicebot:roll:${roomId}`;
        if (activeTimers.has(rollKey)) {
          clearTimeout(activeTimers.get(rollKey));
          activeTimers.delete(rollKey);
        }
        
        setTimeout(async () => {
          await processRoundEnd(io, roomId);
        }, 500);
      }
    } else {
      socket.emit('system:message', {
        roomId,
        message: result.message,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
    }
    
    return true;
  }
  
  if (lowerMessage === '!cancel') {
    const isAdmin = await dicebotService.isRoomAdmin(roomId, userId);
    const isSysAdmin = await dicebotService.isSystemAdmin(userId);
    
    if (!isAdmin && !isSysAdmin) {
      socket.emit('chat:message', {
        id: generateMessageId(),
        roomId,
        message: `DiceBot: Only room admin can cancel the game.`,
        messageType: 'error',
        type: 'bot',
        botType: 'dicebot',
        timestamp: new Date().toISOString()
      });
      return true;
    }
    
    clearGameTimers(roomId);
    const result = await dicebotService.cancelGame(roomId, 'Cancelled by admin');
    sendBotMessage(io, roomId, result.message);
    
    return true;
  }
  
  return false;
};

module.exports = {
  handleDicebotCommand,
  sendBotMessage,
  clearGameTimers
};
