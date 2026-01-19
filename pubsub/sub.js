const { createClient } = require('redis');
const logger = require('../utils/logger');
const { CHANNELS } = require('./pub');
const { generateMessageId } = require('../utils/idGenerator');

let subClient = null;
let io = null;

const BOT_CONFIGS = {
  lowcard: {
    username: 'LowCardBot',
    usernameColor: '#719c35',
    messageColor: '#347499'
  },
  dice: {
    username: 'DiceBot',
    usernameColor: '#e74c3c',
    messageColor: '#c0392b'
  },
  flagbot: {
    username: 'FlagBot',
    usernameColor: '#f39c12',
    messageColor: '#d68910'
  }
};

const initSubscriber = async (socketIo) => {
  io = socketIo;
  
  const redisUrl = process.env.REDIS_URL || 
    `redis://default:${process.env.REDIS_PASSWORD}@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`;
  
  subClient = createClient({ url: redisUrl });
  
  subClient.on('error', (err) => {
    logger.error('PUBSUB_SUBSCRIBER_ERROR', err);
  });
  
  await subClient.connect();
  
  await subClient.subscribe(CHANNELS.GAME_RESULT, handleGameResult);
  await subClient.subscribe(CHANNELS.CHAT_MESSAGE, handleChatMessage);
  
  logger.info('PUBSUB_SUBSCRIBER_READY', { 
    channels: [CHANNELS.GAME_RESULT, CHANNELS.CHAT_MESSAGE] 
  });
  
  return subClient;
};

const handleGameResult = (message) => {
  try {
    const data = JSON.parse(message);
    
    switch (data.type) {
      case 'bot_message':
        emitBotMessage(data.roomId, data.botType, data.message);
        break;
        
      case 'batch_messages':
        for (const msg of data.messages) {
          emitBotMessage(data.roomId, data.botType, msg);
        }
        break;
        
      case 'game_result':
        if (data.roomId && data.message) {
          emitBotMessage(data.roomId, data.botType || 'lowcard', data.message);
        }
        break;
        
      default:
        logger.warn('PUBSUB_UNKNOWN_TYPE', { type: data.type });
    }
  } catch (err) {
    logger.error('PUBSUB_GAME_RESULT_ERROR', err);
  }
};

const handleChatMessage = (message) => {
  try {
    const data = JSON.parse(message);
    
    if (data.roomId && data.message) {
      io.to(`room:${data.roomId}`).emit('chat:message', {
        id: generateMessageId(),
        roomId: data.roomId,
        username: data.username || 'System',
        message: data.message,
        messageType: data.messageType || 'chat',
        type: data.type || 'system',
        timestamp: new Date().toISOString()
      });
    }
  } catch (err) {
    logger.error('PUBSUB_CHAT_MESSAGE_ERROR', err);
  }
};

const emitBotMessage = (roomId, botType, message) => {
  if (!io) return;
  
  const config = BOT_CONFIGS[botType] || BOT_CONFIGS.lowcard;
  
  io.to(`room:${roomId}`).emit('chat:message', {
    id: generateMessageId(),
    roomId,
    username: config.username,
    message: message,
    messageType: botType,
    type: 'bot',
    botType: botType,
    userType: 'bot',
    usernameColor: config.usernameColor,
    messageColor: config.messageColor,
    timestamp: new Date().toISOString()
  });
};

const getSubscriber = () => subClient;

const closeSubscriber = async () => {
  if (subClient) {
    await subClient.unsubscribe();
    await subClient.quit();
    subClient = null;
  }
};

module.exports = {
  initSubscriber,
  getSubscriber,
  closeSubscriber,
  emitBotMessage
};
