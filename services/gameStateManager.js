const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');

const GAME_TYPES = {
  DICE: 'dice',
  LOWCARD: 'lowcard',
  FLAGBOT: 'flagbot'
};

const getActiveGameType = async (roomId) => {
  const redis = getRedisClient();
  const gameType = await redis.get(`room:${roomId}:activeGameType`);
  return gameType || null;
};

const setActiveGameType = async (roomId, gameType) => {
  const redis = getRedisClient();
  await redis.set(`room:${roomId}:activeGameType`, gameType);
  logger.info(`[GameState] Room ${roomId} set to game type: ${gameType}`);
};

const clearActiveGameType = async (roomId) => {
  const redis = getRedisClient();
  await redis.del(`room:${roomId}:activeGameType`);
  logger.info(`[GameState] Room ${roomId} game type cleared`);
};

const isGameTypeActive = async (roomId, gameType) => {
  const currentType = await getActiveGameType(roomId);
  return currentType === gameType;
};

const canStartGame = async (roomId, requestedType) => {
  const currentType = await getActiveGameType(roomId);
  
  if (!currentType) {
    return { allowed: true };
  }
  
  if (currentType === requestedType) {
    return { allowed: true };
  }
  
  const typeNames = {
    [GAME_TYPES.DICE]: 'DiceBot',
    [GAME_TYPES.LOWCARD]: 'LowCard', 
    [GAME_TYPES.FLAGBOT]: 'FlagBot'
  };
  
  return { 
    allowed: false, 
    message: `${typeNames[currentType]} is active in this room. Cannot start ${typeNames[requestedType]}.`
  };
};

const getCommandOwner = (command) => {
  const cmd = command.toLowerCase().trim();
  
  if (cmd.startsWith('!fg') || cmd.startsWith('!b ') || cmd === '/bot flagh add' || 
      cmd === '/add bot flagh' || cmd === '/bot flagh off' || cmd === '/bot stop flagh') {
    return GAME_TYPES.FLAGBOT;
  }
  
  if (cmd === '!r' || cmd === '!roll') {
    return GAME_TYPES.DICE;
  }
  
  if (cmd === '!d') {
    return GAME_TYPES.LOWCARD;
  }
  
  return null;
};

module.exports = {
  GAME_TYPES,
  getActiveGameType,
  setActiveGameType,
  clearActiveGameType,
  isGameTypeActive,
  canStartGame,
  getCommandOwner
};
