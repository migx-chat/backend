const logger = require('../utils/logger');
const { getRedisClient } = require('../redis');
const { query } = require('../db/db');
const gameStateManager = require('./gameStateManager');

const GROUPS = {
  j: { name: 'Jerman', emoji: '🇩🇪', flag: 'jerman', code: 'J' },
  m: { name: 'Malaysia', emoji: '🇲🇾', flag: 'malaysia', code: 'M' },
  a: { name: 'Arab', emoji: '🇸🇦', flag: 'arab', code: 'A' },
  d: { name: 'Denmark', emoji: '🇩🇰', flag: 'denmark', code: 'D' },
  s: { name: 'Swedia', emoji: '🇸🇪', flag: 'swedia', code: 'S' },
  r: { name: 'Rusia', emoji: '🇷🇺', flag: 'rusia', code: 'R' }
};

const MULTIPLIERS = {
  1: 0,
  2: 2,
  3: 3,
  4: 5,
  5: 8,
  6: 15
};

const BETTING_TIME = 45;
const MIN_BET = 1;  // Minimal 1 coin, no max limit

const getGameKey = (roomId) => `flagbot:room:${roomId}`;
const getBetsKey = (roomId) => `flagbot:room:${roomId}:bets`;
const getBotActiveKey = (roomId) => `flagbot:active:${roomId}`;

const activateBot = async (roomId) => {
  const redis = getRedisClient();
  await redis.set(getBotActiveKey(roomId), '1');
  await gameStateManager.setActiveGameType(roomId, gameStateManager.GAME_TYPES.FLAGBOT);
  return { success: true };
};

const deactivateBot = async (roomId) => {
  const redis = getRedisClient();
  await redis.del(getBotActiveKey(roomId));
  await redis.del(getGameKey(roomId));
  await redis.del(getBetsKey(roomId));
  await gameStateManager.clearActiveGameType(roomId);
  return { success: true };
};

const isBotActive = async (roomId) => {
  const redis = getRedisClient();
  const active = await redis.get(getBotActiveKey(roomId));
  return active === '1';
};

const startGame = async (roomId, starterUsername) => {
  const redis = getRedisClient();
  const gameKey = getGameKey(roomId);
  const lockKey = `flagbot:lock:${roomId}`;
  
  const lockAcquired = await redis.set(lockKey, '1', 'EX', 5, 'NX');
  if (!lockAcquired) {
    return { success: false, error: 'Please wait, another action is in progress.' };
  }
  
  try {
    const existing = await redis.get(gameKey);
    if (existing) {
      const game = JSON.parse(existing);
      if (game.phase === 'betting' || game.phase === 'calculating') {
        return { success: false, error: 'Game already in progress' };
      }
    }
    
    const gameState = {
      roomId,
      phase: 'betting',
      startedBy: starterUsername,
      startedAt: Date.now(),
      endsAt: Date.now() + (BETTING_TIME * 1000),
      totalPool: 0,
      bets: {}
    };
    
    await redis.set(gameKey, JSON.stringify(gameState), { EX: 300 });
    await redis.del(getBetsKey(roomId));
    
    return { 
      success: true, 
      game: gameState,
      bettingTime: BETTING_TIME,
      groups: GROUPS
    };
  } finally {
    await redis.del(lockKey);
  }
};

const placeBet = async (roomId, userId, username, groupCode, amount) => {
  const redis = getRedisClient();
  const gameKey = getGameKey(roomId);
  const betsKey = getBetsKey(roomId);
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, error: 'No active game' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.phase !== 'betting') {
    return { success: false, error: 'Betting phase ended' };
  }
  
  if (Date.now() > game.endsAt) {
    return { success: false, error: 'Time is up' };
  }
  
  const group = GROUPS[groupCode.toLowerCase()];
  if (!group) {
    return { success: false, error: `Invalid group. Use: ${Object.keys(GROUPS).join(', ')}` };
  }
  
  const betAmount = parseInt(amount);
  if (isNaN(betAmount) || betAmount < MIN_BET) {
    return { success: false, error: `Minimum bet is ${MIN_BET} COINS` };
  }
  
  const betData = {
    userId,
    username,
    group: groupCode.toLowerCase(),
    groupName: group.name,
    groupEmoji: group.emoji,
    amount: betAmount,
    timestamp: Date.now()
  };
  
  await redis.hSet(betsKey, `${userId}:${groupCode.toLowerCase()}`, JSON.stringify(betData));
  await redis.expire(betsKey, 300);
  
  game.totalPool += betAmount;
  if (!game.bets[groupCode.toLowerCase()]) {
    game.bets[groupCode.toLowerCase()] = 0;
  }
  game.bets[groupCode.toLowerCase()] += betAmount;
  
  await redis.set(gameKey, JSON.stringify(game), { EX: 300 });
  
  const totals = Object.entries(game.bets)
    .map(([g, amt]) => `${GROUPS[g].name} ${amt}`)
    .join(', ');
  
  return {
    success: true,
    bet: betData,
    totalBid: totals,
    game
  };
};

const generateResults = () => {
  const groupKeys = Object.keys(GROUPS);
  const results = [];
  
  for (let i = 0; i < 6; i++) {
    const randomIndex = Math.floor(Math.random() * groupKeys.length);
    results.push(groupKeys[randomIndex]);
  }
  
  return results;
};

const countOccurrences = (results) => {
  const counts = {};
  results.forEach(r => {
    counts[r] = (counts[r] || 0) + 1;
  });
  return counts;
};

const calculateWinners = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = getGameKey(roomId);
  const betsKey = getBetsKey(roomId);
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, error: 'No active game' };
  }
  
  const game = JSON.parse(gameData);
  game.phase = 'calculating';
  await redis.set(gameKey, JSON.stringify(game), { EX: 300 });
  
  const results = generateResults();
  const occurrences = countOccurrences(results);
  
  const multipliers = {};
  Object.entries(occurrences).forEach(([group, count]) => {
    multipliers[group] = {
      count,
      multiplier: MULTIPLIERS[count] || 1,
      group: GROUPS[group]
    };
  });
  
  const allBets = await redis.hGetAll(betsKey);
  const winners = [];
  const losers = [];
  
  for (const [key, betStr] of Object.entries(allBets)) {
    const bet = JSON.parse(betStr);
    const groupOccurrence = occurrences[bet.group] || 0;
    const multiplier = MULTIPLIERS[groupOccurrence] || 0;
    
    if (groupOccurrence >= 2 && multiplier > 0) {
      const winAmount = bet.amount * multiplier;
      winners.push({
        ...bet,
        multiplier,
        winAmount,
        profit: winAmount - bet.amount
      });
    } else {
      losers.push({
        ...bet,
        lostAmount: bet.amount
      });
    }
  }
  
  game.phase = 'finished';
  game.results = results;
  game.occurrences = occurrences;
  game.multipliers = multipliers;
  game.winners = winners;
  game.losers = losers;
  game.finishedAt = Date.now();
  
  await redis.set(gameKey, JSON.stringify(game), { EX: 60 });
  
  return {
    success: true,
    results,
    resultsEmoji: results.map(r => GROUPS[r].emoji),
    resultsFlags: results.map(r => GROUPS[r].flag),
    occurrences,
    multipliers,
    winners,
    losers,
    game
  };
};

const getGameState = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = getGameKey(roomId);
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return null;
  }
  
  return JSON.parse(gameData);
};

const saveGameHistory = async (roomId, game, winners, losers) => {
  try {
    const roundResult = await query(
      `INSERT INTO legend_rounds (room_id, started_by, result_symbols, total_pool, ended_at)
       VALUES ($1, $2, $3, $4, NOW())
       RETURNING id`,
      [roomId, game.startedBy, game.results.join(','), game.totalPool]
    );
    
    const roundId = roundResult.rows[0].id;
    
    for (const winner of winners) {
      await query(
        `INSERT INTO legend_bets (round_id, user_id, username, group_code, group_name, bet_amount, win_amount, multiplier, is_winner)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)`,
        [roundId, winner.userId, winner.username, winner.group, winner.groupName, winner.amount, winner.winAmount, winner.multiplier]
      );
    }
    
    for (const loser of losers) {
      await query(
        `INSERT INTO legend_bets (round_id, user_id, username, group_code, group_name, bet_amount, win_amount, multiplier, is_winner)
         VALUES ($1, $2, $3, $4, $5, $6, 0, 0, false)`,
        [roundId, loser.userId, loser.username, loser.group, loser.groupName, loser.amount]
      );
    }
    
    logger.info(`📊 Legend game history saved: Round #${roundId}`);
    return roundId;
  } catch (error) {
    console.error('Error saving legend game history:', error);
    return null;
  }
};

const endGame = async (roomId) => {
  const redis = getRedisClient();
  
  const game = await getGameState(roomId);
  if (game && game.winners && game.losers) {
    await saveGameHistory(roomId, game, game.winners, game.losers);
  }
  
  await redis.del(getGameKey(roomId));
  await redis.del(getBetsKey(roomId));
};

const getAllBets = async (roomId) => {
  const redis = getRedisClient();
  const betsKey = getBetsKey(roomId);
  
  const allBets = await redis.hGetAll(betsKey);
  const bets = [];
  
  for (const [key, betStr] of Object.entries(allBets)) {
    bets.push(JSON.parse(betStr));
  }
  
  return bets;
};

const getUserBets = async (roomId, userId) => {
  const redis = getRedisClient();
  const betsKey = getBetsKey(roomId);
  
  const allBets = await redis.hGetAll(betsKey);
  const userBets = [];
  
  for (const [key, betStr] of Object.entries(allBets)) {
    if (key.startsWith(`${userId}:`)) {
      userBets.push(JSON.parse(betStr));
    }
  }
  
  return userBets;
};

module.exports = {
  GROUPS,
  MULTIPLIERS,
  BETTING_TIME,
  MIN_BET,
  startGame,
  placeBet,
  getUserBets,
  generateResults,
  calculateWinners,
  getGameState,
  endGame,
  getAllBets,
  activateBot,
  deactivateBot,
  isBotActive
};
