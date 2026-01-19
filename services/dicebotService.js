const { query } = require('../db/db');
const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');
const merchantTagService = require('./merchantTagService');
const gameStateManager = require('./gameStateManager');
const { 
  getDiceEmoji, 
  getDiceCode, 
  formatDiceRoll, 
  formatDiceRollEmoji, 
  isBalakSix 
} = require('../utils/diceMapping');

const JOIN_TIMEOUT = 30000;
const ROLL_TIMEOUT = 20000;
const COUNTDOWN_DELAY = 3000;
const MIN_ENTRY = 1;
const MAX_ENTRY = 999999999; // No limit
const HOUSE_FEE_PERCENT = 10;

const rollDice = () => {
  const die1 = Math.floor(Math.random() * 6) + 1;
  const die2 = Math.floor(Math.random() * 6) + 1;
  return { die1, die2, total: die1 + die2 };
};

const formatDiceTags = (die1, die2) => {
  return `[DICE:${die1}] [DICE:${die2}]`;
};

const isDoubleSix = (die1, die2) => {
  return die1 === 6 && die2 === 6;
};

const formatCoins = (amount) => {
  return amount.toLocaleString('id-ID');
};

const getUserCredits = async (userId) => {
  try {
    const redis = getRedisClient();
    const cached = await redis.get(`credits:${userId}`);
    if (cached !== null) {
      return parseInt(cached);
    }
    const result = await query('SELECT credits FROM users WHERE id = $1', [userId]);
    const balance = result.rows[0]?.credits || 0;
    await redis.set(`credits:${userId}`, balance, 'EX', 300);
    return parseInt(balance);
  } catch (error) {
    logger.error('DICEBOT_GET_CREDITS_ERROR', error);
    return 0;
  }
};

const logGameTransaction = async (userId, username, amount, transactionType, description) => {
  try {
    await query(
      `INSERT INTO credit_logs (from_user_id, from_username, amount, transaction_type, description, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)`,
      [userId, username, amount, transactionType, description]
    );
  } catch (error) {
    logger.error('DICEBOT_LOG_TRANSACTION_ERROR', error);
  }
};

const deductCredits = async (userId, amount, username = null, reason = null, gameSessionId = null) => {
  try {
    const redis = getRedisClient();
    
    const taggedBalance = await merchantTagService.getTaggedBalance(userId);
    let usedTaggedCredits = 0;
    let remainingAmount = amount;
    
    if (taggedBalance > 0) {
      const consumeResult = await merchantTagService.consumeForGame(userId, 'dicebot', amount, gameSessionId);
      if (consumeResult.success) {
        usedTaggedCredits = consumeResult.usedTaggedCredits || 0;
        remainingAmount = consumeResult.remainingAmount;
        if (usedTaggedCredits > 0) {
          logger.info('DICEBOT_TAGGED_CREDITS_USED', { userId, usedTaggedCredits, remainingAmount });
        }
      }
    }
    
    if (remainingAmount <= 0) {
      const current = await getUserCredits(userId);
      if (username && reason) {
        await logGameTransaction(userId, username, -amount, 'game_bet', `${reason} (Tagged Credits)`);
      }
      return { success: true, balance: current, usedTaggedCredits };
    }
    
    const current = await getUserCredits(userId);
    if (current < remainingAmount) {
      return { success: false, balance: current };
    }
    
    const result = await query(
      'UPDATE users SET credits = credits - $1 WHERE id = $2 AND credits >= $1 RETURNING credits',
      [remainingAmount, userId]
    );
    
    if (result.rows.length === 0) {
      return { success: false, balance: current };
    }
    
    const newBalance = parseInt(result.rows[0].credits);
    await redis.set(`credits:${userId}`, newBalance, 'EX', 300);
    
    if (username && reason) {
      const desc = usedTaggedCredits > 0 ? `${reason} (${usedTaggedCredits} tagged + ${remainingAmount} regular)` : reason;
      await logGameTransaction(userId, username, -amount, 'game_bet', desc);
    }
    
    return { success: true, balance: newBalance, usedTaggedCredits };
  } catch (error) {
    logger.error('DICEBOT_DEDUCT_CREDITS_ERROR', error);
    return { success: false, balance: 0 };
  }
};

const addCredits = async (userId, amount, username = null, reason = null) => {
  try {
    const redis = getRedisClient();
    const result = await query(
      'UPDATE users SET credits = credits + $1 WHERE id = $2 RETURNING credits',
      [amount, userId]
    );
    
    if (result.rows.length > 0) {
      const newBalance = parseInt(result.rows[0].credits);
      await redis.set(`credits:${userId}`, newBalance, 'EX', 300);
      
      if (username && reason) {
        await logGameTransaction(userId, username, amount, reason.includes('Refund') ? 'game_refund' : 'game_win', reason);
      }
      
      return { success: true, balance: newBalance };
    }
    return { success: false, balance: 0 };
  } catch (error) {
    logger.error('DICEBOT_ADD_CREDITS_ERROR', error);
    return { success: false, balance: 0 };
  }
};

const isRoomManaged = async (roomId) => {
  try {
    const result = await query('SELECT owner_id FROM rooms WHERE id = $1', [roomId]);
    return result.rows.length > 0 && result.rows[0].owner_id !== null;
  } catch (error) {
    logger.error('DICEBOT_CHECK_ROOM_ERROR', error);
    return false;
  }
};

const isRoomAdmin = async (roomId, userId) => {
  try {
    const result = await query(
      `SELECT 1 FROM rooms WHERE id = $1 AND owner_id = $2
       UNION
       SELECT 1 FROM room_admins WHERE room_id = $1 AND user_id = $2`,
      [roomId, userId]
    );
    return result.rows.length > 0;
  } catch (error) {
    logger.error('DICEBOT_CHECK_ADMIN_ERROR', error);
    return false;
  }
};

const isSystemAdmin = async (userId) => {
  try {
    const result = await query(
      "SELECT 1 FROM users WHERE id = $1 AND role IN ('admin', 'super_admin')",
      [userId]
    );
    return result.rows.length > 0;
  } catch (error) {
    logger.error('DICEBOT_CHECK_SYSADMIN_ERROR', error);
    return false;
  }
};

const addBotToRoom = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `dicebot:bot:${roomId}`;
  
  const exists = await redis.exists(botKey);
  if (exists) {
    return { success: false, message: 'DiceBot is already active in this room.' };
  }
  
  const lowcardActive = await redis.exists(`lowcard:bot:${roomId}`);
  if (lowcardActive) {
    return { success: false, message: 'LowCardBot is active. Remove it first with /bot lowcard off' };
  }
  
  const legendActive = await redis.exists(`legend:bot:${roomId}`);
  if (legendActive) {
    return { success: false, message: 'FlagBot is active. Remove it first.' };
  }
  
  await redis.set(botKey, JSON.stringify({
    active: true,
    defaultAmount: 1000,
    createdAt: new Date().toISOString()
  }), 'EX', 86400 * 7);
  
  await gameStateManager.setActiveGameType(roomId, gameStateManager.GAME_TYPES.DICE);
  
  return { success: true, message: '[PVT] You started the game. Please wait 3s.' };
};

const removeBotFromRoom = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `dicebot:bot:${roomId}`;
  const gameKey = `dicebot:game:${roomId}`;
  
  const exists = await redis.exists(botKey);
  if (!exists) {
    return { success: false, message: 'No DiceBot in this room.' };
  }
  
  const gameData = await redis.get(gameKey);
  if (gameData) {
    const game = JSON.parse(gameData);
    if (game.status === 'waiting') {
      for (const player of game.players) {
        await addCredits(player.userId, game.entryAmount, player.username, 'DiceBot Refund - Bot removed');
      }
    }
  }
  
  await redis.del(botKey);
  await redis.del(gameKey);
  
  await gameStateManager.clearActiveGameType(roomId);
  
  return { success: true, message: 'DiceBot has left the room.' };
};

const isBotActive = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `dicebot:bot:${roomId}`;
  return await redis.exists(botKey);
};

const getBotStatus = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `dicebot:bot:${roomId}`;
  const data = await redis.get(botKey);
  return data ? JSON.parse(data) : null;
};

const getActiveGame = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  const data = await redis.get(gameKey);
  return data ? JSON.parse(data) : null;
};

const startGame = async (roomId, userId, username, amount) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  const lockKey = `dicebot:lock:${roomId}`;
  
  const lockAcquired = await redis.set(lockKey, '1', 'EX', 5, 'NX');
  if (!lockAcquired) {
    return { success: false, message: 'Please wait, another action is in progress.' };
  }
  
  try {
    const existingGame = await redis.get(gameKey);
    if (existingGame) {
      const game = JSON.parse(existingGame);
      if (game.status === 'waiting' || game.status === 'playing') {
        return { success: false, message: 'A game is already in progress. Use !j to join.' };
      }
    }
    
    const requestedAmount = parseInt(amount) || MIN_ENTRY;
    
    if (requestedAmount < MIN_ENTRY) {
      return { success: false, message: `Minimal ${formatCoins(MIN_ENTRY)} COINS to start game.` };
    }
    
    const entryAmount = Math.min(MAX_ENTRY, requestedAmount);
    
    const deductResult = await deductCredits(userId, entryAmount, username, `DiceBot Bet - Start game`);
    if (!deductResult.success) {
      return { success: false, message: `Not enough credits. You need ${formatCoins(entryAmount)} COINS to start.` };
    }
    
    // Track spending for merchant tag commission
    await merchantTagService.trackTaggedUserSpending(userId, 'dicebot', entryAmount);
    
    const gameId = Date.now();
    
    const game = {
      id: gameId,
      roomId,
      status: 'waiting',
      entryAmount,
      pot: entryAmount,
      currentRound: 0,
      botTarget: null,
      players: [{
        userId: userId,
        username,
        isEliminated: false,
        hasRolled: false,
        die1: null,
        die2: null,
        total: null,
        isIn: null,
        hasImmunity: false,
        earnedImmunity: false
      }],
      startedBy: userId,
      startedByUsername: username,
      createdAt: new Date().toISOString(),
      joinDeadline: Date.now() + JOIN_TIMEOUT
    };
    
    await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
    
    return {
      success: true,
      gameId,
      newBalance: deductResult.balance,
      message: `Game started by ${username}. Enter !j to join the game. Cost: ${formatCoins(entryAmount)} COINS [30s]`
    };
  } finally {
    await redis.del(lockKey);
  }
};

const joinGame = async (roomId, userId, username) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, message: 'No active game. Use !start [amount] to start one.' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'waiting') {
    return { success: false, message: 'Game already in progress. Wait for the next round.' };
  }
  
  if (Date.now() > game.joinDeadline) {
    return { success: false, message: 'Join period has ended.' };
  }
  
  const alreadyJoined = game.players.find(p => p.userId == userId);
  if (alreadyJoined) {
    return { success: false, message: 'You have already joined this game.' };
  }
  
  const deductResult = await deductCredits(userId, game.entryAmount, username, `DiceBot Bet - Join game`);
  if (!deductResult.success) {
    return { success: false, message: `Not enough credits. Entry costs ${formatCoins(game.entryAmount)} COINS.` };
  }
  
  // Track spending for merchant tag commission
  await merchantTagService.trackTaggedUserSpending(userId, 'dicebot', game.entryAmount);
  
  game.players.push({
    userId,
    username,
    isEliminated: false,
    hasRolled: false,
    die1: null,
    die2: null,
    total: null,
    isIn: null,
    hasImmunity: false,
    earnedImmunity: false
  });
  game.pot += game.entryAmount;
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  return {
    success: true,
    message: `${username} joined the game.`,
    playerCount: game.players.length,
    pot: game.pot,
    newBalance: deductResult.balance
  };
};

const beginGame = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'waiting') {
    return null;
  }
  
  if (game.players.length < 2) {
    for (const player of game.players) {
      await addCredits(player.userId, game.entryAmount, player.username, 'DiceBot Refund - Not enough players');
    }
    await redis.del(gameKey);
    return { cancelled: true, message: 'Not enough players. Game cancelled. Credits refunded.' };
  }
  
  game.status = 'playing';
  game.currentRound = 0;
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const playerNames = game.players.map(p => p.username).join(', ');
  
  return {
    started: true,
    message: `Game begins! Bot rolls first, match or beat total to stay IN!, ready to roll in 3 seconds.`,
    playerNames,
    playerCount: game.players.length,
    pot: game.pot
  };
};

const startNextRound = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'playing') return null;
  
  game.currentRound++;
  
  const { die1, die2, total } = rollDice();
  game.botTarget = { die1, die2, total };
  
  for (const player of game.players) {
    if (!player.isEliminated) {
      if (player.earnedImmunity) {
        player.hasImmunity = true;
        player.earnedImmunity = false;
      }
      player.hasRolled = false;
      player.die1 = null;
      player.die2 = null;
      player.total = null;
      player.isIn = null;
    }
  }
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const diceDisplay = formatDiceTags(die1, die2);
  
  return {
    round: game.currentRound,
    botDice: diceDisplay,
    botTarget: total,
    message: `ROUND #${game.currentRound}: Players. !r to ROLL. 20 seconds.`,
    targetMessage: `Bot rolled: ${diceDisplay} Your target is ${total}!`
  };
};

const rollPlayerDice = async (roomId, userId, username) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, message: 'No active game.' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'playing') {
    return { success: false, message: 'Game not in rolling phase.' };
  }
  
  if (!game.botTarget) {
    return { success: false, message: 'Round not started yet.' };
  }
  
  const player = game.players.find(p => p.userId == userId && !p.isEliminated);
  if (!player) {
    return { success: false, message: 'You are not in this game or have been eliminated.' };
  }
  
  if (player.hasRolled) {
    return { success: false, message: 'You have already rolled this round.' };
  }
  
  const { die1, die2, total } = rollDice();
  
  player.hasRolled = true;
  player.die1 = die1;
  player.die2 = die2;
  player.total = total;
  
  const meetsTarget = total >= game.botTarget.total;
  const gotDoubleSix = isDoubleSix(die1, die2);
  
  if (gotDoubleSix) {
    player.earnedImmunity = true;
  }
  
  if (meetsTarget || player.hasImmunity) {
    player.isIn = true;
  } else {
    player.isIn = false;
  }
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const diceDisplay = formatDiceTags(die1, die2);
  
  let status;
  let immunityMessage = '';
  
  if (gotDoubleSix) {
    status = 'IN!';
    immunityMessage = ' IMMUNITY for next round!';
  } else if (player.hasImmunity && !meetsTarget) {
    status = 'IMMUNE - stays IN!';
  } else {
    status = player.isIn ? 'IN!' : 'OUT!';
  }
  
  const activePlayers = game.players.filter(p => !p.isEliminated);
  const allRolled = activePlayers.every(p => p.hasRolled);
  
  return {
    success: true,
    username,
    die1,
    die2,
    total,
    diceDisplay,
    isIn: player.isIn,
    gotDoubleSix,
    usedImmunity: player.hasImmunity && !meetsTarget,
    allRolled,
    message: `${username} rolls: ${diceDisplay} ${status}${immunityMessage}`
  };
};

const autoRollForTimeout = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return [];
  
  const game = JSON.parse(gameData);
  const results = [];
  
  if (!game.botTarget) return [];
  
  const activePlayers = game.players.filter(p => !p.isEliminated && !p.hasRolled);
  
  for (const player of activePlayers) {
    const { die1, die2, total } = rollDice();
    
    player.hasRolled = true;
    player.die1 = die1;
    player.die2 = die2;
    player.total = total;
    
    const meetsTarget = total >= game.botTarget.total;
    const gotDoubleSix = isDoubleSix(die1, die2);
    
    if (gotDoubleSix) {
      player.earnedImmunity = true;
    }
    
    if (meetsTarget || player.hasImmunity) {
      player.isIn = true;
    } else {
      player.isIn = false;
    }
    
    const diceDisplay = formatDiceTags(die1, die2);
    
    let status;
    let immunityMessage = '';
    
    if (gotDoubleSix) {
      status = 'IN!';
      immunityMessage = ' IMMUNITY for next round!';
    } else if (player.hasImmunity && !meetsTarget) {
      status = 'IMMUNE - stays IN!';
    } else {
      status = player.isIn ? 'IN!' : 'OUT!';
    }
    
    results.push({
      username: player.username,
      die1,
      die2,
      total,
      diceDisplay,
      isIn: player.isIn,
      gotDoubleSix,
      usedImmunity: player.hasImmunity && !meetsTarget,
      message: `${player.username} rolls: ${diceDisplay} ${status}${immunityMessage}`
    });
  }
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  return results;
};

const tallyRound = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'playing') return null;
  
  const activePlayers = game.players.filter(p => !p.isEliminated);
  
  const survivors = activePlayers.filter(p => p.isIn === true);
  const eliminated = activePlayers.filter(p => p.isIn === false);
  
  for (const player of activePlayers) {
    if (player.hasImmunity) {
      player.hasImmunity = false;
    }
  }
  
  if (survivors.length === 0) {
    for (const player of activePlayers) {
      player.hasRolled = false;
      player.die1 = null;
      player.die2 = null;
      player.total = null;
      player.isIn = null;
    }
    game.botTarget = null;
    
    await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
    
    return {
      allFailed: true,
      message: "Nobody won, so we'll try again!",
      nextRound: game.currentRound + 1
    };
  }
  
  for (const player of eliminated) {
    player.isEliminated = true;
  }
  
  const remainingPlayers = game.players.filter(p => !p.isEliminated);
  
  if (remainingPlayers.length === 1) {
    await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
    return await finalizeGame(roomId);
  }
  
  game.botTarget = null;
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const playerNames = remainingPlayers.map(p => p.username).join(', ');
  
  return {
    roundComplete: true,
    eliminatedCount: eliminated.length,
    remainingPlayers: remainingPlayers.length,
    playerNames,
    message: `Players are: ${playerNames}`,
    followUp: `Players [${remainingPlayers.length}], next round starts in 3 seconds.`,
    nextRound: game.currentRound + 1
  };
};

const finalizeGame = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  const winner = game.players.find(p => !p.isEliminated);
  if (!winner) {
    await redis.del(gameKey);
    return { error: true, message: 'Game ended with no winner.' };
  }
  
  const houseFee = Math.floor(game.pot * HOUSE_FEE_PERCENT / 100);
  const winnings = game.pot - houseFee;
  
  const addResult = await addCredits(winner.userId, winnings, winner.username, `DiceBot Win - ${winnings} COINS`);
  
  game.status = 'finished';
  game.winnerId = winner.userId;
  game.winnerUsername = winner.username;
  game.winnings = winnings;
  game.houseFee = houseFee;
  game.finishedAt = new Date().toISOString();
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 60);
  
  setTimeout(async () => {
    await redis.del(gameKey);
  }, 60000);
  
  return {
    gameOver: true,
    winnerId: winner.userId,
    winnerUsername: winner.username,
    pot: game.pot,
    winnings,
    houseFee,
    newBalance: addResult.balance,
    message: `Dice game over! ${winner.username} WINS ${formatCoins(winnings)} COINS!\nCONGRATS!`,
    playAgain: `Play now: !start to enter. Cost: ${formatCoins(MIN_ENTRY)} COINS.\nFor custom entry, !start [amount]`
  };
};

const cancelGame = async (roomId, reason = 'Game cancelled') => {
  const redis = getRedisClient();
  const gameKey = `dicebot:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, message: 'No active game to cancel.' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.status === 'finished') {
    return { success: false, message: 'Game already finished.' };
  }
  
  for (const player of game.players) {
    if (!player.isEliminated) {
      await addCredits(player.userId, game.entryAmount, player.username, `DiceBot Refund - ${reason}`);
    }
  }
  
  await redis.del(gameKey);
  
  return { success: true, message: `Game cancelled. All active players refunded.` };
};

module.exports = {
  JOIN_TIMEOUT,
  ROLL_TIMEOUT,
  COUNTDOWN_DELAY,
  MIN_ENTRY,
  MAX_ENTRY,
  HOUSE_FEE_PERCENT,
  rollDice,
  formatDiceTags,
  isDoubleSix,
  formatCoins,
  getDiceEmoji,
  getDiceCode,
  formatDiceRoll,
  formatDiceRollEmoji,
  isBalakSix,
  getUserCredits,
  deductCredits,
  addCredits,
  isRoomManaged,
  isRoomAdmin,
  isSystemAdmin,
  addBotToRoom,
  removeBotFromRoom,
  isBotActive,
  getBotStatus,
  getActiveGame,
  startGame,
  joinGame,
  beginGame,
  startNextRound,
  rollPlayerDice,
  autoRollForTimeout,
  tallyRound,
  finalizeGame,
  cancelGame
};
