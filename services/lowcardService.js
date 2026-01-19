const { query } = require('../db/db');
const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');
const merchantTagService = require('./merchantTagService');
const gameStateManager = require('./gameStateManager');

const CARD_SUITS = ['h', 'd', 'c', 's'];
const CARD_VALUES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];

const JOIN_TIMEOUT = 30000;
const DRAW_TIMEOUT = 20000;
const COUNTDOWN_DELAY = 3000;
const MIN_ENTRY = 1;              // Min bet for regular Lowcard room
const MAX_ENTRY = 999999999;      // No limit
const MIN_ENTRY_BIG_GAME = 50;    // Min bet for Big Game room

const getCardCode = (value) => {
  if (value === 11) return 'j';
  if (value === 12) return 'q';
  if (value === 13) return 'k';
  if (value === 14) return 'a';
  return value.toString();
};

const generateDeck = () => {
  const deck = [];
  for (const suit of CARD_SUITS) {
    for (const value of CARD_VALUES) {
      const code = `lc_${getCardCode(value)}${suit}`;
      deck.push({ value, suit, code, image: `${code}.png` });
    }
  }
  return shuffleDeck(deck);
};

const shuffleDeck = (deck) => {
  const shuffled = [...deck];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
};

const getCardEmoji = (card) => {
  if (!card) return '(?)';
  return `[CARD:${card.code}]`;
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
    logger.error('LOWCARD_GET_CREDITS_ERROR', error);
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
    logger.error('LOWCARD_LOG_TRANSACTION_ERROR', error);
  }
};

const deductCredits = async (userId, amount, username = null, reason = null, gameSessionId = null) => {
  try {
    const redis = getRedisClient();
    
    const taggedBalance = await merchantTagService.getTaggedBalance(userId);
    let usedTaggedCredits = 0;
    let remainingAmount = amount;
    
    if (taggedBalance > 0) {
      const consumeResult = await merchantTagService.consumeForGame(userId, 'lowcard', amount, gameSessionId);
      if (consumeResult.success) {
        usedTaggedCredits = consumeResult.usedTaggedCredits || 0;
        remainingAmount = consumeResult.remainingAmount;
        if (usedTaggedCredits > 0) {
          logger.info('LOWCARD_TAGGED_CREDITS_USED', { userId, usedTaggedCredits, remainingAmount });
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
    logger.error('LOWCARD_DEDUCT_CREDITS_ERROR', error);
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
    logger.error('LOWCARD_ADD_CREDITS_ERROR', error);
    return { success: false, balance: 0 };
  }
};

const isRoomManaged = async (roomId) => {
  try {
    const result = await query('SELECT owner_id FROM rooms WHERE id = $1', [roomId]);
    return result.rows.length > 0 && result.rows[0].owner_id !== null;
  } catch (error) {
    logger.error('LOWCARD_CHECK_ROOM_ERROR', error);
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
    logger.error('LOWCARD_CHECK_ADMIN_ERROR', error);
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
    logger.error('LOWCARD_CHECK_SYSADMIN_ERROR', error);
    return false;
  }
};

const addBotToRoom = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `lowcard:bot:${roomId}`;
  
  const exists = await redis.exists(botKey);
  if (exists) {
    return { success: false, message: 'LowCardBot is already active in this room.' };
  }
  
  const dicebotActive = await redis.exists(`dicebot:bot:${roomId}`);
  if (dicebotActive) {
    return { success: false, message: 'DiceBot is active. Remove it first with /bot dice remove' };
  }
  
  const legendActive = await redis.exists(`legend:bot:${roomId}`);
  if (legendActive) {
    return { success: false, message: 'FlagBot is active. Remove it first.' };
  }
  
  await redis.set(botKey, JSON.stringify({
    active: true,
    defaultAmount: 50,
    createdAt: new Date().toISOString()
  }), 'EX', 86400 * 7);
  
  await gameStateManager.setActiveGameType(roomId, gameStateManager.GAME_TYPES.LOWCARD);
  
  return { success: true, message: `[PVT] Bot is running. Min: ${MIN_ENTRY} COINS` };
};

const removeBotFromRoom = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `lowcard:bot:${roomId}`;
  const gameKey = `lowcard:game:${roomId}`;
  
  const exists = await redis.exists(botKey);
  if (!exists) {
    return { success: false, message: 'No LowCard bot in this room.' };
  }
  
  const gameData = await redis.get(gameKey);
  if (gameData) {
    const game = JSON.parse(gameData);
    if (game.status === 'waiting') {
      for (const player of game.players) {
        await addCredits(player.userId, game.entryAmount, player.username, 'LowCard Refund - Bot removed');
      }
    }
  }
  
  await redis.del(botKey);
  await redis.del(gameKey);
  await clearDeck(roomId);
  
  await gameStateManager.clearActiveGameType(roomId);
  
  return { success: true, message: 'LowCardBot has left the room.' };
};

const isBotActive = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `lowcard:bot:${roomId}`;
  return await redis.exists(botKey);
};

const getBotStatus = async (roomId) => {
  const redis = getRedisClient();
  const botKey = `lowcard:bot:${roomId}`;
  const data = await redis.get(botKey);
  return data ? JSON.parse(data) : null;
};

const startGame = async (roomId, userId, username, amount) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  const lockKey = `lowcard:lock:${roomId}`;
  
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
    
    const roomResult = await query('SELECT name FROM rooms WHERE id = $1', [roomId]);
    const roomName = roomResult.rows[0]?.name || '';
    const isBigGame = roomName.toLowerCase().includes('big game');
    const minEntry = isBigGame ? MIN_ENTRY_BIG_GAME : MIN_ENTRY;
    
    const requestedAmount = parseInt(amount) || minEntry;
    
    if (requestedAmount < minEntry) {
      return { success: false, message: `Minimal ${minEntry.toLocaleString()} COINS to start game.` };
    }
    
    // Max entry only applies to regular Lowcard room, Big Game has no max limit
    if (!isBigGame && requestedAmount > MAX_ENTRY) {
      return { success: false, message: `Maximal ${MAX_ENTRY.toLocaleString()} COINS to start game.` };
    }
    
    const entryAmount = requestedAmount;
    
    const deductResult = await deductCredits(userId, entryAmount, username, `LowCard Bet - Start game`);
    if (!deductResult.success) {
      return { success: false, message: `Not enough credits. You need ${entryAmount} COINS to start.` };
    }
    
    // Track spending for merchant tag commission
    await merchantTagService.trackTaggedUserSpending(userId, 'lowcard', entryAmount);
    
    const gameId = Date.now();
    
    const game = {
      id: gameId,
      roomId,
      status: 'waiting',
      entryAmount,
      pot: entryAmount,
      currentRound: 0,
      players: [{
        userId: userId,
        username,
        isEliminated: false,
        hasDrawn: false,
        currentCard: null
      }],
      startedBy: userId,
      startedByUsername: username,
      createdAt: new Date().toISOString(),
      joinDeadline: Date.now() + JOIN_TIMEOUT
    };
    
    await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
    
    await query(
      `INSERT INTO lowcard_games (room_id, status, entry_amount, pot_amount, started_by, started_by_username)
       VALUES ($1, 'waiting', $2, $3, $4, $5)`,
      [roomId, entryAmount, entryAmount, userId, username]
    ).catch(err => logger.error('LOWCARD_DB_INSERT_ERROR', err));
    
    return {
      success: true,
      gameId,
      newBalance: deductResult.balance,
      message: `LowCard started by ${username}. Enter !j to join the game. Cost: ${entryAmount} COINS [30s]`
    };
  } finally {
    await redis.del(lockKey);
  }
};

const joinGame = async (roomId, userId, username) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
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
  
  const deductResult = await deductCredits(userId, game.entryAmount, username, `LowCard Bet - Join game`);
  if (!deductResult.success) {
    return { success: false, message: `Not enough credits. Entry costs ${game.entryAmount} COINS.` };
  }
  
  // Track spending for merchant tag commission
  await merchantTagService.trackTaggedUserSpending(userId, 'lowcard', game.entryAmount);
  
  game.players.push({
    userId,
    username,
    isEliminated: false,
    hasDrawn: false,
    currentCard: null
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
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'waiting') {
    return null;
  }
  
  if (game.players.length < 2) {
    for (const player of game.players) {
      await addCredits(player.userId, game.entryAmount, player.username, 'LowCard Refund - Not enough players');
    }
    await redis.del(gameKey);
    return { cancelled: true, message: 'Not enough players. Game cancelled. Credits refunded.' };
  }
  
  game.status = 'playing';
  game.currentRound = 1;
  delete game.deck;
  
  await initializeDeck(roomId);
  
  for (const player of game.players) {
    player.hasDrawn = false;
    player.currentCard = null;
  }
  
  game.countdownEndsAt = Date.now() + COUNTDOWN_DELAY;
  game.roundDeadline = Date.now() + COUNTDOWN_DELAY + DRAW_TIMEOUT;
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const playerNames = game.players.map(p => p.username).join(', ');
  
  return {
    started: true,
    playerCount: game.players.length,
    playerNames,
    message: 'Game begins! Lowest card is OUT!, ready to draw in 3 seconds.'
  };
};

const drawCardFromDeck = async (roomId) => {
  const redis = getRedisClient();
  const deckKey = `lowcard:deck:${roomId}`;
  
  let deckData = await redis.get(deckKey);
  let deck = deckData ? JSON.parse(deckData) : null;
  
  if (!deck || deck.length === 0) {
    deck = generateDeck();
  }
  
  const card = deck.pop();
  await redis.set(deckKey, JSON.stringify(deck), 'EX', 3600);
  
  return card;
};

const initializeDeck = async (roomId) => {
  const redis = getRedisClient();
  const deckKey = `lowcard:deck:${roomId}`;
  const deck = generateDeck();
  await redis.set(deckKey, JSON.stringify(deck), 'EX', 3600);
  return deck;
};

const clearDeck = async (roomId) => {
  const redis = getRedisClient();
  const deckKey = `lowcard:deck:${roomId}`;
  await redis.del(deckKey);
};

const drawCardForPlayer = async (roomId, userId, username) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, message: 'No active game.' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.status !== 'playing') {
    return { success: false, message: 'Game is not in progress.' };
  }
  
  if (game.countdownEndsAt && Date.now() < game.countdownEndsAt) {
    return { success: false, message: 'Wait for countdown to finish.', silent: true };
  }
  
  const playerIndex = game.players.findIndex(p => p.userId == userId && !p.isEliminated);
  if (playerIndex === -1) {
    return { success: false, message: 'You are not in this game or already eliminated.' };
  }
  
  const player = game.players[playerIndex];
  
  if (player.hasDrawn) {
    return { success: false, message: 'You have already drawn this round.' };
  }
  
  const card = await drawCardFromDeck(roomId);
  game.players[playerIndex].currentCard = card;
  game.players[playerIndex].hasDrawn = true;
  
  delete game.deck;
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  return {
    success: true,
    card,
    cardDisplay: getCardEmoji(card),
    message: `${username}: ${getCardEmoji(card)}`
  };
};

const autoDrawForTimeout = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return [];
  
  const game = JSON.parse(gameData);
  const autoDrawn = [];
  
  for (let i = 0; i < game.players.length; i++) {
    const player = game.players[i];
    if (!player.isEliminated && !player.hasDrawn) {
      const card = await drawCardFromDeck(roomId);
      game.players[i].currentCard = card;
      game.players[i].hasDrawn = true;
      autoDrawn.push({
        username: player.username,
        card,
        cardDisplay: getCardEmoji(card),
        message: `Bot draws - ${player.username}: ${getCardEmoji(card)}`
      });
    }
  }
  
  delete game.deck;
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  return autoDrawn;
};

const tallyRound = async (roomId, isTimedOut = false) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) return null;
  
  const game = JSON.parse(gameData);
  
  const activePlayers = game.players.filter(p => !p.isEliminated && p.currentCard);
  
  if (activePlayers.length === 0) {
    return { error: true, message: 'No active players with cards.' };
  }
  
  if (activePlayers.length === 3 && !isTimedOut) {
    const highestValue = Math.max(...activePlayers.map(p => p.currentCard.value));
    const winners = activePlayers.filter(p => p.currentCard.value === highestValue);
    
    if (winners.length === 1) {
      const winner = winners[0];
      game.status = 'finished';
      
      const commission = Math.floor(game.pot * 0.05);
      const winnings = game.pot - commission;
      
      const creditResult = await addCredits(winner.userId, winnings, winner.username, `LowCard Win - Pot ${game.pot} COINS`);
      
      await query(
        `UPDATE lowcard_games SET status = 'finished', winner_id = $1, winner_username = $2, pot_amount = $3, finished_at = NOW()
         WHERE room_id = $4 AND status = 'playing'`,
        [winner.userId, winner.username, game.pot, roomId]
      ).catch(err => logger.error('LOWCARD_DB_UPDATE_ERROR', err));
      
      await query(
        `INSERT INTO lowcard_history (game_id, winner_id, winner_username, total_pot, commission, players_count)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [game.id, winner.userId, winner.username, game.pot, commission, game.players.length]
      ).catch(err => logger.error('LOWCARD_HISTORY_INSERT_ERROR', err));
      
      await redis.del(gameKey);
      await clearDeck(roomId);
      
      return {
        gameOver: true,
        winner: winner.username,
        winnerId: winner.userId,
        winnings,
        newBalance: creditResult.balance,
        message: `LowCard game over! ${winner.username} WINS ${winnings.toFixed(1)} COINS with highest card ${getCardEmoji(winner.currentCard)}! CONGRATS!`,
        followUp: `Play now: !start to enter. Cost: ${game.entryAmount} COINS. For custom entry, !start [amount]`
      };
    }
  }
  
  const lowestValue = Math.min(...activePlayers.map(p => p.currentCard.value));
  const losers = activePlayers.filter(p => p.currentCard.value === lowestValue);
  
  if (losers.length > 1 && losers.length < activePlayers.length) {
    if (isTimedOut && game.previousTiedLosers && game.previousTiedLosers.length > 0) {
      const previousLoserIds = game.previousTiedLosers;
      for (const loserId of previousLoserIds) {
        const idx = game.players.findIndex(p => p.userId == loserId);
        if (idx !== -1) {
          game.players[idx].isEliminated = true;
        }
      }
      
      delete game.previousTiedLosers;
      
      const remainingPlayers = game.players.filter(p => !p.isEliminated);
      
      if (remainingPlayers.length === 1) {
        const winner = remainingPlayers[0];
        game.status = 'finished';
        
        const commission = Math.floor(game.pot * 0.05);
        const winnings = game.pot - commission;
        
        const creditResult = await addCredits(winner.userId, winnings, winner.username, `LowCard Win - Pot ${game.pot} COINS`);
        
        await query(
          `UPDATE lowcard_games SET status = 'finished', winner_id = $1, winner_username = $2, pot_amount = $3, finished_at = NOW()
           WHERE room_id = $4 AND status = 'playing'`,
          [winner.userId, winner.username, game.pot, roomId]
        ).catch(err => logger.error('LOWCARD_DB_UPDATE_ERROR', err));
        
        await query(
          `INSERT INTO lowcard_history (game_id, winner_id, winner_username, total_pot, commission, players_count)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [game.id, winner.userId, winner.username, game.pot, commission, game.players.length]
        ).catch(err => logger.error('LOWCARD_HISTORY_INSERT_ERROR', err));
        
        await redis.del(gameKey);
        await clearDeck(roomId);
        
        return {
          gameOver: true,
          winner: winner.username,
          winnerId: winner.userId,
          winnings,
          newBalance: creditResult.balance,
          message: `LowCard game over! ${winner.username} WINS ${winnings.toFixed(1)} COINS! CONGRATS!`,
          followUp: `Play now: !start to enter. Cost: ${game.entryAmount} COINS. For custom entry, !start [amount]`
        };
      }
      
      game.currentRound++;
      for (let i = 0; i < game.players.length; i++) {
        if (!game.players[i].isEliminated) {
          game.players[i].hasDrawn = false;
          game.players[i].currentCard = null;
        }
      }
      game.countdownEndsAt = Date.now() + COUNTDOWN_DELAY;
      game.roundDeadline = Date.now() + COUNTDOWN_DELAY + DRAW_TIMEOUT;
      
      await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
      
      const eliminatedNames = previousLoserIds.map(id => {
        const p = game.players.find(pl => pl.userId == id);
        return p ? p.username : 'Unknown';
      });
      
      const remainingNames = remainingPlayers.map(p => p.username).join(', ');
      return {
        eliminated: eliminatedNames.map(name => `${name}: OUT! (Tie-breaker timeout)`),
        remainingCount: remainingPlayers.length,
        nextRound: game.currentRound,
        message: eliminatedNames.map(name => `${name}: OUT! (Tie-breaker timeout)`).join('\n'),
        followUp: `Players [${remainingPlayers.length}], next round starts in 3 seconds.`,
        playerList: `Players are: ${remainingNames}`
      };
    }
    
    game.previousTiedLosers = losers.map(p => p.userId);
    
    for (let i = 0; i < game.players.length; i++) {
      if (!game.players[i].isEliminated) {
        const isTied = losers.find(l => l.userId === game.players[i].userId);
        if (isTied) {
          game.players[i].hasDrawn = false;
          game.players[i].currentCard = null;
        }
      }
    }
    
    await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
    
    const tiedNames = losers.map(p => p.username).join(', ');
    return {
      tie: true,
      tiedPlayers: losers.map(p => p.username),
      message: `Tied players: ${tiedNames}`,
      followUp: 'Tied players ONLY draw again. Next round starts in 3 seconds.'
    };
  }
  
  for (const loser of losers) {
    const idx = game.players.findIndex(p => p.userId === loser.userId);
    if (idx !== -1) {
      game.players[idx].isEliminated = true;
    }
  }
  
  delete game.previousTiedLosers;
  
  const remainingPlayers = game.players.filter(p => !p.isEliminated);
  
  if (remainingPlayers.length === 1) {
    const winner = remainingPlayers[0];
    game.status = 'finished';
    
    const commission = Math.floor(game.pot * 0.05);
    const winnings = game.pot - commission;
    
    const creditResult = await addCredits(winner.userId, winnings, winner.username, `LowCard Win - Pot ${game.pot} COINS`);
    
    await query(
      `UPDATE lowcard_games SET status = 'finished', winner_id = $1, winner_username = $2, pot_amount = $3, finished_at = NOW()
       WHERE room_id = $4 AND status = 'playing'`,
      [winner.userId, winner.username, game.pot, roomId]
    ).catch(err => logger.error('LOWCARD_DB_UPDATE_ERROR', err));
    
    await query(
      `INSERT INTO lowcard_history (game_id, winner_id, winner_username, total_pot, commission, players_count)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [game.id, winner.userId, winner.username, game.pot, commission, game.players.length]
    ).catch(err => logger.error('LOWCARD_HISTORY_INSERT_ERROR', err));
    
    await redis.del(gameKey);
    await clearDeck(roomId);
    
    return {
      gameOver: true,
      winner: winner.username,
      winnerId: winner.userId,
      winnings,
      newBalance: creditResult.balance,
      message: `LowCard game over! ${winner.username} WINS ${winnings.toFixed(1)} COINS! CONGRATS!`,
      followUp: `Play now: !start to enter. Cost: ${game.entryAmount} COINS. For custom entry, !start [amount]`
    };
  }
  
  let eliminatedMessages;
  if (isTimedOut) {
    eliminatedMessages = losers.map(p => `${p.username}: OUT! (Lowest card)`);
  } else {
    eliminatedMessages = losers.map(p => `${p.username}: OUT with the lowest card! ${getCardEmoji(p.currentCard)}`);
  }
  
  game.currentRound++;
  for (let i = 0; i < game.players.length; i++) {
    if (!game.players[i].isEliminated) {
      game.players[i].hasDrawn = false;
      game.players[i].currentCard = null;
    }
  }
  game.countdownEndsAt = Date.now() + COUNTDOWN_DELAY;
  game.roundDeadline = Date.now() + COUNTDOWN_DELAY + DRAW_TIMEOUT;
  
  await redis.set(gameKey, JSON.stringify(game), 'EX', 3600);
  
  const remainingNames = remainingPlayers.map(p => p.username).join(', ');
  return {
    eliminated: eliminatedMessages,
    remainingCount: remainingPlayers.length,
    nextRound: game.currentRound,
    message: eliminatedMessages.join('\n'),
    followUp: `Players [${remainingPlayers.length}], next round starts in 3 seconds.`,
    playerList: `Players are: ${remainingNames}`
  };
};

const stopGame = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  if (!gameData) {
    return { success: false, message: 'No active game to stop.' };
  }
  
  const game = JSON.parse(gameData);
  
  if (game.status === 'playing') {
    return { success: false, message: 'Cannot stop game once it has started.' };
  }
  
  for (const player of game.players) {
    await addCredits(player.userId, game.entryAmount, player.username, 'LowCard Refund - Game stopped');
  }
  
  await redis.del(gameKey);
  await clearDeck(roomId);
  
  return { success: true, message: 'Game stopped. All credits have been refunded.' };
};

const getActiveGame = async (roomId) => {
  const redis = getRedisClient();
  const gameKey = `lowcard:game:${roomId}`;
  
  const gameData = await redis.get(gameKey);
  return gameData ? JSON.parse(gameData) : null;
};

const getActivePlayers = async (roomId) => {
  const game = await getActiveGame(roomId);
  if (!game) return [];
  return game.players.filter(p => !p.isEliminated);
};

const allPlayersDrawn = async (roomId) => {
  const game = await getActiveGame(roomId);
  if (!game || game.status !== 'playing') return false;
  
  const activePlayers = game.players.filter(p => !p.isEliminated);
  if (activePlayers.length === 0) return false;
  
  return activePlayers.every(p => p.hasDrawn === true);
};

module.exports = {
  isRoomManaged,
  isRoomAdmin,
  isSystemAdmin,
  addBotToRoom,
  removeBotFromRoom,
  isBotActive,
  getBotStatus,
  startGame,
  joinGame,
  beginGame,
  drawCardForPlayer,
  autoDrawForTimeout,
  tallyRound,
  stopGame,
  getActiveGame,
  getActivePlayers,
  allPlayersDrawn,
  addCredits,
  JOIN_TIMEOUT,
  DRAW_TIMEOUT,
  COUNTDOWN_DELAY
};
