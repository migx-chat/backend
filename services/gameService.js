const { query, getClient } = require('../db/db');
const { generateGameId } = require('../utils/idGenerator');
const { checkGameLimit } = require('../utils/floodControl');
const creditService = require('./creditService');
const merchantService = require('./merchantService');
const { addXp, XP_REWARDS } = require('../utils/xpLeveling');

const GAME_TYPES = {
  COIN_FLIP: 'coin_flip',
  DICE_ROLL: 'dice_roll',
  NUMBER_GUESS: 'number_guess',
  SLOTS: 'slots',
  ROCK_PAPER_SCISSORS: 'rock_paper_scissors'
};

const GAME_MULTIPLIERS = {
  [GAME_TYPES.COIN_FLIP]: 1.9,
  [GAME_TYPES.DICE_ROLL]: 5.5,
  [GAME_TYPES.NUMBER_GUESS]: 9,
  [GAME_TYPES.SLOTS]: 10,
  [GAME_TYPES.ROCK_PAPER_SCISSORS]: 1.9
};

const playGame = async (userId, username, gameType, betAmount, choice, merchantId = null, io = null) => {
  try {
    const rateCheck = await checkGameLimit(userId);
    if (!rateCheck.allowed) {
      return { success: false, error: rateCheck.message };
    }
    
    if (betAmount < 1) {
      return { success: false, error: 'Minimum bet is 1 credit' };
    }
    
    if (betAmount > 10000) {
      return { success: false, error: 'Maximum bet is 10,000 credits' };
    }
    
    const deductResult = await creditService.deductCredits(
      userId, 
      betAmount, 
      'game_spend', 
      `Game: ${gameType}`
    );
    
    if (!deductResult.success) {
      return { success: false, error: deductResult.error };
    }
    
    let gameResult;
    switch (gameType) {
      case GAME_TYPES.COIN_FLIP:
        gameResult = playCoinFlip(choice);
        break;
      case GAME_TYPES.DICE_ROLL:
        gameResult = playDiceRoll(choice);
        break;
      case GAME_TYPES.NUMBER_GUESS:
        gameResult = playNumberGuess(choice);
        break;
      case GAME_TYPES.SLOTS:
        gameResult = playSlots();
        break;
      case GAME_TYPES.ROCK_PAPER_SCISSORS:
        gameResult = playRockPaperScissors(choice);
        break;
      default:
        return { success: false, error: 'Invalid game type' };
    }
    
    const multiplier = GAME_MULTIPLIERS[gameType] || 2;
    const rewardAmount = gameResult.win ? Math.floor(betAmount * multiplier) : 0;
    
    if (rewardAmount > 0) {
      await creditService.addCredits(userId, rewardAmount, 'reward', `Won ${gameType}`);
      
      if (merchantId) {
        await merchantService.recordTaggedUserWin(merchantId, userId, username, gameType, rewardAmount);
      }
    }
    
    await query(
      `INSERT INTO game_history (user_id, username, game_type, bet_amount, result, reward_amount, merchant_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, username, gameType, betAmount, gameResult.win ? 'win' : 'lose', rewardAmount, merchantId]
    );
    
    await addXp(userId, XP_REWARDS.PLAY_GAME, 'play_game', io);
    if (gameResult.win) {
      await addXp(userId, XP_REWARDS.WIN_GAME, 'win_game', io);
    }
    
    const newBalance = await creditService.getBalance(userId);
    
    return {
      success: true,
      gameId: generateGameId(),
      gameType,
      betAmount,
      choice,
      result: gameResult,
      win: gameResult.win,
      rewardAmount,
      newBalance,
      multiplier
    };
  } catch (error) {
    console.error('Error playing game:', error);
    return { success: false, error: 'Game failed' };
  }
};

const playCoinFlip = (choice) => {
  const outcomes = ['heads', 'tails'];
  const result = outcomes[Math.floor(Math.random() * 2)];
  return {
    outcome: result,
    playerChoice: choice,
    win: choice.toLowerCase() === result
  };
};

const playDiceRoll = (choice) => {
  const result = Math.floor(Math.random() * 6) + 1;
  const playerNumber = parseInt(choice);
  return {
    outcome: result,
    playerChoice: playerNumber,
    win: playerNumber === result
  };
};

const playNumberGuess = (choice) => {
  const result = Math.floor(Math.random() * 10) + 1;
  const playerNumber = parseInt(choice);
  return {
    outcome: result,
    playerChoice: playerNumber,
    win: playerNumber === result
  };
};

const playSlots = () => {
  const symbols = ['7', 'BAR', 'CHERRY', 'LEMON', 'BELL', 'STAR'];
  const reel1 = symbols[Math.floor(Math.random() * symbols.length)];
  const reel2 = symbols[Math.floor(Math.random() * symbols.length)];
  const reel3 = symbols[Math.floor(Math.random() * symbols.length)];
  
  const win = reel1 === reel2 && reel2 === reel3;
  const partialWin = reel1 === reel2 || reel2 === reel3 || reel1 === reel3;
  
  return {
    outcome: [reel1, reel2, reel3],
    win,
    partialWin: !win && partialWin,
    jackpot: win && reel1 === '7'
  };
};

const playRockPaperScissors = (choice) => {
  const options = ['rock', 'paper', 'scissors'];
  const computerChoice = options[Math.floor(Math.random() * 3)];
  const playerChoice = choice.toLowerCase();
  
  let win = false;
  let draw = false;
  
  if (playerChoice === computerChoice) {
    draw = true;
  } else if (
    (playerChoice === 'rock' && computerChoice === 'scissors') ||
    (playerChoice === 'paper' && computerChoice === 'rock') ||
    (playerChoice === 'scissors' && computerChoice === 'paper')
  ) {
    win = true;
  }
  
  return {
    outcome: computerChoice,
    playerChoice,
    win,
    draw
  };
};

const getGameHistory = async (userId, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT * FROM game_history
       WHERE user_id = $1
       ORDER BY created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting game history:', error);
    return [];
  }
};

const getGameStats = async (userId) => {
  try {
    const result = await query(
      `SELECT 
        COUNT(*) as total_games,
        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'lose' THEN 1 ELSE 0 END) as losses,
        SUM(bet_amount) as total_bet,
        SUM(reward_amount) as total_won
       FROM game_history
       WHERE user_id = $1`,
      [userId]
    );
    
    const stats = result.rows[0];
    return {
      totalGames: parseInt(stats.total_games) || 0,
      wins: parseInt(stats.wins) || 0,
      losses: parseInt(stats.losses) || 0,
      totalBet: parseInt(stats.total_bet) || 0,
      totalWon: parseInt(stats.total_won) || 0,
      winRate: stats.total_games > 0 ? ((stats.wins / stats.total_games) * 100).toFixed(2) : 0
    };
  } catch (error) {
    console.error('Error getting game stats:', error);
    return null;
  }
};

const getLeaderboard = async (gameType = null, limit = 10) => {
  try {
    let queryStr = `
      SELECT user_id, username, 
        SUM(reward_amount) as total_winnings,
        COUNT(*) as games_played
      FROM game_history
      WHERE result = 'win'
    `;
    
    const params = [];
    if (gameType) {
      queryStr += ' AND game_type = $1';
      params.push(gameType);
    }
    
    queryStr += ` GROUP BY user_id, username
                  ORDER BY total_winnings DESC
                  LIMIT $${params.length + 1}`;
    params.push(limit);
    
    const result = await query(queryStr, params);
    return result.rows;
  } catch (error) {
    console.error('Error getting game leaderboard:', error);
    return [];
  }
};

module.exports = {
  GAME_TYPES,
  GAME_MULTIPLIERS,
  playGame,
  getGameHistory,
  getGameStats,
  getLeaderboard
};
