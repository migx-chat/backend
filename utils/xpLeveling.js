const { query } = require('../db/db');
const { client } = require('../redis');

const XP_REWARDS = {
  SEND_MESSAGE: 1,
  JOIN_ROOM: 5,
  PLAY_GAME: 3,
  TRANSFER_CREDIT: 2,
  WIN_GAME: 10,
  FIRST_MESSAGE_DAY: 20,
  DAILY_LOGIN: 15
};

const LEVEL_THRESHOLDS = [
  0,      // Level 1
  20,     // Level 2 - Very easy
  50,     // Level 3
  100,    // Level 4
  160,    // Level 5
  240,    // Level 6
  340,    // Level 7
  460,    // Level 8
  600,    // Level 9
  800,    // Level 10 - ~4 days active use
  1100,   // Level 11
  1500,   // Level 12
  2000,   // Level 13
  2600,   // Level 14
  3300,   // Level 15
  4100,   // Level 16
  5000,   // Level 17
  6000,   // Level 18
  7200,   // Level 19
  8600,   // Level 20
  10200,  // Level 21
  12000,  // Level 22
  14000,  // Level 23
  16200,  // Level 24
  18600,  // Level 25
  21200,  // Level 26
  24000,  // Level 27
  27000,  // Level 28
  30200,  // Level 29
  33600   // Level 30
];

const getXpThreshold = (level) => {
  if (level <= 0) return 0;
  if (level <= LEVEL_THRESHOLDS.length) {
    return LEVEL_THRESHOLDS[level - 1];
  }
  const lastThreshold = LEVEL_THRESHOLDS[LEVEL_THRESHOLDS.length - 1];
  const extraLevels = level - LEVEL_THRESHOLDS.length;
  return lastThreshold + (extraLevels * 4000) + (extraLevels * extraLevels * 500);
};

const calculateLevel = (xp) => {
  let level = 1;
  while (getXpThreshold(level + 1) <= xp) {
    level++;
  }
  return level;
};

const getXpForNextLevel = (currentLevel) => {
  return getXpThreshold(currentLevel + 1);
};

const getLevelProgress = (xp, level) => {
  const currentLevelXp = getXpThreshold(level);
  const nextLevelXp = getXpThreshold(level + 1);
  const progress = ((xp - currentLevelXp) / (nextLevelXp - currentLevelXp)) * 100;
  return Math.min(Math.max(progress, 0), 100);
};

const addXp = async (userId, amount, action, io = null) => {
  try {
    const result = await query(
      `INSERT INTO user_levels (user_id, xp, level)
       VALUES ($1, $2, 1)
       ON CONFLICT (user_id) 
       DO UPDATE SET xp = user_levels.xp + $2, updated_at = CURRENT_TIMESTAMP
       RETURNING xp, level`,
      [userId, amount]
    );
    
    const { xp, level: oldLevel } = result.rows[0];
    const newLevel = calculateLevel(xp);
    
    if (newLevel > oldLevel) {
      await query(
        'UPDATE user_levels SET level = $1 WHERE user_id = $2',
        [newLevel, userId]
      );
      
      const userResult = await query('SELECT username FROM users WHERE id = $1', [userId]);
      const username = userResult.rows[0]?.username || 'User';
      
      if (io) {
        const socketId = await client.get(`user:${userId}:socket`);
        if (socketId) {
          io.to(socketId).emit('user:levelUp', {
            userId,
            username,
            oldLevel,
            newLevel,
            xp,
            nextLevelXp: getXpForNextLevel(newLevel)
          });
        }
      }
      
      return { xp, level: newLevel, leveledUp: true, oldLevel };
    }
    
    return { xp, level: newLevel, leveledUp: false };
  } catch (error) {
    console.error('Error adding XP:', error);
    return null;
  }
};

const getUserLevel = async (userId) => {
  try {
    const result = await query(
      'SELECT xp, level FROM user_levels WHERE user_id = $1',
      [userId]
    );
    
    if (result.rows.length === 0) {
      await query(
        'INSERT INTO user_levels (user_id, xp, level) VALUES ($1, 0, 1)',
        [userId]
      );
      return { xp: 0, level: 1, progress: 0, nextLevelXp: getXpThreshold(2) };
    }
    
    const { xp, level } = result.rows[0];
    return {
      xp,
      level,
      progress: getLevelProgress(xp, level),
      nextLevelXp: getXpForNextLevel(level)
    };
  } catch (error) {
    console.error('Error getting user level:', error);
    return { xp: 0, level: 1, progress: 0, nextLevelXp: getXpThreshold(2) };
  }
};

const getLeaderboard = async (limit = 10) => {
  try {
    const result = await query(
      `SELECT ul.user_id, ul.xp, ul.level, u.username, u.avatar
       FROM user_levels ul
       JOIN users u ON ul.user_id = u.id
       ORDER BY ul.xp DESC
       LIMIT $1`,
      [limit]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting leaderboard:', error);
    return [];
  }
};

module.exports = {
  XP_REWARDS,
  calculateLevel,
  getXpForNextLevel,
  getLevelProgress,
  addXp,
  getUserLevel,
  getLeaderboard
};
