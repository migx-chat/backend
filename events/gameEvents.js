const gameService = require('../services/gameService');
const creditService = require('../services/creditService');
const merchantService = require('../services/merchantService');

module.exports = (io, socket) => {
  const playGame = async (data) => {
    try {
      const { userId, username, gameType, betAmount, choice, merchantId, roomId } = data;
      
      if (!userId || !username || !gameType || !betAmount) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const validGameTypes = Object.values(gameService.GAME_TYPES);
      if (!validGameTypes.includes(gameType)) {
        socket.emit('error', { message: 'Invalid game type' });
        return;
      }
      
      const result = await gameService.playGame(
        userId,
        username,
        gameType,
        betAmount,
        choice,
        merchantId,
        io
      );
      
      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }
      
      socket.emit('game:result', result);
      
      if (roomId) {
        io.to(`room:${roomId}`).emit('system:message', {
          roomId,
          message: result.win 
            ? `🎉 ${username} won ${result.rewardAmount} credits in ${gameType}!`
            : `🎲 ${username} played ${gameType} and lost ${betAmount} credits`,
          timestamp: new Date().toISOString(),
          type: 'game'
        });
      }
      
    } catch (error) {
      console.error('Error playing game:', error);
      socket.emit('error', { message: 'Game failed' });
    }
  };

  const getGameHistory = async (data) => {
    try {
      const { userId, limit = 50, offset = 0 } = data;
      
      if (!userId) {
        socket.emit('error', { message: 'User ID required' });
        return;
      }
      
      const history = await gameService.getGameHistory(userId, limit, offset);
      
      socket.emit('game:history', {
        userId,
        games: history,
        hasMore: history.length === limit
      });
      
    } catch (error) {
      console.error('Error getting game history:', error);
      socket.emit('error', { message: 'Failed to get game history' });
    }
  };

  const getGameStats = async (data) => {
    try {
      const { userId } = data;
      
      if (!userId) {
        socket.emit('error', { message: 'User ID required' });
        return;
      }
      
      const stats = await gameService.getGameStats(userId);
      
      socket.emit('game:stats', {
        userId,
        stats
      });
      
    } catch (error) {
      console.error('Error getting game stats:', error);
      socket.emit('error', { message: 'Failed to get game stats' });
    }
  };

  const getGameLeaderboard = async (data) => {
    try {
      const { gameType = null, limit = 10 } = data || {};
      
      const leaderboard = await gameService.getLeaderboard(gameType, limit);
      
      socket.emit('game:leaderboard', {
        gameType,
        leaderboard
      });
      
    } catch (error) {
      console.error('Error getting game leaderboard:', error);
      socket.emit('error', { message: 'Failed to get leaderboard' });
    }
  };

  const getGameTypes = async () => {
    try {
      const gameTypes = Object.entries(gameService.GAME_TYPES).map(([key, value]) => ({
        id: value,
        name: key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, l => l.toUpperCase()),
        multiplier: gameService.GAME_MULTIPLIERS[value]
      }));
      
      socket.emit('game:types', {
        games: gameTypes
      });
      
    } catch (error) {
      console.error('Error getting game types:', error);
      socket.emit('error', { message: 'Failed to get game types' });
    }
  };

  const gameSpend = async (data) => {
    try {
      const { userId, username, gameType, winAmount, merchantId } = data;
      
      if (!userId || !gameType || !winAmount || !merchantId) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const spendResult = await merchantService.recordTaggedUserWin(
        merchantId,
        userId,
        username,
        gameType,
        winAmount
      );
      
      if (!spendResult.success) {
        socket.emit('error', { message: spendResult.error });
        return;
      }
      
      socket.emit('game:spend:success', {
        winAmount,
        commissionAmount: spendResult.commissionAmount
      });
      
    } catch (error) {
      console.error('Error processing game spend:', error);
      socket.emit('error', { message: 'Game spend failed' });
    }
  };

  socket.on('game:play', playGame);
  socket.on('game:history:get', getGameHistory);
  socket.on('game:stats:get', getGameStats);
  socket.on('game:leaderboard:get', getGameLeaderboard);
  socket.on('game:types:get', getGameTypes);
  socket.on('game:spend', gameSpend);
};
