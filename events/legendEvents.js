const legendService = require('../services/legendService');
const creditService = require('../services/creditService');
const merchantTagService = require('../services/merchantTagService');
const { generateMessageId } = require('../utils/idGenerator');

const activeTimers = new Map();

const sendBotMessage = (io, roomId, message, options = {}) => {
  const { type = 'flagbot', bigEmoji = false, hasFlags = false } = typeof options === 'string' ? { type: options } : options;
  io.to(`room:${roomId}`).emit('chat:message', {
    id: generateMessageId(),
    roomId,
    username: 'FlagBot',
    message: message,
    messageType: type,
    type: 'bot',
    botType: 'flagbot',
    userType: 'bot',
    usernameColor: '#e67e22',
    messageColor: '#2ecc71',
    bigEmoji: bigEmoji,
    hasFlags: hasFlags,
    timestamp: new Date().toISOString()
  });
};

const clearGameTimer = (roomId) => {
  const timerKey = `legend:timer:${roomId}`;
  if (activeTimers.has(timerKey)) {
    clearTimeout(activeTimers.get(timerKey));
    activeTimers.delete(timerKey);
  }
};

const startBettingTimer = (io, roomId) => {
  clearGameTimer(roomId);
  
  const timerKey = `legend:timer:${roomId}`;
  
  const timer = setTimeout(async () => {
    activeTimers.delete(timerKey);
    await endBettingPhase(io, roomId);
  }, legendService.BETTING_TIME * 1000);
  
  activeTimers.set(timerKey, timer);
};

const endBettingPhase = async (io, roomId) => {
  sendBotMessage(io, roomId, "Times Up ‼️ Betting Ends..");
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  sendBotMessage(io, roomId, "Bot is calculating result...");
  
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  const result = await legendService.calculateWinners(roomId);
  
  if (!result.success) {
    sendBotMessage(io, roomId, result.error || "Error calculating results");
    return;
  }
  
  const resultsDisplay = result.resultsFlags.map(f => `[FLAG:${f}]`).join(' ');
  sendBotMessage(io, roomId, `Results: ${resultsDisplay}`, { bigEmoji: true, hasFlags: true });
  
  await new Promise(resolve => setTimeout(resolve, 1500));
  
  const matchingLines = [];
  Object.entries(result.multipliers).forEach(([group, data]) => {
    if (data.count >= 2) {
      matchingLines.push(`${data.group.emoji} ${data.group.name}: ${data.count}x`);
    }
  });
  
  if (matchingLines.length > 0) {
    sendBotMessage(io, roomId, `Matching guesses...\n${matchingLines.join('\n')}`);
  } else {
    sendBotMessage(io, roomId, "No matching results!");
  }
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  if (result.winners.length > 0) {
    for (const winner of result.winners) {
      try {
        await creditService.addCredits(winner.userId, winner.winAmount, 'flagbot_win', `Won FlagBot game betting on ${winner.groupName}`);
        
        io.to(`room:${roomId}`).emit('credits:updated', {
          userId: winner.userId,
          username: winner.username
        });
      } catch (err) {
        console.error(`Failed to pay winner ${winner.username}:`, err);
      }
      
      sendBotMessage(io, roomId, `• ${winner.username} has won ${winner.winAmount} credits for placing ${winner.amount} credits on ${winner.groupName}`);
    }
  } else {
    sendBotMessage(io, roomId, "No winners this round. Better luck next time!");
  }
  
  await legendService.endGame(roomId);
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  sendBotMessage(io, roomId, "FlagBot ready for the next round. Type !fg to start.");
};

const isSystemAdminOnly = async (userId) => {
  const userService = require('../services/userService');
  const user = await userService.getUserById(userId);
  return user && (user.role === 'admin' || user.role === 'super_admin');
};

const handleLegendCommand = async (io, socket, data) => {
  const { roomId, userId, username, message } = data;
  
  const lowerMessage = message.toLowerCase().trim();
  
  if (lowerMessage === '/bot flagh add' || lowerMessage === '/add bot flagh') {
    const userService = require('../services/userService');
    const user = await userService.getUserById(userId);
    
    if (!user || (user.role !== 'super_admin' && user.role !== 'admin')) {
      io.to(`room:${roomId}`).emit('chat:message', {
        id: generateMessageId(),
        roomId,
        message: 'Error: Only admin can perform this action.',
        messageType: 'error',
        type: 'error',
        timestamp: new Date().toISOString()
      });
      return true;
    }
    
    // Check if LowCard is active - only one game per room
    const lowcardService = require('../services/lowcardService');
    const lowcardActive = await lowcardService.isBotActive(roomId);
    if (lowcardActive) {
      socket.emit('system:message', {
        roomId,
        message: "LowCard is active in this room. Remove it first with /bot lowcard off",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const isActive = await legendService.isBotActive(roomId);
    if (isActive) {
      socket.emit('system:message', {
        roomId,
        message: "FlagBot is already active in this room!",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    await legendService.activateBot(roomId);
    sendBotMessage(io, roomId, "FlagBot has been activated! Type !fg to start a game.");
    return true;
  }
  
  if (lowerMessage === '/bot flagh off') {
    const hasPermission = await isSystemAdminOnly(userId);
    if (!hasPermission) {
      socket.emit('system:message', {
        roomId,
        message: "Only admin can remove bot",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const isActive = await legendService.isBotActive(roomId);
    if (!isActive) {
      socket.emit('system:message', {
        roomId,
        message: "FlagBot is not active in this room",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const game = await legendService.getGameState(roomId);
    if (game && (game.phase === 'betting' || game.phase === 'calculating')) {
      socket.emit('system:message', {
        roomId,
        message: "Cannot remove bot while game is in progress. Use /bot stop flagh first.",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    clearGameTimer(roomId);
    await legendService.deactivateBot(roomId);
    sendBotMessage(io, roomId, "FlagBot has been deactivated. Goodbye!");
    return true;
  }
  
  if (lowerMessage === '/bot stop flagh') {
    const hasPermission = await isSystemAdminOnly(userId);
    if (!hasPermission) {
      socket.emit('system:message', {
        roomId,
        message: "Only admin can stop the game",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const game = await legendService.getGameState(roomId);
    if (!game || game.phase === 'finished') {
      socket.emit('system:message', {
        roomId,
        message: "No active game to stop",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const allBets = await legendService.getAllBets(roomId);
    for (const bet of allBets) {
      await creditService.addCredits(bet.userId, bet.amount, 'flagbot_refund', 'Game stopped - bet refunded');
    }
    
    clearGameTimer(roomId);
    await legendService.endGame(roomId);
    sendBotMessage(io, roomId, "Game stopped by moderator. All bets have been refunded.");
    return true;
  }
  
  const isBotActive = await legendService.isBotActive(roomId);
  if (!isBotActive) {
    return false;
  }
  
  // Check if LowCard game is running - prevent conflict
  const lowcardService = require('../services/lowcardService');
  const lowcardGame = await lowcardService.getActiveGame(roomId);
  if (lowcardGame && lowerMessage === '!fg') {
    socket.emit('system:message', {
      roomId,
      message: 'LowCard game is running. Please wait until LowCard game ends.',
      timestamp: new Date().toISOString(),
      type: 'warning'
    });
    return true;
  }
  
  if (lowerMessage === '!fg') {
    const currentGame = await legendService.getGameState(roomId);
    if (currentGame && (currentGame.phase === 'betting' || currentGame.phase === 'calculating')) {
      sendBotMessage(io, roomId, "A game is already in progress!");
      return true;
    }
    
    const result = await legendService.startGame(roomId, username);
    
    if (!result.success) {
      sendBotMessage(io, roomId, result.error);
      return true;
    }
    
    const groupList = Object.entries(legendService.GROUPS)
      .map(([key, g]) => `${g.emoji} ${g.name}`)
      .join(', ');
    
    sendBotMessage(io, roomId, `FlagBot game started! Type !b [group] [amount] to bid.\nAvailable groups: ${groupList}. [${legendService.BETTING_TIME} seconds]`);
    
    startBettingTimer(io, roomId);
    return true;
  }
  
  if (lowerMessage.startsWith('!b ')) {
    const parts = lowerMessage.split(' ');
    const groupCode = parts[1];
    const amount = parts[2];
    
    if (!groupCode || !amount) {
      socket.emit('system:message', {
        roomId,
        message: "Usage: !b [group] [amount]. Example: !b r 500",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    // Check if game is active and in betting phase
    const currentGame = await legendService.getGameState(roomId);
    if (!currentGame || currentGame.phase !== 'betting') {
      socket.emit('system:message', {
        roomId,
        message: "No active betting phase. Type !fg to start a game first.",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const userBalance = await creditService.getBalance(userId);
    const betAmount = parseInt(amount);
    
    if (isNaN(betAmount) || betAmount < legendService.MIN_BET) {
      socket.emit('system:message', {
        roomId,
        message: `Minimum bet is ${legendService.MIN_BET} COINS`,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    if (userBalance < betAmount) {
      socket.emit('system:message', {
        roomId,
        message: `Not enough credits. Your balance: ${userBalance} COINS`,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const taggedBalance = await merchantTagService.getTaggedBalance(userId);
    let usedTaggedCredits = 0;
    let remainingAmount = betAmount;
    let newBalance = userBalance - betAmount;
    
    if (taggedBalance > 0) {
      const consumeResult = await merchantTagService.consumeForGame(userId, 'flagbot', betAmount);
      if (consumeResult.success) {
        usedTaggedCredits = consumeResult.usedTaggedCredits || 0;
        remainingAmount = consumeResult.remainingAmount;
      }
    }
    
    if (remainingAmount > 0) {
      const deductResult = await creditService.deductCredits(userId, remainingAmount, 'flagbot_bet', `Bet on FlagBot game`);
      if (!deductResult.success) {
        socket.emit('system:message', {
          roomId,
          message: deductResult.error || "Failed to place bet",
          timestamp: new Date().toISOString(),
          type: 'warning'
        });
        return true;
      }
      newBalance = deductResult.newBalance;
    }
    
    const result = await legendService.placeBet(roomId, userId, username, groupCode, betAmount);
    
    if (!result.success) {
      await creditService.addCredits(userId, betAmount, 'flagbot_refund', 'Bet refunded');
      socket.emit('system:message', {
        roomId,
        message: result.error,
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    socket.emit('credits:updated', { balance: newBalance });
    
    const userBets = await legendService.getUserBets(roomId, userId);
    const userBetsList = userBets.map(bet => `${bet.groupName} ${bet.amount}`).join(', ');
    
    sendBotMessage(io, roomId, `${username} placed ${betAmount} on ${result.bet.groupName}.\nTotal bid: ${userBetsList}`);
    return true;
  }
  
  if (lowerMessage === '!lock') {
    const game = await legendService.getGameState(roomId);
    
    if (!game || game.phase !== 'betting') {
      socket.emit('system:message', {
        roomId,
        message: "No active betting phase to lock",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    if (game.startedBy !== username) {
      const userService = require('../services/userService');
      const user = await userService.getUserById(userId);
      if (user?.role !== 'admin') {
        socket.emit('system:message', {
          roomId,
          message: "Only the game starter or admin can lock betting",
          timestamp: new Date().toISOString(),
          type: 'warning'
        });
        return true;
      }
    }
    
    clearGameTimer(roomId);
    await endBettingPhase(io, roomId);
    return true;
  }
  
  if (lowerMessage === '!cancel') {
    const game = await legendService.getGameState(roomId);
    
    if (!game) {
      socket.emit('system:message', {
        roomId,
        message: "No active game to cancel",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const userService = require('../services/userService');
    const user = await userService.getUserById(userId);
    
    if (user?.role !== 'admin' && game.startedBy !== username) {
      socket.emit('system:message', {
        roomId,
        message: "Only admin or game starter can cancel",
        timestamp: new Date().toISOString(),
        type: 'warning'
      });
      return true;
    }
    
    const allBets = await legendService.getAllBets(roomId);
    for (const bet of allBets) {
      await creditService.addCredits(bet.userId, bet.amount, 'flagbot_refund', 'Game cancelled - bet refunded');
    }
    
    clearGameTimer(roomId);
    await legendService.endGame(roomId);
    
    sendBotMessage(io, roomId, "Game cancelled! All bets have been refunded.");
    return true;
  }
  
  return false;
};

module.exports = { handleLegendCommand };
