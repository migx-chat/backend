const { getRedisClient } = require('../redis');

const VOTE_KICK_DURATION = 60; // 60 seconds
const VOTE_KICK_COOLDOWN = 120; // 2 minutes
const VOTE_KICK_PAYMENT = 500; // 500 COINS to initiate vote kick
const VOTE_UPDATE_INTERVALS = [60, 40, 20, 0];

const activeVotes = new Map();

function getVotesNeeded(roomUserCount) {
  // If 10 or more users: need 10 votes
  // Otherwise: need majority (ceil(online / 2))
  if (roomUserCount >= 10) {
    return 10;
  }
  return Math.ceil(roomUserCount / 2);
}

async function startVoteKick(io, roomId, starterUsername, targetUsername, roomUserCount, starterUserId) {
  const redis = getRedisClient();
  const voteKey = `kick:${roomId}:${targetUsername}`;
  const votesKey = `kickVotes:${roomId}:${targetUsername}`;

  const existingVote = await redis.get(voteKey);
  if (existingVote) {
    return { success: false, error: 'A vote to kick this user is already in progress.' };
  }

  // Check and deduct payment (500 COINS)
  const userService = require('../services/userService');
  const starterCredits = await userService.getUserCredits(starterUserId);
  if (starterCredits < VOTE_KICK_PAYMENT) {
    return { 
      success: false, 
      error: `Not enough credits. Need 500 COINS to start vote kick (you have ${starterCredits} COINS).`,
      insufficientCredit: true
    };
  }

  // Deduct payment
  const updatedUser = await userService.updateUserCredits(starterUserId, -VOTE_KICK_PAYMENT);
  if (!updatedUser) {
    return { success: false, error: 'Failed to process payment.' };
  }

  // Emit credit update to the starter
  io.to(`user:${starterUsername}`).emit('user:balance:update', {
    credits: updatedUser.credits
  });

  const votesNeeded = getVotesNeeded(roomUserCount);
  
  await redis.set(voteKey, JSON.stringify({
    starter: starterUsername,
    target: targetUsername,
    startTime: Date.now(),
    roomUserCount,
    votesNeeded
  }), { EX: VOTE_KICK_DURATION });

  await redis.sAdd(votesKey, starterUsername);
  await redis.expire(votesKey, VOTE_KICK_DURATION);

  // Get room info for proper formatting
  const roomService = require('../services/roomService');
  const room = await roomService.getRoomById(roomId);

  io.to(`room:${roomId}`).emit('chat:message', {
    id: `votekick-start-${Date.now()}`,
    roomId,
    username: room?.name || 'System',
    message: `A vote to kick ${targetUsername} has been started by ${starterUsername}, ${votesNeeded - 1} more votes needed. ${VOTE_KICK_DURATION}s remaining.`,
    timestamp: new Date().toISOString(),
    type: 'system',
    messageType: 'voteKick',
    isSystem: true
  });

  const voteId = `${roomId}:${targetUsername}`;
  
  const intervals = [20000, 20000, 20000];
  let intervalIndex = 0;
  let remainingTime = 60;

  const timerCallback = async () => {
    remainingTime -= 20;
    
    const voteData = await redis.get(voteKey);
    if (!voteData) {
      clearVoteTimer(voteId);
      return;
    }

    const currentVotes = await redis.sCard(votesKey);
    const neededMore = votesNeeded - currentVotes;

    const roomService = require('../services/roomService');
    const room = await roomService.getRoomById(roomId);

    io.to(`room:${roomId}`).emit('chat:message', {
      id: `votekick-update-${Date.now()}`,
      roomId,
      username: room?.name || 'System',
      message: `Vote to kick ${targetUsername}: ${currentVotes} vote${currentVotes > 1 ? 's' : ''}, ${neededMore > 0 ? neededMore + ' more needed' : 'enough votes'}. ${remainingTime}s remaining.`,
      timestamp: new Date().toISOString(),
      type: 'system',
      messageType: 'voteKick',
      isSystem: true
    });

    intervalIndex++;
    if (intervalIndex < intervals.length) {
      activeVotes.set(voteId, setTimeout(timerCallback, intervals[intervalIndex]));
    } else {
      await finalizeVote(io, roomId, targetUsername, votesNeeded);
    }
  };

  activeVotes.set(voteId, setTimeout(timerCallback, intervals[0]));

  return { success: true, votesNeeded };
}

async function addVote(io, roomId, voterUsername, targetUsername) {
  const redis = getRedisClient();
  const voteKey = `kick:${roomId}:${targetUsername}`;
  const votesKey = `kickVotes:${roomId}:${targetUsername}`;

  const voteData = await redis.get(voteKey);
  if (!voteData) {
    return { success: false, error: 'No active vote for this user.' };
  }

  const parsed = JSON.parse(voteData);
  const votesNeeded = parsed.votesNeeded;

  const alreadyVoted = await redis.sIsMember(votesKey, voterUsername);
  if (alreadyVoted) {
    return { success: false, error: 'You have already voted.' };
  }

  await redis.sAdd(votesKey, voterUsername);
  const currentVotes = await redis.sCard(votesKey);
  const neededMore = votesNeeded - currentVotes;

  const ttl = await redis.ttl(voteKey);

  const roomService = require('../services/roomService');
  const room = await roomService.getRoomById(roomId);

  io.to(`room:${roomId}`).emit('chat:message', {
    id: `votekick-vote-${Date.now()}`,
    roomId,
    username: room?.name || 'System',
    message: `Vote to kick ${targetUsername}: ${currentVotes} vote${currentVotes > 1 ? 's' : ''}, ${neededMore > 0 ? neededMore + ' more needed' : 'enough votes'}. ${ttl}s remaining.`,
    timestamp: new Date().toISOString(),
    type: 'system',
    messageType: 'voteKick',
    isSystem: true
  });

  if (currentVotes >= votesNeeded) {
    await executeVoteKick(io, roomId, targetUsername);
    return { success: true, kicked: true };
  }

  return { success: true, currentVotes, neededMore };
}

async function finalizeVote(io, roomId, targetUsername, votesNeeded) {
  const redis = getRedisClient();
  const voteKey = `kick:${roomId}:${targetUsername}`;
  const votesKey = `kickVotes:${roomId}:${targetUsername}`;

  const currentVotes = await redis.sCard(votesKey);

  if (currentVotes >= votesNeeded) {
    await executeVoteKick(io, roomId, targetUsername);
  } else {
    const roomService = require('../services/roomService');
    const room = await roomService.getRoomById(roomId);

    io.to(`room:${roomId}`).emit('chat:message', {
      id: `votekick-failed-${Date.now()}`,
      roomId,
      username: room?.name || 'System',
      message: `Failed to kick ${targetUsername}`,
      timestamp: new Date().toISOString(),
      type: 'system',
      messageType: 'voteKick',
      isSystem: true
    });
  }

  await redis.del(voteKey);
  await redis.del(votesKey);
  clearVoteTimer(`${roomId}:${targetUsername}`);
}

async function executeVoteKick(io, roomId, targetUsername) {
  const redis = getRedisClient();
  const cooldownKey = `cooldown:voteKick:${targetUsername}:${roomId}`;
  const voteKey = `kick:${roomId}:${targetUsername}`;
  const votesKey = `kickVotes:${roomId}:${targetUsername}`;

  await redis.set(cooldownKey, '1', { EX: VOTE_KICK_COOLDOWN });

  // Get room info for room name
  const roomService = require('../services/roomService');
  const room = await roomService.getRoomById(roomId);

  // Send private message to kicked user
  const allSockets = await io.in(`room:${roomId}`).fetchSockets();
  for (const targetSocket of allSockets) {
    if (targetSocket.username === targetUsername || targetSocket.handshake?.auth?.username === targetUsername) {
      targetSocket.emit('chat:message', {
        id: `kick-private-votekick-${Date.now()}`,
        roomId,
        username: room?.name || 'System',
        message: `You have been vote-kicked from the room`,
        timestamp: new Date().toISOString(),
        type: 'system',
        messageType: 'kick',
        isPrivate: true
      });
    }
  }

  // Send system message to room
  io.to(`room:${roomId}`).emit('chat:message', {
    id: `kick-system-votekick-${Date.now()}`,
    roomId,
    username: room?.name || 'System',
    message: `${targetUsername} has been vote-kicked from the room`,
    timestamp: new Date().toISOString(),
    type: 'system',
    messageType: 'kick',
    isSystem: true
  });

  await redis.del(voteKey);
  await redis.del(votesKey);
  clearVoteTimer(`${roomId}:${targetUsername}`);

  return { success: true };
}

function clearVoteTimer(voteId) {
  const timer = activeVotes.get(voteId);
  if (timer) {
    clearTimeout(timer);
    activeVotes.delete(voteId);
  }
}

async function hasActiveVote(roomId, targetUsername) {
  const redis = getRedisClient();
  const voteKey = `kick:${roomId}:${targetUsername}`;
  const exists = await redis.get(voteKey);
  return !!exists;
}

module.exports = {
  startVoteKick,
  addVote,
  finalizeVote,
  executeVoteKick,
  hasActiveVote,
  VOTE_KICK_DURATION,
  VOTE_KICK_COOLDOWN,
  VOTE_KICK_PAYMENT
};
