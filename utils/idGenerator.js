let counter = 0;

const generateMessageId = () => {
  const timestamp = Date.now();
  counter = (counter + 1) % 10000;
  return `msg_${timestamp}_${counter.toString().padStart(4, '0')}`;
};

const generateRoomId = () => {
  const timestamp = Date.now();
  return `room_${timestamp}_${Math.random().toString(36).substring(2, 8)}`;
};

const generateSessionId = () => {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 15);
  return `sess_${timestamp}_${random}`;
};

const generateTransactionId = () => {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 10);
  return `txn_${timestamp}_${random}`;
};

const generateGameId = () => {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 8);
  return `game_${timestamp}_${random}`;
};

module.exports = {
  generateMessageId,
  generateRoomId,
  generateSessionId,
  generateTransactionId,
  generateGameId
};
