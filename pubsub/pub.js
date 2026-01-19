const { getRedisClient } = require('../redis');
const logger = require('../utils/logger');

const CHANNELS = {
  GAME_COMMAND: 'game:command',
  GAME_RESULT: 'game:result',
  CHAT_MESSAGE: 'chat:message'
};

const getPublisher = () => {
  return getRedisClient();
};

const initPublisher = async () => {
  logger.info('PUBSUB_PUBLISHER_READY', { channels: Object.values(CHANNELS) });
  return getPublisher();
};

const publishGameCommand = async (data) => {
  const pub = getPublisher();
  const payload = JSON.stringify({
    type: 'game_command',
    timestamp: Date.now(),
    ...data
  });
  await pub.publish(CHANNELS.GAME_COMMAND, payload);
};

const publishGameResult = async (data) => {
  const pub = getPublisher();
  const payload = JSON.stringify({
    type: 'game_result',
    timestamp: Date.now(),
    ...data
  });
  await pub.publish(CHANNELS.GAME_RESULT, payload);
};

const publishChatMessage = async (roomId, message, metadata = {}) => {
  const pub = getPublisher();
  const payload = JSON.stringify({
    type: 'chat_message',
    roomId,
    message,
    timestamp: Date.now(),
    ...metadata
  });
  await pub.publish(CHANNELS.CHAT_MESSAGE, payload);
};

const publishBotMessage = async (roomId, botType, message) => {
  const pub = getPublisher();
  const payload = JSON.stringify({
    type: 'bot_message',
    roomId,
    botType,
    message,
    timestamp: Date.now()
  });
  await pub.publish(CHANNELS.GAME_RESULT, payload);
};

const publishBatchMessages = async (roomId, messages, botType = 'lowcard') => {
  const pub = getPublisher();
  const payload = JSON.stringify({
    type: 'batch_messages',
    roomId,
    botType,
    messages,
    timestamp: Date.now()
  });
  await pub.publish(CHANNELS.GAME_RESULT, payload);
};

module.exports = {
  CHANNELS,
  initPublisher,
  getPublisher,
  publishGameCommand,
  publishGameResult,
  publishChatMessage,
  publishBotMessage,
  publishBatchMessages
};
