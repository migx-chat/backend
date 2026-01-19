const { 
  CHANNELS,
  initPublisher,
  getPublisher,
  publishGameCommand,
  publishGameResult,
  publishChatMessage,
  publishBotMessage,
  publishBatchMessages
} = require('./pub');

const {
  initSubscriber,
  getSubscriber,
  closeSubscriber,
  emitBotMessage
} = require('./sub');

const initPubSub = async (io) => {
  await initPublisher();
  await initSubscriber(io);
  console.log('✅ Redis Pub/Sub initialized');
};

module.exports = {
  CHANNELS,
  initPubSub,
  initPublisher,
  initSubscriber,
  getPublisher,
  getSubscriber,
  closeSubscriber,
  publishGameCommand,
  publishGameResult,
  publishChatMessage,
  publishBotMessage,
  publishBatchMessages,
  emitBotMessage
};
