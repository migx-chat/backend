const DICE_IMAGES = {
  1: 'dice_1.jpg',
  2: 'dice_2.jpg',
  3: 'dice_3.jpg',
  4: 'dice_4.jpg',
  5: 'dice_5.jpg',
  6: 'dice_6.jpg'
};

const DICE_EMOJI = {
  1: '⚀',
  2: '⚁',
  3: '⚂',
  4: '⚃',
  5: '⚄',
  6: '⚅'
};

const getDiceImage = (value) => {
  return DICE_IMAGES[value] || null;
};

const getDiceEmoji = (value) => {
  return DICE_EMOJI[value] || '?';
};

const getDiceCode = (value) => {
  return `[DICE:${value}]`;
};

const formatDiceRoll = (die1, die2) => {
  return `${getDiceCode(die1)} ${getDiceCode(die2)}`;
};

const formatDiceRollEmoji = (die1, die2) => {
  return `${getDiceEmoji(die1)} ${getDiceEmoji(die2)}`;
};

const isBalakSix = (die1, die2) => {
  return die1 === 6 && die2 === 6;
};

module.exports = {
  DICE_IMAGES,
  DICE_EMOJI,
  getDiceImage,
  getDiceEmoji,
  getDiceCode,
  formatDiceRoll,
  formatDiceRollEmoji,
  isBalakSix
};
