const { client } = require('../redis');

const MERCHANT_INCOME_KEY = (merchantId) => `merchant:${merchantId}:income`;
const MERCHANT_DAILY_KEY = (merchantId, date) => `merchant:${merchantId}:daily:${date}`;
const MERCHANT_STATS_KEY = (merchantId) => `merchant:${merchantId}:stats`;

const DEFAULT_COMMISSION_RATE = 30;
const TAGGED_USER_WIN_COMMISSION_RATE = 10;

const calculateCommission = (spendAmount, commissionRate = DEFAULT_COMMISSION_RATE) => {
  return Math.floor(spendAmount * (commissionRate / 100));
};

const calculateTaggedUserWinCommission = (winAmount) => {
  return Math.floor(winAmount * (TAGGED_USER_WIN_COMMISSION_RATE / 100));
};

const addMerchantIncome = async (merchantId, amount) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    await client.incrBy(MERCHANT_INCOME_KEY(merchantId), amount);
    
    await client.incrBy(MERCHANT_DAILY_KEY(merchantId, today), amount);
    await client.expire(MERCHANT_DAILY_KEY(merchantId, today), 86400 * 7);
    
    await client.hIncrBy(MERCHANT_STATS_KEY(merchantId), 'total_transactions', 1);
    await client.hIncrBy(MERCHANT_STATS_KEY(merchantId), 'total_income', amount);
    
    return true;
  } catch (error) {
    console.error('Error adding merchant income:', error);
    return false;
  }
};

const getMerchantIncome = async (merchantId) => {
  try {
    const income = await client.get(MERCHANT_INCOME_KEY(merchantId));
    return parseInt(income) || 0;
  } catch (error) {
    console.error('Error getting merchant income:', error);
    return 0;
  }
};

const getMerchantDailyIncome = async (merchantId, date = null) => {
  try {
    const targetDate = date || new Date().toISOString().split('T')[0];
    const income = await client.get(MERCHANT_DAILY_KEY(merchantId, targetDate));
    return parseInt(income) || 0;
  } catch (error) {
    console.error('Error getting merchant daily income:', error);
    return 0;
  }
};

const getMerchantStats = async (merchantId) => {
  try {
    const stats = await client.hGetAll(MERCHANT_STATS_KEY(merchantId));
    return {
      totalTransactions: parseInt(stats.total_transactions) || 0,
      totalIncome: parseInt(stats.total_income) || 0
    };
  } catch (error) {
    console.error('Error getting merchant stats:', error);
    return { totalTransactions: 0, totalIncome: 0 };
  }
};

const resetMerchantIncome = async (merchantId) => {
  try {
    await client.set(MERCHANT_INCOME_KEY(merchantId), '0');
    return true;
  } catch (error) {
    console.error('Error resetting merchant income:', error);
    return false;
  }
};

const withdrawMerchantIncome = async (merchantId, amount) => {
  try {
    const currentIncome = await getMerchantIncome(merchantId);
    if (currentIncome < amount) {
      return { success: false, message: 'Insufficient income balance' };
    }
    
    await client.decrBy(MERCHANT_INCOME_KEY(merchantId), amount);
    return { success: true, newBalance: currentIncome - amount };
  } catch (error) {
    console.error('Error withdrawing merchant income:', error);
    return { success: false, message: 'Withdrawal failed' };
  }
};

const getMerchantTag = (level, totalIncome) => {
  if (totalIncome >= 1000000) return { tag: 'Diamond Merchant', color: '#B9F2FF' };
  if (totalIncome >= 500000) return { tag: 'Platinum Merchant', color: '#E5E4E2' };
  if (totalIncome >= 100000) return { tag: 'Gold Merchant', color: '#FFD700' };
  if (totalIncome >= 50000) return { tag: 'Silver Merchant', color: '#C0C0C0' };
  if (totalIncome >= 10000) return { tag: 'Bronze Merchant', color: '#CD7F32' };
  return { tag: 'Merchant', color: '#808080' };
};

module.exports = {
  DEFAULT_COMMISSION_RATE,
  TAGGED_USER_WIN_COMMISSION_RATE,
  calculateCommission,
  calculateTaggedUserWinCommission,
  addMerchantIncome,
  getMerchantIncome,
  getMerchantDailyIncome,
  getMerchantStats,
  resetMerchantIncome,
  withdrawMerchantIncome,
  getMerchantTag
};
