const merchantService = require('../services/merchantService');
const userService = require('../services/userService');
const { getUserSocket } = require('../utils/presence');

module.exports = (io, socket) => {
  const createMerchant = async (data) => {
    try {
      const { userId, mentorId, commissionRate = 30 } = data;
      
      if (!userId || !mentorId) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const result = await merchantService.createMerchant(userId, mentorId, commissionRate);
      
      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }
      
      socket.emit('merchant:created', {
        merchant: result.merchant
      });
      
      const userSocketId = await getUserSocket(userId);
      if (userSocketId) {
        io.to(userSocketId).emit('user:role:changed', {
          role: 'merchant',
          merchantId: result.merchant.id
        });
      }
      
    } catch (error) {
      console.error('Error creating merchant:', error);
      socket.emit('error', { message: 'Failed to create merchant' });
    }
  };

  const disableMerchant = async (data) => {
    try {
      const { merchantId, mentorId } = data;
      
      if (!merchantId || !mentorId) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const result = await merchantService.disableMerchant(merchantId, mentorId);
      
      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }
      
      socket.emit('merchant:disabled', {
        merchantId
      });
      
    } catch (error) {
      console.error('Error disabling merchant:', error);
      socket.emit('error', { message: 'Failed to disable merchant' });
    }
  };

  const enableMerchant = async (data) => {
    try {
      const { merchantId, mentorId } = data;
      
      if (!merchantId || !mentorId) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const result = await merchantService.enableMerchant(merchantId, mentorId);
      
      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }
      
      socket.emit('merchant:enabled', {
        merchantId
      });
      
    } catch (error) {
      console.error('Error enabling merchant:', error);
      socket.emit('error', { message: 'Failed to enable merchant' });
    }
  };

  const getMerchantProfile = async (data) => {
    try {
      const { merchantId } = data;
      
      if (!merchantId) {
        socket.emit('error', { message: 'Merchant ID required' });
        return;
      }
      
      const profile = await merchantService.getMerchantProfile(merchantId);
      
      if (!profile) {
        socket.emit('error', { message: 'Merchant not found' });
        return;
      }
      
      socket.emit('merchant:profile', {
        merchant: profile
      });
      
    } catch (error) {
      console.error('Error getting merchant profile:', error);
      socket.emit('error', { message: 'Failed to get merchant profile' });
    }
  };

  const getMerchantIncome = async (data) => {
    try {
      const { merchantId } = data;
      
      if (!merchantId) {
        socket.emit('error', { message: 'Merchant ID required' });
        return;
      }
      
      const income = await merchantService.getMerchantIncomeTotal(merchantId);
      const spendLogs = await merchantService.getMerchantSpendLogs(merchantId, 20);
      
      socket.emit('merchant:income', {
        merchantId,
        income,
        recentTransactions: spendLogs
      });
      
    } catch (error) {
      console.error('Error getting merchant income:', error);
      socket.emit('error', { message: 'Failed to get merchant income' });
    }
  };

  const withdrawEarnings = async (data) => {
    try {
      const { merchantId, amount } = data;
      
      if (!merchantId || !amount) {
        socket.emit('error', { message: 'Missing required fields' });
        return;
      }
      
      const result = await merchantService.withdrawMerchantEarnings(merchantId, amount);
      
      if (!result.success) {
        socket.emit('error', { message: result.error });
        return;
      }
      
      socket.emit('merchant:withdraw:success', {
        merchantId,
        amount,
        newBalance: result.newBalance
      });
      
    } catch (error) {
      console.error('Error withdrawing earnings:', error);
      socket.emit('error', { message: 'Withdrawal failed' });
    }
  };

  const getAllMerchants = async (data) => {
    try {
      const { activeOnly = true, limit = 50 } = data || {};
      
      const merchants = await merchantService.getAllMerchants(activeOnly, limit);
      
      socket.emit('merchants:list', {
        merchants
      });
      
    } catch (error) {
      console.error('Error getting merchants:', error);
      socket.emit('error', { message: 'Failed to get merchants' });
    }
  };

  socket.on('merchant:create', createMerchant);
  socket.on('merchant:disable', disableMerchant);
  socket.on('merchant:enable', enableMerchant);
  socket.on('merchant:profile:get', getMerchantProfile);
  socket.on('merchant:income:get', getMerchantIncome);
  socket.on('merchant:withdraw', withdrawEarnings);
  socket.on('merchants:list:get', getAllMerchants);
};
