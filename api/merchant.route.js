const express = require('express');
const router = express.Router();
const merchantService = require('../services/merchantService');
const merchantTagService = require('../services/merchantTagService');
const userService = require('../services/userService');
const authMiddleware = require('../middleware/auth');

router.post('/create', authMiddleware, async (req, res) => {
  try {
    const { userId, commissionRate = 30 } = req.body;
    // 🔐 Use authenticated user ID as mentor
    const mentorId = req.user.id;
    
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const isMentor = await userService.isMentor(mentorId);
    if (!isMentor) {
      return res.status(403).json({ error: 'Only mentors can create merchants' });
    }
    
    const result = await merchantService.createMerchant(userId, mentorId, commissionRate);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      merchant: result.merchant
    });
    
  } catch (error) {
    console.error('Create merchant error:', error);
    res.status(500).json({ error: 'Failed to create merchant' });
  }
});

router.post('/disable', authMiddleware, async (req, res) => {
  try {
    const { merchantId } = req.body;
    // 🔐 Use authenticated user ID as mentor
    const mentorId = req.user.id;
    
    if (!merchantId) {
      return res.status(400).json({ error: 'Merchant ID is required' });
    }
    
    const result = await merchantService.disableMerchant(merchantId, mentorId);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Disable merchant error:', error);
    res.status(500).json({ error: 'Failed to disable merchant' });
  }
});

router.post('/enable', authMiddleware, async (req, res) => {
  try {
    const { merchantId } = req.body;
    // 🔐 Use authenticated user ID as mentor
    const mentorId = req.user.id;
    
    if (!merchantId) {
      return res.status(400).json({ error: 'Merchant ID is required' });
    }
    
    const result = await merchantService.enableMerchant(merchantId, mentorId);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true
    });
    
  } catch (error) {
    console.error('Enable merchant error:', error);
    res.status(500).json({ error: 'Failed to enable merchant' });
  }
});

router.get('/profile/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const profile = await merchantService.getMerchantProfile(id);
    
    if (!profile) {
      return res.status(404).json({ error: 'Merchant not found' });
    }
    
    res.json({
      merchant: profile
    });
    
  } catch (error) {
    console.error('Get merchant profile error:', error);
    res.status(500).json({ error: 'Failed to get merchant profile' });
  }
});

router.get('/income/:merchantId', async (req, res) => {
  try {
    const { merchantId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const income = await merchantService.getMerchantIncomeTotal(merchantId);
    const spendLogs = await merchantService.getMerchantSpendLogs(merchantId, parseInt(limit), parseInt(offset));
    
    res.json({
      merchantId,
      income,
      transactions: spendLogs,
      count: spendLogs.length
    });
    
  } catch (error) {
    console.error('Get merchant income error:', error);
    res.status(500).json({ error: 'Failed to get merchant income' });
  }
});

router.get('/list', async (req, res) => {
  try {
    const { activeOnly = 'true', limit = 50 } = req.query;
    const merchants = await merchantService.getAllMerchants(activeOnly === 'true', parseInt(limit));
    
    res.json({
      merchants,
      count: merchants.length
    });
    
  } catch (error) {
    console.error('Get merchants error:', error);
    res.status(500).json({ error: 'Failed to get merchants' });
  }
});

router.post('/withdraw', async (req, res) => {
  try {
    const { merchantId, amount } = req.body;
    
    if (!merchantId || !amount || amount <= 0) {
      return res.status(400).json({ error: 'Valid merchant ID and amount required' });
    }
    
    const result = await merchantService.withdrawMerchantEarnings(merchantId, amount);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      amount,
      newBalance: result.newBalance
    });
    
  } catch (error) {
    console.error('Withdraw error:', error);
    res.status(500).json({ error: 'Withdrawal failed' });
  }
});

router.get('/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const merchant = await merchantService.getMerchantByUserId(userId);
    
    if (!merchant) {
      return res.status(404).json({ error: 'Merchant not found for this user' });
    }
    
    res.json({
      merchant
    });
    
  } catch (error) {
    console.error('Get merchant by user error:', error);
    res.status(500).json({ error: 'Failed to get merchant' });
  }
});

router.get('/dashboard/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await merchantService.getMerchantDashboard(userId);
    
    if (!result.success) {
      return res.status(404).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get merchant dashboard error:', error);
    res.status(500).json({ error: 'Failed to get dashboard' });
  }
});

router.get('/commissions/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const result = await merchantService.getTaggedUserCommissions(
      userId,
      parseInt(limit),
      parseInt(offset)
    );
    
    if (!result.success) {
      return res.status(404).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get commissions error:', error);
    res.status(500).json({ error: 'Failed to get commissions' });
  }
});

router.get('/recharge-history/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { months = 6 } = req.query;
    
    const result = await merchantService.getMonthlyRechargeHistory(
      userId,
      parseInt(months)
    );
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get recharge history error:', error);
    res.status(500).json({ error: 'Failed to get recharge history' });
  }
});

router.get('/transfer-status/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await merchantService.getMerchantTransferStatus(userId);
    
    if (!result.success) {
      return res.status(404).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get transfer status error:', error);
    res.status(500).json({ error: 'Failed to get transfer status' });
  }
});

router.post('/tag', async (req, res) => {
  try {
    const { merchantUserId, targetUsername } = req.body;
    
    if (!merchantUserId || !targetUsername) {
      return res.status(400).json({ error: 'Merchant user ID and target username are required' });
    }
    
    const result = await merchantTagService.tagUser(merchantUserId, targetUsername);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      tag: result.tag,
      message: `Successfully tagged ${targetUsername} with ${merchantTagService.TAG_AMOUNT} COINS`
    });
  } catch (error) {
    console.error('Tag user error:', error);
    res.status(500).json({ error: 'Failed to tag user' });
  }
});

router.get('/tags/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await merchantTagService.getTaggedUsers(userId);
    
    if (!result.success) {
      return res.status(404).json({ error: result.error });
    }
    
    const formattedList = result.tags.map(tag => 
      `Tag ${tag.slot} [${tag.username}]`
    );
    
    res.json({
      success: true,
      tags: result.tags,
      formattedList,
      count: result.tags.length
    });
  } catch (error) {
    console.error('Get tagged users error:', error);
    res.status(500).json({ error: 'Failed to get tagged users' });
  }
});

router.get('/tags/:userId/:tagId/spend', async (req, res) => {
  try {
    const { tagId } = req.params;
    const result = await merchantTagService.getTagSpendHistory(tagId);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get tag spend history error:', error);
    res.status(500).json({ error: 'Failed to get spend history' });
  }
});

router.get('/tag-commissions/pending/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await merchantTagService.getPendingCommissions(userId);
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get pending commissions error:', error);
    res.status(500).json({ error: 'Failed to get pending commissions' });
  }
});

router.get('/tag-commissions/history/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 50, offset = 0 } = req.query;
    
    const result = await merchantTagService.getCommissionHistory(
      userId,
      parseInt(limit),
      parseInt(offset)
    );
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get commission history error:', error);
    res.status(500).json({ error: 'Failed to get commission history' });
  }
});

router.delete('/tag/:userId/:tagId', async (req, res) => {
  try {
    const { userId, tagId } = req.params;
    const result = await merchantTagService.removeTag(parseInt(userId), parseInt(tagId));
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json({
      success: true,
      refundedAmount: result.refundedAmount,
      message: `Tag removed. ${result.refundedAmount} COINS refunded to your account.`
    });
  } catch (error) {
    console.error('Remove tag error:', error);
    res.status(500).json({ error: 'Failed to remove tag' });
  }
});

router.get('/tagged-balance/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const balance = await merchantTagService.getTaggedBalance(parseInt(userId));
    
    res.json({
      success: true,
      taggedBalance: balance
    });
  } catch (error) {
    console.error('Get tagged balance error:', error);
    res.status(500).json({ error: 'Failed to get tagged balance' });
  }
});

router.get('/user-commissions/pending/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await merchantTagService.getUserPendingCommissions(parseInt(userId));
    
    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }
    
    res.json(result);
  } catch (error) {
    console.error('Get user pending commissions error:', error);
    res.status(500).json({ error: 'Failed to get pending commissions' });
  }
});

module.exports = router;
