const merchantTagService = require('../services/merchantTagService');

const PAYOUT_INTERVAL = 60 * 60 * 1000;

let isRunning = false;
let intervalId = null;

const runPayoutJob = async () => {
  if (isRunning) {
    console.log('[COMMISSION PAYOUT JOB] Already running, skipping...');
    return;
  }
  
  isRunning = true;
  console.log('[COMMISSION PAYOUT JOB] Starting commission payout processing...');
  
  try {
    const result = await merchantTagService.processMaturedCommissions();
    
    if (result.success) {
      if (result.processed > 0) {
        console.log(`[COMMISSION PAYOUT JOB] Processed ${result.processed} commissions`);
        console.log(`[COMMISSION PAYOUT JOB] Merchant payouts: ${result.totalMerchantPayout} COINS`);
        console.log(`[COMMISSION PAYOUT JOB] User payouts: ${result.totalUserPayout} COINS`);
        console.log(`[COMMISSION PAYOUT JOB] Batch ID: ${result.batchId}`);
      } else {
        console.log('[COMMISSION PAYOUT JOB] No matured commissions to process');
      }
    } else {
      console.error('[COMMISSION PAYOUT JOB] Error:', result.error);
    }
  } catch (error) {
    console.error('[COMMISSION PAYOUT JOB] Unexpected error:', error);
  } finally {
    isRunning = false;
  }
};

const startCommissionPayoutJob = () => {
  if (intervalId) {
    console.log('[COMMISSION PAYOUT JOB] Already started');
    return;
  }
  
  console.log('[COMMISSION PAYOUT JOB] Starting job scheduler (runs every hour)');
  
  setTimeout(() => {
    runPayoutJob();
  }, 10000);
  
  intervalId = setInterval(runPayoutJob, PAYOUT_INTERVAL);
  
  return intervalId;
};

const stopCommissionPayoutJob = () => {
  if (intervalId) {
    clearInterval(intervalId);
    intervalId = null;
    console.log('[COMMISSION PAYOUT JOB] Stopped');
  }
};

module.exports = {
  startCommissionPayoutJob,
  stopCommissionPayoutJob,
  runPayoutJob
};
