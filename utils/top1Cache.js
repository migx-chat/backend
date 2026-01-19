const { query } = require('../db/db');
const { getRedisClient } = require('../redis');

const TOP1_CACHE_KEY = 'leaderboard:top1:users';
const TOP1_LOCK_KEY = 'leaderboard:top1:lock';
const TOP1_CACHE_TTL = 300; // 5 minutes
const LOCK_TTL = 10; // 10 seconds lock to prevent stampede

let refreshInProgress = false;
let scheduledRefreshStarted = false;

async function getTop1Users() {
  const redis = getRedisClient();
  
  try {
    const cached = await redis.get(TOP1_CACHE_KEY);
    if (cached) {
      return JSON.parse(cached);
    }
  } catch (error) {
    console.error('Error reading top1 cache:', error);
  }
  
  return await safeRefreshTop1Cache();
}

async function safeRefreshTop1Cache() {
  const redis = getRedisClient();
  
  if (refreshInProgress) {
    await new Promise(resolve => setTimeout(resolve, 500));
    try {
      const cached = await redis.get(TOP1_CACHE_KEY);
      if (cached) {
        return JSON.parse(cached);
      }
    } catch (error) {}
    return { top1_user_ids: [], top_merchant_id: null, updated_at: null };
  }
  
  try {
    const lockAcquired = await redis.set(TOP1_LOCK_KEY, '1', { NX: true, EX: LOCK_TTL });
    
    if (!lockAcquired) {
      await new Promise(resolve => setTimeout(resolve, 300));
      const cached = await redis.get(TOP1_CACHE_KEY);
      if (cached) {
        return JSON.parse(cached);
      }
      return { top1_user_ids: [], top_merchant_id: null, updated_at: null };
    }
    
    refreshInProgress = true;
    const result = await refreshTop1Cache();
    await redis.del(TOP1_LOCK_KEY);
    refreshInProgress = false;
    return result;
  } catch (error) {
    refreshInProgress = false;
    console.error('Error in safeRefreshTop1Cache:', error);
    return { top1_user_ids: [], top_merchant_id: null, updated_at: null };
  }
}

async function refreshTop1Cache() {
  const redis = getRedisClient();
  const now = new Date();
  const currentMonth = now.toISOString().slice(0, 7);
  const weekNum = Math.ceil(now.getDate() / 7);
  const weekYear = `${now.getFullYear()}-W${weekNum}`;
  
  try {
    const [topLevel, topGiftSender, topGiftReceiver, topFootprint, topGamer, topGet, topMerchant, topLikes] = await Promise.all([
      query(`SELECT u.id FROM users u LEFT JOIN user_levels ul ON u.id = ul.user_id WHERE u.is_active = true ORDER BY COALESCE(ul.level, 1) DESC, COALESCE(ul.xp, 0) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN user_gifts ug ON u.id = ug.sender_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(ug.id) > 0 ORDER BY COUNT(ug.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN user_gifts ug ON u.id = ug.receiver_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(ug.id) > 0 ORDER BY COUNT(ug.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN profile_footprints pf ON u.id = pf.profile_id WHERE u.is_active = true GROUP BY u.id HAVING COUNT(pf.id) > 0 ORDER BY COUNT(pf.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN game_history gh ON u.id = gh.user_id AND gh.created_at >= NOW() - INTERVAL '7 days' WHERE u.is_active = true GROUP BY u.id HAVING COUNT(gh.id) > 0 ORDER BY COUNT(gh.id) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN game_history gh ON u.id = gh.user_id AND gh.created_at >= NOW() - INTERVAL '7 days' AND gh.result = 'win' WHERE u.is_active = true GROUP BY u.id HAVING SUM(gh.reward_amount) > 0 ORDER BY SUM(gh.reward_amount) DESC LIMIT 1`),
      query(`SELECT u.id FROM users u LEFT JOIN merchant_leaderboard ml ON u.id = ml.user_id AND ml.month_year = $1 WHERE u.is_active = true AND u.role = 'merchant' ORDER BY COALESCE(ml.total_spent, 0) DESC LIMIT 1`, [currentMonth]),
      query(`SELECT u.id FROM users u LEFT JOIN user_likes_leaderboard ull ON u.id = ull.user_id AND ull.week_year = $1 WHERE u.is_active = true GROUP BY u.id, ull.likes_count HAVING COALESCE(ull.likes_count, 0) > 0 ORDER BY ull.likes_count DESC LIMIT 1`, [weekYear])
    ]);
    
    const top1UserIds = new Set();
    if (topLevel.rows[0]) top1UserIds.add(topLevel.rows[0].id);
    if (topGiftSender.rows[0]) top1UserIds.add(topGiftSender.rows[0].id);
    if (topGiftReceiver.rows[0]) top1UserIds.add(topGiftReceiver.rows[0].id);
    if (topFootprint.rows[0]) top1UserIds.add(topFootprint.rows[0].id);
    if (topGamer.rows[0]) top1UserIds.add(topGamer.rows[0].id);
    if (topGet.rows[0]) top1UserIds.add(topGet.rows[0].id);
    if (topMerchant.rows[0]) top1UserIds.add(topMerchant.rows[0].id);
    if (topLikes.rows[0]) top1UserIds.add(topLikes.rows[0].id);
    
    const result = {
      top1_user_ids: Array.from(top1UserIds),
      top_merchant_id: topMerchant.rows[0]?.id || null,
      updated_at: now.toISOString()
    };
    
    await redis.set(TOP1_CACHE_KEY, JSON.stringify(result), { EX: TOP1_CACHE_TTL });
    console.log('[Top1Cache] Cache refreshed:', result.top1_user_ids.length, 'top users');
    
    return result;
  } catch (error) {
    console.error('Error refreshing top1 cache:', error);
    return { top1_user_ids: [], top_merchant_id: null, updated_at: null };
  }
}

async function isTop1User(userId) {
  const data = await getTop1Users();
  return data.top1_user_ids.includes(userId);
}

async function isTopMerchant(userId) {
  const data = await getTop1Users();
  return data.top_merchant_id === userId;
}

function startScheduledRefresh() {
  if (scheduledRefreshStarted) return;
  scheduledRefreshStarted = true;
  
  console.log('[Top1Cache] Starting scheduled refresh job (every 5 minutes)');
  
  setTimeout(() => safeRefreshTop1Cache().catch(console.error), 5000);
  
  setInterval(() => {
    safeRefreshTop1Cache().catch(console.error);
  }, TOP1_CACHE_TTL * 1000);
}

module.exports = {
  getTop1Users,
  refreshTop1Cache: safeRefreshTop1Cache,
  isTop1User,
  isTopMerchant,
  startScheduledRefresh
};
