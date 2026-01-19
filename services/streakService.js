const db = require('../db/db');
const { getRedisClient } = require('../redis');

class StreakService {
  async updateStreak(userId) {
    try {
      const user = await db.query('SELECT login_streak, last_login_date FROM users WHERE id = $1', [userId]);
      
      if (user.rows.length === 0) {
        throw new Error('User not found');
      }

      const { login_streak, last_login_date } = user.rows[0];
      const today = new Date().toISOString().split('T')[0];
      const lastLogin = last_login_date ? new Date(last_login_date).toISOString().split('T')[0] : null;

      let newStreak = login_streak || 0;
      let rewardAmount = 0;

      // If user hasn't logged in today
      if (lastLogin !== today) {
        // If last login was yesterday, increment streak
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const yesterdayStr = yesterday.toISOString().split('T')[0];

        if (lastLogin === yesterdayStr) {
          newStreak += 1;
        } else {
          // Streak broken, reset to 1
          newStreak = 1;
        }

        // Calculate reward based on streak
        rewardAmount = this.calculateReward(newStreak);

        // Update user streak and last login date
        await db.query(
          'UPDATE users SET login_streak = $1, last_login_date = $2, credits = credits + $3 WHERE id = $4',
          [newStreak, today, rewardAmount, userId]
        );
        
        // Invalidate Redis credit cache
        try {
          const redis = getRedisClient();
          await redis.del(`credits:${userId}`);
        } catch (cacheError) {
          console.error('Cache invalidation error:', cacheError);
        }
      }

      return {
        streak: newStreak,
        reward: rewardAmount,
        message: rewardAmount > 0 ? `Streak day ${newStreak}! Earned ${rewardAmount} credits!` : 'Welcome back!'
      };
    } catch (error) {
      console.error('Error updating streak:', error);
      throw error;
    }
  }

  calculateReward(streak) {
    // Base reward: 10 credits per day
    let baseReward = 10;
    
    // Bonus multiplier based on streak
    if (streak >= 7) return baseReward * 3;    // 30 credits
    if (streak >= 3) return baseReward * 2;    // 20 credits
    return baseReward;                           // 10 credits
  }

  async getStreakInfo(userId) {
    try {
      const result = await db.query(
        'SELECT login_streak, last_login_date FROM users WHERE id = $1',
        [userId]
      );

      if (result.rows.length === 0) {
        throw new Error('User not found');
      }

      return result.rows[0];
    } catch (error) {
      console.error('Error getting streak info:', error);
      throw error;
    }
  }
}

module.exports = new StreakService();
