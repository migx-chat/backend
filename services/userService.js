const { query } = require('../db/db');
const { setUserStatus, getUserStatus, setUserSocket, removeUserSocket } = require('../utils/presence');
const { getRedisClient } = require('../redis');

const createUser = async (username, password = null, email = null) => {
  try {
    const result = await query(
      `INSERT INTO users (username, password_hash, email, is_active)
       VALUES ($1, $2, $3, true)
       RETURNING id, username, credits, role, status, created_at`,
      [username, password, email]
    );
    
    await query(
      'INSERT INTO user_levels (user_id, xp, level) VALUES ($1, 0, 1)',
      [result.rows[0].id]
    );
    
    return result.rows[0];
  } catch (error) {
    if (error.code === '23505') {
      return { error: 'Username already exists' };
    }
    console.error('Error creating user:', error);
    return null;
  }
};

const createUserWithRegistration = async ({ username, passwordHash, email, country, gender, activationToken }) => {
  try {
    const result = await query(
      `INSERT INTO users (username, password_hash, email, country, gender, activation_token, is_active)
       VALUES ($1, $2, $3, $4, $5, $6, false)
       RETURNING id, username, email, country, gender, created_at`,
      [username, passwordHash, email, country, gender, activationToken]
    );
    
    await query(
      'INSERT INTO user_levels (user_id, xp, level) VALUES ($1, 0, 1)',
      [result.rows[0].id]
    );
    
    return result.rows[0];
  } catch (error) {
    if (error.code === '23505') {
      return { error: 'Username or email already exists' };
    }
    console.error('Error creating user:', error);
    return null;
  }
};

const activateUser = async (token) => {
  try {
    const result = await query(
      `UPDATE users 
       SET is_active = true, activation_token = null, updated_at = CURRENT_TIMESTAMP
       WHERE activation_token = $1 AND is_active = false
       RETURNING id, username, email`,
      [token]
    );
    
    if (result.rows.length === 0) {
      return { success: false, error: 'Invalid or expired activation token' };
    }
    
    return { success: true, user: result.rows[0] };
  } catch (error) {
    console.error('Error activating user:', error);
    return { success: false, error: 'Activation failed' };
  }
};

const getUserByEmail = async (email) => {
  try {
    const result = await query(
      `SELECT u.*, ul.xp, ul.level 
       FROM users u
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.email = $1`,
      [email]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting user by email:', error);
    return null;
  }
};

const updateUserInvisible = async (userId, invisible) => {
  try {
    const result = await query(
      `UPDATE users SET is_invisible = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, is_invisible`,
      [invisible, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating invisible status:', error);
    return null;
  }
};

const getUserByUsername = async (username) => {
  try {
    if (!username) return null;
    const result = await query(
      `SELECT u.*, ul.xp, ul.level 
       FROM users u
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE LOWER(u.username) = LOWER($1)`,
      [username.trim()]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting user by username:', error);
    return null;
  }
};

const getUserById = async (userId) => {
  try {
    const result = await query(
      `SELECT u.*, ul.xp, ul.level 
       FROM users u
       LEFT JOIN user_levels ul ON u.id = ul.user_id
       WHERE u.id = $1`,
      [userId]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting user by id:', error);
    return null;
  }
};

const updateUserCredits = async (userId, amount) => {
  try {
    const result = await query(
      `UPDATE users SET credits = credits + $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, credits`,
      [amount, userId]
    );
    
    // Invalidate Redis credit cache
    try {
      const redis = getRedisClient();
      await redis.del(`credits:${userId}`);
    } catch (cacheError) {
      console.error('Cache invalidation error:', cacheError);
    }
    
    return result.rows[0];
  } catch (error) {
    console.error('Error updating user credits:', error);
    return null;
  }
};

const setCredits = async (userId, credits) => {
  try {
    const result = await query(
      `UPDATE users SET credits = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, credits`,
      [credits, userId]
    );
    
    // Invalidate Redis credit cache
    try {
      const redis = getRedisClient();
      await redis.del(`credits:${userId}`);
    } catch (cacheError) {
      console.error('Cache invalidation error:', cacheError);
    }
    
    return result.rows[0];
  } catch (error) {
    console.error('Error setting user credits:', error);
    return null;
  }
};

const getUserCredits = async (userId) => {
  try {
    const result = await query(
      'SELECT credits FROM users WHERE id = $1',
      [userId]
    );
    return result.rows[0]?.credits || 0;
  } catch (error) {
    console.error('Error getting user credits:', error);
    return 0;
  }
};

const updateUserRole = async (userId, role) => {
  try {
    const validRoles = ['user', 'mentor', 'merchant', 'admin', 'customer_service', 'super_admin'];
    if (!validRoles.includes(role)) {
      return { error: 'Invalid role' };
    }
    
    const result = await query(
      `UPDATE users SET role = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, role`,
      [role, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating user role:', error);
    return null;
  }
};

const getUserRole = async (userId) => {
  try {
    const result = await query(
      'SELECT role FROM users WHERE id = $1',
      [userId]
    );
    return result.rows[0]?.role || 'user';
  } catch (error) {
    console.error('Error getting user role:', error);
    return 'user';
  }
};

const isAdmin = async (userId) => {
  const role = await getUserRole(userId);
  return role === 'admin' || role === 'super_admin' || role === 'customer_service' || role === 'cs';
};

const isMentor = async (userId) => {
  const role = await getUserRole(userId);
  return role === 'mentor' || role === 'admin' || role === 'super_admin';
};

const isMerchant = async (userId) => {
  const role = await getUserRole(userId);
  return role === 'merchant';
};

const updateUserStatus = async (userId, status) => {
  try {
    await query(
      `UPDATE users SET status = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2`,
      [status, userId]
    );
    return true;
  } catch (error) {
    console.error('Error updating user status:', error);
    return false;
  }
};

const connectUser = async (userId, socketId) => {
  try {
    await setUserSocket(userId, socketId);
    await updateUserStatus(userId, 'online');
    return true;
  } catch (error) {
    console.error('Error connecting user:', error);
    return false;
  }
};

const disconnectUser = async (userId) => {
  try {
    await removeUserSocket(userId);
    await updateUserStatus(userId, 'offline');
    return true;
  } catch (error) {
    console.error('Error disconnecting user:', error);
    return false;
  }
};

const searchUsers = async (searchTerm, limit = 20) => {
  try {
    const result = await query(
      `SELECT id, username, avatar, role, status 
       FROM users 
       WHERE username ILIKE $1
       LIMIT $2`,
      [`%${searchTerm}%`, limit]
    );
    return result.rows;
  } catch (error) {
    console.error('Error searching users:', error);
    return [];
  }
};

const getOnlineUsers = async (limit = 50) => {
  try {
    const result = await query(
      `SELECT id, username, avatar, role, status 
       FROM users 
       WHERE status = 'online'
       ORDER BY username
       LIMIT $1`,
      [limit]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting online users:', error);
    return [];
  }
};

const updatePassword = async (userId, passwordHash) => {
  try {
    const result = await query(
      `UPDATE users SET password_hash = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username`,
      [passwordHash, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating password:', error);
    return null;
  }
};

const storeEmailOtp = async (userId, otp, newEmail) => {
  try {
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000); // 10 minutes
    await query(
      `INSERT INTO email_otp (user_id, otp, new_email, expires_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (user_id) 
       DO UPDATE SET otp = $2, new_email = $3, expires_at = $4, created_at = CURRENT_TIMESTAMP`,
      [userId, otp, newEmail, expiresAt]
    );
    return true;
  } catch (error) {
    console.error('Error storing email OTP:', error);
    return false;
  }
};

const verifyEmailOtp = async (userId, otp, newEmail) => {
  try {
    const result = await query(
      `SELECT * FROM email_otp 
       WHERE user_id = $1 AND otp = $2 AND new_email = $3 AND expires_at > CURRENT_TIMESTAMP`,
      [userId, otp, newEmail]
    );
    
    if (result.rows.length > 0) {
      // Delete OTP after verification
      await query('DELETE FROM email_otp WHERE user_id = $1', [userId]);
      return true;
    }
    return false;
  } catch (error) {
    console.error('Error verifying email OTP:', error);
    return false;
  }
};

const updateEmail = async (userId, newEmail) => {
  try {
    const result = await query(
      `UPDATE users SET email = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, email`,
      [newEmail, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating email:', error);
    return null;
  }
};

const storeRegistrationOtp = async (userId, email, otp) => {
  try {
    const expiresAt = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes
    await query(
      `DELETE FROM otp_verifications WHERE user_id = $1 AND type = 'registration'`,
      [userId]
    );
    await query(
      `INSERT INTO otp_verifications (user_id, email, otp, type, expires_at)
       VALUES ($1, $2, $3, 'registration', $4)`,
      [userId, email, otp, expiresAt]
    );
    return true;
  } catch (error) {
    console.error('Error storing registration OTP:', error);
    return false;
  }
};

const verifyRegistrationOtp = async (userId, otp) => {
  try {
    const result = await query(
      `SELECT * FROM otp_verifications 
       WHERE user_id = $1 AND otp = $2 AND type = 'registration' 
       AND expires_at > CURRENT_TIMESTAMP AND verified = false`,
      [userId, otp]
    );
    
    if (result.rows.length > 0) {
      await query(
        `UPDATE otp_verifications SET verified = true WHERE id = $1`,
        [result.rows[0].id]
      );
      return true;
    }
    return false;
  } catch (error) {
    console.error('Error verifying registration OTP:', error);
    return false;
  }
};

const activateUserById = async (userId) => {
  try {
    const result = await query(
      `UPDATE users 
       SET is_active = true, activation_token = null, updated_at = CURRENT_TIMESTAMP
       WHERE id = $1 AND is_active = false
       RETURNING id, username, email`,
      [userId]
    );
    
    if (result.rows.length === 0) {
      return { success: false, error: 'User not found or already activated' };
    }
    
    return { success: true, user: result.rows[0] };
  } catch (error) {
    console.error('Error activating user by ID:', error);
    return { success: false, error: 'Activation failed' };
  }
};

const storeForgotPasswordOtp = async (userId, otp) => {
  try {
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000); // 10 minutes
    await query(
      `DELETE FROM forgot_password_otp WHERE user_id = $1`,
      [userId]
    );
    await query(
      `INSERT INTO forgot_password_otp (user_id, otp, expires_at)
       VALUES ($1, $2, $3)`,
      [userId, otp, expiresAt]
    );
    return true;
  } catch (error) {
    console.error('Error storing forgot password OTP:', error);
    return false;
  }
};

const verifyForgotPasswordOtp = async (userId, otp) => {
  try {
    const result = await query(
      `SELECT * FROM forgot_password_otp 
       WHERE user_id = $1 AND otp = $2 AND expires_at > CURRENT_TIMESTAMP`,
      [userId, otp]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error verifying forgot password OTP:', error);
    return false;
  }
};

const deleteForgotPasswordOtp = async (userId) => {
  try {
    await query('DELETE FROM forgot_password_otp WHERE user_id = $1', [userId]);
    return true;
  } catch (error) {
    console.error('Error deleting forgot password OTP:', error);
    return false;
  }
};

const updateStatusMessage = async (userId, statusMessage) => {
  try {
    const result = await query(
      `UPDATE users SET status_message = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, status_message`,
      [statusMessage, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating status message:', error);
    return null;
  }
};

const suspendUser = async (userId, suspendedBy) => {
  try {
    const result = await query(
      `UPDATE users SET status = 'suspended', suspended_at = CURRENT_TIMESTAMP, suspended_by = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, status, suspended_at, suspended_by`,
      [suspendedBy, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error suspending user:', error);
    return null;
  }
};

const unsuspendUser = async (userId) => {
  try {
    const result = await query(
      `UPDATE users SET status = 'offline', suspended_at = NULL, suspended_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
       RETURNING id, username, status`,
      [userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error unsuspending user:', error);
    return null;
  }
};

const isSuspended = async (userId) => {
  try {
    const result = await query(
      'SELECT status, suspended_at, suspended_by FROM users WHERE id = $1',
      [userId]
    );
    return result.rows[0]?.status === 'suspended';
  } catch (error) {
    console.error('Error checking suspension:', error);
    return false;
  }
};

const updateUserLastIp = async (userId, ipAddress) => {
  try {
    const result = await query(
      `UPDATE users SET last_ip = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, last_ip`,
      [ipAddress, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating user last IP:', error);
    return null;
  }
};

module.exports = {
  createUser,
  createUserWithRegistration,
  activateUser,
  activateUserById,
  getUserByUsername,
  getUserByEmail,
  getUserById,
  updateUserCredits,
  setCredits,
  getUserCredits,
  updateUserRole,
  getUserRole,
  isAdmin,
  isMentor,
  isMerchant,
  updateUserStatus,
  updateUserInvisible,
  connectUser,
  disconnectUser,
  searchUsers,
  getOnlineUsers,
  updatePassword,
  storeEmailOtp,
  verifyEmailOtp,
  updateEmail,
  storeRegistrationOtp,
  verifyRegistrationOtp,
  storeForgotPasswordOtp,
  verifyForgotPasswordOtp,
  deleteForgotPasswordOtp,
  updateStatusMessage,
  suspendUser,
  unsuspendUser,
  isSuspended,
  updateUserLastIp
};
