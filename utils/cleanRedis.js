
const { getRedisClient } = require('../redis');

async function cleanRedisKeys() {
  try {
    const client = getRedisClient();
    
    console.log('🧹 Starting Redis cleanup...');
    
    // Patterns to clean
    const patterns = [
      'user:rooms:*',
      'room:lastmsg:*',
      'room:users:*',
      'room:active:*',
      'room:presence:*'
    ];
    
    for (const pattern of patterns) {
      console.log(`\n🔍 Scanning for pattern: ${pattern}`);
      
      // Get all keys matching pattern
      const keys = await client.keys(pattern);
      
      if (keys.length > 0) {
        console.log(`Found ${keys.length} keys to delete`);
        
        // Delete each key
        for (const key of keys) {
          const keyType = await client.type(key);
          console.log(`  Deleting ${key} (type: ${keyType})`);
          await client.del(key);
        }
      } else {
        console.log(`No keys found for pattern ${pattern}`);
      }
    }
    
    console.log('\n✅ Redis cleanup completed!');
    console.log('You can now restart the app and the keys will be recreated with correct types.\n');
    
  } catch (error) {
    console.error('❌ Error cleaning Redis:', error);
  }
}

module.exports = { cleanRedisKeys };

// Run if called directly
if (require.main === module) {
  const { connectRedis } = require('../redis');
  
  connectRedis()
    .then(() => cleanRedisKeys())
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Failed:', err);
      process.exit(1);
    });
}
