const { Pool } = require('pg');

const isProduction = process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProduction 
    ? { rejectUnauthorized: true }
    : { rejectUnauthorized: false }
});

pool.on('connect', () => {
  console.log('Connected to PostgreSQL database');
});

pool.on('error', (err) => {
  console.error('PostgreSQL pool error:', err);
});

const query = async (text, params) => {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    const duration = Date.now() - start;
    if (process.env.NODE_ENV === 'development') {
      console.log('Executed query', { text: text.substring(0, 50), duration, rows: res.rowCount });
    }
    return res;
  } catch (error) {
    console.error('Database query error:', error);
    throw error;
  }
};

const getClient = async () => {
  return await pool.connect();
};

const getPool = () => {
  return pool;
};

const initDatabase = async () => {
  const fs = require('fs');
  const path = require('path');
  
  if (isProduction) {
    console.log('Production mode: Skipping auto schema init. Use migrations instead.');
    return;
  }
  
  try {
    const schemaPath = path.join(__dirname, 'schema.sql');
    const schema = fs.readFileSync(schemaPath, 'utf8');
    
    const statements = schema
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0);
    
    for (const statement of statements) {
      try {
        await pool.query(statement);
      } catch (err) {
        if (!statement.toUpperCase().includes('INSERT') && 
            !err.message.includes('already exists')) {
          console.warn('Schema statement warning:', err.message.substring(0, 100));
        }
      }
    }
    
    console.log('Database schema initialized (development mode)');
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
};

module.exports = {
  pool,
  query,
  getClient,
  getPool,
  initDatabase
};
