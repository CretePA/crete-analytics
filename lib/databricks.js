/**
 * Shared Databricks SQL connection for Next.js API routes.
 * Uses @databricks/sql connector with OAuth service principal or PAT auth.
 */

const { DBSQLClient } = require('@databricks/sql');

const HOST = process.env.DATABRICKS_SERVER_HOSTNAME
  || process.env.DATABRICKS_HOST
  || process.env.DATABRICKS_INSTANCE
  || '';
const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID || '';
const HTTP_PATH = process.env.DATABRICKS_HTTP_PATH
  || (WAREHOUSE_ID ? `/sql/1.0/warehouses/${WAREHOUSE_ID}` : '');
const TOKEN = process.env.DATABRICKS_TOKEN || '';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || '';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || '';

const CATALOG = 'sandbox';
const SCHEMA = 'gold';

function tbl(name) {
  return `${CATALOG}.${SCHEMA}.${name}`;
}

async function getOAuthToken() {
  const res = await fetch(`https://${HOST}/oidc/v1/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      scope: 'all-apis',
    }),
  });
  if (!res.ok) throw new Error(`OAuth failed: ${res.status}`);
  const data = await res.json();
  return data.access_token;
}

async function getConnection() {
  const client = new DBSQLClient();

  let authOptions;
  if (TOKEN) {
    authOptions = { token: TOKEN };
  } else if (CLIENT_ID && CLIENT_SECRET) {
    const token = await getOAuthToken();
    authOptions = { token };
  } else {
    // Fall back to default auth (Databricks Apps provides this)
    authOptions = { token: process.env.DATABRICKS_TOKEN || '' };
  }

  const connection = await client.connect({
    host: HOST,
    path: HTTP_PATH,
    ...authOptions,
  });
  return connection;
}

function serializeValue(val) {
  if (val === null || val === undefined) return val;
  if (typeof val === 'bigint') return Number(val);
  if (val instanceof Date) return val.toISOString();
  if (typeof val === 'object' && val.constructor?.name === 'Decimal') return Number(val);
  return val;
}

async function runQuery(sql) {
  const connection = await getConnection();
  try {
    const session = await connection.openSession();
    const operation = await session.executeStatement(sql);
    const result = await operation.fetchAll();
    const schema = await operation.getSchema();
    const columns = schema?.columns?.map(c => c.columnName) || [];
    await operation.close();
    await session.close();

    const rows = result.map(row => {
      const obj = {};
      for (const col of columns) {
        obj[col] = serializeValue(row[col]);
      }
      return obj;
    });

    return { columns, rows };
  } finally {
    await connection.close();
  }
}

// Simple in-memory cache
const cache = {};
const CACHE_TTL = 120_000; // 2 minutes
const CACHE_TTL_SLOW = 3_600_000; // 1 hour

function cached(key, fn, ttl = CACHE_TTL) {
  const entry = cache[key];
  if (entry && Date.now() - entry.ts < ttl) {
    return Promise.resolve(entry.data);
  }
  return fn().then(data => {
    cache[key] = { data, ts: Date.now() };
    return data;
  });
}

module.exports = { runQuery, cached, tbl, CACHE_TTL, CACHE_TTL_SLOW, HOST };
