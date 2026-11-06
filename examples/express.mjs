import express from 'express';
import Redis from 'ioredis';

const kv = new Redis(process.env.EXPENSE_KV_URL || 'redis://127.0.0.1:6380/0', {
  protocol: 2,
  password: process.env.EXPENSE_KV_PASSWORD || undefined,
  lazyConnect: true,
  enableReadyCheck: false,
  enableOfflineQueue: false,
  maxRetriesPerRequest: 0,
  autoResendUnfulfilledCommands: false,
  connectTimeout: 1000,
  commandTimeout: 1000,
});
kv.on('error', () => console.warn('ExpenseKV unavailable; computing without cache'));
await kv.connect().catch(() => {});

const app = express();
app.get('/summary', async (_request, response) => {
  const key = 'demo:express:v1:summary';
  try {
    const cached = await kv.get(key);
    if (cached !== null) return response.json({ source: 'cache', data: JSON.parse(cached) });
  } catch {}
  const data = { total_minor: 12500, currency: 'RUB' };
  try {
    await kv.set(key, JSON.stringify(data), 'EX', 60);
  } catch {}
  return response.json({ source: 'computed', data });
});

const server = app.listen(Number(process.env.PORT || 3000), '127.0.0.1');
function stop() {
  kv.disconnect();
  server.close();
}
process.once('SIGTERM', stop);
process.once('SIGINT', stop);
