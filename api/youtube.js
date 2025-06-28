// /api/youtube.js (Final Version - Using 'redis' package)

import { createClient } from 'redis';

// 定義快取的鍵值和過期時間 (30 分鐘)
// *** 我們再次更改了這個鍵值的名稱來強制清除被測試資料污染的快取 ***
const CACHE_KEY = 'vspo-youtube-data-v3';
const CACHE_TTL_SECONDS = 1800; // 30 minutes * 60 seconds

// 代理函式主體
export default async function handler(request, response) {
  
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis store is not configured correctly.' });
  }

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    const cachedResult = await redisClient.get(CACHE_KEY);
    if (cachedResult) {
      console.log('Cache hit! Serving from Redis.');
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'HIT');
      await redisClient.quit();
      return response.status(200).json(JSON.parse(cachedResult));
    }
  } catch (error) {
    console.error('Error with Redis connection or cache read:', error);
    if (redisClient?.isOpen) await redisClient.quit();
  }

  console.log('Cache miss. Fetching from YouTube API...');
  
  const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
  ].filter(key => key);

  if (apiKeys.length === 0) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'API keys not configured on server.' });
  }
  
  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const endpoint = searchParams.get('endpoint');
  searchParams.delete('endpoint');
  
  let quotaErrorCount = 0;
  for (const apiKey of apiKeys) {
    const youtubeApiUrl = `https://www.googleapis.com/youtube/v3/${endpoint}?${searchParams.toString()}&key=${apiKey}`;

    try {
      const youtubeResponse = await fetch(youtubeApiUrl);
      const data = await youtubeResponse.json();

      if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
        console.warn(`Key starting with ${apiKey.substring(0, 8)}... has exceeded its quota. Trying next key.`);
        quotaErrorCount++;
        continue;
      }
      if (data.error) { throw new Error(data.error.message); }
      
      try {
        if (!redisClient?.isOpen) {
             redisClient = createClient({ url: redisConnectionString });
             await redisClient.connect();
        }
        await redisClient.set(CACHE_KEY, JSON.stringify(data), { EX: CACHE_TTL_SECONDS });
        console.log('Data saved to Redis cache.');
      } catch (error) {
        console.error('Error writing to Redis cache:', error);
      }
      
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'MISS');
      if (redisClient?.isOpen) await redisClient.quit();
      return response.status(200).json(data);

    } catch (error) {
      console.error(`Error with key starting with ${apiKey.substring(0, 8)}...`, error);
    }
  }
  
  if (redisClient?.isOpen) await redisClient.quit();
  response.setHeader('Access-Control-Allow-Origin', '*');

  if (quotaErrorCount === apiKeys.length) {
    return response.status(429).json({ error: 'All API keys have exceeded their daily quota.' });
  }

  return response.status(503).json({ error: 'All API keys failed for reasons other than quota.' });
}
