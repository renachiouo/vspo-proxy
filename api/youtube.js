// /api/youtube.js (Final Version - Using 'redis' package)

import { createClient } from 'redis';

// 定義快取的鍵值和過期時間 (30 分鐘)
const CACHE_KEY = 'vspo-youtube-data';
const CACHE_TTL_SECONDS = 1800; // 30 minutes * 60 seconds

// 代理函式主體
export default async function handler(request, response) {
  
  // 檢查唯一的 REDIS_URL 環境變數是否存在
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    console.error('REDIS_URL environment variable not found.');
    return response.status(500).json({ error: 'Redis store is not configured correctly on the server. Please check environment variables and redeploy.' });
  }

  let redisClient;
  try {
    // 建立 Redis Client 並連線
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    // 1. 嘗試從 Redis 快取讀取資料
    const cachedResult = await redisClient.get(CACHE_KEY);
    if (cachedResult) {
      console.log('Cache hit! Serving from Redis.');
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'HIT');
      await redisClient.quit(); // 完成後關閉連線
      return response.status(200).json(JSON.parse(cachedResult));
    }
  } catch (error) {
    console.error('Error with Redis connection or cache read:', error);
    if (redisClient?.isOpen) await redisClient.quit();
    // 即使讀取快取失敗，我們依然可以繼續從 YouTube 抓取
  }

  // 2. 如果沒有快取，則從 YouTube API 抓取新資料
  console.log('Cache miss. Fetching from YouTube API...');
  
  const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
  ].filter(key => key);

  if (apiKeys.length === 0) {
    return response.status(500).json({ error: 'API keys not configured on server.' });
  }
  
  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const endpoint = searchParams.get('endpoint');
  searchParams.delete('endpoint');
  
  // 嘗試使用每一組金鑰，直到成功為止
  for (const apiKey of apiKeys) {
    const youtubeApiUrl = `https://www.googleapis.com/youtube/v3/${endpoint}?${searchParams.toString()}&key=${apiKey}`;

    try {
      const youtubeResponse = await fetch(youtubeApiUrl);
      const data = await youtubeResponse.json();

      if (data.error && data.error.message.toLowerCase().includes('quota')) {
        console.warn(`Key starting with ${apiKey.substring(0, 8)}... has exceeded its quota. Trying next key.`);
        continue;
      }
      if (data.error) { throw new Error(data.error.message); }
      
      // 成功！將資料寫入快取並回傳
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
  return response.status(503).json({ error: 'All API keys have exceeded their quotas or failed.' });
}
