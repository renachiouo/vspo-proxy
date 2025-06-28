// /api/youtube.js (Final Robust Version)

import { createClient } from '@vercel/kv';

// 定義快取的鍵值和過期時間 (30 分鐘)
const CACHE_KEY = 'vspo-youtube-data';
const CACHE_TTL_SECONDS = 1800; // 30 minutes * 60 seconds

// 代理函式主體
export default async function handler(request, response) {
  // 檢查 REDIS_URL 是否存在
  if (!process.env.REDIS_URL) {
    console.error('REDIS_URL environment variable not found.');
    return response.status(500).json({ error: 'KV/Redis store is not configured correctly on the server. Please check environment variables and redeploy.' });
  }

  // 手動從 REDIS_URL 解析出 token 和 https url
  let kv;
  try {
    const redisUrl = process.env.REDIS_URL;
    const url = new URL(redisUrl);

    // 從 URL 中提取 token 和 address
    const token = url.password;
    const httpsUrl = `https://` + url.hostname;

    if (!token) {
        throw new Error("Token not found in REDIS_URL");
    }

    // 使用解析出的資訊建立 KV Client
    kv = createClient({
      url: httpsUrl,
      token: token,
    });

  } catch (e) {
      console.error("Failed to parse REDIS_URL and create KV client:", e);
      return response.status(500).json({ error: 'Failed to initialize KV client from REDIS_URL.' });
  }


  // 1. 嘗試從 Vercel KV 讀取快取
  try {
    const cachedData = await kv.get(CACHE_KEY);
    if (cachedData) {
      console.log('Cache hit! Serving from Vercel KV.');
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'HIT');
      return response.status(200).json(cachedData);
    }
  } catch (error) {
    console.error('Error reading from Vercel KV:', error);
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
      
      try {
        await kv.set(CACHE_KEY, data, { ex: CACHE_TTL_SECONDS });
        console.log('Data saved to Vercel KV cache.');
      } catch (error) {
        console.error('Error writing to Vercel KV:', error);
      }
      
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'MISS');
      return response.status(200).json(data);

    } catch (error) {
      console.error(`Error with key starting with ${apiKey.substring(0, 8)}...`, error);
    }
  }

  return response.status(503).json({ error: 'All API keys have exceeded their quotas or failed.' });
}
