// /api/youtube.js (Final Version - Manual URL Parsing)

import { createClient } from '@vercel/kv';

// 定義快取的鍵值和過期時間 (30 分鐘)
const CACHE_KEY = 'vspo-youtube-data';
const CACHE_TTL_SECONDS = 1800; // 30 minutes * 60 seconds

// 代理函式主體
export default async function handler(request, response) {
  
  let kv;
  try {
    // 檢查唯一的 REDIS_URL 環境變數是否存在
    const redisConnectionString = process.env.REDIS_URL;
    if (!redisConnectionString) {
      throw new Error('REDIS_URL environment variable not found.');
    }

    // 手動從 redis://default:TOKEN@ADDRESS:PORT 格式中解析出 token 和 address
    // 這是解決問題的核心
    const match = redisConnectionString.match(/redis:\/\/default:(.+)@(.+)/);
    if (!match || match.length < 3) {
      throw new Error('Could not parse token and address from REDIS_URL.');
    }
    
    const token = match[1];
    const address = match[2]; // address in format: hostname:port
    
    // 組合出 Upstash REST API 需要的 https 網址
    const restApiUrl = `https://${address.split(':')[0]}`;

    // 使用解析出的資訊建立 KV Client
    kv = createClient({
      url: restApiUrl,
      token: token,
    });

  } catch (e) {
      console.error("Failed to initialize KV client:", e);
      return response.status(500).json({ error: `Failed to initialize KV client: ${e.message}` });
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
