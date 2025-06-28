// /api/youtube.js

import { createClient } from '@vercel/kv';

// 從環境變數中讀取 Vercel KV 的連線資訊
const kv = createClient({
  url: process.env.KV_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// 定義快取的鍵值和過期時間 (30 分鐘)
const CACHE_KEY = 'vspo-youtube-data';
const CACHE_TTL_SECONDS = 1800; // 30 minutes * 60 seconds

// 代理函式主體
export default async function handler(request, response) {
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
  
  // 從環境變數中安全地讀取您的所有 API 金鑰
  const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
  ].filter(key => key); // 過濾掉未設定的金鑰

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

      // 檢查是否為配額錯誤
      if (data.error && data.error.message.toLowerCase().includes('quota')) {
        console.warn(`Key starting with ${apiKey.substring(0, 8)}... has exceeded its quota. Trying next key.`);
        continue; // 如果是配額錯誤，就繼續循環，嘗試下一個金鑰
      }

      // 如果是其他錯誤，直接回傳
      if (data.error) {
        throw new Error(data.error.message);
      }
      
      // 成功！將資料寫入快取並回傳
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
      // 捕捉網路錯誤等問題
      console.error(`Error with key starting with ${apiKey.substring(0, 8)}...`, error);
      // 繼續嘗試下一個金鑰
    }
  }

  // 如果所有金鑰都嘗試失敗了
  return response.status(503).json({ error: 'All API keys have exceeded their quotas or failed.' });
}
