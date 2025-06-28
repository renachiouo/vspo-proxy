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
      // 如果有快取，直接回傳
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'HIT');
      return response.status(200).json(cachedData);
    }
  } catch (error) {
    console.error('Error reading from Vercel KV:', error);
    // 如果讀取快取失敗，沒關係，我們稍後會繼續從 YouTube 抓取
  }

  // 2. 如果沒有快取，則從 YouTube API 抓取新資料
  console.log('Cache miss. Fetching from YouTube API...');
  const apiKey = process.env.YOUTUBE_API_KEY_1; // 我們只用一個金鑰就夠了，因為現在呼叫次數很少

  if (!apiKey) {
    return response.status(500).json({ error: 'API key not configured on server.' });
  }
  
  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const endpoint = searchParams.get('endpoint');
  searchParams.delete('endpoint');
  
  const youtubeApiUrl = `https://www.googleapis.com/youtube/v3/${endpoint}?${searchParams.toString()}&key=${apiKey}`;

  try {
    const youtubeResponse = await fetch(youtubeApiUrl);
    const data = await youtubeResponse.json();

    if (data.error) {
        throw new Error(data.error.message);
    }

    // 3. 將新資料寫入 Vercel KV 快取，並設定過期時間
    try {
      await kv.set(CACHE_KEY, data, { ex: CACHE_TTL_SECONDS });
      console.log('Data saved to Vercel KV cache.');
    } catch (error) {
      console.error('Error writing to Vercel KV:', error);
    }

    // 4. 將新資料回傳給前端
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('X-Cache-Status', 'MISS');
    response.status(200).json(data);

  } catch (error) {
    response.status(500).json({ error: `Failed to fetch from YouTube: ${error.message}` });
  }
}
