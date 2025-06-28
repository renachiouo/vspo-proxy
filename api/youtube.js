// /api/youtube.js (Final Architecture)

import { createClient } from 'redis';

const CACHE_KEY = 'vspo-app-final-data'; // 使用一個固定的快取鑰匙
const CACHE_TTL_SECONDS = 1800; // 30分鐘快取

const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const OFFICIAL_CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCGCb_d_H-A7A2y52M4S3W-w', 'UCyLGc23ry_p-zEAS_p_2qlg',
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
].filter(key => key);

// Helper to batch array processing
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

// Main handler function
export default async function handler(request, response) {
  // --- 1. 檢查 Redis 連線 ---
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis store is not configured.' });
  }

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    // --- 2. 嘗試讀取快取 ---
    const cachedResult = await redisClient.get(CACHE_KEY);
    if (cachedResult) {
      console.log('Cache hit! Serving final data from Redis.');
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('X-Cache-Status', 'HIT');
      await redisClient.quit();
      return response.status(200).json(JSON.parse(cachedResult));
    }
  } catch (error) {
    console.error('Error with Redis connection or cache read:', error);
    if (redisClient?.isOpen) await redisClient.quit();
  }

  // --- 3. 如果沒有快取，則執行完整的 YouTube API 抓取流程 ---
  console.log('Cache miss. Performing full data fetch from YouTube API...');
  
  try {
    let activeApiKey = '';
    let quotaErrorCount = 0;

    // Function to make a YouTube API call with key rotation
    const fetchYouTube = async (endpoint, params) => {
        for (const apiKey of apiKeys) {
            const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                    console.warn(`Key starting with ${apiKey.substring(0,8)}... has quota error. Trying next.`);
                    quotaErrorCount++;
                    continue; // Try next key
                }
                if(data.error) throw new Error(data.error.message);
                activeApiKey = apiKey; // Mark this key as working
                return data;
            } catch(e) {
                 console.error(`Error with key starting with ${apiKey.substring(0,8)}...`, e);
            }
        }
        if (quotaErrorCount === apiKeys.length) {
            throw new Error('All API keys have exceeded their daily quota.');
        }
        throw new Error('All API keys failed.');
    };

    // --- 開始執行所有 API 請求 ---
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
    // a. 搜尋所有關鍵字
    const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
    const searchResults = await Promise.all(searchPromises);

    const videoSnippets = new Map();
    for (const result of searchResults) {
      result.items?.forEach(item => {
        if (item.id.videoId && !videoSnippets.has(item.id.videoId) && !OFFICIAL_CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
          videoSnippets.set(item.id.videoId, item.snippet);
        }
      });
    }

    const videoIds = Array.from(videoSnippets.keys());
    if (videoIds.length === 0) {
      response.setHeader('Access-Control-Allow-Origin', '*');
      if (redisClient?.isOpen) await redisClient.quit();
      return response.status(200).json([]); // 回傳空陣列代表真的沒影片
    }

    // b. 批次獲取影片詳細資訊
    const videoDetailBatches = batchArray(videoIds, 50);
    const videoDetailPromises = videoDetailBatches.map(id => fetchYouTube('videos', { part: 'statistics,snippet', id: id.join(',') }));
    const videoDetailResults = await Promise.all(videoDetailPromises);
    
    const videoDetailsMap = new Map();
    videoDetailResults.forEach(result => result.items?.forEach(item => videoDetailsMap.set(item.id, item)));

    // c. 批次獲取頻道詳細資訊
    const channelIds = [...new Set(Array.from(videoSnippets.values()).map(s => s.channelId))];
    const channelDetailBatches = batchArray(channelIds, 50);
    const channelDetailPromises = channelDetailBatches.map(id => fetchYouTube('channels', { part: 'statistics', id: id.join(',') }));
    const channelDetailResults = await Promise.all(channelDetailPromises);
    
    const channelStatsMap = new Map();
    channelDetailResults.forEach(result => result.items?.forEach(item => channelStatsMap.set(item.id, item.statistics)));

    // d. 組裝最終資料
    const finalVideos = videoIds.map(id => {
      const detail = videoDetailsMap.get(id);
      if (!detail) return null;
      const channelStats = channelStatsMap.get(detail.snippet.channelId);
      return {
        id,
        title: detail.snippet.title,
        thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
        channelId: detail.snippet.channelId,
        channelTitle: detail.snippet.channelTitle,
        publishedAt: detail.snippet.publishedAt,
        viewCount: detail.statistics ? parseInt(detail.statistics.viewCount, 10) : 0,
        subscriberCount: channelStats ? parseInt(channelStats.subscriberCount, 10) : 0,
      };
    }).filter(Boolean);
    
    // --- 4. 將最終的完整結果寫入快取 ---
    try {
      if (!redisClient?.isOpen) {
           redisClient = createClient({ url: redisConnectionString });
           await redisClient.connect();
      }
      await redisClient.set(CACHE_KEY, JSON.stringify(finalVideos), { EX: CACHE_TTL_SECONDS });
      console.log('Final data structure saved to Redis cache.');
    } catch (error) {
      console.error('Error writing final data to Redis cache:', error);
    }
    
    // --- 5. 回傳最終結果 ---
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('X-Cache-Status', 'MISS');
    if (redisClient?.isOpen) await redisClient.quit();
    return response.status(200).json(finalVideos);

  } catch (error) {
    console.error("Handler Error:", error.message);
    if (redisClient?.isOpen) await redisClient.quit();
    response.setHeader('Access-Control-Allow-Origin', '*');
    const status = error.message.toLowerCase().includes('quota') ? 429 : 500;
    return response.status(status).json({ error: error.message });
  }
}
