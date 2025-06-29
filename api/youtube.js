// /api/youtube.js (Final Architecture with Timestamp)

import { createClient } from 'redis';

const CACHE_KEY = 'vspo-app-final-data-with-timestamp'; // 使用新的快取鑰匙
const CACHE_TTL_SECONDS = 1800; // 30 分鐘快取

const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const OFFICIAL_CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCGCb_d_H-A7A2y52M4S3W-w', 'UCyLGc23ry_p-zEAS_p_2qlg',
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
].filter(key => key);

const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

export default async function handler(request, response) {
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis store is not configured.' });
  }

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

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

  console.log('Cache miss. Performing full data fetch from YouTube API...');
  
  try {
    const fetchYouTube = async (endpoint, params) => {
        for (const apiKey of apiKeys) {
            const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                    console.warn(`Key starting with ${apiKey.substring(0,8)}... has quota error. Trying next.`);
                    continue;
                }
                if(data.error) throw new Error(data.error.message);
                return data;
            } catch(e) {
                 console.error(`Error with key starting with ${apiKey.substring(0,8)}...`, e);
            }
        }
        throw new Error('All API keys have failed.');
    };

    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
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
    
    let finalVideos = [];
    if (videoIds.length > 0) {
        const videoDetailBatches = batchArray(videoIds, 50);
        const videoDetailPromises = videoDetailBatches.map(id => fetchYouTube('videos', { part: 'statistics,snippet', id: id.join(',') }));
        const videoDetailResults = await Promise.all(videoDetailPromises);
        
        const videoDetailsMap = new Map();
        videoDetailResults.forEach(result => result.items?.forEach(item => videoDetailsMap.set(item.id, item)));

        const channelIds = [...new Set(Array.from(videoSnippets.values()).map(s => s.channelId))];
        const channelDetailBatches = batchArray(channelIds, 50);
        const channelDetailPromises = channelDetailBatches.map(id => fetchYouTube('channels', { part: 'statistics', id: id.join(',') }));
        const channelDetailResults = await Promise.all(channelDetailPromises);
        
        const channelStatsMap = new Map();
        channelDetailResults.forEach(result => result.items?.forEach(item => channelStatsMap.set(item.id, item.statistics)));

        finalVideos = videoIds.map(id => {
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
    }
    
    // *** 這是關鍵的修正：建立一個包含資料和時間戳的包裹 ***
    const dataToCache = {
        videos: finalVideos,
        timestamp: new Date().toISOString(),
    };
    
    try {
      if (!redisClient?.isOpen) {
           redisClient = createClient({ url: redisConnectionString });
           await redisClient.connect();
      }
      await redisClient.set(CACHE_KEY, JSON.stringify(dataToCache), { EX: CACHE_TTL_SECONDS });
      console.log('Final data structure with timestamp saved to Redis cache.');
    } catch (error) {
      console.error('Error writing final data to Redis cache:', error);
    }
    
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('X-Cache-Status', 'MISS');
    if (redisClient?.isOpen) await redisClient.quit();
    return response.status(200).json(dataToCache);

  } catch (error) {
    console.error("Handler Error:", error.message);
    if (redisClient?.isOpen) await redisClient.quit();
    response.setHeader('Access-Control-Allow-Origin', '*');
    const status = error.message.toLowerCase().includes('quota') ? 429 : 500;
    return response.status(status).json({ error: error.message });
  }
}
