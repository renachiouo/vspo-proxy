// /api/youtube.js (Admin Force Refresh)

import { createClient } from 'redis';

const CACHE_KEY = 'vspo-app-final-data-with-timestamp';
const CACHE_TTL_SECONDS = 1800; // 30 分鐘

const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 
  'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 
  'UCnERutXxnHTLqckbGCUwtAg', 
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
].filter(key => key);

const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

async function updateAndGetVisitorCount(redisClient) {
    try {
        const todayStr = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Taipei' }).format(new Date());
        const todayKey = `visits:today:${todayStr}`;
        const [totalVisits, todayVisits] = await Promise.all([
            redisClient.incr('visits:total'),
            redisClient.incr(todayKey)
        ]);
        await redisClient.expire(todayKey, 90000);
        return { totalVisits, todayVisits };
    } catch (error) {
        console.error("Failed to update visitor count:", error);
        return { totalVisits: 0, todayVisits: 0 };
    }
}

async function getFullYouTubeData() {
    const fetchYouTube = async (endpoint, params) => {
        for (const apiKey of apiKeys) {
            const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.warn(`Key starting with ${apiKey.substring(0,8)}... has quota error. Trying next.`);
                continue;
            }
            if(data.error) throw new Error(data.error.message);
            return data;
        }
        throw new Error('All API keys have failed.');
    };

    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
    const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
    const searchResults = await Promise.all(searchPromises);

    const videoSnippets = new Map();
    searchResults.forEach(result => result.items?.forEach(item => {
        if (item.id.videoId && !videoSnippets.has(item.id.videoId) && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
          videoSnippets.set(item.id.videoId, item.snippet);
        }
    }));
    
    let finalVideos = [];
    const videoIds = Array.from(videoSnippets.keys());
    if (videoIds.length > 0) {
        const videoDetailBatches = batchArray(videoIds, 50);
        const videoDetailPromises = videoDetailBatches.map(id => fetchYouTube('videos', { part: 'statistics,snippet', id: id.join(',') }));
        const videoDetailResults = await Promise.all(videoDetailPromises);
        
        const videoDetailsMap = new Map();
        videoDetailResults.forEach(result => result.items?.forEach(item => videoDetailsMap.set(item.id, item)));

        const channelIds = [...new Set(Array.from(videoSnippets.values()).map(s => s.channelId))];
        const channelDetailBatches = batchArray(channelIds, 50);
        const channelDetailPromises = channelDetailBatches.map(id => fetchYouTube('channels', { part: 'statistics,snippet', id: id.join(',') }));
        const channelDetailResults = await Promise.all(channelDetailPromises);
        
        const channelStatsMap = new Map();
        channelDetailResults.forEach(result => result.items?.forEach(item => channelStatsMap.set(item.id, item)));

        finalVideos = videoIds.map(id => {
          const detail = videoDetailsMap.get(id);
          if (!detail) return null;
          const channelDetails = channelStatsMap.get(detail.snippet.channelId);
          return {
            id, title: detail.snippet.title,
            thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
            channelId: detail.snippet.channelId, channelTitle: detail.snippet.channelTitle,
            channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
            publishedAt: detail.snippet.publishedAt,
            viewCount: detail.statistics ? parseInt(detail.statistics.viewCount, 10) : 0,
            subscriberCount: channelDetails?.statistics ? parseInt(channelDetails.statistics.subscriberCount, 10) : 0,
          };
        }).filter(Boolean);
    }
    return finalVideos;
}


export default async function handler(request, response) {
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis store is not configured.' });
  }

  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const forceRefresh = searchParams.get('force_refresh') === 'true';
  const providedPassword = searchParams.get('password');
  const adminPassword = process.env.ADMIN_PASSWORD;

  let redisClient;
  let visitorCount = { totalVisits: 0, todayVisits: 0 };

  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();
    visitorCount = await updateAndGetVisitorCount(redisClient);

    // *** 這是關鍵的修正：檢查是否為強制更新請求 ***
    if (!forceRefresh) {
        const cachedResult = await redisClient.get(CACHE_KEY);
        if (cachedResult) {
          console.log('Cache hit! Serving final data from Redis.');
          const cachedData = JSON.parse(cachedResult);
          cachedData.totalVisits = visitorCount.totalVisits;
          cachedData.todayVisits = visitorCount.todayVisits;

          response.setHeader('Access-Control-Allow-Origin', '*');
          response.setHeader('X-Cache-Status', 'HIT');
          await redisClient.quit();
          return response.status(200).json(cachedData);
        }
    } else {
        // 如果是強制更新，先驗證密碼
        if (!adminPassword || providedPassword !== adminPassword) {
            await redisClient.quit();
            response.setHeader('Access-Control-Allow-Origin', '*');
            return response.status(401).json({ error: 'Invalid password for force refresh.' });
        }
        console.log("Admin password verified. Forcing cache refresh.");
    }

  } catch (error) {
    console.error('Error with Redis connection or cache read:', error);
    if (redisClient?.isOpen) await redisClient.quit();
  }

  console.log('Cache miss or force refresh. Performing full data fetch from YouTube API...');
  
  try {
    const finalVideos = await getFullYouTubeData();
    
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
    
    dataToCache.totalVisits = visitorCount.totalVisits;
    dataToCache.todayVisits = visitorCount.todayVisits;

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
