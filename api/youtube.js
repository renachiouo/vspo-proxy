// /api/youtube.js (Hybrid Mode: Whitelist + Keyword Search)

import { createClient } from 'redis';

const CACHE_KEY = 'vspo-app-hybrid-data'; // 使用新的快取鑰匙以清除舊資料
const CACHE_TTL_SECONDS = 1800; // 30 分鐘

// --- 白名單與關鍵字設定 ---
const CHANNEL_WHITELIST = [
  'UCho67roS3D13GlRwrSkdF_w', // Irin_translate
];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 
  'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 
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
    
    // --- 混合模式資料抓取邏輯 ---
    const videoSnippets = new Map();

    // 任務 A: 白名單模式
    console.log("Fetching from whitelist channels...");
    if (CHANNEL_WHITELIST.length > 0) {
        const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: CHANNEL_WHITELIST.join(',') });
        const uploadPlaylistIds = channelsResponse.items?.map(item => item.contentDetails.relatedPlaylists.uploads).filter(Boolean) || [];
        const playlistItemsPromises = uploadPlaylistIds.map(playlistId => fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }));
        const playlistItemsResults = await Promise.all(playlistItemsPromises);
        for (const result of playlistItemsResults) {
            result.items?.forEach(item => {
                if (new Date(item.snippet.publishedAt) > oneMonthAgo) {
                    videoSnippets.set(item.snippet.resourceId.videoId, item.snippet);
                }
            });
        }
    }

    // 任務 B: 關鍵字搜尋模式
    console.log("Fetching from keyword search...");
    const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
    const searchResults = await Promise.all(searchPromises);

    for (const result of searchResults) {
      result.items?.forEach(item => {
        // 如果影片尚未被白名單加入，且頻道不在黑名單中，則加入
        if (item.id.videoId && !videoSnippets.has(item.id.videoId) && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
          videoSnippets.set(item.id.videoId, item.snippet);
        }
      });
    }
    
    let finalVideos = [];
    const videoIds = Array.from(videoSnippets.keys());
    if (videoIds.length > 0) {
        // --- 獲取詳細資訊 (邏輯不變) ---
        const videoDetailBatches = batchArray(videoIds, 50);
        const videoDetailPromises = videoDetailBatches.map(id => fetchYouTube('videos', { part: 'statistics,snippet', id: id.join(',') }));
        const videoDetailResults = await Promise.all(videoDetailPromises);
        
        const videoDetailsMap = new Map();
        videoDetailResults.forEach(result => result.items?.forEach(item => videoDetailsMap.set(item.id, item)));

        const allChannelIds = [...new Set(Array.from(videoSnippets.values()).map(s => s.channelId))];
        const channelDetailBatches = batchArray(allChannelIds, 50);
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
            channelId: detail.snippet.channelId, 
            channelTitle: detail.snippet.channelTitle,
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
