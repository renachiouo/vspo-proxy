import { createClient } from 'redis';

// --- Redis Keys Configuration ---
const KEY_PREFIX = 'vspo-db:v1:';
const VIDEOS_SET_KEY = `${KEY_PREFIX}video_ids`;
const VIDEO_HASH_PREFIX = `${KEY_PREFIX}video:`;
const META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:last_updated`;
const UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:update_lock`;

const UPDATE_INTERVAL_SECONDS = 1800; // 30 分鐘

// --- YouTube API 設定 (維持不變) ---
const CHANNEL_WHITELIST = [
  'UCFZ7BPHTgEo5FXuvC9GVY7Q', 
  'UCWq4bX9UMV1ir3liKRIvCHg', // Irin_translate
  'UCbsHmeSh_NGyO8ymoYG02sw',
  'UCd3YtBLO0sGhQ2eTWs80bcg',
  'UCGy_n5NeGfeVzravayHk65Q',
  'UColeV1H-x8MuVLSAdohTOVQ',
];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 
  'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 
  'UCGZK4lLrDYcOKxmWJIERmjQ', 'UCnERutXxnHTLqckbGCUwtAg', 
  'UC-wCI2w1jvR3SgijNeg29qg', 'UCIvTtZq1vMaEQ1iLnp3MEVQ',
  'UCBf3eLt6Nj7AJwkDysm0JWw', 
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
].filter(key => key);

// --- 輔助函式 (維持不變) ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

const isVideoValid = (videoDetail, keywords) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const { title, description, tags } = videoDetail.snippet;
    const searchText = `${title} ${description} ${tags ? tags.join(' ') : ''}`.toLowerCase();
    return keywords.some(keyword => searchText.includes(keyword.toLowerCase()));
};

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

// --- 核心邏輯函式 ---

async function getVideosFromDB(redisClient) {
    const videoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
    if (!videoIds || videoIds.length === 0) return [];

    const pipeline = redisClient.multi();
    videoIds.forEach(id => pipeline.hGetAll(`${VIDEO_HASH_PREFIX}${id}`));
    const results = await pipeline.exec();

    const videos = results.map(video => {
        if (video && Object.keys(video).length > 0) {
            return { ...video, viewCount: parseInt(video.viewCount, 10) || 0, subscriberCount: parseInt(video.subscriberCount, 10) || 0 };
        }
        return null;
    }).filter(Boolean);

    videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
    return videos;
}

const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.warn(`金鑰 ${apiKey.substring(0,8)}... 配額錯誤，嘗試下一個。`);
                continue;
            }
            if(data.error) throw new Error(data.error.message);
            return data;
        } catch(e) {
             console.error(`金鑰 ${apiKey.substring(0,8)}... 發生錯誤`, e);
        }
    }
    throw new Error('所有 API 金鑰都已失效。');
};

async function processAndStoreVideos(videoIds, redisClient) {
    if (videoIds.length === 0) {
        console.log('沒有需要處理的影片。');
        return 0;
    }
    console.log(`準備處理總共 ${videoIds.length} 部影片的資訊...`);

    const videoDetailBatches = batchArray(videoIds, 50);
    const videoDetailPromises = videoDetailBatches.map(id => fetchYouTube('videos', { part: 'statistics,snippet', id: id.join(',') }));
    const videoDetailResults = await Promise.all(videoDetailPromises);
    
    const videoDetailsMap = new Map();
    videoDetailResults.forEach(result => result.items?.forEach(item => videoDetailsMap.set(item.id, item)));

    const allChannelIds = [...new Set(Array.from(videoDetailsMap.values()).map(d => d.snippet.channelId))];
    const channelDetailBatches = batchArray(allChannelIds, 50);
    const channelDetailPromises = channelDetailBatches.map(id => fetchYouTube('channels', { part: 'statistics,snippet', id: id.join(',') }));
    const channelDetailResults = await Promise.all(channelDetailPromises);
    
    const channelStatsMap = new Map();
    channelDetailResults.forEach(result => result.items?.forEach(item => channelStatsMap.set(item.id, item)));
    
    const validVideoIds = new Set();
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    
    for (const videoId of videoIds) {
        const detail = videoDetailsMap.get(videoId);
        if (!detail) continue;

        const isChannelBlacklisted = CHANNEL_BLACKLIST.includes(detail.snippet.channelId);
        const isExpired = new Date(detail.snippet.publishedAt) < oneMonthAgo;
        const isContentValid = isVideoValid(detail, SEARCH_KEYWORDS);
        const isFromWhitelist = CHANNEL_WHITELIST.includes(detail.snippet.channelId);

        if (!isChannelBlacklisted && !isExpired && (isContentValid || isFromWhitelist)) {
            validVideoIds.add(videoId);
            const channelDetails = channelStatsMap.get(detail.snippet.channelId);
            const videoData = {
                id: videoId,
                title: detail.snippet.title,
                thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
                channelId: detail.snippet.channelId, 
                channelTitle: detail.snippet.channelTitle,
                channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
                publishedAt: detail.snippet.publishedAt,
                viewCount: detail.statistics ? (detail.statistics.viewCount || 0) : 0,
                subscriberCount: channelDetails?.statistics ? (channelDetails.statistics.subscriberCount || 0) : 0,
            };
            await redisClient.hSet(`${VIDEO_HASH_PREFIX}${videoId}`, videoData);
        }
    }
    return validVideoIds;
}

/**
 * 新增：深度回填函式
 */
async function deepSearchAndStoreData(redisClient) {
    console.log('開始執行深度回填程序...');
    const allFoundIds = new Set();
    const today = new Date();
    
    for (let i = 0; i < 30; i++) {
        const targetDate = new Date(today);
        targetDate.setDate(today.getDate() - i);

        const publishedAfter = new Date(targetDate.setHours(0, 0, 0, 0)).toISOString();
        const publishedBefore = new Date(targetDate.setHours(23, 59, 59, 999)).toISOString();
        
        console.log(`正在深度搜尋 ${targetDate.toLocaleDateString()} 的影片...`);

        const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter, publishedBefore }));
        const searchResults = await Promise.all(searchPromises);
        
        let dailyFoundCount = 0;
        for (const result of searchResults) {
            result.items?.forEach(item => {
                if (item.id.videoId && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
                    allFoundIds.add(item.id.videoId);
                    dailyFoundCount++;
                }
            });
        }
        console.log(` -> 當日找到 ${dailyFoundCount} 個候選影片。`);
    }

    console.log(`深度回填共找到 ${allFoundIds.size} 個不重複的影片ID，開始處理...`);
    const validVideoIds = await processAndStoreVideos([...allFoundIds], redisClient);
    
    // 將深度回填的結果直接設為新的影片總名單
    if (validVideoIds.size > 0) {
        await redisClient.del(VIDEOS_SET_KEY);
        await redisClient.sAdd(VIDEOS_SET_KEY, [...validVideoIds]);
    }
    console.log(`深度回填完成，資料庫現有 ${validVideoIds.size} 部有效影片。`);
}


/**
 * 標準更新函式
 */
async function updateAndStoreYouTubeData(redisClient) {
    console.log('開始執行標準更新程序...');
    
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
    const newVideoCandidates = new Set();
    if (CHANNEL_WHITELIST.length > 0) {
        const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: CHANNEL_WHITELIST.join(',') });
        const uploadPlaylistIds = channelsResponse.items?.map(item => item.contentDetails.relatedPlaylists.uploads).filter(Boolean) || [];
        const playlistItemsPromises = uploadPlaylistIds.map(playlistId => fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }));
        const playlistItemsResults = await Promise.all(playlistItemsPromises);
        for (const result of playlistItemsResults) {
            result.items?.forEach(item => {
                if (new Date(item.snippet.publishedAt) > oneMonthAgo) {
                    newVideoCandidates.add(item.snippet.resourceId.videoId);
                }
            });
        }
    }

    const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
    const searchResults = await Promise.all(searchPromises);
    for (const result of searchResults) {
      result.items?.forEach(item => {
        if (item.id.videoId && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
          newVideoCandidates.add(item.id.videoId);
        }
      });
    }

    const existingVideoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
    const masterVideoIdList = [...new Set([...newVideoCandidates, ...existingVideoIds])];
    
    const validVideoIds = await processAndStoreVideos(masterVideoIdList, redisClient);
    
    const idsToDelete = existingVideoIds.filter(id => !validVideoIds.has(id));
    if (idsToDelete.length > 0) {
        console.log(`準備刪除 ${idsToDelete.length} 部失效/過期影片...`);
        const pipeline = redisClient.multi();
        pipeline.sRem(VIDEOS_SET_KEY, idsToDelete);
        idsToDelete.forEach(id => pipeline.del(`${VIDEO_HASH_PREFIX}${id}`));
        await pipeline.exec();
    }
    
    console.log(`標準更新完成，資料庫現有 ${validVideoIds.size} 部有效影片。`);
}


// --- 主要的 Handler 函式 ---
export default async function handler(request, response) {
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis 儲存庫未設定。' });
  }

  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const forceRefresh = searchParams.get('force_refresh') === 'true';
  const mode = searchParams.get('mode'); // 'deep' or null
  const providedPassword = searchParams.get('password');
  const adminPassword = process.env.ADMIN_PASSWORD;

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    const visitorCount = await updateAndGetVisitorCount(redisClient);

    const lastUpdated = await redisClient.get(META_LAST_UPDATED_KEY);
    const needsUpdate = !lastUpdated || (Date.now() - parseInt(lastUpdated, 10)) > UPDATE_INTERVAL_SECONDS * 1000;

    if (forceRefresh) {
        if (!adminPassword || providedPassword !== adminPassword) {
            await redisClient.quit();
            response.setHeader('Access-Control-Allow-Origin', '*');
            return response.status(401).json({ error: '無效的管理員密碼。' });
        }
        
        if (mode === 'deep') {
            console.log("管理員密碼驗證成功，強制執行深度回填。");
            await deepSearchAndStoreData(redisClient);
        } else {
            console.log("管理員密碼驗證成功，強制執行標準更新。");
            await updateAndStoreYouTubeData(redisClient);
        }
        await redisClient.set(META_LAST_UPDATED_KEY, Date.now());

    } else if (needsUpdate) {
        const lockAcquired = await redisClient.set(UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 300 }); // 鎖延長至 300 秒以應對深度搜尋
        if (lockAcquired) {
            console.log('需要更新且已獲取鎖，開始標準更新資料。');
            try {
                await updateAndStoreYouTubeData(redisClient);
                await redisClient.set(META_LAST_UPDATED_KEY, Date.now());
            } finally {
                await redisClient.del(UPDATE_LOCK_KEY);
            }
        } else {
            console.log('需要更新，但已有其他程序正在更新。將提供現有資料。');
        }
    }

    console.log('從資料庫獲取影片以回應請求...');
    const videos = await getVideosFromDB(redisClient);
    
    const responseData = {
        videos: videos,
        timestamp: new Date(parseInt(await redisClient.get(META_LAST_UPDATED_KEY), 10) || Date.now()).toISOString(),
        totalVisits: visitorCount.totalVisits,
        todayVisits: visitorCount.todayVisits,
    };

    response.setHeader('Access-Control-Allow-Origin', '*');
    if (redisClient.isOpen) await redisClient.quit();
    return response.status(200).json(responseData);

  } catch (error) {
    console.error("Handler 錯誤:", error.message);
    if (redisClient?.isOpen) await redisClient.quit();
    response.setHeader('Access-Control-Allow-Origin', '*');
    const status = error.message.toLowerCase().includes('quota') ? 429 : 500;
    return response.status(status).json({ error: error.message });
  }
}
