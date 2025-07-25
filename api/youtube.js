import { createClient } from 'redis';

// --- 版本指紋 ---
const SCRIPT_VERSION = '10.0'; 

// --- Redis Keys Configuration (版本提升至 v2) ---
const KEY_PREFIX = 'vspo-db:v2:';
const VIDEOS_SET_KEY = `${KEY_PREFIX}video_ids`;
const VIDEO_HASH_PREFIX = `${KEY_PREFIX}video:`;
const META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:last_updated`;
const UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:update_lock`;
const PENDING_CLASSIFICATION_SET_KEY = `${KEY_PREFIX}pending_classification`; // 新增：待分類影片列表
const CLASSIFICATION_LOCK_KEY = `${KEY_PREFIX}meta:classification_lock`; // 新增：分類工作鎖

const UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘

// --- YouTube API 設定 (與原先相同) ---
const CHANNEL_WHITELIST = [ /* 頻道白名單 */ ];
const SPECIAL_WHITELIST = [ /* 頻道特殊白名單 */ 'UCwFDCL9otNlHyP4yMXunohQ', 'UCPfB7gx9yKfzfVirTX77LFA', 'UCqrTGkqVijNpTMPeWZ4_CgA', 'UCmOdNcq3ttIqrI0RBfdEwcQ', 'UCz4GIV8wNBsLBzZy2wA2KKw', 'UColeV1H-x8MuVLSAdohTOVQ', 'UCGy_n5NeGfeVzravayHk65Q', 'UCibI94U5KocgrbyY9gx3cIg', 'UCd3YtBLO0sGhQ2eTWs80bcg', 'UC9xEUSRrMWbbb-59IehNv3g', 'UCGy_n5NeGfeVzravayHk65Q', 'UCbsHmeSh_NGyO8ymoYG02sw', 'UCWq4bX9UMV1ir3liKRIvCHg', 'UCFZ7BPHTgEo5FXuvC9GVY7Q', 'UCu_KJNiq48jSwBVY7T4-hUQ', 'UC4QGmmdtxLZFFASDVee_atQ', 'UCBiATOGgCqf8uoFfX-pZ1gA', 'UCzqsI2AoNYe2F2WGRYqkEjA', 'UCIgvpS92srpQrGbXDlc8HFQ', 'UC7lPYbAxGzvbobFq_JxxftA', 'UCa_gZCepQ7ZAUAsxnQkuMMA', 'UC0XdcEuxl03Pj6Rm7jcHEnw' ];
const SPECIAL_KEYWORDS = ["vspo", "ぶいすぽ"];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"]; 
const CHANNEL_BLACKLIST = [  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 'UCGZK4lLrDYcOKxmWJIERmjQ', 'UCnERutXxnHTLqckbGCUwtAg', 'UC-wCI2w1vMaEQ1iLnp3MEVQ', 'UCIvTtZq1vMaEQ1iLnp3MEVQ', 'UCBf3eLt6Nj7AJwkDysm0JWw', 'UCnusRHKhMAR7dNM00mk44BA', 'UCEShI32SUz7g9J9ICOs5Y0g' ];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
].filter(key => key);

// --- 輔助函式  ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));
const isVideoValid = (videoDetail, keywords) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description}`.toLowerCase();
    return keywords.some(keyword => searchText.includes(keyword.toLowerCase()));
};
const containsBlacklistedKeyword = (videoDetail, blacklist) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description}`.toLowerCase();
    return blacklist.some(keyword => searchText.includes(keyword.toLowerCase()));
};
async function checkIfShort(videoId) {
    try {
        const response = await fetch(`https://www.youtube.com/shorts/${videoId}`, { method: 'HEAD', redirect: 'manual' });
        return response.status === 200;
    } catch (error) {
        console.error(`探測 Shorts 失敗 (Video ID: ${videoId}):`, error.name);
        return false; // 發生錯誤時，安全起見回傳 false
    }
}
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
        console.error("更新訪客計數失敗:", error);
        return { totalVisits: 0, todayVisits: 0 };
    }
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

// --- 核心邏輯函式 (重構) ---

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

// 重構：處理並儲存影片，加入選項控制是否進行分類
async function processAndStoreVideos(videoIds, redisClient, options = { classify: true, addToPending: false }) {
    if (videoIds.length === 0) return { validVideoIds: new Set(), idsToDelete: [] };
    console.log(`準備處理 ${videoIds.length} 部影片，分類選項: ${options.classify}`);

    const videoDetailsMap = new Map();
    const videoDetailBatches = batchArray(videoIds, 50);
    for (const batch of videoDetailBatches) {
        const result = await fetchYouTube('videos', { part: 'statistics,snippet', id: batch.join(',') });
        result.items?.forEach(item => videoDetailsMap.set(item.id, item));
    }
    
    const channelStatsMap = new Map();
    const allChannelIds = [...new Set(Array.from(videoDetailsMap.values()).map(d => d.snippet.channelId))];
    if (allChannelIds.length > 0) {
        const channelDetailBatches = batchArray(allChannelIds, 50);
        for (const batch of channelDetailBatches) {
            const result = await fetchYouTube('channels', { part: 'statistics,snippet', id: batch.join(',') });
            result.items?.forEach(item => channelStatsMap.set(item.id, item));
        }
    }
    
    const validVideoIds = new Set();
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    
    let shortsMap = new Map();
    if (options.classify) {
        console.log(`開始對 ${videoIds.length} 部影片進行 Shorts 分類...`);
        const shortsCheckPromises = videoIds.map(async (videoId) => {
            if (videoDetailsMap.has(videoId)) {
                const isShort = await checkIfShort(videoId);
                return { videoId, isShort };
            }
            return { videoId, isShort: false };
        });
        const shortsCheckResults = await Promise.all(shortsCheckPromises);
        shortsMap = new Map(shortsCheckResults.map(item => [item.videoId, item.isShort]));
        console.log('Shorts 分類完成。');
    }

    const pipeline = redisClient.multi();
    const videosToClassify = [];

    for (const videoId of videoIds) {
        const detail = videoDetailsMap.get(videoId);
        if (!detail) continue;

        const channelId = detail.snippet.channelId;
        const isChannelBlacklisted = CHANNEL_BLACKLIST.includes(channelId);
        const isKeywordBlacklisted = containsBlacklistedKeyword(detail, KEYWORD_BLACKLIST);
        const isExpired = new Date(detail.snippet.publishedAt) < oneMonthAgo;

        let isContentValid = false;
        if (CHANNEL_WHITELIST.includes(channelId)) isContentValid = true; 
        else if (SPECIAL_WHITELIST.includes(channelId)) isContentValid = isVideoValid(detail, SPECIAL_KEYWORDS); 
        else isContentValid = isVideoValid(detail, SEARCH_KEYWORDS); 

        if (!isChannelBlacklisted && !isKeywordBlacklisted && !isExpired && isContentValid) {
            validVideoIds.add(videoId);
            const channelDetails = channelStatsMap.get(channelId);
            const { title, description } = detail.snippet;
            
            const videoData = {
                id: videoId,
                title: title,
                searchableText: `${title || ''} ${description || ''}`.toLowerCase(),
                thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
                channelId: channelId, 
                channelTitle: detail.snippet.channelTitle,
                channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
                publishedAt: detail.snippet.publishedAt,
                viewCount: detail.statistics?.viewCount || 0,
                subscriberCount: channelDetails?.statistics?.subscriberCount || 0,
            };

            if (options.classify) {
                videoData.videoType = shortsMap.get(videoId) ? 'short' : 'video';
            } else if (options.addToPending) {
                videosToClassify.push(videoId);
            }
            
            pipeline.hSet(`${VIDEO_HASH_PREFIX}${videoId}`, videoData);
        }
    }
    
    if (options.addToPending && videosToClassify.length > 0) {
        console.log(`將 ${videosToClassify.length} 部影片加入待分類列表。`);
        pipeline.sAdd(PENDING_CLASSIFICATION_SET_KEY, videosToClassify);
    }

    await pipeline.exec();
    
    const allIdsInDB = await redisClient.sMembers(VIDEOS_SET_KEY);
    const idsToDelete = allIdsInDB.filter(id => !validVideoIds.has(id));

    return { validVideoIds, idsToDelete };
}

// 重構：主更新函式，加入選項
async function updateAndStoreYouTubeData(redisClient, options = { classify: true, addToPending: false }) {
    console.log(`開始執行更新程序，選項:`, options);
    
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
    const newVideoCandidates = new Set();
    
    const allWhitelists = [...CHANNEL_WHITELIST, ...SPECIAL_WHITELIST];
    if (allWhitelists.length > 0) {
        const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: allWhitelists.join(',') });
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

    const { validVideoIds, idsToDelete } = await processAndStoreVideos([...newVideoCandidates], redisClient, options);
    
    const pipeline = redisClient.multi();
    if (idsToDelete.length > 0) {
        console.log(`準備刪除 ${idsToDelete.length} 部失效/過期影片...`);
        pipeline.sRem(VIDEOS_SET_KEY, idsToDelete);
        idsToDelete.forEach(id => pipeline.del(`${VIDEO_HASH_PREFIX}${id}`));
    }
    if (validVideoIds.size > 0) {
        pipeline.sAdd(VIDEOS_SET_KEY, [...validVideoIds]);
    }
    await pipeline.exec();
    
    console.log(`更新程序完成。`);
}

// --- API 端點處理函式 ---

// GET /api/youtube (主要資料獲取)
async function handleGetData(request, response, redisClient) {
    const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
    const forceRefresh = searchParams.get('force_refresh') === 'true';
    const providedPassword = searchParams.get('password');
    const adminPassword = process.env.ADMIN_PASSWORD;

    const visitorCount = await updateAndGetVisitorCount(redisClient);
    const lastUpdated = await redisClient.get(META_LAST_UPDATED_KEY);
    const needsUpdate = !lastUpdated || (Date.now() - parseInt(lastUpdated, 10)) > UPDATE_INTERVAL_SECONDS * 1000;

    if (forceRefresh) {
        if (!adminPassword || providedPassword !== adminPassword) {
            return response.status(401).json({ error: '無效的管理員密碼。' });
        }
        console.log("管理員密碼驗證成功，強制執行快速更新 (不分類)。");
        // 強制更新時，不分類，而是將任務加入待辦清單
        await updateAndStoreYouTubeData(redisClient, { classify: false, addToPending: true });
        await redisClient.set(META_LAST_UPDATED_KEY, Date.now());
    } else if (needsUpdate) {
        const lockAcquired = await redisClient.set(UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 300 });
        if (lockAcquired) {
            console.log('需要更新且已獲取鎖，開始背景標準更新 (含分類)。');
            try {
                // 排程更新時，完整執行所有分類
                await updateAndStoreYouTubeData(redisClient, { classify: true, addToPending: false });
                await redisClient.set(META_LAST_UPDATED_KEY, Date.now());
            } finally {
                await redisClient.del(UPDATE_LOCK_KEY);
            }
        } else {
            console.log('需要更新，但已有其他程序正在更新。');
        }
    }

    console.log('從資料庫獲取影片以回應請求...');
    const videos = await getVideosFromDB(redisClient);
    const updatedTimestamp = await redisClient.get(META_LAST_UPDATED_KEY);
    
    return response.status(200).json({
        videos: videos,
        timestamp: new Date(parseInt(updatedTimestamp, 10) || Date.now()).toISOString(),
        totalVisits: visitorCount.totalVisits,
        todayVisits: visitorCount.todayVisits,
        script_version: SCRIPT_VERSION,
    });
}

// POST /api/classify-videos (觸發背景分類工作)
async function handleClassifyVideos(request, response, redisClient) {
    const lockAcquired = await redisClient.set(CLASSIFICATION_LOCK_KEY, 'locked', { NX: true, EX: 600 });
    if (!lockAcquired) {
        return response.status(429).json({ message: "已有分類任務正在進行中。" });
    }

    try {
        const videoIdsToClassify = await redisClient.sMembers(PENDING_CLASSIFICATION_SET_KEY);
        if (videoIdsToClassify.length === 0) {
            return response.status(200).json({ message: "沒有需要分類的影片。" });
        }
        
        console.log(`開始背景分類 ${videoIdsToClassify.length} 部影片...`);

        for (const videoId of videoIdsToClassify) {
            const isShort = await checkIfShort(videoId);
            const videoType = isShort ? 'short' : 'video';
            // 更新 Redis 中的影片資料，並從待辦清單中移除
            await redisClient.hSet(`${VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
            await redisClient.sRem(PENDING_CLASSIFICATION_SET_KEY, videoId);
        }

        console.log(`背景分類完成。`);
        return response.status(200).json({ message: "分類成功。" });
    } catch(error) {
        console.error("分類影片時發生錯誤:", error);
        return response.status(500).json({ error: "分類時發生內部錯誤。" });
    } finally {
        await redisClient.del(CLASSIFICATION_LOCK_KEY);
    }
}

// GET /api/check-status (輪詢分類狀態)
async function handleCheckStatus(request, response, redisClient) {
    const count = await redisClient.sCard(PENDING_CLASSIFICATION_SET_KEY);
    return response.status(200).json({ isDone: count === 0 });
}


// --- 主要的 Handler 函式 (路由) ---
export default async function handler(request, response) {
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis 儲存庫未設定。' });
  }

  const url = new URL(request.url, `http://${request.headers.host}`);
  const path = url.pathname;
  
  // 設定 CORS 標頭
  response.setHeader('Access-Control-Allow-Origin', '*');
  response.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  response.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (request.method === 'OPTIONS') {
    return response.status(204).end();
  }

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    // 根據路徑路由到不同的處理函式
    if (path.endsWith('/api/youtube')) {
        return await handleGetData(request, response, redisClient);
    } else if (path.endsWith('/api/classify-videos')) {
        return await handleClassifyVideos(request, response, redisClient);
    } else if (path.endsWith('/api/check-status')) {
        return await handleCheckStatus(request, response, redisClient);
    } else {
        return response.status(404).json({ error: '找不到指定的 API 端點。' });
    }

  } catch (error) {
    console.error(`Handler 錯誤 (${path}):`, error.message);
    const status = error.message.toLowerCase().includes('quota') ? 429 : 500;
    return response.status(status).json({ error: error.message });
  } finally {
    if (redisClient?.isOpen) await redisClient.quit();
  }
}
