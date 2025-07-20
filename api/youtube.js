// Force rebuild at 2025-07-19
import { createClient } from 'redis';

// --- 版本指紋 ---
const SCRIPT_VERSION = '8.4-FINAL'; // 版本號提升

// --- Redis Keys Configuration ---
const KEY_PREFIX = 'vspo-db:v1:';
const VIDEOS_SET_KEY = `${KEY_PREFIX}video_ids`;
const VIDEO_HASH_PREFIX = `${KEY_PREFIX}video:`;
const META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:last_updated`;
const UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:update_lock`;
const BACKFILL_COUNTER_KEY = `${KEY_PREFIX}meta:backfill_counter`;

const UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘
const BACKFILL_BATCH_SIZE = 15;

// --- YouTube API 設定 ---
const CHANNEL_WHITELIST = [ // 第一層：完全信任，不檢查關鍵字
  'UCFZ7BPHTgEo5FXuvC9GVY7Q', 
  'UCWq4bX9UMV1ir3liKRIvCHg', 
  'UCbsHmeSh_NGyO8ymoYG02sw',
  'UCGy_n5NeGfeVzravayHk65Q',
  'UC9xEUSRrMWbbb-59IehNv3g',
];

const SPECIAL_WHITELIST = [ // 第二層特殊白名單
    'UCz4GIV8wNBsLBzZy2wA2KKw', 'UColeV1H-x8MuVLSAdohTOVQ',
    'UCGy_n5NeGfeVzravayHk65Q', 'UCibI94U5KocgrbyY9gx3cIg',
    'UCd3YtBLO0sGhQ2eTWs80bcg', 
];
const SPECIAL_KEYWORDS = ["vspo"];

// 第三層：一般搜尋用的關鍵字
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"]; 
const CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 
  'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 
  'UCGZK4lLrDYcOKxmWJIERmjQ', 'UCnERutXxnHTLqckbGCUwtAg', 
  'UC-wCI2w1vMaEQ1iLnp3MEVQ', 'UCIvTtZq1vMaEQ1iLnp3MEVQ',
  'UCBf3eLt6Nj7AJwkDysm0JWw', 'UCnusRHKhMAR7dNM00mk44BA',
  'UCEShI32SUz7g9J9ICOs5Y0g',
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
    process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5,
    process.env.YOUTUBE_API_KEY_6,
].filter(key => key);

// --- 輔助函式 ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

const isVideoValid = (videoDetail, keywords) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const { title, description } = videoDetail.snippet;
    const searchText = `${title} ${description}`.toLowerCase();
    return keywords.some(keyword => searchText.includes(keyword.toLowerCase()));
};

const containsBlacklistedKeyword = (videoDetail, blacklist) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const { title, description } = videoDetail.snippet;
    const searchText = `${title} ${description}`.toLowerCase();
    return blacklist.some(keyword => searchText.includes(keyword.toLowerCase()));
};

async function checkIfShort(videoId) {
    try {
        // 使用 HEAD 方法，我們只關心 status code，不需要 body，這樣更快
        const response = await fetch(`https://www.youtube.com/shorts/${videoId}`, { method: 'HEAD', redirect: 'manual' });
        // status 200 代表這是一個有效的 Shorts 頁面
        // status 303 (See Other) 通常會將 /shorts/xyz 導向 /watch?v=xyz，代表它不是 Shorts
        return response.status === 200;
    } catch (error) {
        console.error(`探測 Shorts 失敗 (Video ID: ${videoId}):`, error);
        return false;
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
        await redisClient.expire(todayKey, 90000); // 設置過期時間 (略大於一天)
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
        return { validVideoIds: new Set(), idsToDelete: [], classifiedIds: new Set() };
    }
    console.log(`準備處理總共 ${videoIds.length} 部影片的資訊...`);

    const classifiedIds = new Set();
    
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
    
    const pipeline = redisClient.multi();

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
            const searchableText = `${title || ''} ${description || ''}`.toLowerCase();
            
            const existingVideoType = await redisClient.hGet(`${VIDEO_HASH_PREFIX}${videoId}`, 'videoType');
            let videoType = existingVideoType;
            
            if (!videoType) {
                const isShort = await checkIfShort(videoId);
                videoType = isShort ? 'short' : 'video';
                classifiedIds.add(videoId);
                console.log(`[分類] 影片 ${videoId} 已分類為: ${videoType}`);
            }

            const videoData = {
                id: videoId,
                title: title,
                searchableText: searchableText,
                thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
                channelId: channelId, 
                channelTitle: detail.snippet.channelTitle,
                channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
                publishedAt: detail.snippet.publishedAt,
                viewCount: detail.statistics ? (detail.statistics.viewCount || 0) : 0,
                subscriberCount: channelDetails?.statistics ? (channelDetails.statistics.subscriberCount || 0) : 0,
                videoType: videoType,
            };
            pipeline.hSet(`${VIDEO_HASH_PREFIX}${videoId}`, videoData);
        }
    }
    await pipeline.exec();
    
    const allIdsInDB = await redisClient.sMembers(VIDEOS_SET_KEY);
    const idsToDelete = allIdsInDB.filter(id => !validVideoIds.has(id));

    return { validVideoIds, idsToDelete, classifiedIds };
}

async function searchSingleDayAndStoreData(dateString, redisClient) {
    // ... 此函式內容不變
}

async function deepSearchAndStoreData(redisClient) {
    // ... 此函式內容不變
}

async function updateAndStoreYouTubeData(redisClient) {
    console.log('開始執行標準更新程序...');
    
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

    const existingVideoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
    
    const videosToBackfill = [];
    if (existingVideoIds.length > 0) {
        const pipelineCheck = redisClient.multi();
        existingVideoIds.forEach(id => pipelineCheck.hGet(`${VIDEO_HASH_PREFIX}${id}`, 'videoType'));
        const getTypeResults = await pipelineCheck.exec();
        
        for (let i = 0; i < existingVideoIds.length; i++) {
            if (!getTypeResults[i]) { 
                videosToBackfill.push(existingVideoIds[i]);
            }
        }
    }
    
    const backfillBatch = videosToBackfill.slice(0, BACKFILL_BATCH_SIZE);
    const backfillBatchSet = new Set(backfillBatch);
    if (backfillBatch.length > 0) {
        console.log(`[漸進式更新] 發現 ${videosToBackfill.length} 部舊影片待分類，本次處理 ${backfillBatch.length} 部。`);
    }

    const masterVideoIdList = [...new Set([...newVideoCandidates, ...backfillBatch])];
    
    const { validVideoIds, idsToDelete, classifiedIds } = await processAndStoreVideos(masterVideoIdList, redisClient);
    
    let backfilledCount = 0;
    for (const id of classifiedIds) {
        if (backfillBatchSet.has(id)) {
            backfilledCount++;
        }
    }

    const pipeline = redisClient.multi();

    if (backfilledCount > 0) {
        console.log(` -> 本次共回填 ${backfilledCount} 部舊影片，更新計數器。`);
        pipeline.decrBy(BACKFILL_COUNTER_KEY, backfilledCount);
    }
    
    if (idsToDelete.length > 0) {
        console.log(`準備刪除 ${idsToDelete.length} 部失效/過期影片...`);
        pipeline.sRem(VIDEOS_SET_KEY, idsToDelete);
        idsToDelete.forEach(id => pipeline.del(`${VIDEO_HASH_PREFIX}${id}`));
    }
    
    if (validVideoIds.size > 0) {
        pipeline.sAdd(VIDEOS_SET_KEY, [...validVideoIds]);
    }

    await pipeline.exec();
    
    console.log(`標準更新完成。`);
}

async function initializeBackfill(redisClient) {
    console.log('開始初始化 Shorts 回填計數器...');
    const allVideoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
    if (allVideoIds.length === 0) {
        await redisClient.set(BACKFILL_COUNTER_KEY, 0);
        console.log('資料庫中沒有影片，計數器設為 0。');
        return 0;
    }

    let unclassifiedCount = 0;
    const idBatches = batchArray(allVideoIds, 200); 

    for (const batch of idBatches) {
        const pipeline = redisClient.multi();
        batch.forEach(id => {
            pipeline.hGet(`${VIDEO_HASH_PREFIX}${id}`, 'videoType');
        });
        const results = await pipeline.exec();
        unclassifiedCount += results.filter(videoType => !videoType).length;
    }
    
    await redisClient.set(BACKFILL_COUNTER_KEY, unclassifiedCount);
    console.log(`計算完成：共有 ${unclassifiedCount} 部影片需要回填分類。計數器已設定。`);
    return unclassifiedCount;
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
  const mode = searchParams.get('mode');
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
        
        // --- 管理員指令區 ---
        if (mode === 'deep') {
            console.log("管理員密碼驗證成功，強制執行深度回填。");
            await deepSearchAndStoreData(redisClient);
        } else if (mode === 'start_backfill') {
            console.log("管理員密碼驗證成功，開始初始化 Shorts 回填計數器。");
            const count = await initializeBackfill(redisClient);
            response.setHeader('Access-Control-Allow-Origin', '*');
            if (redisClient.isOpen) await redisClient.quit();
            return response.status(200).json({ message: `Shorts 回填已初始化，共有 ${count} 部影片待處理。` });
        } else if (mode === 'reset_counter') {
            console.log("管理員密碼驗證成功，強制重置回填計數器。");
            await redisClient.set(BACKFILL_COUNTER_KEY, 0);
            response.setHeader('Access-Control-Allow-Origin', '*');
            if (redisClient.isOpen) await redisClient.quit();
            return response.status(200).json({ message: "回填計數器已成功重置為 0。" });
        } else if (mode === 'reset_classification') { // ★ 新增：重置分類模式
            console.log("管理員指令：重置所有影片的分類狀態。");
            const allVideoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
            if (allVideoIds.length > 0) {
                const pipeline = redisClient.multi();
                allVideoIds.forEach(id => {
                    pipeline.hDel(`${VIDEO_HASH_PREFIX}${id}`, 'videoType');
                });
                await pipeline.exec();
            }
            const newCount = await initializeBackfill(redisClient); // 重置後立刻重新計算
            response.setHeader('Access-Control-Allow-Origin', '*');
            if (redisClient.isOpen) await redisClient.quit();
            return response.status(200).json({ message: `已重置 ${allVideoIds.length} 部影片的分類狀態。新的待辦計數為: ${newCount}` });
        } else if (mode === 'inspect_videos') { // ★ 新增：抽查模式
            console.log("管理員指令：抽查影片分類狀態。");
            const allVideoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
            const sampleSize = Math.min(20, allVideoIds.length);
            const sampleIds = allVideoIds.sort(() => 0.5 - Math.random()).slice(0, sampleSize);
            
            let sampleData = [];
            if (sampleSize > 0) {
                const pipeline = redisClient.multi();
                sampleIds.forEach(id => pipeline.hGetAll(`${VIDEO_HASH_PREFIX}${id}`));
                sampleData = await pipeline.exec();
            }
            
            response.setHeader('Access-Control-Allow-Origin', '*');
            if (redisClient.isOpen) await redisClient.quit();
            return response.status(200).json({ 
                message: `抽查 ${sampleSize} 部影片的結果。請檢查 'videoType' 欄位。`,
                sample_videos: sampleData 
            });
        } else if (mode && /^\d{8}$/.test(mode)) {
            console.log(`管理員密碼驗證成功，強制執行指定日期搜尋：${mode}`);
            await searchSingleDayAndStoreData(mode, redisClient);
        } else {
            console.log("管理員密碼驗證成功，強制執行標準更新。");
            await updateAndStoreYouTubeData(redisClient);
        }
        await redisClient.set(META_LAST_UPDATED_KEY, Date.now());

    } else if (needsUpdate) {
        const lockAcquired = await redisClient.set(UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 300 });
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
    const backfillRemaining = parseInt(await redisClient.get(BACKFILL_COUNTER_KEY) || '0', 10);
    
    const responseData = {
        videos: videos,
        timestamp: new Date(parseInt(await redisClient.get(META_LAST_UPDATED_KEY), 10) || Date.now()).toISOString(),
        totalVisits: visitorCount.totalVisits,
        todayVisits: visitorCount.todayVisits,
        backfill_remaining: backfillRemaining,
        script_version: SCRIPT_VERSION,
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
