import { createClient } from 'redis';

// --- 版本指紋 ---
const SCRIPT_VERSION = '11.0-BACKEND-PREP'; 

// --- Redis Keys Configuration (版本提升至 v3) ---
const KEY_PREFIX = 'vspo-db:v3:';
// --- 主要(中文)影片資料庫 ---
const VIDEOS_SET_KEY = `${KEY_PREFIX}video_ids`;
const VIDEO_HASH_PREFIX = `${KEY_PREFIX}video:`;
const META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:last_updated`;
const UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:update_lock`;
const PENDING_CLASSIFICATION_SET_KEY = `${KEY_PREFIX}pending_classification`;
const CLASSIFICATION_LOCK_KEY = `${KEY_PREFIX}meta:classification_lock`;
// --- 新增：外文影片資料庫 ---
const FOREIGN_VIDEOS_SET_KEY = `${KEY_PREFIX}foreign_video_ids`;
const FOREIGN_VIDEO_HASH_PREFIX = `${KEY_PREFIX}foreign_video:`;
const FOREIGN_META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:foreign_last_updated`;
const FOREIGN_UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:foreign_update_lock`;
// --- 新增：元配信索引 ---
const STREAM_INDEX_PREFIX = `${KEY_PREFIX}index:`;

// --- 更新頻率設定 ---
const UPDATE_INTERVAL_SECONDS = 1200; // 中文影片：20 分鐘
const FOREIGN_UPDATE_INTERVAL_SECONDS = 3600; // 外文影片：1 小時

// --- YouTube API 設定 ---
// 【修改】所有列表改為雙列排序
const CHANNEL_WHITELIST = [ /* 頻道白名單 */ ];
const SPECIAL_WHITELIST = [ 
    'UCNdRb8JHTX6a-8V1j7olvAQ', 'UCwFDCL9otNlHyP4yMXunohQ', 
    'UCPfB7gx9yKfzfVirTX77LFA', 'UCqrTGkqVijNpTMPeWZ4_CgA', 
    'UCmOdNcq3ttIqrI0RBfdEwcQ', 'UCz4GIV8wNBsLBzZy2wA2KKw', 
    'UColeV1H-x8MuVLSAdohTOVQ', 'UCGy_n5NeGfeVzravayHk65Q', 
    'UCibI94U5KocgrbyY9gx3cIg', 'UCd3YtBLO0sGhQ2eTWs80bcg', 
    'UC9xEUSRrMWbbb-59IehNv3g', 'UCbsHmeSh_NGyO8ymoYG02sw', 
    'UCWq4bX9UMV1ir3liKRIvCHg', 'UCFZ7BPHTgEo5FXuvC9GVY7Q', 
    'UCu_KJNiq48jSwBVY7T4-hUQ', 'UC4QGmmdtxLZFFASDVee_atQ', 
    'UCBiATOGgCqf8uoFfX-pZ1gA', 'UCzqsI2AoNYe2F2WGRYqkEjA', 
    'UCIgvpS92srpQrGbXDlc8HFQ', 'UC7lPYbAxGzvbobFq_JxxftA', 
    'UCa_gZCepQ7ZAUAsxnQkuMMA', 'UC0XdcEuxl03Pj6Rm7jcHEnw' 
];
const CHANNEL_BLACKLIST = [
    'UCuI5_lA2o-arAIKukGvIEcQ', 'UCOnlV05C1t4d-x2NP-kgyzw', 
    'UCjOaP5dTW_0s1Ui11jm4Rzg', 'UCGZK4lLrDYcOKxmWJIERmjQ', 
    'UCnERutXxnHTLqckbGCUwtAg', 'UC-wCI2w1vMaEQ1iLnp3MEVQ', 
    'UCIvTtZq1vMaEQ1iLnp3MEVQ', 'UCBf3eLt6Nj7AJwkDysm0JWw', 
    'UCnusRHKhMAR7dNM00mk44BA', 'UCEShI32SUz7g9J9ICOs5Y0g' 
];
// 【新增】外文頻道白名單
const FOREIGN_CHANNEL_WHITELIST = [
    'UCDGJlKGFkvxX-4MXSeNHteg', 'UCbAffpeZmYjQp0u4wtNd2rg',
    'UCWnhOhucHHQubSAkOi8xpew', 'UCLaBpbUPBvBIBHoKB4IYY_w',
    'UCoM5tr4Uf8qYDe48IOyMLNw', 'UC3zUXFjSuh4d5lqAQn2WycA',
    'UCnKtimjem240E6SltCV5XsA', 'UC-ZBjcW60WZsbmD_w7CzFrQ',
    'UCwH9u8cS5i6P-ms9Ij_5c3A', 'UCxMtLpKehgF1Ryx4X8U79aQ',
    'UCW6Tau824RZGEdpp7voGvCQ', 'UCBMXkz7a-SKTvDQGkvPkgEA',
    'UCOizB6qqhzU10djIjuZrXJA', 'UCEHlq4NtouTi4aXLtPyeMzQ',
    'UCcC2iASzr8hxAlEuSzhBNpg', 'UC4Ep1Uy6bEk8J049mRI8UWQ',
    'UCgYEO91NqJiU_Z09-OLoOLQ', 'UCiwCmmQcBRHcH20O_A4L-rg',
    'UCBsNQ9CAiZ_9fik4_N0MjPA', 'UCqy8AfibWWLtlIvuIbRHOTA',
    'UCYv3ljhqjA_K3NMa4ux6Sag', 'UCrMIzdEEMtJGN95HaaNA1Sg',
    'UCl_H9O5f_uG-7tSgGKyqhKA', 'UCXOE_414xpAd1FFcwEDpikA',
    'UCHl9Lk_Nene0QbwuJJFdEyQ', 'UC78ZpgFpbA4KyHinPRQ-vcg',
    'UC2bTmb1UK1gv9rAvjiORRuQ', 'UCLNI_djAPvsMFihOFblOhhg',
    'UCo2e68YuP_JkQcnz27J-5qA', 'UC-PzhxFaPulWcgsZ-kH45FA',
    'UC7TgomjTg0YnXXokfRwccBA', 'UC0_zoJAoiwQSMFdMjkGatAQ',
    'UCAMXaQMurqT9Xkm-BnNRbDw', 'UCY52ChlPzGWIF4CzI2SdF4Q',
    'UCmVhuHYFM_HshlRwhesXeuA', 'UCAeN4qh2CJW0qxZCgVO_cVA',
    'UCowE5eik3vn-pshdaQksdww', 'UCQZ6f0GyKnxQJZe2VZWLIeA',
    'UC6ln0uzKuMLQaSl7-LMVu5A', 'UCeDnvJ66mIu_D4MtRoKRE_Q',
    'UC7sjX8lHyqbYAVREGuUrtQg', 'UCPK8tMKReXvewGOz7d0zS9g'
];

const SPECIAL_KEYWORDS = ["vspo", "ぶいすぽ"];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"]; 
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
].filter(key => key);

// --- 輔助函式 ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));
const isVideoValid = (videoDetail, keywords) => { /* ... */ };
const containsBlacklistedKeyword = (videoDetail, blacklist) => { /* ... */ };
async function checkIfShort(videoId) { /* ... */ }
async function updateAndGetVisitorCount(redisClient) { /* ... */ }
const fetchYouTube = async (endpoint, params) => { /* ... */ };

// 【新增】解析元配信資訊的函式
function parseOriginalStreamInfo(description) {
    if (!description) return null;
    // 優先匹配 YouTube 連結
    const ytRegex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
    const ytMatch = description.match(ytRegex);
    if (ytMatch && ytMatch[1]) {
        return { platform: 'youtube', id: ytMatch[1] };
    }
    // 若無，則匹配 Twitch 連結
    const twitchRegex = /(?:https?:\/\/)?(?:www\.)?twitch\.tv\/videos\/(\d+)/;
    const twitchMatch = description.match(twitchRegex);
    if (twitchMatch && twitchMatch[1]) {
        return { platform: 'twitch', id: twitchMatch[1] };
    }
    return null;
}

// --- 核心邏輯函式 ---

async function getVideosFromDB(redisClient) { /* ... */ }

// 【修改】處理並儲存影片的函式，加入元配信解析
async function processAndStoreVideos(videoIds, redisClient, storageKeys) {
    if (videoIds.length === 0) return { validVideoIds: new Set(), idsToDelete: [] };
    
    // ... (獲取影片與頻道詳細資料的邏輯不變)

    const pipeline = redisClient.multi();
    for (const videoId of videoIds) {
        // ... (影片有效性、黑名單等檢查邏輯不變)
        
        if (isContentValid) {
            // ... (建立 videoData 物件的邏輯不變)

            // 【新增】解析並儲存元配信資訊
            const originalStreamInfo = parseOriginalStreamInfo(detail.snippet.description);
            if (originalStreamInfo) {
                videoData.originalStreamInfo = JSON.stringify(originalStreamInfo); // 存為 JSON 字串
                // 【新增】建立索引
                const indexKey = `${STREAM_INDEX_PREFIX}${originalStreamInfo.platform}:${originalStreamInfo.id}`;
                pipeline.sAdd(indexKey, videoId);
            }
            
            pipeline.hSet(`${storageKeys.hashPrefix}${videoId}`, videoData);
        }
    }
    await pipeline.exec();
    
    // ... (刪除過期影片的邏輯不變，但使用傳入的 storageKeys)
}

// 【修改】主更新函式，專注於中文影片
async function updateAndStoreYouTubeData(redisClient, options) {
    // ... (此函式邏輯基本不變，但 processAndStoreVideos 會傳入中文影片的 storageKeys)
}

// 【新增】專門用來更新外文影片的函式
async function updateForeignClips(redisClient) {
    console.log('開始執行外文影片更新程序...');
    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

    const newVideoCandidates = new Set();
    const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: FOREIGN_CHANNEL_WHITELIST.join(',') });
    const uploadPlaylistIds = channelsResponse.items?.map(item => item.contentDetails.relatedPlaylists.uploads).filter(Boolean) || [];
    
    for (const playlistId of uploadPlaylistIds) {
        const result = await fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }); // 可調整 maxResults
        result.items?.forEach(item => {
            if (new Date(item.snippet.publishedAt) > oneMonthAgo) {
                newVideoCandidates.add(item.snippet.resourceId.videoId);
            }
        });
    }

    const storageKeys = {
        setKey: FOREIGN_VIDEOS_SET_KEY,
        hashPrefix: FOREIGN_VIDEO_HASH_PREFIX,
    };
    
    // 外文影片直接信任，不需分類 Shorts，也不需檢查關鍵字
    const { validVideoIds, idsToDelete } = await processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys);
    
    const pipeline = redisClient.multi();
    if (idsToDelete.length > 0) {
        pipeline.sRem(storageKeys.setKey, idsToDelete);
        idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`));
    }
    if (validVideoIds.size > 0) {
        pipeline.sAdd(storageKeys.setKey, [...validVideoIds]);
    }
    await pipeline.exec();
    console.log('外文影片更新程序完成。');
}


// --- API 端點處理函式 ---

// GET /api/youtube (主要資料獲取)
async function handleGetData(request, response, redisClient) {
    // ... (此函式前半部分的邏輯不變)

    // 【新增】非阻塞地觸發外文影片更新
    const lastForeignUpdate = await redisClient.get(FOREIGN_META_LAST_UPDATED_KEY);
    const needsForeignUpdate = !lastForeignUpdate || (Date.now() - parseInt(lastForeignUpdate, 10)) > FOREIGN_UPDATE_INTERVAL_SECONDS * 1000;
    
    if (needsForeignUpdate) {
        const lockAcquired = await redisClient.set(FOREIGN_UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 600 });
        if (lockAcquired) {
            console.log('需要更新外文影片且已獲取鎖，開始背景更新。');
            // "Fire-and-forget" - 不使用 await，讓它在背景執行
            updateForeignClips(redisClient)
                .then(() => redisClient.set(FOREIGN_META_LAST_UPDATED_KEY, Date.now()))
                .catch(err => console.error("背景更新外文影片失敗:", err))
                .finally(() => redisClient.del(FOREIGN_UPDATE_LOCK_KEY));
        }
    }

    // ... (回傳主要中文影片資料的邏輯不變)
}

// POST /api/classify-videos (觸發背景分類工作)
async function handleClassifyVideos(request, response, redisClient) { /* ... */ }

// GET /api/check-status (輪詢分類狀態)
async function handleCheckStatus(request, response, redisClient) { /* ... */ }

// 【新增】GET /api/get-related-clips (獲取相關剪輯)
async function handleGetRelatedClips(request, response, redisClient) {
    const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
    const platform = searchParams.get('platform');
    const id = searchParams.get('id');

    if (!platform || !id) {
        return response.status(400).json({ error: '缺少 platform 或 id 參數。' });
    }

    const indexKey = `${STREAM_INDEX_PREFIX}${platform}:${id}`;
    const relatedVideoIds = await redisClient.sMembers(indexKey);

    if (relatedVideoIds.length === 0) {
        return response.status(200).json({ videos: [] });
    }

    const pipeline = redisClient.multi();
    relatedVideoIds.forEach(videoId => {
        // 嘗試從兩個資料庫中獲取，一個會成功，一個會是 null
        pipeline.hGetAll(`${VIDEO_HASH_PREFIX}${videoId}`);
        pipeline.hGetAll(`${FOREIGN_VIDEO_HASH_PREFIX}${videoId}`);
    });
    const results = await pipeline.exec();

    const videos = [];
    for (let i = 0; i < results.length; i += 2) {
        const mainVideo = results[i];
        const foreignVideo = results[i+1];
        const videoData = Object.keys(mainVideo).length > 0 ? mainVideo : foreignVideo;
        
        if (Object.keys(videoData).length > 0) {
            // 將 originalStreamInfo 從字串轉回物件
            if (videoData.originalStreamInfo) {
                videoData.originalStreamInfo = JSON.parse(videoData.originalStreamInfo);
            }
            videos.push({ ...videoData, viewCount: parseInt(videoData.viewCount, 10) || 0 });
        }
    }

    videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
    return response.status(200).json({ videos });
}


// --- 主要的 Handler 函式 (路由) ---
export default async function handler(request, response) {
  // ... (CORS 和 Redis 連線邏輯不變)

  let redisClient;
  try {
    redisClient = createClient({ url: redisConnectionString });
    await redisClient.connect();

    // 【修改】根據路徑路由到不同的處理函式
    if (path.endsWith('/api/youtube')) {
        return await handleGetData(request, response, redisClient);
    } else if (path.endsWith('/api/classify-videos')) {
        return await handleClassifyVideos(request, response, redisClient);
    } else if (path.endsWith('/api/check-status')) {
        return await handleCheckStatus(request, response, redisClient);
    } else if (path.endsWith('/api/get-related-clips')) { // 【新增】路由
        return await handleGetRelatedClips(request, response, redisClient);
    } else {
        return response.status(404).json({ error: '找不到指定的 API 端點。' });
    }

  } catch (error) {
    // ... (錯誤處理邏輯不變)
  } finally {
    if (redisClient?.isOpen) await redisClient.quit();
  }
}
