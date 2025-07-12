import { createClient } from 'redis';

// --- Redis Keys Configuration ---
// 使用有版本的金鑰前綴，方便未來進行資料遷移
const KEY_PREFIX = 'vspo-db:v1:';
const VIDEOS_SET_KEY = `${KEY_PREFIX}video_ids`; // 一個包含所有影片 ID 的 Set
const VIDEO_HASH_PREFIX = `${KEY_PREFIX}video:`; // 每部影片 Hash 資料的前綴
const META_LAST_UPDATED_KEY = `${KEY_PREFIX}meta:last_updated`; // 上次成功更新的時間戳
const UPDATE_LOCK_KEY = `${KEY_PREFIX}meta:update_lock`; // 用於防止多個程序同時更新的鎖

const UPDATE_INTERVAL_SECONDS = 1800; // 每 30 分鐘更新一次資料

// --- YouTube API 設定 (維持不變) ---
const CHANNEL_WHITELIST = [
  'UCFZ7BPHTgEo5FXuvC9GVY7Q', 
  'UCWq4bX9UMV1ir3liKRIvCHg', // Irin_translate
  'UCbsHmeSh_NGyO8ymoYG02sw',
  'UCd3YtBLO0sGhQ2eTWs80bcg',
  'UCGy_n5NeGfeVzravayHk65Q',
];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const CHANNEL_BLACKLIST = [
  'UCuI5_lA2o-arAIKukGvIEcQ', 'UCWnhOhucHHQubSAkOi8xpew', 
  'UCOnlV05C1t4d-x2NP-kgyzw', 'UCjOaP5dTW_0s1Ui11jm4Rzg', 
  'UCGZK4lLrDYcOKxmWJIERmjQ', 'UCnERutXxnHTLqckbGCUwtAg', 
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
        await redisClient.expire(todayKey, 90000); // 25 小時
        return { totalVisits, todayVisits };
    } catch (error) {
        console.error("Failed to update visitor count:", error);
        return { totalVisits: 0, todayVisits: 0 };
    }
}

// --- 新的核心邏輯函式 ---

/**
 * 從 Redis 資料庫中獲取最新的影片資料。
 * @param {object} redisClient - 已連線的 Redis 客戶端。
 * @returns {Promise<Array>} 一個已排序的影片物件陣列。
 */
async function getVideosFromDB(redisClient) {
    const videoIds = await redisClient.sMembers(VIDEOS_SET_KEY);
    if (!videoIds || videoIds.length === 0) {
        return [];
    }

    const pipeline = redisClient.multi();
    videoIds.forEach(id => {
        pipeline.hGetAll(`${VIDEO_HASH_PREFIX}${id}`);
    });
    const results = await pipeline.exec();

    const videos = results.map(video => {
        if (video && Object.keys(video).length > 0) {
            // 將字串格式的數字轉回整數
            return {
                ...video,
                viewCount: parseInt(video.viewCount, 10) || 0,
                subscriberCount: parseInt(video.subscriberCount, 10) || 0,
            };
        }
        return null;
    }).filter(Boolean);

    // 依照發布時間由新到舊排序
    videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
    return videos;
}

/**
 * 主要的資料更新與儲存程序。
 * 從 YouTube 抓取資料，進行驗證，並更新 Redis 資料庫。
 * @param {object} redisClient - 已連線的 Redis 客戶端。
 */
async function updateAndStoreYouTubeData(redisClient) {
    console.log('開始執行 YouTube 資料更新程序...');
    
    // --- 階段一：從 YouTube API 抓取新的影片候選名單 ---
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

    const oneMonthAgo = new Date();
    oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
    const publishedAfter = oneMonthAgo.toISOString();
    
    const videoSnippets = new Map();

    // 白名單頻道
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

    // 關鍵字搜尋
    const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
    const searchResults = await Promise.all(searchPromises);
    for (const result of searchResults) {
      result.items?.forEach(item => {
        if (item.id.videoId && !videoSnippets.has(item.id.videoId) && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) {
          videoSnippets.set(item.id.videoId, item.snippet);
        }
      });
    }

    const videoIdsToUpdate = Array.from(videoSnippets.keys());
    if (videoIdsToUpdate.length === 0) {
        console.log('未找到新的影片候選名單，更新程序結束。');
        return;
    }
    
    // --- 階段二：獲取完整詳細資訊並處理 ---
    const videoDetailBatches = batchArray(videoIdsToUpdate, 50);
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

    // --- 階段三：更新 Redis 資料庫 ---
    const pipeline = redisClient.multi();
    let updatedCount = 0;

    for (const [videoId, detail] of videoDetailsMap.entries()) {
        if (!detail) continue;

        const isChannelBlacklisted = CHANNEL_BLACKLIST.includes(detail.snippet.channelId);
        if (isChannelBlacklisted) continue;
        
        const isContentValid = isVideoValid(detail, SEARCH_KEYWORDS);
        const isFromWhitelist = CHANNEL_WHITELIST.includes(detail.snippet.channelId);

        if (isContentValid || isFromWhitelist) {
            const channelDetails = channelStatsMap.get(detail.snippet.channelId);
            const videoData = {
                id: videoId,
                title: detail.snippet.title,
                thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
                channelId: detail.snippet.channelId, 
                channelTitle: detail.snippet.channelTitle,
                channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
                publishedAt: detail.snippet.publishedAt,
                viewCount: detail.statistics ? parseInt(detail.statistics.viewCount, 10) : 0,
                subscriberCount: channelDetails?.statistics ? parseInt(channelDetails.statistics.subscriberCount, 10) : 0,
            };
            
            // 將指令加入 pipeline
            pipeline.sAdd(VIDEOS_SET_KEY, videoId);
            pipeline.hSet(`${VIDEO_HASH_PREFIX}${videoId}`, videoData);
            updatedCount++;
        }
    }
    
    if (updatedCount > 0) {
        await pipeline.exec();
        console.log(`已在資料庫中更新/新增 ${updatedCount} 部影片。`);
    }

    // --- 階段四：清理過期影片 ---
    console.log('開始清理過期影片...');
    const allVideoIdsInDB = await redisClient.sMembers(VIDEOS_SET_KEY);
    const oldVideoIds = [];
    
    const checkPipeline = redisClient.multi();
    allVideoIdsInDB.forEach(id => checkPipeline.hGet(`${VIDEO_HASH_PREFIX}${id}`, 'publishedAt'));
    const publishedDates = await checkPipeline.exec();

    publishedDates.forEach((publishedAt, index) => {
        if (publishedAt && new Date(publishedAt) < oneMonthAgo) {
            oldVideoIds.push(allVideoIdsInDB[index]);
        }
    });

    if (oldVideoIds.length > 0) {
        const cleanupPipeline = redisClient.multi();
        cleanupPipeline.sRem(VIDEOS_SET_KEY, oldVideoIds);
        oldVideoIds.forEach(id => cleanupPipeline.del(`${VIDEO_HASH_PREFIX}${id}`));
        await cleanupPipeline.exec();
        console.log(`已清理 ${oldVideoIds.length} 部過期影片。`);
    } else {
        console.log('沒有需要清理的過期影片。');
    }
}


// --- 主要的 Handler 函式 (已重構) ---
export default async function handler(request, response) {
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    response.setHeader('Access-Control-Allow-Origin', '*');
    return response.status(500).json({ error: 'Redis 儲存庫未設定。' });
  }

  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const forceRefresh = searchParams.get('force_refresh') === 'true';
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
        console.log("管理員密碼驗證成功，強制更新資料。");
        await updateAndStoreYouTubeData(redisClient);
        await redisClient.set(META_LAST_UPDATED_KEY, Date.now());
    } else if (needsUpdate) {
        // 嘗試獲取一個鎖，以防止多個實例同時更新。
        // 使用 SET 搭配 NX (Not Exists) 和 EX (Expire) 是實現此功能的原子操作。
        const lockAcquired = await redisClient.set(UPDATE_LOCK_KEY, 'locked', {
            NX: true,
            EX: 60 // 鎖在 60 秒後過期，以防卡住
        });

        if (lockAcquired) {
            console.log('需要更新且已獲取鎖，開始更新資料。');
            try {
                await updateAndStoreYouTubeData(redisClient);
                await redisClient.set(META_LAST_UPDATED_KEY, Date.now());
            } finally {
                // 釋放鎖
                await redisClient.del(UPDATE_LOCK_KEY);
            }
        } else {
            console.log('需要更新，但已有其他程序正在更新。將提供現有資料。');
        }
    }

    // 無論更新狀態如何，都從資料庫提供資料
    console.log('從資料庫獲取影片以回應請求...');
    const videos = await getVideosFromDB(redisClient);
    
    const responseData = {
        videos: videos,
        timestamp: new Date(parseInt(lastUpdated, 10) || Date.now()).toISOString(),
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
