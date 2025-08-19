import { createClient } from 'redis';

// --- 版本指紋 ----
const SCRIPT_VERSION = '12.8-CRON'; 

// --- Redis Keys Configuration ---
const V10_PENDING_CLASSIFICATION_SET_KEY = 'vspo-db:v2:pending_classification';
const V10_CLASSIFICATION_LOCK_KEY = 'vspo-db:v2:meta:classification_lock';

const v11_KEY_PREFIX = 'vspo-db:v3:';
const v11_VIDEOS_SET_KEY = `${v11_KEY_PREFIX}video_ids`;
const v11_VIDEO_HASH_PREFIX = `${v11_KEY_PREFIX}video:`;
const v11_FOREIGN_VIDEOS_SET_KEY = `${v11_KEY_PREFIX}foreign_video_ids`;
const v11_FOREIGN_VIDEO_HASH_PREFIX = `${v11_KEY_PREFIX}foreign_video:`;
const v11_META_LAST_UPDATED_KEY = `${v11_KEY_PREFIX}meta:last_updated`;
const v11_FOREIGN_META_LAST_UPDATED_KEY = `${v11_KEY_PREFIX}meta:foreign_last_updated`;
const v11_UPDATE_LOCK_KEY = `${v11_KEY_PREFIX}meta:update_lock`;
const v11_FOREIGN_UPDATE_LOCK_KEY = `${v11_KEY_PREFIX}meta:foreign_update_lock`;
const v11_STREAM_INDEX_PREFIX = `${v11_KEY_PREFIX}index:`;

// --- YouTube API 設定 ---
const CHANNEL_WHITELIST = [ ];
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
    'UCa_gZCepQ7ZAUAsxnQkuMMA', 'UC0XdcEuxl03Pj6Rm7jcHEnw',
    'UCb7sghJ15e22ZuaHO-n65RQ', 'UCHTu2VhgTLkmmspsfItxgyA',
];
const CHANNEL_BLACKLIST = [
    'UCuI5_lA2o-arAIKukGvIEcQ', 'UCOnlV05C1t4d-x2NP-kgyzw', 
    'UCjOaP5dTW_0s1Ui11jm4Rzg', 'UCGZK4lLrDYcOKxmWJIERmjQ', 
    'UCnERutXxnHTLqckbGCUwtAg', 'UC-wCI2w1vMaEQ1iLnp3MEVQ', 
    'UCIvTtZq1vMaEQ1iLnp3MEVQ', 'UCBf3eLt6Nj7AJwkDysm0JWw', 
    'UCnusRHKhMAR7dNM00mk44BA', 'UCEShI32SUz7g9J9ICOs5Y0g' 
];
const FOREIGN_CHANNEL_WHITELIST = [
    'UCDGJlKGFkvxX-4MXSeNHteg', 'UCbAffpeZmYjQp0u4wtNd2rg',
    'UCWnhOhucHHQubSAkOi8xpew', 'UCLaBpbUPBvBIBHoKB4IYY_w',
    'UCoM5tr4Uf8qYDe48IOyMLNw', 'UC3zUXFjSuh4d5lqAQn2WycA',
    'UCnKtimjem240E6SltCV5XsA', 'UC-ZBjcW60WZsbmD_w7CzFrQ',
    'UCwH9u8cS5i6P-mshIj_5c3A', 'UCxMtLpKehgF1Ryx4X8U79aQ',
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
    process.env.YOUTUBE_API_KEY_7,
].filter(key => key);

// --- 輔助函式 ---
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
        return false;
    }
}
async function getVisitorCount(redisClient) {
    try {
        const todayStr = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Taipei' }).format(new Date());
        const todayKey = `visits:today:${todayStr}`;
        const [totalVisits, todayVisits] = await Promise.all([
            redisClient.get('visits:total'),
            redisClient.get(todayKey)
        ]);
        return { totalVisits: parseInt(totalVisits, 10) || 0, todayVisits: parseInt(todayVisits, 10) || 0 };
    } catch (error) {
        console.error("讀取訪客計數失敗:", error);
        return { totalVisits: 0, todayVisits: 0 };
    }
}
async function incrementAndGetVisitorCount(redisClient) {
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
function parseOriginalStreamInfo(description) {
    if (!description) return null;
    const ytRegex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
    const ytMatch = description.match(ytRegex);
    if (ytMatch && ytMatch[1]) {
        return { platform: 'youtube', id: ytMatch[1] };
    }
    const twitchRegex = /(?:https?:\/\/)?(?:www\.)?twitch\.tv\/videos\/(\d+)/;
    const twitchMatch = description.match(twitchRegex);
    if (twitchMatch && twitchMatch[1]) {
        return { platform: 'twitch', id: twitchMatch[1] };
    }
    return null;
}

// --- v11 業務邏輯 ---
function v11_normalizeVideoData(videoData) {
    if (!videoData || Object.keys(videoData).length === 0) {
        return null;
    }
    const video = { ...videoData };
    video.viewCount = parseInt(video.viewCount, 10) || 0;
    video.subscriberCount = parseInt(video.subscriberCount, 10) || 0;
    if (video.originalStreamInfo && typeof video.originalStreamInfo === 'string') {
        try {
            video.originalStreamInfo = JSON.parse(video.originalStreamInfo);
        } catch {
            video.originalStreamInfo = null;
        }
    }
    if (!video.videoType) {
        video.videoType = 'video';
    }
    return video;
}
const v11_logic = {
    async getVideosFromDB(redisClient, storageKeys) {
        const videoIds = await redisClient.sMembers(storageKeys.setKey);
        if (!videoIds || videoIds.length === 0) return [];
        const pipeline = redisClient.multi();
        videoIds.forEach(id => pipeline.hGetAll(`${storageKeys.hashPrefix}${id}`));
        const results = await pipeline.exec();
        const videos = results
            .map(video => v11_normalizeVideoData(video))
            .filter(Boolean);
        videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
        return videos;
    },
    async processAndStoreVideos(videoIds, redisClient, storageKeys, options = { checkKeywords: true, retentionMonths: 1, classify: true, addToPending: false }) {
        if (videoIds.length === 0) return { validVideoIds: new Set(), idsToDelete: [] };
        const videoDetailsMap = new Map();
        const videoDetailBatches = batchArray(videoIds, 50);
        for (const batch of videoDetailBatches) { const result = await fetchYouTube('videos', { part: 'statistics,snippet', id: batch.join(',') }); result.items?.forEach(item => videoDetailsMap.set(item.id, item)); }
        const channelStatsMap = new Map();
        const allChannelIds = [...new Set(Array.from(videoDetailsMap.values()).map(d => d.snippet.channelId))];
        if (allChannelIds.length > 0) { const channelDetailBatches = batchArray(allChannelIds, 50); for (const batch of channelDetailBatches) { const result = await fetchYouTube('channels', { part: 'statistics,snippet', id: batch.join(',') }); result.items?.forEach(item => channelStatsMap.set(item.id, item)); } }
        
        const validVideoIds = new Set();
        const retentionDate = new Date();
        retentionDate.setMonth(retentionDate.getMonth() - options.retentionMonths);
        
        const pipeline = redisClient.multi();
        const videosToClassify = [];

        for (const videoId of videoIds) {
            const detail = videoDetailsMap.get(videoId);
            if (!detail) continue;
            const channelId = detail.snippet.channelId;
            const isChannelBlacklisted = CHANNEL_BLACKLIST.includes(channelId);
            const isKeywordBlacklisted = containsBlacklistedKeyword(detail, KEYWORD_BLACKLIST);
            const isExpired = new Date(detail.snippet.publishedAt) < retentionDate;
            let isContentValid = false;
            if (options.checkKeywords) { if (CHANNEL_WHITELIST.includes(channelId)) isContentValid = true; else if (SPECIAL_WHITELIST.includes(channelId)) isContentValid = isVideoValid(detail, SPECIAL_KEYWORDS); else isContentValid = isVideoValid(detail, SEARCH_KEYWORDS); } else { isContentValid = true; }
            
            if (!isChannelBlacklisted && !isKeywordBlacklisted && !isExpired && isContentValid) {
                validVideoIds.add(videoId);
                const channelDetails = channelStatsMap.get(channelId);
                const { title, description } = detail.snippet;
                const videoData = { id: videoId, title: title, searchableText: `${title || ''} ${description || ''}`.toLowerCase(), thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url, channelId: channelId, channelTitle: detail.snippet.channelTitle, channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '', publishedAt: detail.snippet.publishedAt, viewCount: detail.statistics?.viewCount || 0, subscriberCount: channelDetails?.statistics?.subscriberCount || 0, };
                
                if (options.addToPending) {
                    videosToClassify.push(videoId);
                }

                const originalStreamInfo = parseOriginalStreamInfo(description);
                if (originalStreamInfo) { 
                    videoData.originalStreamInfo = JSON.stringify(originalStreamInfo); 
                    const indexKey = `${v11_STREAM_INDEX_PREFIX}${originalStreamInfo.platform}:${originalStreamInfo.id}`; 
                    pipeline.sAdd(indexKey, `${storageKeys.type}:${videoId}`); 
                }
                pipeline.hSet(`${storageKeys.hashPrefix}${videoId}`, videoData);
            }
        }
        
        if (options.addToPending && videosToClassify.length > 0) {
            pipeline.sAdd(V10_PENDING_CLASSIFICATION_SET_KEY, videosToClassify);
        }

        await pipeline.exec();
        const allIdsInDB = await redisClient.sMembers(storageKeys.setKey);
        const idsToDelete = allIdsInDB.filter(id => !validVideoIds.has(id));
        return { validVideoIds, idsToDelete };
    },
    async updateAndStoreYouTubeData(redisClient) {
        console.log(`[v11] 開始執行中文影片常規更新程序...`);
        const oneMonthAgo = new Date(); oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1); const publishedAfter = oneMonthAgo.toISOString();
        const newVideoCandidates = new Set();
        const allWhitelists = [...CHANNEL_WHITELIST, ...SPECIAL_WHITELIST];
        if (allWhitelists.length > 0) { 
            const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: allWhitelists.join(',') }); 
            const uploadPlaylistIds = (channelsResponse.items || []).map(item => item.contentDetails.relatedPlaylists.uploads).filter(Boolean);
            const playlistItemsPromises = uploadPlaylistIds.map(playlistId => fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 })); 
            const playlistItemsResults = await Promise.all(playlistItemsPromises); 
            for (const result of playlistItemsResults) { result.items?.forEach(item => { if (new Date(item.snippet.publishedAt) > oneMonthAgo) { newVideoCandidates.add(item.snippet.resourceId.videoId); } }); } 
        }
        const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
        const searchResults = await Promise.all(searchPromises);
        for (const result of searchResults) { result.items?.forEach(item => { if (item.id.videoId && !CHANNEL_BLACKLIST.includes(item.snippet.channelId)) { newVideoCandidates.add(item.id.videoId); } }); }
        const storageKeys = { setKey: v11_VIDEOS_SET_KEY, hashPrefix: v11_VIDEO_HASH_PREFIX, type: 'main' };
        
        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, { checkKeywords: true, retentionMonths: 1, addToPending: true });
        
        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log(`[v11] 中文影片常規更新程序完成。`);
    },
    async updateForeignClips(redisClient) {
        console.log('[v11] 開始執行外文影片常規更新程序...');
        const threeMonthsAgo = new Date(); threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
        const newVideoCandidates = new Set();
        const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: FOREIGN_CHANNEL_WHITELIST.join(',') });
        const uploadPlaylistIds = (channelsResponse.items || []).map(item => item.contentDetails.relatedPlaylists.uploads).filter(Boolean);
        for (const playlistId of uploadPlaylistIds) { const result = await fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }); result.items?.forEach(item => { if (new Date(item.snippet.publishedAt) > threeMonthsAgo) { newVideoCandidates.add(item.snippet.resourceId.videoId); } }); }
        const storageKeys = { setKey: v11_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v11_FOREIGN_VIDEO_HASH_PREFIX, type: 'foreign' };
        
        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, { checkKeywords: false, retentionMonths: 3, addToPending: true });
        
        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log('[v11] 外文影片常規更新程序完成。');
    },
};

// --- 主 Handler ---
export default async function handler(request, response) {
    const redisConnectionString = process.env.REDIS_URL;
    if (!redisConnectionString) {
        return response.status(500).json({ error: 'Redis 儲存庫未設定。' });
    }

    const url = new URL(request.url, `http://${request.headers.host}`);
    const path = url.pathname;
    const { searchParams } = url;

    let redisClient;
    try {
        redisClient = createClient({ url: redisConnectionString });
        await redisClient.connect();
        
        if (path === '/api/cron/update') {
            const providedSecret = request.headers['authorization'];
            if (providedSecret !== `Bearer ${process.env.CRON_SECRET}`) {
                return response.status(401).json({ error: 'Unauthorized' });
            }
            
            const lang = searchParams.get('lang') || 'cn';
            console.log(`[CRON] 收到 ${lang} 語言的定時更新請求...`);
            
            if (lang === 'jp') {
                const lockAcquired = await redisClient.set(v11_FOREIGN_UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 900 });
                if (!lockAcquired) {
                    return response.status(429).json({ message: "已有日文影片更新任務正在進行中。" });
                }
                try {
                    await v11_logic.updateForeignClips(redisClient);
                    await redisClient.set(v11_FOREIGN_META_LAST_UPDATED_KEY, Date.now());
                } finally {
                    await redisClient.del(v11_FOREIGN_UPDATE_LOCK_KEY);
                }
            } else { // 預設為 'cn'
                const lockAcquired = await redisClient.set(v11_UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 900 });
                if (!lockAcquired) {
                    return response.status(429).json({ message: "已有中文影片更新任務正在進行中。" });
                }
                try {
                    await v11_logic.updateAndStoreYouTubeData(redisClient);
                    await redisClient.set(v11_META_LAST_UPDATED_KEY, Date.now());
                } finally {
                    await redisClient.del(v11_UPDATE_LOCK_KEY);
                }
            }
            return response.status(200).json({ message: `${lang} 語言的定時更新已成功觸發。` });
        }
        
        if (path === '/api/classify-videos') {
            try {
                const lockAcquired = await redisClient.set(V10_CLASSIFICATION_LOCK_KEY, 'locked', { NX: true, EX: 600 });
                if (!lockAcquired) { return response.status(429).json({ message: "已有分類任務正在進行中。" }); }
                try {
                    const videoIdsToClassify = await redisClient.sMembers(V10_PENDING_CLASSIFICATION_SET_KEY);
                    if (videoIdsToClassify.length === 0) { return response.status(200).json({ message: "沒有需要分類的影片。" }); }
                    
                    for (const videoId of videoIdsToClassify) {
                        const isShort = await checkIfShort(videoId);
                        const videoType = isShort ? 'short' : 'video';
                        await redisClient.hSet(`${v11_VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
                        await redisClient.hSet(`${v11_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
                        await redisClient.sRem(V10_PENDING_CLASSIFICATION_SET_KEY, videoId);
                    }
                    return response.status(200).json({ message: "分類成功。" });
                } finally {
                    await redisClient.del(V10_CLASSIFICATION_LOCK_KEY);
                }
            } catch (e) {
                console.error(`[API /api/classify-videos] Error:`, e);
                return response.status(500).json({ error: '處理分類請求時發生內部錯誤。', details: e.message });
            }
        }

        if (path === '/api/check-status') {
            try {
                const count = await redisClient.sCard(V10_PENDING_CLASSIFICATION_SET_KEY);
                return response.status(200).json({ isDone: count === 0 });
            } catch (e) {
                console.error(`[API /api/check-status] Error:`, e);
                return response.status(500).json({ error: '處理狀態檢查請求時發生內部錯誤。', details: e.message });
            }
        }

        if (path === '/api/youtube') {
             try {
                const lang = searchParams.get('lang') || 'cn';
                const isForeign = lang === 'jp';
                const forceRefresh = searchParams.get('force_refresh') === 'true';
                
                const noIncrement = searchParams.get('no_increment') === 'true';
                const visitorCount = noIncrement
                    ? await getVisitorCount(redisClient)
                    : await incrementAndGetVisitorCount(redisClient);

                // 管理員強制刷新邏輯
                if (forceRefresh) {
                    const providedPassword = request.headers.authorization?.split(' ')[1] || searchParams.get('password');
                    const adminPassword = process.env.ADMIN_PASSWORD;
                    if (!adminPassword || providedPassword !== adminPassword) {
                        return response.status(401).json({ error: '未授權：無效的管理員憑證。' });
                    }
                    if (isForeign) {
                        await v11_logic.updateForeignClips(redisClient);
                        await redisClient.set(v11_FOREIGN_META_LAST_UPDATED_KEY, Date.now());
                    } else {
                        await v11_logic.updateAndStoreYouTubeData(redisClient);
                        await redisClient.set(v11_META_LAST_UPDATED_KEY, Date.now());
                    }
                }

                const storageKeys = isForeign ? { setKey: v11_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v11_FOREIGN_VIDEO_HASH_PREFIX } : { setKey: v11_VIDEOS_SET_KEY, hashPrefix: v11_VIDEO_HASH_PREFIX };
                const videos = await v11_logic.getVideosFromDB(redisClient, storageKeys);
                const updatedTimestamp = await redisClient.get(isForeign ? v11_FOREIGN_META_LAST_UPDATED_KEY : v11_META_LAST_UPDATED_KEY);
                
                return response.status(200).json({
                     videos: videos,
                     timestamp: new Date(parseInt(updatedTimestamp, 10) || Date.now()).toISOString(),
                     totalVisits: visitorCount.totalVisits,
                     todayVisits: visitorCount.todayVisits,
                     script_version: SCRIPT_VERSION,
                });

            } catch (e) {
                console.error(`[API /api/youtube] Error:`, e);
                return response.status(500).json({ error: '處理 /api/youtube 請求時發生內部錯誤。', details: e.message });
            }
        }
        
        if (path === '/api/get-related-clips') {
            try {
                const platform = searchParams.get('platform');
                const id = searchParams.get('id');
                if (!platform || !id || !['youtube', 'twitch'].includes(platform)) { 
                    return response.status(400).json({ error: '無效的請求：必須提供有效的 platform (youtube/twitch) 和 id 參數。' }); 
                }

                const indexKey = `${v11_STREAM_INDEX_PREFIX}${platform}:${id}`;
                const relatedVideoIdentifiers = await redisClient.sMembers(indexKey);
                if (relatedVideoIdentifiers.length === 0) { return response.status(200).json({ videos: [] }); }
                
                const pipeline = redisClient.multi();
                relatedVideoIdentifiers.forEach(identifier => {
                    if (typeof identifier === 'string' && identifier.includes(':')) {
                        const [type, videoId] = identifier.split(':');
                        if (type === 'main') { pipeline.hGetAll(`${v11_VIDEO_HASH_PREFIX}${videoId}`); } 
                        else if (type === 'foreign') { pipeline.hGetAll(`${v11_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`); }
                    } else {
                        console.warn(`[API /api/get-related-clips] 發現無效的 identifier: ${identifier}`);
                    }
                });
                const results = await pipeline.exec();
                
                const videos = results
                    .map(videoData => {
                        if (videoData instanceof Error) {
                            console.error("[API /api/get-related-clips] Redis pipeline error:", videoData);
                            return null;
                        }
                        return v11_normalizeVideoData(videoData);
                    })
                    .filter(Boolean);

                videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
                return response.status(200).json({ videos });
            } catch (e) {
                console.error(`[API /api/get-related-clips] Error:`, e);
                return response.status(500).json({ error: '處理同元配信請求時發生內部錯誤。', details: e.message });
            }
        }
        
        return response.status(404).json({ error: `找不到指定的 API 端點: ${path}` });

    } catch (error) {
        console.error(`Handler 頂層錯誤 (${path}):`, error);
        const isQuotaError = error.message?.toLowerCase().includes('quota');
        const status = isQuotaError ? 429 : 500;
        const message = isQuotaError ? 'API 配額已用盡，請稍後再試。' : '伺服器內部錯誤。';
        return response.status(status).json({ error: message, details: error.message });
    } finally {
        if (redisClient?.isOpen) await redisClient.quit();
    }
}