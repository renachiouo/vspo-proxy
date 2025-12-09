import { createClient } from 'redis';

// --- 版本指紋 ---
const SCRIPT_VERSION = '16.8-V12-Leaderboard';

// --- Redis Keys Configuration ---
// V12 使用 v4 版本的 Key
const V12_WHITELIST_CN_KEY = 'vspo-db:v4:whitelist:cn';
const V12_WHITELIST_JP_KEY = 'vspo-db:v4:whitelist:jp';
const V12_WHITELIST_PENDING_JP_KEY = 'vspo-db:v4:whitelist:pending:jp';
const V12_BLACKLIST_CN_KEY = 'vspo-db:v4:blacklist:cn';
const V12_BLACKLIST_JP_KEY = 'vspo-db:v4:blacklist:jp';
const V12_ANNOUNCEMENT_KEY = 'vspo-db:v4:announcement';
const V12_VIDEO_BLACKLIST_KEY = 'vspo-db:v4:video-blacklist'; // New Video Blacklist Key
const V12_PENDING_ATTRIBUTION_QUEUE = 'vspo-db:v4:attribution_queue'; // Async Attribution Queue
const V12_LEADERBOARD_CACHE_KEY = 'vspo-db:v4:leaderboard:cache';

// 影片資料仍沿用 v3 架構以保留舊資料，但邏輯層升級為 V12
const v12_KEY_PREFIX = 'vspo-db:v3:';
const v12_VIDEOS_SET_KEY = `${v12_KEY_PREFIX}video_ids`;
const v12_VIDEO_HASH_PREFIX = `${v12_KEY_PREFIX}video:`;
const v12_FOREIGN_VIDEOS_SET_KEY = `${v12_KEY_PREFIX}foreign_video_ids`;
const v12_FOREIGN_VIDEO_HASH_PREFIX = `${v12_KEY_PREFIX}foreign_video:`;
const v12_META_LAST_UPDATED_KEY = `${v12_KEY_PREFIX}meta:last_updated`;
const v12_FOREIGN_META_LAST_UPDATED_KEY = `${v12_KEY_PREFIX}meta:foreign_last_updated`;
const v12_FOREIGN_META_LAST_SEARCH_KEY = `${v12_KEY_PREFIX}meta:foreign_last_search`;
const v12_UPDATE_LOCK_KEY = `${v12_KEY_PREFIX}meta:update_lock`;
const v12_FOREIGN_UPDATE_LOCK_KEY = `${v12_KEY_PREFIX}meta:foreign_update_lock`;
const v12_STREAM_INDEX_PREFIX = `${v12_KEY_PREFIX}index:`;

// 舊版 V10 分類 Key (保留以防萬一，或可考慮移除)
const V10_PENDING_CLASSIFICATION_SET_KEY = `vspo-db:v2:pending_classification`;
const V10_CLASSIFICATION_LOCK_KEY = `vspo-db:v2:meta:classification_lock`;
const V10_VIDEO_HASH_PREFIX = `vspo-db:v2:video:`;



// --- 更新頻率設定 ---
const UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘
const FOREIGN_UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘 (白名單更新頻率)
const FOREIGN_SEARCH_INTERVAL_SECONDS = 3600; // 60 分鐘 (關鍵字搜尋頻率)
const LEADERBOARD_CACHE_TTL = 3600; // 1 小時 (排行榜快取時間)

// --- YouTube API 設定 ---
const SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const FOREIGN_SEARCH_KEYWORDS = ["ぶいすぽ 切り抜き"];
const FOREIGN_SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO精華", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"];
const VSPO_MEMBER_KEYWORDS = [
    "花芽すみれ", "花芽なずな", "小雀とと", "一ノ瀬うるは", "胡桃のあ", "兎咲ミミ", "空澄セナ", "橘ひなの", "英リサ", "如月れん", "神成きゅぴ", "八雲べに", "藍沢エマ", "紫宮るな", "猫汰つな", "白波らむね", "小森めと", "夢野あかり", "夜乃くろむ", "紡木こかげ", "千燈ゆうひ", "蝶屋はなび", "甘結もか",
    "Remia", "Arya", "Jira", "Narin", "Riko", "Eris",
    "小針彩", "白咲露理", "帕妃", "千郁郁",
    "ひなーの", "ひなの", "べに", "つな", "らむち", "らむね", "めと", "なずな", "なずぴ", "すみー", "すみれ", "ととち", "とと", "のせ", "うるは", "のあ", "ミミ", "たや", "セナ", "あしゅみ", "リサ", "れん", "きゅぴ", "エマたそ", "るな", "あかり", "あかりん", "くろむ", "こかげ", "つむお", "うひ", "ゆうひ", "はなび", "もか"
];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9,
].filter(key => key);

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'vspo123';
const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID;
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET;

// --- Helper Functions ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

// Twitch Token Cache
let twitchAccessToken = null;
let twitchTokenExpiry = 0;

async function getTwitchToken() {
    if (twitchAccessToken && Date.now() < twitchTokenExpiry) {
        return twitchAccessToken;
    }

    if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
        console.warn('Twitch Credentials not found, skipping Twitch API calls.');
        return null;
    }

    try {
        const response = await fetch('https://id.twitch.tv/oauth2/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                client_id: TWITCH_CLIENT_ID,
                client_secret: TWITCH_CLIENT_SECRET,
                grant_type: 'client_credentials'
            })
        });

        if (!response.ok) throw new Error(`Twitch Token Error: ${response.status}`);
        const data = await response.json();
        twitchAccessToken = data.access_token;
        twitchTokenExpiry = Date.now() + (data.expires_in * 1000) - 60000; // Buffer 60s
        console.log('[Twitch] Token refreshed.');
        return twitchAccessToken;
    } catch (e) {
        console.error('[Twitch] Failed to get token:', e);
        return null;
    }
}

async function fetchTwitch(endpoint, params = {}) {
    const token = await getTwitchToken();
    if (!token) return null;

    const queryString = new URLSearchParams(params).toString();
    const url = `https://api.twitch.tv/helix/${endpoint}?${queryString}`;

    try {
        const response = await fetch(url, {
            headers: {
                'Client-Id': TWITCH_CLIENT_ID,
                'Authorization': `Bearer ${token}`
            }
        });
        if (!response.ok) {
            const errText = await response.text();
            console.error(`[Twitch] API Error ${response.status}: ${errText}`);
            return null;
        }
        return await response.json();
    } catch (e) {
        console.error(`[Twitch] Network Error:`, e);
        return null;
    }
}

const isVideoValid = (videoDetail, keywords, useDoubleKeywordCheck = false) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description}`.toLowerCase();

    // 1. 檢查是否包含許諾番號 (License)
    const hasLicense = keywords.some(keyword => searchText.includes(keyword.toLowerCase()));
    if (!hasLicense) return false;

    // 2. 如果需要雙重驗證，檢查是否包含 VSPO 成員關鍵字
    if (useDoubleKeywordCheck) {
        const hasMemberKeyword = VSPO_MEMBER_KEYWORDS.some(keyword => searchText.includes(keyword.toLowerCase()));
        if (!hasMemberKeyword) return false;
    }

    // 3. 檢查直播狀態 (排除 live 和 upcoming)
    if (videoDetail.snippet.liveBroadcastContent === 'live' || videoDetail.snippet.liveBroadcastContent === 'upcoming') {
        return false;
    }

    return true;
};
const containsBlacklistedKeyword = (videoDetail, blacklist) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description}`.toLowerCase();
    return blacklist.some(keyword => searchText.includes(keyword.toLowerCase()));
};

function parseISODuration(duration) {
    if (!duration) return 0;
    let minutes = 0;
    const hoursMatch = duration.match(/(\d+)H/);
    const minutesMatch = duration.match(/(\d+)M/);
    const secondsMatch = duration.match(/(\d+)S/);
    if (hoursMatch) minutes += parseInt(hoursMatch[1]) * 60;
    if (minutesMatch) minutes += parseInt(minutesMatch[1]);
    if (secondsMatch) minutes += Math.round(parseInt(secondsMatch[1]) / 60);
    return minutes;
}

async function checkIfShort(videoId) {
    try {
        const response = await fetch(`https://www.youtube.com/shorts/${videoId}`);
        return response.url.includes('/shorts/');
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
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.warn(`金鑰 ${apiKey.substring(0, 8)}... 配額/速率限制錯誤 (${data.error.reason}): ${data.error.message}, 重試中...`);
                await sleep(1000); // 1s backoff
                continue;
            }
            if (data.error) throw new Error(data.error.message);
            return data;
        } catch (e) {
            console.error(`金鑰 ${apiKey.substring(0, 8)}... 發生錯誤`, e);
            await sleep(500); // 0.5s backoff on network error
        }
    }
    throw new Error('所有 API 金鑰都已失效。');
};
function parseOriginalStreamInfo(description) {
    if (!description) return [];

    const results = [];

    // YouTube
    const ytRegex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/g;
    const ytMatches = [...description.matchAll(ytRegex)];
    ytMatches.forEach(m => {
        if (m[1] && !results.some(r => r.platform === 'youtube' && r.id === m[1])) {
            results.push({ platform: 'youtube', id: m[1] });
        }
    });

    // Twitch
    const twitchRegex = /(?:https?:\/\/)?(?:www\.)?twitch\.tv\/videos\/(\d+)/g;
    const twitchMatches = [...description.matchAll(twitchRegex)];
    twitchMatches.forEach(m => {
        if (m[1] && !results.some(r => r.platform === 'twitch' && r.id === m[1])) {
            results.push({ platform: 'twitch', id: m[1] });
        }
    });

    return results;
}
function v12_normalizeVideoData(videoData) {
    if (!videoData || Object.keys(videoData).length === 0) {
        return null;
    }
    const video = { ...videoData };
    if (!video.id) return null; // 確保影片 ID 存在，否則視為無效資料
    video.viewCount = parseInt(video.viewCount, 10) || 0;
    video.subscriberCount = parseInt(video.subscriberCount, 10) || 0;
    if (video.originalStreamInfo && typeof video.originalStreamInfo === 'string') {
        try {
            video.originalStreamInfo = JSON.parse(video.originalStreamInfo);
        } catch {
            video.originalStreamInfo = [];
        }
    }
    // Parse targetChannelIds
    if (video.targetChannelIds && typeof video.targetChannelIds === 'string') {
        try {
            video.targetChannelIds = JSON.parse(video.targetChannelIds);
        } catch {
            video.targetChannelIds = [];
        }
    }
    return video;
}

const v12_logic = {
    async getVideosFromDB(redisClient, storageKeys) {
        const videoIds = await redisClient.sMembers(storageKeys.setKey);
        if (!videoIds || videoIds.length === 0) return [];
        const pipeline = redisClient.multi();
        videoIds.forEach(id => pipeline.hGetAll(`${storageKeys.hashPrefix}${id}`));
        const results = await pipeline.exec();
        const videos = results.map(video => v12_normalizeVideoData(video)).filter(Boolean);
        videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
        return videos;
    },
    async processAndStoreVideos(videoIds, redisClient, storageKeys, options = {}) {
        const { retentionDate, validKeywords, blacklist = [], useDoubleKeywordCheck = false } = options;

        if (videoIds.length === 0) return { validVideoIds: new Set(), idsToDelete: [] };
        const videoDetailsMap = new Map();
        const videoDetailBatches = batchArray(videoIds, 50);
        for (const batch of videoDetailBatches) { const result = await fetchYouTube('videos', { part: 'statistics,snippet,contentDetails', id: batch.join(',') }); result.items?.forEach(item => videoDetailsMap.set(item.id, item)); }

        // Fetch Video Blacklist
        const videoBlacklist = await redisClient.sMembers(V12_VIDEO_BLACKLIST_KEY);

        const channelStatsMap = new Map();
        const allChannelIds = [...new Set(Array.from(videoDetailsMap.values()).map(d => d.snippet.channelId))];
        if (allChannelIds.length > 0) { const channelDetailBatches = batchArray(allChannelIds, 50); for (const batch of channelDetailBatches) { const result = await fetchYouTube('channels', { part: 'statistics,snippet', id: batch.join(',') }); result.items?.forEach(item => channelStatsMap.set(item.id, item)); } }

        const validVideoIds = new Set();
        const pipeline = redisClient.multi();
        const videosToClassify = [];

        // Pre-fetch videoTypes to avoid re-classifying
        const typeCheckPipeline = redisClient.multi();
        videoIds.forEach(id => typeCheckPipeline.hGet(`${storageKeys.hashPrefix}${id}`, 'videoType'));
        const existingVideoTypes = await typeCheckPipeline.exec();
        const classifiedMap = new Map();
        videoIds.forEach((id, index) => {
            if (existingVideoTypes[index]) {
                classifiedMap.set(id, true);
            }
        });

        for (const videoId of videoIds) {
            const detail = videoDetailsMap.get(videoId);
            if (!detail) continue;

            // Check Video Blacklist
            if (videoBlacklist.includes(videoId)) {
                console.log(`[Filter] 影片 ${videoId} 在影片黑名單中，跳過。`);
                continue;
            }

            const channelId = detail.snippet.channelId;
            const isChannelBlacklisted = blacklist.includes(channelId);
            const isKeywordBlacklisted = containsBlacklistedKeyword(detail, KEYWORD_BLACKLIST);
            const isExpired = new Date(detail.snippet.publishedAt) < retentionDate;
            const isContentValid = isVideoValid(detail, validKeywords, useDoubleKeywordCheck);

            if (!isChannelBlacklisted && !isKeywordBlacklisted && !isExpired && isContentValid) {
                validVideoIds.add(videoId);
                const channelDetails = channelStatsMap.get(channelId);
                const { title, description } = detail.snippet;
                const videoData = { id: videoId, title: title, searchableText: `${title || ''} ${description || ''}`.toLowerCase(), thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url, channelId: channelId, channelTitle: detail.snippet.channelTitle, channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '', publishedAt: detail.snippet.publishedAt, viewCount: detail.statistics?.viewCount || 0, subscriberCount: channelDetails?.statistics?.subscriberCount || 0, duration: detail.contentDetails?.duration || '' };

                // Only add to pending classification if not already classified
                if (!classifiedMap.has(videoId)) {
                    videosToClassify.push(videoId);
                }

                const originalStreamInfo = parseOriginalStreamInfo(description);
                if (originalStreamInfo && originalStreamInfo.length > 0) {
                    videoData.originalStreamInfo = JSON.stringify(originalStreamInfo);
                    // Async Attribution: Push to Queue
                    pipeline.sAdd(V12_PENDING_ATTRIBUTION_QUEUE, videoId);
                }
                pipeline.hSet(`${storageKeys.hashPrefix}${videoId}`, videoData);
            }
        }

        if (videosToClassify.length > 0) {
            console.log(`[Classify] ${videosToClassify.length} 部影片缺少 videoType，已加入待分類清單。`);
            pipeline.sAdd(V10_PENDING_CLASSIFICATION_SET_KEY, videosToClassify);
        }

        await pipeline.exec();
        const allIdsInDB = await redisClient.sMembers(storageKeys.setKey);
        const idsToDelete = allIdsInDB.filter(id => !validVideoIds.has(id));
        return { validVideoIds, idsToDelete };
    },
    async updateAndStoreYouTubeData(redisClient) {
        console.log(`[v16.8] 開始執行中文影片常規更新程序...`);
        const oneMonthAgo = new Date(); oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1); const publishedAfter = oneMonthAgo.toISOString();

        // [MUTUAL EXCLUSION] Fetch JP Whitelist as well
        const [currentWhitelist, blacklist, jpWhitelist] = await Promise.all([
            redisClient.sMembers(V12_WHITELIST_CN_KEY),
            redisClient.sMembers(V12_BLACKLIST_CN_KEY),
            redisClient.sMembers(V12_WHITELIST_JP_KEY)
        ]);
        console.log(`[v16.8] 從 Redis 載入 ${currentWhitelist.length} 個中文白名單頻道、${blacklist.length} 個中文黑名單頻道以及 ${jpWhitelist.length} 個日文白名單頻道。`);

        const newVideoCandidates = new Set();

        if (currentWhitelist.length > 0) {
            const uploadPlaylistIds = [];
            const channelBatches = batchArray(currentWhitelist, 50);
            for (const batch of channelBatches) {
                const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
                (channelsResponse.items || []).forEach(item => {
                    if (item.contentDetails?.relatedPlaylists?.uploads) {
                        uploadPlaylistIds.push(item.contentDetails.relatedPlaylists.uploads);
                    }
                });
            }
            const playlistItemsPromises = uploadPlaylistIds.map(playlistId => fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }));
            const playlistItemsResults = await Promise.all(playlistItemsPromises);
            for (const result of playlistItemsResults) { result.items?.forEach(item => { if (new Date(item.snippet.publishedAt) > oneMonthAgo) { newVideoCandidates.add(item.snippet.resourceId.videoId); } }); }
        }

        const searchPromises = SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter }));
        const searchResults = await Promise.all(searchPromises);
        const searchResultVideoIds = new Set();
        for (const result of searchResults) { result.items?.forEach(item => { if (item.id.videoId && !blacklist.includes(item.snippet.channelId)) { newVideoCandidates.add(item.id.videoId); searchResultVideoIds.add(item.id.videoId); } }); }

        if (searchResultVideoIds.size > 0) {
            const videoIdBatches = batchArray([...searchResultVideoIds], 50);
            const newChannelsToAdd = new Set();
            for (const batch of videoIdBatches) {
                const videoDetailsResponse = await fetchYouTube('videos', { part: 'snippet', id: batch.join(',') });
                (videoDetailsResponse.items || []).forEach(video => {
                    const channelId = video.snippet.channelId;
                    // [MUTUAL EXCLUSION] Check if channel is in JP whitelist
                    if (!currentWhitelist.includes(channelId) && !jpWhitelist.includes(channelId) && (video.snippet.description || '').includes('ぶいすぽ')) {
                        newChannelsToAdd.add(channelId);
                    }
                });
            }
            if (newChannelsToAdd.size > 0) {
                await redisClient.sAdd(V12_WHITELIST_CN_KEY, [...newChannelsToAdd]);
                console.log(`[v16.8] 自動新增 ${newChannelsToAdd.size} 個新頻道到中文白名單: ${[...newChannelsToAdd].join(', ')}`);
            }
        }

        const storageKeys = { setKey: v12_VIDEOS_SET_KEY, hashPrefix: v12_VIDEO_HASH_PREFIX, type: 'main' };
        // [DOUBLE CHECK] Enable for CN
        const options = { retentionDate: oneMonthAgo, validKeywords: SPECIAL_KEYWORDS, blacklist, useDoubleKeywordCheck: true };

        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, options);

        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log(`[v16.8] 中文影片常規更新程序完成。`);
    },
    async updateForeignClips(redisClient) {
        console.log('[v16.8] 開始執行外文影片常規更新程序...');
        const threeMonthsAgo = new Date(); threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

        const [currentWhitelist, blacklist] = await Promise.all([
            redisClient.sMembers(V12_WHITELIST_JP_KEY),
            redisClient.sMembers(V12_BLACKLIST_JP_KEY)
        ]);
        console.log(`[v16.8] 從 Redis 載入 ${currentWhitelist.length} 個日文白名單頻道和 ${blacklist.length} 個日文黑名單頻道。`);

        if (currentWhitelist.length === 0) {
            console.warn("[v16.8] 日文白名單為空，更新程序終止。");
            return;
        }

        const newVideoCandidates = new Set();
        const uploadPlaylistIds = [];
        const channelBatches = batchArray(currentWhitelist, 50);
        for (const batch of channelBatches) {
            const channelsResponse = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
            (channelsResponse.items || []).forEach(item => {
                if (item.contentDetails?.relatedPlaylists?.uploads) {
                    uploadPlaylistIds.push(item.contentDetails.relatedPlaylists.uploads);
                }
            });
        }

        const playlistItemsPromises = uploadPlaylistIds.map(playlistId => fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }));
        const playlistItemsResults = await Promise.all(playlistItemsPromises);
        for (const result of playlistItemsResults) { result.items?.forEach(item => { if (new Date(item.snippet.publishedAt) > threeMonthsAgo) { newVideoCandidates.add(item.snippet.resourceId.videoId); } }); }

        const storageKeys = { setKey: v12_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v12_FOREIGN_VIDEO_HASH_PREFIX, type: 'foreign' };
        // [DOUBLE CHECK] Disable for JP
        const options = { retentionDate: threeMonthsAgo, validKeywords: FOREIGN_SPECIAL_KEYWORDS, blacklist, useDoubleKeywordCheck: false };

        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, options);

        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log('[v16.8] 外文影片常規更新程序 (白名單) 完成。');

        // --- START: 關鍵字搜尋 (每 60 分鐘執行一次) ---
        const lastSearchTime = await redisClient.get(v12_FOREIGN_META_LAST_SEARCH_KEY);
        const shouldSearch = !lastSearchTime || (Date.now() - parseInt(lastSearchTime, 10)) > FOREIGN_SEARCH_INTERVAL_SECONDS * 1000;

        if (shouldSearch) {
            console.log('[v16.8] 距離上次搜尋已超過 60 分鐘，開始執行日文關鍵字探索...');
            const searchPromises = FOREIGN_SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q }));
            const searchResults = await Promise.all(searchPromises);

            const discoveredChannelIds = new Set();
            for (const result of searchResults) {
                result.items?.forEach(item => discoveredChannelIds.add(item.snippet.channelId));
            }

            if (discoveredChannelIds.size > 0) {
                const [cnWhitelist, jpWhitelist, pendingWhitelist, jpBlacklist] = await Promise.all([
                    redisClient.sMembers(V12_WHITELIST_CN_KEY),
                    redisClient.sMembers(V12_WHITELIST_JP_KEY),
                    redisClient.sMembers(V12_WHITELIST_PENDING_JP_KEY),
                    redisClient.sMembers(V12_BLACKLIST_JP_KEY)
                ]);
                const allExistingIds = new Set([...cnWhitelist, ...jpWhitelist, ...pendingWhitelist, ...jpBlacklist]);
                const newChannelsToAdd = [...discoveredChannelIds].filter(id => !allExistingIds.has(id));

                if (newChannelsToAdd.length > 0) {
                    await redisClient.sAdd(V12_WHITELIST_PENDING_JP_KEY, newChannelsToAdd);
                    console.log(`[v16.8] 自動探索發現 ${newChannelsToAdd.length} 個新頻道，已加入待審核列表。`);
                } else {
                    console.log('[v16.8] 自動探索完成，未發現新頻道。');
                }
            }
            await redisClient.set(v12_FOREIGN_META_LAST_SEARCH_KEY, Date.now());
        } else {
            console.log('[v16.8] 距離上次搜尋未滿 60 分鐘，跳過關鍵字探索。');
        }
        // --- END: 關鍵字搜尋 ---
    },
};

// --- 主 Handler ---
export default async function handler(request, response) {
    // Enable CORS
    response.setHeader('Access-Control-Allow-Credentials', true);
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,PATCH,DELETE,POST,PUT');
    response.setHeader(
        'Access-Control-Allow-Headers',
        'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version, Authorization'
    );

    if (request.method === 'OPTIONS') {
        return response.status(200).end();
    }
    const redisConnectionString = process.env.REDIS_URL;
    if (!redisConnectionString) {
        return response.status(500).json({ error: 'Redis 儲存庫未設定。' });
    }
    const url = new URL(request.url, `http://${request.headers.host}`);
    const path = url.pathname;
    const { searchParams } = url;
    const clientVersion = searchParams.get('version');
    let redisClient;
    try {
        redisClient = createClient({ url: redisConnectionString });
        await redisClient.connect();

        let body = {};
        if (request.method === 'POST') {
            try {
                if (request.body) {
                    body = typeof request.body === 'string' ? JSON.parse(request.body) : request.body;
                } else if (typeof request.json === 'function') {
                    body = await request.json();
                }
            } catch (e) {
                console.error("解析 JSON body 失敗:", e);
                body = { parseError: true };
            }
        }

        const authenticate = () => {
            const providedPassword = request.headers.authorization?.split(' ')[1] || searchParams.get('password') || body.password;
            const adminPassword = process.env.ADMIN_PASSWORD;
            if (!adminPassword || providedPassword !== adminPassword) {
                response.status(401).json({ error: '未授權：無效的管理員憑證。' });
                return false;
            }
            return true;
        };

        if (path === '/api/leaderboard') {
            try {
                // 1. 檢查快取
                const cachedData = await redisClient.get(V12_LEADERBOARD_CACHE_KEY);
                if (cachedData) {
                    return response.status(200).json(JSON.parse(cachedData));
                }

                // 2. 計算排行榜
                const videoIds = await redisClient.sMembers(v12_VIDEOS_SET_KEY);
                if (!videoIds || videoIds.length === 0) {
                    return response.status(200).json([]);
                }

                const pipeline = redisClient.multi();
                videoIds.forEach(id => pipeline.hGetAll(`${v12_VIDEO_HASH_PREFIX}${id}`));
                const results = await pipeline.exec();

                const videos = results.map(video => v12_normalizeVideoData(video)).filter(Boolean);
                const thirtyDaysAgo = new Date();
                thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

                const channelStats = {};

                videos.forEach(video => {
                    if (new Date(video.publishedAt) >= thirtyDaysAgo) {
                        const durationMinutes = parseISODuration(video.duration);
                        if (!channelStats[video.channelId]) {
                            channelStats[video.channelId] = {
                                channelId: video.channelId,
                                channelTitle: video.channelTitle,
                                channelAvatarUrl: video.channelAvatarUrl,
                                totalMinutes: 0,
                                videoCount: 0
                            };
                        }
                        channelStats[video.channelId].totalMinutes += durationMinutes;
                        channelStats[video.channelId].videoCount += 1;
                    }
                });

                const leaderboard = Object.values(channelStats)
                    .sort((a, b) => b.totalMinutes - a.totalMinutes)
                    .slice(0, 20); // 取前 20 名

                // 3. 寫入快取
                await redisClient.set(V12_LEADERBOARD_CACHE_KEY, JSON.stringify(leaderboard), { EX: LEADERBOARD_CACHE_TTL });

                return response.status(200).json(leaderboard);

            } catch (e) {
                console.error(`[API /api/leaderboard] Error:`, e);
                return response.status(500).json({ error: '處理排行榜請求時發生內部錯誤。', details: e.message });
            }
        }

        if (path === '/api/discover-jp-channels') {
            if (!authenticate()) return;
            console.log('[v16.8] 開始執行日文頻道探索任務...');
            const searchPromises = FOREIGN_SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q }));
            const searchResults = await Promise.all(searchPromises);

            const discoveredChannelIds = new Set();
            for (const result of searchResults) {
                result.items?.forEach(item => discoveredChannelIds.add(item.snippet.channelId));
            }

            if (discoveredChannelIds.size === 0) {
                return response.status(200).json({ message: '探索完成，沒有發現任何頻道。' });
            }

            const [
                cnWhitelist, jpWhitelist, pendingWhitelist, jpBlacklist
            ] = await Promise.all([
                redisClient.sMembers(V12_WHITELIST_CN_KEY),
                redisClient.sMembers(V12_WHITELIST_JP_KEY),
                redisClient.sMembers(V12_WHITELIST_PENDING_JP_KEY),
                redisClient.sMembers(V12_BLACKLIST_JP_KEY)
            ]);
            const allExistingIds = new Set([...cnWhitelist, ...jpWhitelist, ...pendingWhitelist, ...jpBlacklist]);

            const newChannelsToAdd = [...discoveredChannelIds].filter(id => !allExistingIds.has(id));

            if (newChannelsToAdd.length > 0) {
                await redisClient.sAdd(V12_WHITELIST_PENDING_JP_KEY, newChannelsToAdd);
                console.log(`[v16.8] 發現 ${newChannelsToAdd.length} 個新頻道，已加入待審核列表。`);
                return response.status(200).json({ message: `探索完成，發現 ${newChannelsToAdd.length} 個新頻道，已加入待審核列表。` });
            }

            return response.status(200).json({ message: '探索完成，沒有發現需要加入的新頻道。' });
        }

        if (path === '/api/admin/lists') {
            if (!authenticate()) return;
            const [pending_jp_ids, whitelist_cn_ids, whitelist_jp_ids, blacklist_cn_ids, blacklist_jp_ids, announcement] = await Promise.all([
                redisClient.sMembers(V12_WHITELIST_PENDING_JP_KEY),
                redisClient.sMembers(V12_WHITELIST_CN_KEY),
                redisClient.sMembers(V12_WHITELIST_JP_KEY),
                redisClient.sMembers(V12_BLACKLIST_CN_KEY),
                redisClient.sMembers(V12_BLACKLIST_JP_KEY),
                redisClient.hGetAll(V12_ANNOUNCEMENT_KEY)
            ]);

            const allIds = [...new Set([...pending_jp_ids, ...whitelist_cn_ids, ...whitelist_jp_ids, ...blacklist_cn_ids, ...blacklist_jp_ids])];
            const channelDetailsMap = new Map();

            if (allIds.length > 0) {
                const batches = batchArray(allIds, 50);
                for (const batch of batches) {
                    const result = await fetchYouTube('channels', { part: 'snippet', id: batch.join(',') });
                    result.items?.forEach(item => channelDetailsMap.set(item.id, {
                        id: item.id,
                        name: item.snippet.title,
                        avatar: item.snippet.thumbnails.default.url
                    }));
                }
            }

            const getDetails = (ids) => ids.map(id => channelDetailsMap.get(id)).filter(Boolean).sort((a, b) => a.name.localeCompare(b.name));

            return response.status(200).json({
                pending_jp: getDetails(pending_jp_ids),
                whitelist_cn: getDetails(whitelist_cn_ids),
                whitelist_jp: getDetails(whitelist_jp_ids),
                blacklist_cn: getDetails(blacklist_cn_ids),
                blacklist_jp: getDetails(blacklist_jp_ids),
                announcement: announcement
            });
        }

        if (path === '/api/admin/manage') {
            if (request.method !== 'POST') return response.status(405).json({ error: '僅允許 POST 方法' });

            if (body.parseError) {
                return response.status(400).json({ error: '無效的 JSON body' });
            }
            if (!authenticate()) return;

            const { action, channelId, listType } = body;
            if (!action) return response.status(400).json({ error: '缺少 action 參數' });

            switch (action) {
                case 'approve_jp':
                    if (!channelId) return response.status(400).json({ error: '缺少 channelId' });
                    await redisClient.sMove(V12_WHITELIST_PENDING_JP_KEY, V12_WHITELIST_JP_KEY, channelId);
                    break;
                case 'reject_jp':
                    if (!channelId) return response.status(400).json({ error: '缺少 channelId' });
                    await redisClient.sMove(V12_WHITELIST_PENDING_JP_KEY, V12_BLACKLIST_JP_KEY, channelId);
                    break;
                case 'delete': {
                    if (!channelId) return response.status(400).json({ error: '缺少 channelId' });
                    if (!listType) return response.status(400).json({ error: '刪除操作需要 listType 參數' });
                    const keyMap = {
                        cn: V12_WHITELIST_CN_KEY,
                        jp: V12_WHITELIST_JP_KEY,
                        blacklist_cn: V12_BLACKLIST_CN_KEY,
                        blacklist_jp: V12_BLACKLIST_JP_KEY
                    };
                    if (!keyMap[listType]) return response.status(400).json({ error: '無效的 listType' });
                    await redisClient.sRem(keyMap[listType], channelId);
                    break;
                }
                case 'add': {
                    if (!channelId) return response.status(400).json({ error: '缺少 channelId' });
                    if (!listType) return response.status(400).json({ error: '新增操作需要有效的 listType' });
                    const keyMap = {
                        cn: V12_WHITELIST_CN_KEY,
                        jp: V12_WHITELIST_JP_KEY,
                        blacklist_cn: V12_BLACKLIST_CN_KEY,
                        blacklist_jp: V12_BLACKLIST_JP_KEY
                    };
                    if (!keyMap[listType]) return response.status(400).json({ error: '無效的 listType' });
                    await redisClient.sAdd(keyMap[listType], channelId);
                    break;
                }
                case 'update_announcement': {
                    const { content, type, active } = body;
                    await redisClient.hSet(V12_ANNOUNCEMENT_KEY, {
                        content: content || '',
                        type: type || 'info',
                        active: String(active),
                        timestamp: Date.now()
                    });
                    break;
                }
                case 'backfill': {
                    const { date, lang, keywords } = body;
                    if (!date || !lang || !keywords || !Array.isArray(keywords)) {
                        return response.status(400).json({ error: '缺少必要參數 (date, lang, keywords)' });
                    }

                    const startDate = new Date(date);
                    const endDate = new Date(date);
                    endDate.setUTCHours(23, 59, 59, 999);

                    const publishedAfter = startDate.toISOString();
                    const publishedBefore = endDate.toISOString();

                    console.log(`[Backfill] 開始回填 ${date} (${lang})，關鍵字: ${keywords.join(', ')}`);

                    const newVideoCandidates = new Set();
                    const searchPromises = keywords.map(q => fetchYouTube('search', {
                        part: 'snippet',
                        type: 'video',
                        maxResults: 50,
                        q,
                        publishedAfter,
                        publishedBefore
                    }));

                    const searchResults = await Promise.all(searchPromises);

                    const blacklistKey = lang === 'jp' ? V12_BLACKLIST_JP_KEY : V12_BLACKLIST_CN_KEY;
                    const blacklist = await redisClient.sMembers(blacklistKey);

                    for (const result of searchResults) {
                        result.items?.forEach(item => {
                            if (item.id.videoId && !blacklist.includes(item.snippet.channelId)) {
                                newVideoCandidates.add(item.id.videoId);
                            }
                        });
                    }

                    if (newVideoCandidates.size > 0) {
                        const storageKeys = lang === 'jp'
                            ? { setKey: v12_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v12_FOREIGN_VIDEO_HASH_PREFIX, type: 'foreign' }
                            : { setKey: v12_VIDEOS_SET_KEY, hashPrefix: v12_VIDEO_HASH_PREFIX, type: 'main' };

                        const ancientDate = new Date(0);
                        const validKeywords = lang === 'jp' ? FOREIGN_SPECIAL_KEYWORDS : SPECIAL_KEYWORDS;
                        // [DOUBLE CHECK] Enable only for CN (lang !== 'jp')
                        const useDoubleKeywordCheck = lang !== 'jp';
                        const options = { retentionDate: ancientDate, validKeywords, blacklist, useDoubleKeywordCheck };

                        const { validVideoIds, idsToDelete } = await v12_logic.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, options);

                        const pipeline = redisClient.multi();
                        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
                        await pipeline.exec();

                        console.log(`[Backfill] ${date} 回填完成，新增/更新了 ${validVideoIds.size} 部影片。`);
                        return response.status(200).json({ success: true, message: `回填成功，處理了 ${validVideoIds.size} 部影片。`, count: validVideoIds.size });
                    } else {
                        console.log(`[Backfill] ${date} 未找到任何影片。`);
                        return response.status(200).json({ success: true, message: '未找到任何影片。', count: 0 });
                    }
                }
                // --- Video Blacklist Management ---
                case 'add_video_blacklist': {
                    const { videoId } = body;
                    if (!videoId) return response.status(400).json({ error: '缺少 videoId' });

                    await redisClient.sAdd(V12_VIDEO_BLACKLIST_KEY, videoId);

                    // Also remove from existing sets if present
                    await redisClient.sRem(V12_VIDEOS_SET_KEY, videoId);
                    await redisClient.sRem(V12_FOREIGN_VIDEOS_SET_KEY, videoId);
                    await redisClient.del(`${v12_VIDEO_HASH_PREFIX}${videoId}`);
                    await redisClient.del(`${v12_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`);

                    return response.status(200).json({ success: true, message: `影片 ${videoId} 已加入黑名單並從資料庫移除。` });
                }
                case 'remove_video_blacklist': {
                    const { videoId } = body;
                    if (!videoId) return response.status(400).json({ error: '缺少 videoId' });
                    await redisClient.sRem(V12_VIDEO_BLACKLIST_KEY, videoId);
                    return response.status(200).json({ success: true, message: `影片 ${videoId} 已從黑名單移除。` });
                }
                case 'backfill_attribution_queue': {
                    // [Feature] Backfill existing videos into the attribution queue
                    console.log('[Backfill Queue] Starting scan for videos missing attribution...');

                    const mainVideoIds = await redisClient.sMembers(V12_VIDEOS_SET_KEY);
                    const foreignVideoIds = await redisClient.sMembers(V12_FOREIGN_VIDEOS_SET_KEY);
                    const allVideoIds = [...new Set([...mainVideoIds, ...foreignVideoIds])];

                    let queuedCount = 0;
                    const pipeline = redisClient.multi();
                    const CHUNK_SIZE = 100; // Process in chunks to avoid blocking too long

                    // Note: In serverless, we might hit timeout if too many videos. 
                    // But checking 2000-3000 keys with pipeline should be fast enough.
                    // If it times out, user might need to run it multiple times or we need cursor.
                    // For now, let's try a simple loop with pipeline checks.

                    // Optimization: We need to check if they have 'originalStreamInfo' AND missing 'targetChannelIds'
                    // Efficient way: Pipeline HGETALL or HMGET for all hashes? That's too heavy.
                    // Better: We rely on the fact that if they are old and have originalStreamInfo, they *need* attribution.
                    // We can just check existence of 'targetChannelIds' field?
                    // Actually, let's just push ALL videos that have 'originalStreamInfo' to the queue. 
                    // The worker handles duplications/already present data efficiently (it just updates).
                    // Wait, the worker fetches APIs. We DON'T want to fetch 3000 videos if they are already done.

                    // Strategy: 
                    // 1. Fetch small batches of keys. 
                    // 2. Check if they need update.
                    // 3. Queue if needed.

                    for (let i = 0; i < allVideoIds.length; i += CHUNK_SIZE) {
                        const chunk = allVideoIds.slice(i, i + CHUNK_SIZE);
                        const multi = redisClient.multi();

                        chunk.forEach(vid => {
                            // Ideally check both prefixes, but typically a vid is in one.
                            // We can just try HGET for both potential keys for 'targetChannelIds' and 'originalStreamInfo'.
                            // To be precise:
                            const mainKey = `${v12_VIDEO_HASH_PREFIX}${vid}`;
                            const foreignKey = `${v12_FOREIGN_VIDEO_HASH_PREFIX}${vid}`;

                            multi.hmGet(mainKey, ['originalStreamInfo', 'targetChannelIds']);
                            multi.hmGet(foreignKey, ['originalStreamInfo', 'targetChannelIds']);
                        });

                        const results = await multi.exec();
                        // results layout: [ [orig, target], [orig, target] (foreign), ... ] for each video

                        for (let j = 0; j < chunk.length; j++) {
                            const mainRes = results[j * 2];
                            const foreignRes = results[j * 2 + 1];
                            const vid = chunk[j];

                            let infoStr = mainRes?.[0] || foreignRes?.[0];
                            let targetStr = mainRes?.[1] || foreignRes?.[1];

                            if (infoStr && infoStr !== '[]' && (!targetStr || targetStr === '[]')) {
                                pipeline.sAdd(V12_PENDING_ATTRIBUTION_QUEUE, vid);
                                queuedCount++;
                            }
                        }
                    }

                    if (queuedCount > 0) {
                        await pipeline.exec();
                    }

                    console.log(`[Backfill Queue] Queued ${queuedCount} videos for attribution.`);
                    return response.status(200).json({ success: true, message: `已將 ${queuedCount} 部需要歸屬分析的影片加入佇列。`, count: queuedCount });
                }

                case 'get_video_blacklist': {
                    const blacklist = await redisClient.sMembers(V12_VIDEO_BLACKLIST_KEY);
                    return response.status(200).json({ success: true, blacklist });
                }
                default:
                    return response.status(400).json({ error: '無效的 action' });
            }
            return response.status(200).json({ success: true, message: `操作 ${action} 成功` });
        }

        if (path === '/api/seed-whitelist') {
            if (!authenticate()) return;
            return response.status(404).json({ message: '此功能已停用，因初始白名單已從程式碼中移除。請使用管理介面進行操作。' });
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
                        await redisClient.hSet(`${V10_VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
                        await redisClient.hSet(`${v12_VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
                        await redisClient.hSet(`${v12_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`, 'videoType', videoType);
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

        // --- START: 修改 (V12 版本檢查) ---
        // 允許版本 >= 11.0 的客戶端請求 (V11 客戶端將使用 V12 邏輯服務)
        if (clientVersion && parseFloat(clientVersion.replace('V', '')) >= 11.0) {
            if (path === '/api/youtube') {
                try {
                    const lang = searchParams.get('lang') || 'cn';
                    const isForeign = lang === 'jp';
                    const forceRefresh = searchParams.get('force_refresh') === 'true';
                    const noIncrement = searchParams.get('no_increment') === 'true';
                    const visitorCount = noIncrement ? await getVisitorCount(redisClient) : await incrementAndGetVisitorCount(redisClient);
                    if (forceRefresh) {
                        if (!authenticate()) return;
                        if (isForeign) {
                            await v12_logic.updateForeignClips(redisClient);
                            await redisClient.set(v12_FOREIGN_META_LAST_UPDATED_KEY, Date.now());
                        } else {
                            await v12_logic.updateAndStoreYouTubeData(redisClient);
                            await redisClient.set(v12_META_LAST_UPDATED_KEY, Date.now());
                        }
                    } else {
                        const lastUpdateCN = await redisClient.get(v12_META_LAST_UPDATED_KEY);
                        const needsUpdateCN = !lastUpdateCN || (Date.now() - parseInt(lastUpdateCN, 10)) > UPDATE_INTERVAL_SECONDS * 1000;
                        if (needsUpdateCN) {
                            const lockAcquired = await redisClient.set(v12_UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 600 });
                            if (lockAcquired) {
                                try {
                                    console.log(`[v16.8] 觸發中文影片同步更新...`);
                                    await v12_logic.updateAndStoreYouTubeData(redisClient);
                                    await redisClient.set(v12_META_LAST_UPDATED_KEY, Date.now());
                                } catch (e) {
                                    console.error("中文更新失敗:", e);
                                } finally {
                                    await redisClient.del(v12_UPDATE_LOCK_KEY);
                                }
                            }
                        }

                        const lastUpdateJP = await redisClient.get(v12_FOREIGN_META_LAST_UPDATED_KEY);
                        const needsUpdateJP = !lastUpdateJP || (Date.now() - parseInt(lastUpdateJP, 10)) > FOREIGN_UPDATE_INTERVAL_SECONDS * 1000;
                        if (needsUpdateJP) {
                            const lockAcquired = await redisClient.set(v12_FOREIGN_UPDATE_LOCK_KEY, 'locked', { NX: true, EX: 600 });
                            if (lockAcquired) {
                                try {
                                    console.log(`[v16.8] 觸發日文影片同步更新...`);
                                    await v12_logic.updateForeignClips(redisClient);
                                    await redisClient.set(v12_FOREIGN_META_LAST_UPDATED_KEY, Date.now());
                                } catch (e) {
                                    console.error("日文更新失敗:", e);
                                } finally {
                                    await redisClient.del(v12_FOREIGN_UPDATE_LOCK_KEY);
                                }
                            }
                        }
                    }
                    const storageKeys = isForeign ? { setKey: v12_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v12_FOREIGN_VIDEO_HASH_PREFIX } : { setKey: v12_VIDEOS_SET_KEY, hashPrefix: v12_VIDEO_HASH_PREFIX };
                    const videos = await v12_logic.getVideosFromDB(redisClient, storageKeys);
                    const updatedTimestamp = await redisClient.get(isForeign ? v12_FOREIGN_META_LAST_UPDATED_KEY : v12_META_LAST_UPDATED_KEY);

                    const announcement = await redisClient.hGetAll(V12_ANNOUNCEMENT_KEY);

                    return response.status(200).json({
                        videos: videos,
                        timestamp: new Date(parseInt(updatedTimestamp, 10) || Date.now()).toISOString(),
                        totalVisits: visitorCount.totalVisits,
                        todayVisits: visitorCount.todayVisits,
                        script_version: SCRIPT_VERSION,
                        announcement: announcement && announcement.active === 'true' ? announcement : null
                    });
                } catch (e) {
                    console.error(`[API /api/youtube V12] Error:`, e);
                    return response.status(500).json({ error: '處理 V12 /api/youtube 請求時發生內部錯誤。', details: e.message });
                }
            } else if (path === '/api/get-related-clips') {
                try {
                    const platform = searchParams.get('platform');
                    const id = searchParams.get('id');
                    if (!platform || !id || !['youtube', 'twitch'].includes(platform)) {
                        return response.status(400).json({ error: '無效的請求：必須提供有效的 platform (youtube/twitch) 和 id 參數。' });
                    }
                    const indexKey = `${v12_STREAM_INDEX_PREFIX}${platform}:${id}`;
                    const relatedVideoIdentifiers = await redisClient.sMembers(indexKey);
                    if (relatedVideoIdentifiers.length === 0) { return response.status(200).json({ videos: [] }); }
                    const pipeline = redisClient.multi();
                    relatedVideoIdentifiers.forEach(identifier => {
                        if (typeof identifier === 'string' && identifier.includes(':')) {
                            const [type, videoId] = identifier.split(':');
                            if (type === 'main') {
                                pipeline.hGetAll(`${v12_VIDEO_HASH_PREFIX}${videoId}`);
                            } else if (type === 'foreign') {
                                pipeline.hGetAll(`${v12_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`);
                            }
                        } else {
                            console.warn(`[API /api/get-related-clips] 發現無效的 identifier: ${identifier}`);
                        }
                    });
                    const results = await pipeline.exec();
                    const videos = results.map(videoData => {
                        if (videoData instanceof Error) {
                            console.error("[API /api/get-related-clips] Redis pipeline error:", videoData);
                            return null;
                        }
                        return v12_normalizeVideoData(videoData);
                    }).filter(Boolean);
                    videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
                    return response.status(200).json({ videos });
                } catch (e) {
                    console.error(`[API /api/get-related-clips] Error:`, e);
                    return response.status(500).json({ error: '處理同元配信請求時發生內部錯誤。', details: e.message });
                }
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