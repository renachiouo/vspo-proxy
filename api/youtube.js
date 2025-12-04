import { createClient } from 'redis';

// --- 版本指紋 ---
const SCRIPT_VERSION = '16.0-DualBlacklist';

// --- Redis Keys Configuration ---
const V12_WHITELIST_CN_KEY = 'vspo-db:v4:whitelist:cn';
const V12_WHITELIST_JP_KEY = 'vspo-db:v4:whitelist:jp';
const V12_WHITELIST_PENDING_JP_KEY = 'vspo-db:v4:whitelist:pending:jp';
// --- START: 修改 (黑名單分離) ---
const V12_BLACKLIST_CN_KEY = 'vspo-db:v4:blacklist:cn';
const V12_BLACKLIST_JP_KEY = 'vspo-db:v4:blacklist:jp';
// --- END: 修改 ---

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

const V10_PENDING_CLASSIFICATION_SET_KEY = `vspo-db:v2:pending_classification`;
const V10_CLASSIFICATION_LOCK_KEY = `vspo-db:v2:meta:classification_lock`;
const V10_VIDEO_HASH_PREFIX = `vspo-db:v2:video:`;


// --- 更新頻率設定 ---
const UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘
const FOREIGN_UPDATE_INTERVAL_SECONDS = 1200; // 20 分鐘

// --- YouTube API 設定 ---
// 初始白名單已被移除，因為現在透過 UI 在 Redis 中進行管理。

const SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const FOREIGN_SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"];
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9,
].filter(key => key);

// --- 輔助函式 (通用) ---
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
const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.warn(`金鑰 ${apiKey.substring(0, 8)}... 配額錯誤，嘗試下一個。`);
                continue;
            }
            if (data.error) throw new Error(data.error.message);
            return data;
        } catch (e) {
            console.error(`金鑰 ${apiKey.substring(0, 8)}... 發生錯誤`, e);
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
    return video;
}

const v11_logic = {
    async getVideosFromDB(redisClient, storageKeys) {
        const videoIds = await redisClient.sMembers(storageKeys.setKey);
        if (!videoIds || videoIds.length === 0) return [];
        const pipeline = redisClient.multi();
        videoIds.forEach(id => pipeline.hGetAll(`${storageKeys.hashPrefix}${id}`));
        const results = await pipeline.exec();
        const videos = results.map(video => v11_normalizeVideoData(video)).filter(Boolean);
        videos.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
        return videos;
    },
    async processAndStoreVideos(videoIds, redisClient, storageKeys, options = {}) {
        const { retentionDate, validKeywords, blacklist = [] } = options;

        if (videoIds.length === 0) return { validVideoIds: new Set(), idsToDelete: [] };
        const videoDetailsMap = new Map();
        const videoDetailBatches = batchArray(videoIds, 50);
        for (const batch of videoDetailBatches) { const result = await fetchYouTube('videos', { part: 'statistics,snippet', id: batch.join(',') }); result.items?.forEach(item => videoDetailsMap.set(item.id, item)); }

        const channelStatsMap = new Map();
        const allChannelIds = [...new Set(Array.from(videoDetailsMap.values()).map(d => d.snippet.channelId))];
        if (allChannelIds.length > 0) { const channelDetailBatches = batchArray(allChannelIds, 50); for (const batch of channelDetailBatches) { const result = await fetchYouTube('channels', { part: 'statistics,snippet', id: batch.join(',') }); result.items?.forEach(item => channelStatsMap.set(item.id, item)); } }

        const validVideoIds = new Set();
        const pipeline = redisClient.multi();
        const videosToClassify = [];

        for (const videoId of videoIds) {
            const detail = videoDetailsMap.get(videoId);
            if (!detail) continue;
            const channelId = detail.snippet.channelId;
            const isChannelBlacklisted = blacklist.includes(channelId);
            const isKeywordBlacklisted = containsBlacklistedKeyword(detail, KEYWORD_BLACKLIST);
            const isExpired = new Date(detail.snippet.publishedAt) < retentionDate;
            const isContentValid = isVideoValid(detail, validKeywords);

            if (!isChannelBlacklisted && !isKeywordBlacklisted && !isExpired && isContentValid) {
                validVideoIds.add(videoId);
                const channelDetails = channelStatsMap.get(channelId);
                const { title, description } = detail.snippet;
                const videoData = { id: videoId, title: title, searchableText: `${title || ''} ${description || ''}`.toLowerCase(), thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url, channelId: channelId, channelTitle: detail.snippet.channelTitle, channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '', publishedAt: detail.snippet.publishedAt, viewCount: detail.statistics?.viewCount || 0, subscriberCount: channelDetails?.statistics?.subscriberCount || 0, };

                videosToClassify.push(videoId);

                const originalStreamInfo = parseOriginalStreamInfo(description);
                if (originalStreamInfo) {
                    videoData.originalStreamInfo = JSON.stringify(originalStreamInfo);
                    const indexKey = `${v11_STREAM_INDEX_PREFIX}${originalStreamInfo.platform}:${originalStreamInfo.id}`;
                    pipeline.sAdd(indexKey, `${storageKeys.type}:${videoId}`);
                }
                pipeline.hSet(`${storageKeys.hashPrefix}${videoId}`, videoData);
            }
        }

        if (videosToClassify.length > 0) {
            pipeline.sAdd(V10_PENDING_CLASSIFICATION_SET_KEY, videosToClassify);
        }

        await pipeline.exec();
        const allIdsInDB = await redisClient.sMembers(storageKeys.setKey);
        const idsToDelete = allIdsInDB.filter(id => !validVideoIds.has(id));
        return { validVideoIds, idsToDelete };
    },
    async updateAndStoreYouTubeData(redisClient) {
        console.log(`[v16.0] 開始執行中文影片常規更新程序...`);
        const oneMonthAgo = new Date(); oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1); const publishedAfter = oneMonthAgo.toISOString();

        const [currentWhitelist, blacklist] = await Promise.all([
            redisClient.sMembers(V12_WHITELIST_CN_KEY),
            redisClient.sMembers(V12_BLACKLIST_CN_KEY) // --- START: 修改 (使用中文黑名單) ---
        ]);
        console.log(`[v16.0] 從 Redis 載入 ${currentWhitelist.length} 個中文白名單頻道和 ${blacklist.length} 個中文黑名單頻道。`);
        // --- END: 修改 ---

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
                    if (!currentWhitelist.includes(channelId) && (video.snippet.description || '').includes('ぶいすぽ')) {
                        newChannelsToAdd.add(channelId);
                    }
                });
            }
            if (newChannelsToAdd.size > 0) {
                await redisClient.sAdd(V12_WHITELIST_CN_KEY, [...newChannelsToAdd]);
                console.log(`[v16.0] 自動新增 ${newChannelsToAdd.size} 個新頻道到中文白名單: ${[...newChannelsToAdd].join(', ')}`);
            }
        }

        const storageKeys = { setKey: v11_VIDEOS_SET_KEY, hashPrefix: v11_VIDEO_HASH_PREFIX, type: 'main' };
        const options = { retentionDate: oneMonthAgo, validKeywords: SPECIAL_KEYWORDS, blacklist };

        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, options);

        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log(`[v16.0] 中文影片常規更新程序完成。`);
    },
    async updateForeignClips(redisClient) {
        console.log('[v16.0] 開始執行外文影片常規更新程序...');
        const threeMonthsAgo = new Date(); threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

        const [currentWhitelist, blacklist] = await Promise.all([
            redisClient.sMembers(V12_WHITELIST_JP_KEY),
            redisClient.sMembers(V12_BLACKLIST_JP_KEY) // --- START: 修改 (使用日文黑名單) ---
        ]);
        console.log(`[v16.0] 從 Redis 載入 ${currentWhitelist.length} 個日文白名單頻道和 ${blacklist.length} 個日文黑名單頻道。`);
        // --- END: 修改 ---

        if (currentWhitelist.length === 0) {
            console.warn("[v16.0] 日文白名單為空，更新程序終止。");
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

        for (const playlistId of uploadPlaylistIds) { const result = await fetchYouTube('playlistItems', { part: 'snippet', playlistId, maxResults: 50 }); result.items?.forEach(item => { if (new Date(item.snippet.publishedAt) > threeMonthsAgo) { newVideoCandidates.add(item.snippet.resourceId.videoId); } }); }

        const storageKeys = { setKey: v11_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v11_FOREIGN_VIDEO_HASH_PREFIX, type: 'foreign' };
        const options = { retentionDate: threeMonthsAgo, validKeywords: FOREIGN_SPECIAL_KEYWORDS, blacklist };

        const { validVideoIds, idsToDelete } = await this.processAndStoreVideos([...newVideoCandidates], redisClient, storageKeys, options);

        const pipeline = redisClient.multi();
        if (idsToDelete.length > 0) { pipeline.sRem(storageKeys.setKey, idsToDelete); idsToDelete.forEach(id => pipeline.del(`${storageKeys.hashPrefix}${id}`)); }
        if (validVideoIds.size > 0) { pipeline.sAdd(storageKeys.setKey, [...validVideoIds]); }
        await pipeline.exec();
        console.log('[v16.0] 外文影片常規更新程序完成。');
    },
};

// --- 主 Handler ---
export default async function handler(request, response) {
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

        // --- START: 修改 (徹底修復 Body 解析問題) ---
        let body = {};
        if (request.method === 'POST') {
            try {
                // Vercel Node.js 環境通常會自動解析 body
                if (request.body) {
                    body = typeof request.body === 'string' ? JSON.parse(request.body) : request.body;
                } else if (typeof request.json === 'function') {
                    // Edge Runtime 或其他支援 Request API 的環境
                    body = await request.json();
                }
            } catch (e) {
                console.error("解析 JSON body 失敗:", e);
                // 標記解析錯誤，讓後續的路由可以處理這個情況
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
        // --- END: 修改 ---

        if (path === '/api/discover-jp-channels') {
            if (!authenticate()) return;
            console.log('[v16.0] 開始執行日文頻道探索任務...');
            const searchPromises = FOREIGN_SPECIAL_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q }));
            const searchResults = await Promise.all(searchPromises);

            const discoveredChannelIds = new Set();
            for (const result of searchResults) {
                result.items?.forEach(item => discoveredChannelIds.add(item.snippet.channelId));
            }

            if (discoveredChannelIds.size === 0) {
                return response.status(200).json({ message: '探索完成，沒有發現任何頻道。' });
            }

            // --- START: 修改 (使用分離後的黑名單邏輯) ---
            const [
                cnWhitelist, jpWhitelist, pendingWhitelist, jpBlacklist
            ] = await Promise.all([
                redisClient.sMembers(V12_WHITELIST_CN_KEY),
                redisClient.sMembers(V12_WHITELIST_JP_KEY),
                redisClient.sMembers(V12_WHITELIST_PENDING_JP_KEY),
                redisClient.sMembers(V12_BLACKLIST_JP_KEY)
            ]);
            // 中文黑名單中的頻道，仍然可以被加入日文待審核列表，所以這裡不檢查 cnBlacklist
            const allExistingIds = new Set([...cnWhitelist, ...jpWhitelist, ...pendingWhitelist, ...jpBlacklist]);
            // --- END: 修改 ---

            const newChannelsToAdd = [...discoveredChannelIds].filter(id => !allExistingIds.has(id));

            if (newChannelsToAdd.length > 0) {
                await redisClient.sAdd(V12_WHITELIST_PENDING_JP_KEY, newChannelsToAdd);
                console.log(`[v16.0] 發現 ${newChannelsToAdd.length} 個新頻道，已加入待審核列表。`);
                return response.status(200).json({ message: `探索完成，發現 ${newChannelsToAdd.length} 個新頻道，已加入待審核列表。` });
            }

            return response.status(200).json({ message: '探索完成，沒有發現需要加入的新頻道。' });
        }

        if (path === '/api/admin/lists') {
            if (!authenticate()) return;
            // --- START: 修改 (獲取分離後的黑名單) ---
            const [pending_jp_ids, whitelist_cn_ids, whitelist_jp_ids, blacklist_cn_ids, blacklist_jp_ids] = await Promise.all([
                redisClient.sMembers(V12_WHITELIST_PENDING_JP_KEY),
                redisClient.sMembers(V12_WHITELIST_CN_KEY),
                redisClient.sMembers(V12_WHITELIST_JP_KEY),
                redisClient.sMembers(V12_BLACKLIST_CN_KEY),
                redisClient.sMembers(V12_BLACKLIST_JP_KEY)
            ]);

            const allIds = [...new Set([...pending_jp_ids, ...whitelist_cn_ids, ...whitelist_jp_ids, ...blacklist_cn_ids, ...blacklist_jp_ids])];
            // --- END: 修改 ---
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
                blacklist_cn: getDetails(blacklist_cn_ids), // --- START: 修改 ---
                blacklist_jp: getDetails(blacklist_jp_ids), // --- END: 修改 ---
            });
        }

        if (path === '/api/admin/manage') {
            if (request.method !== 'POST') return response.status(405).json({ error: '僅允許 POST 方法' });

            if (body.parseError) {
                return response.status(400).json({ error: '無效的 JSON body' });
            }
            if (!authenticate()) return;

            const { action, channelId, listType } = body;
            if (!action || !channelId) return response.status(400).json({ error: '缺少 action 或 channelId 參數' });

            switch (action) {
                case 'approve_jp':
                    await redisClient.sMove(V12_WHITELIST_PENDING_JP_KEY, V12_WHITELIST_JP_KEY, channelId);
                    break;
                case 'reject_jp':
                    // 否決日文頻道 -> 加入日文黑名單
                    await redisClient.sMove(V12_WHITELIST_PENDING_JP_KEY, V12_BLACKLIST_JP_KEY, channelId);
                    break;
                case 'delete': {
                    if (!listType) return response.status(400).json({ error: '刪除操作需要 listType 參數' });
                    // --- START: 修改 (使用分離後的黑名單 Key) ---
                    const keyMap = {
                        cn: V12_WHITELIST_CN_KEY,
                        jp: V12_WHITELIST_JP_KEY,
                        blacklist_cn: V12_BLACKLIST_CN_KEY,
                        blacklist_jp: V12_BLACKLIST_JP_KEY
                    };
                    if (!keyMap[listType]) return response.status(400).json({ error: '無效的 listType' });
                    // --- END: 修改 ---
                    await redisClient.sRem(keyMap[listType], channelId);
                    break;
                }
                case 'add': {
                    if (!listType) return response.status(400).json({ error: '新增操作需要有效的 listType' });
                    // --- START: 修改 (使用分離後的黑名單 Key) ---
                    const keyMap = {
                        cn: V12_WHITELIST_CN_KEY,
                        jp: V12_WHITELIST_JP_KEY,
                        blacklist_cn: V12_BLACKLIST_CN_KEY,
                        blacklist_jp: V12_BLACKLIST_JP_KEY
                    };
                    if (!keyMap[listType]) return response.status(400).json({ error: '無效的 listType' });
                    // --- END: 修改 ---
                    await redisClient.sAdd(keyMap[listType], channelId);
                    break;
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
                            await v11_logic.updateForeignClips(redisClient);
                            await redisClient.set(v11_FOREIGN_META_LAST_UPDATED_KEY, Date.now());
                        } else {
                            await v11_logic.updateAndStoreYouTubeData(redisClient);
                            await redisClient.set(v11_META_LAST_UPDATED_KEY, Date.now());
                        }
                    } else {
                        const lastUpdateKey = isForeign ? v11_FOREIGN_META_LAST_UPDATED_KEY : v11_META_LAST_UPDATED_KEY;
                        const updateInterval = isForeign ? FOREIGN_UPDATE_INTERVAL_SECONDS : UPDATE_INTERVAL_SECONDS;
                        const lockKey = isForeign ? v11_FOREIGN_UPDATE_LOCK_KEY : v11_UPDATE_LOCK_KEY;
                        const lastUpdate = await redisClient.get(lastUpdateKey);
                        const needsUpdate = !lastUpdate || (Date.now() - parseInt(lastUpdate, 10)) > updateInterval * 1000;
                        if (needsUpdate) {
                            const lockAcquired = await redisClient.set(lockKey, 'locked', { NX: true, EX: 600 });
                            if (lockAcquired) {
                                try {
                                    console.log(`[v16.0] 執行${isForeign ? '日文' : '中文'}影片同步更新...`);
                                    if (isForeign) {
                                        await v11_logic.updateForeignClips(redisClient);
                                    } else {
                                        await v11_logic.updateAndStoreYouTubeData(redisClient);
                                    }
                                    await redisClient.set(lastUpdateKey, Date.now());
                                } finally {
                                    await redisClient.del(lockKey);
                                }
                            }
                        }
                    }
                    const storageKeys = isForeign ? { setKey: v11_FOREIGN_VIDEOS_SET_KEY, hashPrefix: v11_FOREIGN_VIDEO_HASH_PREFIX } : { setKey: v11_VIDEOS_SET_KEY, hashPrefix: v11_VIDEO_HASH_PREFIX };
                    const videos = await v11_logic.getVideosFromDB(redisClient, storageKeys);
                    const updatedTimestamp = await redisClient.get(isForeign ? v11_FOREIGN_META_LAST_UPDATED_KEY : v11_META_LAST_UPDATED_KEY);
                    return response.status(200).json({ videos: videos, timestamp: new Date(parseInt(updatedTimestamp, 10) || Date.now()).toISOString(), totalVisits: visitorCount.totalVisits, todayVisits: visitorCount.todayVisits, script_version: SCRIPT_VERSION, });
                } catch (e) {
                    console.error(`[API /api/youtube V11] Error:`, e);
                    return response.status(500).json({ error: '處理 V11 /api/youtube 請求時發生內部錯誤。', details: e.message });
                }
            } else if (path === '/api/get-related-clips') {
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
                            if (type === 'main') {
                                pipeline.hGetAll(`${v11_VIDEO_HASH_PREFIX}${videoId}`);
                            } else if (type === 'foreign') {
                                pipeline.hGetAll(`${v11_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`);
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
                        return v11_normalizeVideoData(videoData);
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