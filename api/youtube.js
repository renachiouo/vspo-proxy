
import { MongoClient } from 'mongodb';

// --- Configuration ---
const SCRIPT_VERSION = '17.3-Debug-Force';
const UPDATE_INTERVAL_SECONDS = 1200; // CN: 20 mins
const FOREIGN_UPDATE_INTERVAL_SECONDS = 1200; // JP Whitelist: 20 mins
const FOREIGN_SEARCH_INTERVAL_SECONDS = 3600; // JP Keywords: 60 mins
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://magirenaouo_db_user:LAS6RKXKK4AUv3UW@vspoproxy.pdjcq2p.mongodb.net/?appName=vspoproxy';
const DB_NAME = 'vspoproxy';

// --- Constants ---
const SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const FOREIGN_SEARCH_KEYWORDS = ["ぶいすぽ 切り抜き"];
// Removed FOREIGN_SPECIAL_KEYWORDS as requested, using SPECIAL_KEYWORDS universally
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO中文精華", "VSPO精華", "VSPO中文剪輯", "VSPO剪輯"];
const KEYWORD_BLACKLIST = ["MMD"];
const VSPO_MEMBER_KEYWORDS = [
    "花芽すみれ", "花芽なずな", "小雀とと", "一ノ瀬うるは", "胡桃のあ", "兎咲ミミ", "空澄セナ", "橘ひなの", "英リサ", "如月れん", "神成きゅぴ", "八雲べに", "藍沢エマ", "紫宮るな", "猫汰つな", "白波らむね", "小森めと", "夢野あかり", "夜乃くろむ", "紡木こかげ", "千燈ゆうひ", "蝶屋はなび", "甘結もか",
    "Remia", "Arya", "Jira", "Narin", "Riko", "Eris",
    "小針彩", "白咲露理", "帕妃", "千郁郁",
    "ひなーの", "ひなの", "べに", "つな", "らむち", "らむね", "めと", "なずな", "なずぴ", "すみー", "すみれ", "ととち", "とと", "のせ", "うるは", "のあ", "ミミ", "たや", "セナ", "あしゅみ", "リサ", "れん", "きゅぴ", "エマたそ", "るな", "あかり", "あかりん", "くろむ", "こかげ", "つむお", "うひ", "ゆうひ", "はなび", "もか"
];

const VSPO_MEMBERS = [
    { name: "花芽すみれ", ytId: "UCyLGcqYs7RsBb3L0SJfzGYA", twitchId: "695556933" },
    { name: "花芽なずな", ytId: "UCiMG6VdScBabPhJ1ZtaVmbw", twitchId: "790167759" },
    { name: "小雀とと", ytId: "UCgTzsBI0DIRopMylJEDqnog", twitchId: "" },
    { name: "一ノ瀬うるは", ytId: "UC5LyYg6cCA4yHEYvtUsir3g", twitchId: "582689327" },
    { name: "胡桃のあ", ytId: "UCIcAj6WkJ8vZ7DeJVgmeqKw", twitchId: "600770697" },
    { name: "兎咲ミミ", ytId: "UCnvVG9RbOW3J6Ifqo-zKLiw", twitchId: "" },
    { name: "空澄セナ", ytId: "UCF_U2GCKHvDz52jWdizppIA", twitchId: "776751504" },
    { name: "橘ひなの", ytId: "UCvUc0m317LWTTPZoBQV479A", twitchId: "568682215" },
    { name: "英リサ", ytId: "UCurEA8YoqFwimJcAuSHU0MQ", twitchId: "777700650" },
    { name: "如月れん", ytId: "UCGWa1dMU_sDCaRQjdabsVgg", twitchId: "722162135" },
    { name: "神成きゅぴ", ytId: "UCMp55EbT_ZlqiMS3lCj01BQ", twitchId: "550676410" },
    { name: "八雲べに", ytId: "UCjXBuHmWkieBApgBhDuJMMQ", twitchId: "700465409" },
    { name: "藍沢エマ", ytId: "UCPkKpOHxEDcwmUAnRpIu-Ng", twitchId: "848822715" },
    { name: "紫宮るな", ytId: "UCD5W21JqNMv_tV9nfjvF9sw", twitchId: "773185713" },
    { name: "猫汰つな", ytId: "UCIjdfjcSaEgdjwbgjxC3ZWg", twitchId: "858359105" },
    { name: "白波らむね", ytId: "UC61OwuYOVuKkpKnid-43Twg", twitchId: "858359149" },
    { name: "小森めと", ytId: "UCzUNASdzI4PV5SlqtYwAkKQ", twitchId: "801682194" },
    { name: "夢野あかり", ytId: "UCS5l_Y0oMVTjEos2LuyeSZQ", twitchId: "584184005" },
    { name: "夜乃くろむ", ytId: "UCX4WL24YEOUYd7qDsFSLDOw", twitchId: "1250148772" },
    { name: "紡木こかげ", ytId: "UC-WX1CXssCtCtc2TNIRnJzg", twitchId: "1184405770" },
    { name: "千燈ゆうひ", ytId: "UCuDY3ibSP2MFRgf7eo3cojg", twitchId: "1097252496" },
    { name: "蝶屋はなび", ytId: "UCL9hJsdk9eQa0IlWbFB2oRg", twitchId: "1361841459" },
    { name: "甘結もか", ytId: "UC8vKBjGY2HVfbW9GAmgikWw", twitchId: "" },
    { name: "ぶいすぽっ!【公式】", ytId: "UCuI5XaO-6VkOEhHao6ij7JA", twitchId: "" },
    { name: "Remia Aotsuki", ytId: "UCCra1t-eIlO3ULyXQQMD9Xw", twitchId: "1102206195" },
    { name: "Arya Kuroha", ytId: "UCLlJpxXt6L5d-XQ0cDdIyDQ", twitchId: "1102211983" },
    { name: "Jira Jisaki", ytId: "UCeCWj-SiJG9SWN6wGORiLmw", twitchId: "1102212264" },
    { name: "Narin Mikure", ytId: "UCKSpM183c85d5V2cW5qaUjA", twitchId: "1125214436" },
    { name: "Riko Solari", ytId: "UC7Xglp1fske9zmRe7Oj8YyA", twitchId: "1125216387" },
    { name: "Eris Suzukami", ytId: "UCp_3ej2br9l9L1DSoHVDZGw", twitchId: "" }
];


const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9, process.env.YOUTUBE_API_KEY_10,
    process.env.YOUTUBE_API_KEY_11,
].filter(key => key);

// Constants
const LIVE_UPDATE_INTERVAL_SECONDS = 300; // 5 mins for Live Status

// --- DB Connection ---
let cachedClient = null;
let cachedDb = null;
async function getDb() {
    if (cachedDb) return cachedDb;
    if (!cachedClient) {
        cachedClient = new MongoClient(MONGODB_URI);
        await cachedClient.connect();
    }
    cachedDb = cachedClient.db(DB_NAME);
    return cachedDb;
}

// --- Helpers ---
const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

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

const isVideoValid = (videoDetail, keywords = SPECIAL_KEYWORDS) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description} `.toLowerCase();

    // 1. Check Mandatory Keywords (e.g. License)
    const hasLicense = keywords.some(keyword => searchText.includes(keyword.toLowerCase()));
    if (!hasLicense) return false;

    // 2. Check Member Keywords (Mandatory Double Check)
    const hasMemberKeyword = VSPO_MEMBER_KEYWORDS.some(keyword => searchText.includes(keyword.toLowerCase()));
    if (!hasMemberKeyword) return false;

    if (videoDetail.snippet.liveBroadcastContent === 'live' || videoDetail.snippet.liveBroadcastContent === 'upcoming') return false;
    return true;
};

const containsBlacklistedKeyword = (videoDetail, blacklist) => {
    if (!videoDetail || !videoDetail.snippet) return false;
    const searchText = `${videoDetail.snippet.title} ${videoDetail.snippet.description} `.toLowerCase();
    return blacklist.some(keyword => searchText.includes(keyword.toLowerCase()));
};

const logAdminAction = async (db, action, details) => {
    try {
        await db.collection('admin_logs').insertOne({ timestamp: new Date(), action, details });
    } catch (e) { console.error('AdminLog Error:', e); }
};

async function getVisitorCount(db) {
    const total = await db.collection('analytics').findOne({ _id: 'total_visits' });
    const todayStr = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Taipei' }).format(new Date());
    const today = await db.collection('analytics').findOne({ _id: `visits_${todayStr}` });
    return { totalVisits: total?.count || 0, todayVisits: today?.count || 0 };
}

async function incrementAndGetVisitorCount(db) {
    const todayStr = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Taipei' }).format(new Date());
    const total = await db.collection('analytics').findOneAndUpdate(
        { _id: 'total_visits' }, { $inc: { count: 1 }, $set: { lastUpdated: new Date() } }, { upsert: true, returnDocument: 'after' }
    );
    const today = await db.collection('analytics').findOneAndUpdate(
        { _id: `visits_${todayStr}` }, { $inc: { count: 1 }, $set: { date: todayStr, type: 'daily_visit' } }, { upsert: true, returnDocument: 'after' }
    );
    return { totalVisits: total?.count || 0, todayVisits: today?.count || 0 };
}

const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) continue;
            if (data.error) throw new Error(data.error.message);
            return data;
        } catch (e) { /* ignore */ }
    }
    throw new Error('API Quota Exceeded');
};

function parseOriginalStreamInfo(description) {
    if (!description) return null;
    const split = description.match(/(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
    if (split?.[1]) return { platform: 'youtube', id: split[1] };
    const tw = description.match(/(?:https?:\/\/)?(?:www\.)?twitch\.tv\/videos\/(\d+)/);
    if (tw?.[1]) return { platform: 'twitch', id: tw[1] };
    return null;
}

// Twitch Helpers
let twitchToken = null;
let twitchTokenExpiry = 0;

async function getTwitchToken() {
    if (twitchToken && Date.now() < twitchTokenExpiry) return twitchToken;
    const clientId = process.env.TWITCH_CLIENT_ID;
    const clientSecret = process.env.TWITCH_CLIENT_SECRET;
    if (!clientId || !clientSecret) return null;

    try {
        const res = await fetch(`https://id.twitch.tv/oauth2/token?client_id=${clientId}&client_secret=${clientSecret}&grant_type=client_credentials`, { method: 'POST' });
        const data = await res.json();
        if (data.access_token) {
            twitchToken = data.access_token;
            twitchTokenExpiry = Date.now() + (data.expires_in * 1000) - 60000;
            return twitchToken;
        }
    } catch (e) { console.error('Twitch Token Error:', e); }
    return null;
}

async function fetchTwitchStreams(userIds) {
    const token = await getTwitchToken();
    if (!token) return [];
    try {
        const query = userIds.map(id => `user_id=${id}`).join('&');
        const res = await fetch(`https://api.twitch.tv/helix/streams?${query}`, {
            headers: { 'Client-ID': process.env.TWITCH_CLIENT_ID, 'Authorization': `Bearer ${token}` }
        });
        const data = await res.json();
        return data.data || [];
    } catch (e) { console.error('Twitch Stream Error:', e); return []; }
}

// --- Logic ---
const v12_logic = {
    async processAndStoreVideos(videoIds, db, type, options = {}) {
        const { retentionDate, blacklist = [], targetList = null } = options;
        // targetList: If provided (e.g., 'pending_jp'), IDs go there instead of videos collection (for JP keywords)

        if (videoIds.length === 0) return { validVideoIds: new Set() };

        const videoDetailsMap = new Map();
        for (const batch of batchArray(videoIds, 50)) {
            const res = await fetchYouTube('videos', { part: 'statistics,snippet,contentDetails', id: batch.join(',') });
            res.items?.forEach(item => videoDetailsMap.set(item.id, item));
        }

        const channelStatsMap = new Map();
        const allChannelIds = [...new Set([...videoDetailsMap.values()].map(d => d.snippet.channelId))];
        if (allChannelIds.length > 0) {
            for (const batch of batchArray(allChannelIds, 50)) {
                const res = await fetchYouTube('channels', { part: 'statistics,snippet', id: batch.join(',') });
                res.items?.forEach(item => channelStatsMap.set(item.id, item));
            }
        }

        const validVideoIds = new Set();
        const bulkOps = [];
        const vbDoc = await db.collection('lists').findOne({ _id: 'video_blacklist' });
        const videoBlacklist = vbDoc?.items || [];
        const newPendingChannels = new Set();

        for (const videoId of videoIds) {
            const detail = videoDetailsMap.get(videoId);
            if (!detail || videoBlacklist.includes(videoId)) continue;

            const channelId = detail.snippet.channelId;
            if (blacklist.includes(channelId)) continue;
            if (containsBlacklistedKeyword(detail, KEYWORD_BLACKLIST)) continue;
            if (new Date(detail.snippet.publishedAt) < retentionDate) continue;
            // Strict Double Check is now built into isVideoValid
            if (!isVideoValid(detail)) continue;

            validVideoIds.add(videoId);

            // Special Case: JS Keyword Search -> Pending List
            if (targetList) {
                newPendingChannels.add(channelId);
                continue; // Don't store video yet
            }

            const channelDetails = channelStatsMap.get(channelId);
            const title = detail.snippet.title;
            const description = detail.snippet.description;
            const durationMinutes = parseISODuration(detail.contentDetails?.duration);
            const doc = {
                _id: videoId, id: videoId, title,
                searchableText: `${title} ${description}`.toLowerCase(),
                thumbnail: detail.snippet.thumbnails.high?.url || detail.snippet.thumbnails.default?.url,
                channelId, channelTitle: detail.snippet.channelTitle,
                channelAvatarUrl: channelDetails?.snippet?.thumbnails?.default?.url || '',
                publishedAt: new Date(detail.snippet.publishedAt),
                viewCount: parseInt(detail.statistics?.viewCount || 0),
                subscriberCount: parseInt(channelDetails?.statistics?.subscriberCount || 0),
                duration: detail.contentDetails?.duration || '',
                durationMinutes,
                source: type,
                videoType: durationMinutes <= 1.05 ? 'short' : 'video'
            };
            const osi = parseOriginalStreamInfo(description);
            if (osi) doc.originalStreamInfo = osi;

            bulkOps.push({ updateOne: { filter: { _id: videoId }, update: { $set: doc }, upsert: true } });
        }

        if (targetList && newPendingChannels.size > 0) {
            await db.collection('lists').updateOne({ _id: targetList }, { $addToSet: { items: { $each: [...newPendingChannels] } } }, { upsert: true });
        } else if (bulkOps.length > 0) {
            await db.collection('videos').bulkWrite(bulkOps);
        }
        return { validVideoIds };
    },

    async cleanupExpiredVideos(db) {
        const last = await db.collection('metadata').findOne({ _id: 'last_cleanup' });
        if (last && (Date.now() - last.timestamp < 86400000)) return; // Run once daily
        await db.collection('metadata').updateOne({ _id: 'last_cleanup' }, { $set: { timestamp: Date.now() } }, { upsert: true });

        // CN: 30 Days
        const thirtyDaysAgo = new Date(); thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        const resCN = await db.collection('videos').deleteMany({ source: 'main', publishedAt: { $lt: thirtyDaysAgo } });

        // JP: 90 Days
        const ninetyDaysAgo = new Date(); ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
        const resJP = await db.collection('videos').deleteMany({ source: 'foreign', publishedAt: { $lt: ninetyDaysAgo } });

        console.log(`[Cleanup] Deleted ${resCN.deletedCount} CN videos and ${resJP.deletedCount} JP videos.`);
    },

    async updateAndStoreYouTubeData(db) {
        // CN Strategy: 20 mins. Keyword + Whitelist. New channels -> Auto Whitelist.
        console.log('[Mongo] CN Update...');
        const retentionDate = new Date(); retentionDate.setDate(retentionDate.getDate() - 30); // 30 days retention scan
        const [wlCn, blCn, wlJp] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }), db.collection('lists').findOne({ _id: 'blacklist_cn' }), db.collection('lists').findOne({ _id: 'whitelist_jp' })
        ]);
        const whitelist = wlCn?.items || [];
        const blacklist = blCn?.items || [];
        const jpWhitelist = wlJp?.items || [];
        const newVideoCandidates = new Set();

        // 1. Whitelist Scan
        if (whitelist.length > 0) {
            for (const batch of batchArray(whitelist, 50)) {
                try {
                    const res = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
                    const uploads = res.items?.map(i => i.contentDetails?.relatedPlaylists?.uploads).filter(Boolean) || [];
                    const pResults = await Promise.all(uploads.map(pid => fetchYouTube('playlistItems', { part: 'snippet', playlistId: pid, maxResults: 10 })));
                    pResults.forEach(r => r.items?.forEach(i => { if (new Date(i.snippet.publishedAt) > retentionDate) newVideoCandidates.add(i.snippet.resourceId.videoId); }));
                } catch { }
            }
        }

        // 2. Keyword Search
        const sResults = await Promise.all(SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter: retentionDate.toISOString() })));
        const searchResultIds = new Set();
        sResults.forEach(r => r.items?.forEach(i => { if (!blacklist.includes(i.snippet.channelId)) { newVideoCandidates.add(i.id.videoId); searchResultIds.add(i.id.videoId); } }));

        // 3. Auto-add new channels from search results
        if (searchResultIds.size > 0) {
            const newChannels = new Set();
            for (const batch of batchArray([...searchResultIds], 50)) {
                const res = await fetchYouTube('videos', { part: 'snippet', id: batch.join(',') });
                res.items?.forEach(v => {
                    const cid = v.snippet.channelId;
                    // Strict Check: Only add channel if the video is fully valid (License + Member Name)
                    if (!whitelist.includes(cid) && !jpWhitelist.includes(cid) && isVideoValid(v, SPECIAL_KEYWORDS, true)) {
                        newChannels.add(cid);
                    }
                });
            }
            if (newChannels.size > 0) await db.collection('lists').updateOne({ _id: 'whitelist_cn' }, { $addToSet: { items: { $each: [...newChannels] } } }, { upsert: true });
        }

        // Store
        await this.processAndStoreVideos([...newVideoCandidates], db, 'main', { retentionDate, blacklist });
        await this.cleanupExpiredVideos(db);
        await this.verifyActiveVideos(db, 'main');
    },

    async updateForeignClips(db) {
        // JP Strategy: 20 mins Whitelist Only. 
        console.log('[Mongo] JP Whitelist Update...');
        const retentionDate = new Date(); retentionDate.setDate(retentionDate.getDate() - 90); // 90 days
        const [wlJp, blJp] = await Promise.all([db.collection('lists').findOne({ _id: 'whitelist_jp' }), db.collection('lists').findOne({ _id: 'blacklist_jp' })]);
        const whitelist = wlJp?.items || [];
        if (whitelist.length === 0) return;

        const newVideoCandidates = new Set();
        for (const batch of batchArray(whitelist, 50)) {
            const res = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
            const uploads = res.items?.map(i => i.contentDetails?.relatedPlaylists?.uploads).filter(Boolean) || [];

            // Fix: Batch playlist fetching to avoid 429 Too Many Requests
            for (const uploadBatch of batchArray(uploads, 20)) {
                const pResults = await Promise.all(uploadBatch.map(pid => fetchYouTube('playlistItems', { part: 'snippet', playlistId: pid, maxResults: 10 })));
                pResults.forEach(r => r.items?.forEach(i => { if (new Date(i.snippet.publishedAt) > retentionDate) newVideoCandidates.add(i.snippet.resourceId.videoId); }));
                await new Promise(r => setTimeout(r, 200)); // Small delay between batches
            }
        }

        await this.processAndStoreVideos([...newVideoCandidates], db, 'foreign', { retentionDate, blacklist: blJp?.items || [] });
        await this.verifyActiveVideos(db, 'foreign');
    },

    async updateForeignClipsKeywords(db) {
        // JP Keyword Strategy: 60 mins. Search -> Pending List.
        console.log('[Mongo] JP Keyword Update...');
        const retentionDate = new Date(); retentionDate.setDate(retentionDate.getDate() - 7); // Search recent 7 days

        // Fetch ALL lists to ensure mutual exclusion
        const [wlCn, blCn, wlJp, blJp, pendJp] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }),
            db.collection('lists').findOne({ _id: 'blacklist_cn' }),
            db.collection('lists').findOne({ _id: 'whitelist_jp' }),
            db.collection('lists').findOne({ _id: 'blacklist_jp' }),
            db.collection('lists').findOne({ _id: 'pending_jp' })
        ]);

        const allExistingChannels = new Set([
            ...(wlCn?.items || []), ...(blCn?.items || []),
            ...(wlJp?.items || []), ...(blJp?.items || []),
            ...(pendJp?.items || [])
        ]);

        const sResults = await Promise.all(FOREIGN_SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter: retentionDate.toISOString() })));
        const videoIds = new Set();
        sResults.forEach(r => r.items?.forEach(i => {
            const cid = i.snippet.channelId;
            // Strict Exclusion: Check against ALL lists
            if (!allExistingChannels.has(cid)) {
                videoIds.add(i.id.videoId);
            }
        }));

        // Store channels to 'pending_jp' list, NOT videos to DB
        await this.processAndStoreVideos([...videoIds], db, 'foreign', { retentionDate, blacklist: [], targetList: 'pending_jp' });
    },

    async verifyActiveVideos(db, source) {
        console.log(`[Mongo] Verifying ${source} videos availability...`);
        const checkDate = new Date(); checkDate.setDate(checkDate.getDate() - 7); // Check recent 7 days
        const videos = await db.collection('videos').find(
            { source, publishedAt: { $gt: checkDate } },
            { projection: { _id: 1 } }
        ).toArray();

        if (videos.length === 0) return;
        console.log(`[Verify] Checking ${videos.length} videos...`);

        const ids = videos.map(v => v._id);
        const validIds = new Set();

        for (const batch of batchArray(ids, 50)) {
            try {
                // If video is deleted/private, won't appear in items
                const res = await fetchYouTube('videos', { part: 'id', id: batch.join(',') });
                res.items?.forEach(i => validIds.add(i.id));
            } catch (e) { console.error('Verify Fetch Error:', e); }
        }

        const deletedIds = ids.filter(id => !validIds.has(id));

        if (deletedIds.length > 0) {
            console.log(`[Verify] Found ${deletedIds.length} invalid/deleted videos. Removing...`);
            await db.collection('videos').deleteMany({ _id: { $in: deletedIds } });
        } else {
            console.log(`[Verify] All ${ids.length} recent videos are valid.`);
        }
    },

    async updateLiveStatus(db) {
        console.log('[Mongo] Updating Live Status...');
        const members = VSPO_MEMBERS;
        const liveStreams = [];

        // 2. Twitch Live Check
        console.log('[Debug] Starting Twitch Live Check...');
        const twitchIds = members.map(m => m.twitchId).filter(Boolean);
        // Twitch allows up to 100 IDs
        try {
            const streams = await fetchTwitchStreams(twitchIds);
            console.log(`[Debug] Twitch Check Done. Found ${streams.length} streams.`);
            for (const s of streams) {
                if (s.type === 'live') {
                    const member = members.find(m => m.twitchId === s.user_id);
                    if (member) {
                        // Get Avatar from DB (Best effort)
                        const dbChannel = await db.collection('channels').findOne({ _id: member.ytId });
                        const avatarUrl = dbChannel?.thumbnail || '';

                        liveStreams.push({
                            memberName: member.name,
                            platform: 'twitch',
                            channelId: s.user_login, // Use login handle
                            avatarUrl,
                            title: s.title,
                            url: `https://www.twitch.tv/${s.user_login}` // Correct URL using login
                        });
                    }
                }
            }
        } catch (e) { console.error('Twitch Check Error:', e); }

        // --- NEW STRATEGY: RSS + Video Check (Bypass Channels API bug) ---
        console.log('[Debug] Starting RSS Live Check...');
        // 1. Fetch RSS for all channels to get latest video ID
        // 2. Check video status via API

        const ytMembers = members.filter(m => m.ytId);
        console.log(`[Debug] Fetching RSS for ${ytMembers.length} channels...`);
        try {
            // 1. Fetch RSS in parallel
            const rssPromises = ytMembers.map(async m => {
                try {
                    const ctrl = new AbortController();
                    const timeout = setTimeout(() => ctrl.abort(), 3000); // 3s timeout
                    const rssRes = await fetch(`https://www.youtube.com/feeds/videos.xml?channel_id=${m.ytId}`, { signal: ctrl.signal });
                    clearTimeout(timeout);
                    if (!rssRes.ok) return null;
                    const text = await rssRes.text();
                    // Simple Regex Extract
                    const match = text.match(/<yt:videoId>(.*?)<\/yt:videoId>/);
                    return match ? { mid: m.ytId, vid: match[1] } : null;
                } catch (e) { return null; }
            });

            const results = await Promise.all(rssPromises);
            const candidates = results.filter(r => r);

            // --- SAFETY LOCK ---
            // If we have YouTube members but found 0 RSS candidates, it implies network failure or blocking.
            // In this case, we ABORT the update to prevent clearing the active stream list (Preserve old data).
            if (ytMembers.length > 0 && candidates.length === 0) {
                console.warn('[Warning] RSS Check returned 0 candidates. Potential network issue/blocking. Aborting update to preserve old data.');
                // We still want to save Twitch streams if they exist?
                // Complex decision. If we overwrite, we lose YT streams.
                // Better to skip the entire update if YT check fails catastrophically.
                return;
            }

            // 2. Batch Check Videos
            const videoIds = candidates.map(c => c.vid);
            // Deduplicate
            const uniqueVideoIds = [...new Set(videoIds)];

            if (uniqueVideoIds.length > 0) {
                for (const batch of batchArray(uniqueVideoIds, 50)) {
                    try {
                        console.log(`[Debug] Checking YT Video Status Batch: ${batch.length}`);
                        const vRes = await fetchYouTube('videos', { part: 'snippet,liveStreamingDetails', id: batch.join(',') });

                        // Populate Avatar Cache if we missed it (optional, but good for completeness)
                        // Actually we can't easily get channel avatar from videos endpoint part=snippet (it doesn't have avatar url)
                        // But we can assume DB has it or use placeholder.

                        vRes.items?.forEach(v => {
                            // Check if Live
                            const isLive = v.snippet.liveBroadcastContent === 'live';
                            // Also checking liveStreamingDetails to be sure it's not finished
                            const isActive = isLive || (v.snippet.liveBroadcastContent === 'upcoming' && false); // We only want LIVE

                            if (isLive) {
                                const member = ytMembers.find(m => m.ytId === v.snippet.channelId);
                                if (member) {
                                    // Get Avatar from DB 
                                    // We need to fetch from DB because 'videos' endpoint doesn't give channel avatar
                                    // We can try to fetch simple channel info if missing? 
                                    // For now let's hope it's in DB or use PLACEHOLDER

                                    liveStreams.push({
                                        memberName: member.name,
                                        platform: 'youtube',
                                        channelId: v.snippet.channelId,
                                        avatarUrl: '', // Will be filled by DB later or Frontend fallback
                                        title: v.snippet.title,
                                        url: `https://www.youtube.com/watch?v=${v.id}`
                                    });
                                }
                            }
                        });

                    } catch (e) { console.error('YT Video Check Error:', e); }
                }
            }

            // Fill Avatars from DB
            for (const stream of liveStreams) {
                if (!stream.avatarUrl && stream.platform === 'youtube') {
                    const dbCh = await db.collection('channels').findOne({ _id: stream.channelId });
                    if (dbCh) stream.avatarUrl = dbCh.thumbnail;
                }
            }

        } catch (e) { console.error('RSS Check Error:', e); }


        // Store result
        await db.collection('metadata').updateOne({ _id: 'live_status' }, { $set: { streams: liveStreams, timestamp: Date.now() } }, { upsert: true });
        console.log(`[Mongo] Live Status Updated: ${liveStreams.length} active streams.`);
    }
};

// --- Admin Action Handler ---
async function handleAdminAction(req, res, db, body) {
    const authenticate = () => {
        const pass = req.headers.authorization?.split(' ')[1] || new URL(req.url, `http://${req.headers.host}`).searchParams.get('password') || req.body?.password;
        if (process.env.ADMIN_PASSWORD && pass === process.env.ADMIN_PASSWORD) return true;
        return false;
    };

    if (!authenticate()) return res.status(401).json({ error: 'Unauthorized' });

    const { action, channelId, listType, videoId, content, type, active } = body;

    switch (action) {
        case 'add':
            if (!channelId || !listType) return res.status(400).json({ error: 'Missing params' });
            const map = { cn: 'whitelist_cn', jp: 'whitelist_jp', blacklist_cn: 'blacklist_cn', blacklist_jp: 'blacklist_jp', pending_jp: 'pending_jp' };

            // Mutual Exclusion: Check other lists
            const allLists = ['whitelist_cn', 'blacklist_cn', 'whitelist_jp', 'blacklist_jp', 'pending_jp'];
            const targetListId = map[listType];
            const otherLists = allLists.filter(l => l !== targetListId);
            const otherDocs = await db.collection('lists').find({ _id: { $in: otherLists } }).toArray();
            const otherItems = new Set();
            otherDocs.forEach(doc => doc.items?.forEach(i => otherItems.add(i)));

            if (otherItems.has(channelId)) return res.status(409).json({ error: 'Channel exists in another list.' });

            if (targetListId) await db.collection('lists').updateOne({ _id: targetListId }, { $addToSet: { items: channelId } }, { upsert: true });

            // Persistent Storage: Fetch & Store Channel Info
            try {
                const resYt = await fetchYouTube('channels', { part: 'snippet', id: channelId });
                if (resYt.items?.[0]) {
                    const snip = resYt.items[0].snippet;
                    await db.collection('channels').updateOne(
                        { _id: channelId },
                        { $set: { title: snip.title, thumbnail: snip.thumbnails.default?.url || '' } },
                        { upsert: true }
                    );
                }
            } catch (e) { console.error('Failed to fetch channel info:', e); }

            await logAdminAction(db, 'add', { listType, channelId });
            return res.json({ success: true });

        case 'approve_jp':
            if (!channelId) return res.status(400).json({ error: 'Missing channelId' });
            // Move from pending_jp -> whitelist_jp
            await db.collection('lists').updateOne({ _id: 'pending_jp' }, { $pull: { items: channelId } });
            await db.collection('lists').updateOne({ _id: 'whitelist_jp' }, { $addToSet: { items: channelId } }, { upsert: true });

            // Store Channel Info
            try {
                const resYt = await fetchYouTube('channels', { part: 'snippet', id: channelId });
                if (resYt.items?.[0]) {
                    const snip = resYt.items[0].snippet;
                    await db.collection('channels').updateOne(
                        { _id: channelId },
                        { $set: { title: snip.title, thumbnail: snip.thumbnails.default?.url || '' } },
                        { upsert: true }
                    );
                }
            } catch (e) { console.error('Failed to fetch channel info:', e); }

            await logAdminAction(db, 'approve_jp', { channelId });
            return res.json({ success: true });

        case 'reject_jp':
            if (!channelId) return res.status(400).json({ error: 'Missing channelId' });
            // Move from pending_jp -> blacklist_jp
            await db.collection('lists').updateOne({ _id: 'pending_jp' }, { $pull: { items: channelId } });
            await db.collection('lists').updateOne({ _id: 'blacklist_jp' }, { $addToSet: { items: channelId } }, { upsert: true });
            await logAdminAction(db, 'reject_jp', { channelId });
            return res.json({ success: true });

        case 'trigger_live_check':
            console.log('[Admin] Manually triggering Live Status Check...');
            await v12_logic.updateLiveStatus(db);
            return res.json({ success: true });

        case 'delete':
            if (!channelId || !listType) return res.status(400).json({ error: 'Missing params' });
            const mapDel = { cn: 'whitelist_cn', jp: 'whitelist_jp', blacklist_cn: 'blacklist_cn', blacklist_jp: 'blacklist_jp', pending_jp: 'pending_jp' };
            if (mapDel[listType]) await db.collection('lists').updateOne({ _id: mapDel[listType] }, { $pull: { items: channelId } });

            // Remove from persistent storage
            await db.collection('channels').deleteOne({ _id: channelId });

            await logAdminAction(db, 'delete', { listType, channelId });
            return res.json({ success: true });

        case 'update_announcement':
            await db.collection('metadata').updateOne({ _id: 'announcement' }, { $set: { content, type, active: String(active), timestamp: Date.now() } }, { upsert: true });
            await logAdminAction(db, 'update_announcement', { active, type });
            return res.json({ success: true });

        case 'add_video_blacklist':
            await db.collection('lists').updateOne({ _id: 'video_blacklist' }, { $addToSet: { items: videoId } }, { upsert: true });
            await db.collection('videos').deleteOne({ _id: videoId });
            await logAdminAction(db, 'add_video_blacklist', { videoId });
            return res.json({ success: true });

        case 'remove_video_blacklist':
            await db.collection('lists').updateOne({ _id: 'video_blacklist' }, { $pull: { items: videoId } });
            await logAdminAction(db, 'remove_video_blacklist', { videoId });
            return res.json({ success: true });

        case 'get_video_blacklist':
            const doc = await db.collection('lists').findOne({ _id: 'video_blacklist' });
            return res.json({ success: true, blacklist: doc?.items || [] });

        case 'get_admin_logs':
            const logs = await db.collection('admin_logs').find().sort({ timestamp: -1 }).limit(100).toArray();
            return res.status(200).json({ success: true, logs });

        // Mock Backfill
        case 'backfill':
            return res.json({ success: true, message: 'Backfill triggered (Mock)' });

        default: return res.status(400).json({ error: 'Invalid action' });
    }
}

// --- Handler ---
export default async function handler(req, res) {
    if (req.method === 'OPTIONS') return res.status(200).end();

    const db = await getDb();
    const url = new URL(req.url, `http://${req.headers.host}`);
    const { pathname, searchParams } = url;
    let body = {};
    if (req.method === 'POST') {
        try { body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body; } catch { }
    }

    if (pathname === '/api/leaderboard') {
        const d = new Date(); d.setDate(d.getDate() - 30);
        const wl = await db.collection('lists').findOne({ _id: 'whitelist_cn' });
        const allowedChannels = wl?.items || [];

        const lb = await db.collection('videos').aggregate([
            { $match: { channelId: { $in: allowedChannels }, publishedAt: { $gte: d } } }, // Strict: Only Whitelist CN items
            { $group: { _id: "$channelId", channelTitle: { $first: "$channelTitle" }, channelAvatarUrl: { $first: "$channelAvatarUrl" }, totalMinutes: { $sum: "$durationMinutes" }, videoCount: { $sum: 1 } } },
            { $sort: { totalMinutes: -1 } }, { $limit: 20 }
        ]).toArray();
        return res.status(200).json(lb.map(i => ({ channelId: i._id, ...i })));
    }

    if (pathname === '/api/youtube') {
        if (req.method === 'POST' && body.action) {
            return await handleAdminAction(req, res, db, body);
        }

        const lang = searchParams.get('lang') || 'cn';
        const isForeign = lang === 'jp';
        const forceRefresh = searchParams.get('force_refresh') === 'true';
        const noIncrement = searchParams.get('no_increment') === 'true';

        const visits = noIncrement ? await getVisitorCount(db) : await incrementAndGetVisitorCount(db);
        const metaId = isForeign ? 'last_update_jp' : 'last_update_cn';
        let didUpdate = false;

        const authenticate = () => {
            const pass = req.headers.authorization?.split(' ')[1] || searchParams.get('password');
            return process.env.ADMIN_PASSWORD && pass === process.env.ADMIN_PASSWORD;
        };

        if (forceRefresh) {
            if (!authenticate()) return res.status(401).json({ error: 'Unauthorized' });
            if (isForeign) {
                await v12_logic.updateForeignClips(db);
                await v12_logic.updateForeignClipsKeywords(db); // Force refresh does both
            }
            else {
                await v12_logic.updateAndStoreYouTubeData(db);
                await v12_logic.updateLiveStatus(db); // Force refresh also updates live status
            }
            await db.collection('metadata').updateOne({ _id: metaId }, { $set: { timestamp: Date.now() } }, { upsert: true });
            didUpdate = true;
        } else {
            const meta = await db.collection('metadata').findOne({ _id: metaId });
            // Standard Interval check (20 mins)
            if (Date.now() - (meta?.timestamp || 0) > (isForeign ? FOREIGN_UPDATE_INTERVAL_SECONDS : UPDATE_INTERVAL_SECONDS) * 1000) {
                const lockId = metaId + '_lock';
                const lock = await db.collection('metadata').findOne({ _id: lockId });
                if (!lock || Date.now() - lock.timestamp > 600000) {
                    await db.collection('metadata').updateOne({ _id: lockId }, { $set: { timestamp: Date.now() } }, { upsert: true });
                    (async () => {
                        try {
                            if (isForeign) {
                                await v12_logic.updateForeignClips(db);
                                // Check if we need to run Keyword Search (60 mins)
                                const kwMeta = await db.collection('metadata').findOne({ _id: 'last_jp_keyword_search' });
                                if (!kwMeta || Date.now() - kwMeta.timestamp > FOREIGN_SEARCH_INTERVAL_SECONDS * 1000) {
                                    await v12_logic.updateForeignClipsKeywords(db);
                                    await db.collection('metadata').updateOne({ _id: 'last_jp_keyword_search' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                                }
                            } else {
                                await v12_logic.updateAndStoreYouTubeData(db);
                            }
                            await db.collection('metadata').updateOne({ _id: metaId }, { $set: { timestamp: Date.now() } }, { upsert: true });
                        } catch (e) {
                            console.error('BG Update:', e);
                        } finally {
                            await db.collection('metadata').updateOne({ _id: lockId }, { $set: { timestamp: 0 } });
                        }
                    })();
                    didUpdate = true;
                }
            }

            // Independent Live Status Check (5 mins)
            const liveMeta = await db.collection('metadata').findOne({ _id: 'last_live_check' });
            if (Date.now() - (liveMeta?.timestamp || 0) > LIVE_UPDATE_INTERVAL_SECONDS * 1000) {
                const liveLock = await db.collection('metadata').findOne({ _id: 'live_check_lock' });
                if (!liveLock || Date.now() - liveLock.timestamp > 300000) {
                    await db.collection('metadata').updateOne({ _id: 'live_check_lock' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                    (async () => {
                        try {
                            await v12_logic.updateLiveStatus(db);
                            await db.collection('metadata').updateOne({ _id: 'last_live_check' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                        } catch (e) { console.error('BG Live Update:', e); }
                        finally { await db.collection('metadata').updateOne({ _id: 'live_check_lock' }, { $set: { timestamp: 0 } }); }
                    })();
                }
            }
        }


        // Fix: Use 'source' to query, so 'type' can be 'video'/'short'
        const blacklistDoc = await db.collection('lists').findOne({ _id: isForeign ? 'blacklist_jp' : 'blacklist_cn' });
        const blacklist = blacklistDoc?.items || [];

        const query = isForeign ? { source: 'foreign' } : { source: 'main' };
        if (blacklist.length > 0) {
            query.channelId = { $nin: blacklist };
        }

        // Limit 1000 for CN (as requested), 7000 for JP (to cover 90 days)
        // Project to exclude large fields (description) to stay within Vercel payload limits
        const rawVideos = await db.collection('videos')
            .find(query)
            .project({ description: 0, tags: 0 })
            .sort({ publishedAt: -1 })
            .limit(isForeign ? 7000 : 1000)
            .toArray();
        const videos = rawVideos.map(v => ({
            ...v,
            videoType: v.type, // Map 'type' to 'videoType' for frontend
            channelTitle: v.channelTitle || '',
            channelAvatarUrl: v.channelAvatarUrl || ''
        }));

        const ann = await db.collection('metadata').findOne({ _id: 'announcement' });
        const updateMeta = await db.collection('metadata').findOne({ _id: metaId });

        return res.status(200).json({
            videos,
            totalVisits: visits.totalVisits || 0,
            todayVisits: visits.todayVisits || 0,
            timestamp: updateMeta?.timestamp || Date.now(), // Use DB timestamp
            announcement: ann ? { content: ann.content, type: ann.type, active: ann.active === "true" } : null,
            meta: { didUpdate, timestamp: Date.now() }
        });
    }

    if (pathname === '/api/admin/logs' || pathname === '/api/youtube/get_admin_logs') {
        const logs = await db.collection('admin_logs').find().sort({ timestamp: -1 }).limit(100).toArray();
        return res.status(200).json({ success: true, logs });
    }

    if (pathname === '/api/admin/lists') {
        const password = searchParams.get('password');
        if (password !== process.env.ADMIN_PASSWORD) return res.status(401).json({ error: 'Unauthorized' });

        const [wl_cn, wl_jp, bl_cn, bl_jp, p_jp, ann] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }),
            db.collection('lists').findOne({ _id: 'whitelist_jp' }),
            db.collection('lists').findOne({ _id: 'blacklist_cn' }),
            db.collection('lists').findOne({ _id: 'blacklist_jp' }),
            db.collection('lists').findOne({ _id: 'pending_jp' }),
            db.collection('metadata').findOne({ _id: 'announcement' })
        ]);

        const hydrate = async (ids) => {
            if (!ids || ids.length === 0) return [];

            // 1. Fetch from persistent 'channels' collection first
            const storedChannels = await db.collection('channels').find({ _id: { $in: ids } }).toArray();
            const storedMap = new Map(storedChannels.map(c => [c._id, c]));

            // 2. Identify missing IDs to fallback to 'videos' aggregation (Legacy)
            const missingIds = ids.filter(id => !storedMap.has(id));
            let fallbackMap = new Map();

            if (missingIds.length > 0) {
                const pipeline = [
                    { $match: { channelId: { $in: missingIds } } },
                    { $sort: { publishedAt: -1 } },
                    { $group: { _id: "$channelId", name: { $first: "$channelTitle" }, avatar: { $first: "$channelAvatarUrl" } } }
                ];
                const infos = await db.collection('videos').aggregate(pipeline).toArray();
                fallbackMap = new Map(infos.map(i => [i._id, i]));
            }

            const defaultAvatar = 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxNTAiIGhlaWdodD0iMTUwIj48cmVjdCB3aWR0aD0iMTUwIiBoZWlnaHQ9IjE1MCIgZmlsbD0iIzMzMyIvPjwvc3ZnPg==';

            return ids.map(id => {
                const stored = storedMap.get(id);
                const fallback = fallbackMap.get(id);
                return {
                    id,
                    name: stored?.title || fallback?.name || 'Unknown Channel',
                    avatar: stored?.thumbnail || fallback?.avatar || defaultAvatar
                };
            });
        };

        return res.status(200).json({
            whitelist_cn: await hydrate(wl_cn?.items),
            whitelist_jp: await hydrate(wl_jp?.items),
            blacklist_cn: await hydrate(bl_cn?.items),
            blacklist_jp: await hydrate(bl_jp?.items),
            pending_jp: await hydrate(p_jp?.items),
            announcement: ann ? { content: ann.content, type: ann.type, active: ann.active === "true" } : null
        });
    }

    // 6. Get Related Clips
    if (pathname === '/api/get-related-clips') {
        const id = searchParams.get('id');
        if (!id) return res.status(400).json({ error: 'Missing id' });

        // Cache control to prevent stale data
        res.setHeader('Cache-Control', 'no-store, max-age=0');

        // Find the original video to get its stream info
        const originalVideo = await db.collection('videos').findOne({ _id: id });
        if (!originalVideo) return res.status(404).json({ error: 'Video not found' });

        const osi = originalVideo.originalStreamInfo;

        let query = {};
        if (osi && osi.id) {
            // Match by original stream ID
            query = { "originalStreamInfo.id": osi.id, _id: { $ne: id } };
        } else {
            return res.json({ videos: [] });
        }

        const related = await db.collection('videos').find(query).sort({ publishedAt: -1 }).limit(20).toArray();

        return res.status(200).json({
            videos: related.map(v => ({
                id: v._id,
                title: v.title,
                thumbnail: v.thumbnail,
                channelTitle: v.channelTitle || '',
                channelAvatarUrl: v.channelAvatarUrl || '',
                viewCount: v.viewCount || 0,
                duration: v.duration || '',
                publishedAt: v.publishedAt,
                videoType: v.type, // Ensure videoType is returned
                source: v.source
            }))
        });
    }

    // 6. Admin Manage Route
    if (pathname === '/api/admin/manage') {
        if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });
        return await handleAdminAction(req, res, db, body);
    }

    // 7. Check Status
    if (pathname === '/api/check-status') {
        return res.status(200).json({ isDone: true });
    }

    // 8. Classify Videos
    if (pathname === '/api/classify-videos') {
        return res.status(200).json({ message: 'Auto-handled' });
    }

    // 9. Legacy Video Blacklist
    if (pathname === '/api/youtube/get_video_blacklist') {
        const doc = await db.collection('lists').findOne({ _id: 'video_blacklist' });
        return res.status(200).json({ success: true, blacklist: doc?.items || [] });
    }

    // 10. Get Live Status
    if (pathname === '/api/live') {
        const doc = await db.collection('metadata').findOne({ _id: 'live_status' });
        return res.status(200).json({ success: true, streams: doc?.streams || [] });
    }

    return res.status(404).json({ error: 'Not Found', path: pathname });
}