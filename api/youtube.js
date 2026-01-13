
import { MongoClient } from 'mongodb';
import crypto from 'crypto';

// --- Configuration ---
const SCRIPT_VERSION = '17.4-FIXED';
const UPDATE_INTERVAL_SECONDS = 1200; // CN: 20 mins
const FOREIGN_UPDATE_INTERVAL_SECONDS = 1200; // JP Whitelist: 20 mins
const FOREIGN_SEARCH_INTERVAL_SECONDS = 3600; // JP Keywords: 60 mins
const MONGODB_URI = process.env.MONGODB_URL;
if (!MONGODB_URI) {
    throw new Error('Please define the MONGODB_URL environment variable inside .env');
}
const DB_NAME = 'vspoproxy';

// --- Constants ---
const SPECIAL_KEYWORDS = ["ぶいすぽっ！許諾番号"];
const FOREIGN_SEARCH_KEYWORDS = ["ぶいすぽ 切り抜き"];
// Removed FOREIGN_SPECIAL_KEYWORDS as requested, using SPECIAL_KEYWORDS universally
const SEARCH_KEYWORDS = ["VSPO中文", "VSPO精華", "VSPO剪輯"];
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
    { name: "Eris Suzukami", ytId: "UCp_3ej2br9l9L1DSoHVDZGw", twitchId: "" },
    // CN Members (Bilibili Only) - Add customAvatarUrl to fix WAF blocking
    { name: "小針彩", ytId: "", twitchId: "", bilibiliId: "1972360561", customAvatarUrl: "https://i0.hdslb.com/bfs/face/ccee4b98198a72f5de3a8174f42431bdee357270.jpg" },
    { name: "白咲露理", ytId: "", twitchId: "", bilibiliId: "1842209652", customAvatarUrl: "https://i0.hdslb.com/bfs/face/99aa887c27725e4d1dcf2ea071f04d8b29f457d4.jpg" },
    { name: "帕妃", ytId: "", twitchId: "", bilibiliId: "1742801253", customAvatarUrl: "https://i2.hdslb.com/bfs/face/b9915ddaa2d7f1b4279d516d77207bef9cc31856.jpg" },
    { name: "千郁郁", ytId: "", twitchId: "", bilibiliId: "1996441034", customAvatarUrl: "https://i1.hdslb.com/bfs/face/f9784adb001568cdc8f73f3435c0d5658af98c28.jpg" },
    { name: "日向晴", ytId: "", twitchId: "", bilibiliId: "1833448662", customAvatarUrl: "https://i2.hdslb.com/bfs/face/39e4bb7ddf7330bcf11fd6c06f8428d8ad0f0f26.jpg" },
];


const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9, process.env.YOUTUBE_API_KEY_10,
    process.env.YOUTUBE_API_KEY_11, process.env.YOUTUBE_API_KEY_12,
    process.env.YOUTUBE_API_KEY_13,
].filter(key => key);

// Constants
const LIVE_UPDATE_INTERVAL_SECONDS = 300; // 5 mins for Live Status
const INACTIVE_THRESHOLD_CN_MS = 60 * 24 * 60 * 60 * 1000; // CN: 60 Days
const INACTIVE_THRESHOLD_JP_MS = 30 * 24 * 60 * 60 * 1000; // JP: 30 Days
const INACTIVE_SCAN_INTERVAL_CN_MS = 3600 * 1000; // 1 Hour
const INACTIVE_SCAN_INTERVAL_JP_MS = 86400 * 1000; // 24 Hours
const STREAM_UPDATE_INTERVAL_SECONDS = 3600; // 1 Hour

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

// --- Quota Tracker ---
const getQuotaCost = (endpoint) => {
    if (endpoint === 'search') return 100;
    if (endpoint === 'videos' || endpoint === 'channels' || endpoint === 'playlistItems' || endpoint === 'playlists') return 1;
    return 1; // Default conservative cost
};

const updateQuotaUsage = (db, keyIndex, cost, endpoint) => {
    if (!db || cost === 0) return;
    try {
        // PT Midnight (America/Los_Angeles)
        const ptDate = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'America/Los_Angeles',
            year: 'numeric', month: '2-digit', day: '2-digit'
        }).format(new Date());

        const field = `breakdown.key_${keyIndex}`;
        const update = {
            $inc: {
                totalUsed: cost,
                [`${field}.total`]: cost,
                [`${field}.details.${endpoint}`]: cost
            }
        };
        // Fire-and-forget update
        db.collection('quota_usage').updateOne(
            { _id: `quota_${ptDate}` },
            update,
            { upsert: true }
        ).catch(e => console.warn('[Quota] Update Failed:', e.message));
    } catch (e) {
        console.warn('[Quota] Logic Error:', e);
    }
};

const cleanupOldQuotaLogs = async (db) => {
    try {
        const ptFormatter = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'America/Los_Angeles',
            year: 'numeric', month: '2-digit', day: '2-digit'
        });

        const d = new Date();
        d.setDate(d.getDate() - 7);
        const cutoffStr = 'quota_' + ptFormatter.format(d);

        const result = await db.collection('quota_usage').deleteMany({
            _id: { $lt: cutoffStr, $regex: /^quota_/ }
        });

        if (result.deletedCount > 0) {
            console.log(`[Quota] Cleaned up ${result.deletedCount} old records. (Cutoff: ${cutoffStr})`);
        }
    } catch (e) {
        console.warn('[Quota] Cleanup Failed:', e);
    }
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

let currentKeyIndex = 0;
let isKeyIndexSynced = false; // Flag to ensure we load from DB once per cold start

const fetchYouTube = async (endpoint, params) => {
    // 1. Sync Index from DB on First Run
    if (!isKeyIndexSynced && cachedDb) {
        try {
            const doc = await cachedDb.collection('metadata').findOne({ _id: 'api_key_rotator' });
            if (doc && typeof doc.currentIndex === 'number') {
                currentKeyIndex = doc.currentIndex % apiKeys.length;
                console.log(`[API Key] Loaded persisted index: ${currentKeyIndex}`);
            }
        } catch (e) {
            console.warn('[API Key] Failed to load persisted index:', e);
        }
        isKeyIndexSynced = true;
    }

    const totalKeys = apiKeys.length;
    const startIndex = currentKeyIndex;
    let hasLoggedCost = false;

    // console.log(`[Debug] fetchYouTube called for ${endpoint}`);

    for (let i = 0; i < totalKeys; i++) {
        const index = (startIndex + i) % totalKeys;
        const apiKey = apiKeys[index];
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;

        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000); // 3s timeout
            const res = await fetch(url, { signal: controller.signal });
            clearTimeout(timeoutId);

            const data = await res.json();

            // --- Quota Tracking ---
            // Calculate status for flow control
            let isQuotaErr = false;
            if (data.error) {
                const msg = data.error.message.toLowerCase();
                const reason = data.error.reason;
                // Treat 'accessNotConfigured' (Project disabled/invalid) as a Quota Error to force rotation
                if (msg.includes('quota') || reason === 'quotaExceeded' ||
                    reason === 'accessNotConfigured' || msg.includes('has not been used')) {
                    isQuotaErr = true;
                }
            }

            // Only log cost ONCE per request, even if it retries multiple keys
            if (!hasLoggedCost && cachedDb) {
                if (!isQuotaErr) {
                    const cost = getQuotaCost(endpoint);
                    updateQuotaUsage(cachedDb, index, cost, endpoint);
                    hasLoggedCost = true; // Mark as logged so retries don't double count
                }
            }
            // ----------------------

            if (isQuotaErr) {
                console.warn(`[YouTube API] Key ${index} Quota Exceeded.`);

                // [Fix] Aggressive Rotation:
                const nextIndex = (index + 1) % totalKeys;

                console.warn(`[YouTube API] Rotating Global Key Index: ${currentKeyIndex} -> ${nextIndex}`);
                currentKeyIndex = nextIndex;

                // 2. Persist New Index to DB (Fire-and-forget)
                if (cachedDb) {
                    cachedDb.collection('metadata').updateOne(
                        { _id: 'api_key_rotator' },
                        { $set: { currentIndex: currentKeyIndex, lastUpdated: Date.now() } },
                        { upsert: true }
                    ).catch(dbErr => console.warn('[API Key] Failed to persist rotation:', dbErr));
                }

                continue;
            }
            if (data.error) throw new Error(data.error.message); // Will be caught below

            return data;
        } catch (e) {
            // Log error (optional) but continue to next key
            if (e.name === 'AbortError') console.warn(`[YouTube API] Key ${index} Request Timed Out`);
            else console.warn(`[YouTube API] Key ${index} Failed: ${e.message}`); // Inspect why Key 7 fails
        }
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

function parseAllStreamInfos(description) {
    if (!description) return [];
    const results = [];

    // YouTube Regex (Global)
    const ytRegex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/g;
    let valid;
    while ((valid = ytRegex.exec(description)) !== null) {
        results.push({ platform: 'youtube', id: valid[1] });
    }

    // Twitch Regex (Global) - now supports m.twitch.tv
    const twRegex = /(?:https?:\/\/)?(?:www\.|m\.)?twitch\.tv\/videos\/(\d+)/g;
    while ((valid = twRegex.exec(description)) !== null) {
        results.push({ platform: 'twitch', id: valid[1] });
    }

    // Deduplicate by ID
    return [...new Map(results.map(item => [item.id, item])).values()];
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
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5s timeout
        const res = await fetch(`https://id.twitch.tv/oauth2/token?client_id=${clientId}&client_secret=${clientSecret}&grant_type=client_credentials`, {
            method: 'POST',
            signal: controller.signal
        });
        clearTimeout(timeoutId);

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
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout
        const res = await fetch(`https://api.twitch.tv/helix/streams?${query}`, {
            headers: { 'Client-ID': process.env.TWITCH_CLIENT_ID, 'Authorization': `Bearer ${token}` },
            signal: controller.signal
        });
        clearTimeout(timeoutId);

        const data = await res.json();
        return data.data || [];
    } catch (e) { console.error('Twitch Stream Error:', e); return []; }
}
async function fetchTwitchArchives(userId) {
    const token = await getTwitchToken();
    if (!token) return [];
    try {
        // Fetch videos of type 'archive' (VODs)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout
        const res = await fetch(`https://api.twitch.tv/helix/videos?user_id=${userId}&type=archive&first=50`, {
            headers: { 'Client-ID': process.env.TWITCH_CLIENT_ID, 'Authorization': `Bearer ${token}` },
            signal: controller.signal
        });
        clearTimeout(timeoutId);

        if (!res.ok) {
            console.error(`Twitch Archive Error [${userId}]: ${res.status} ${res.statusText}`);
            return [];
        }

        const data = await res.json();
        return data.data || [];
    } catch (e) { console.error(`Twitch Archive Fetch Error User=${userId}:`, e); return []; }
}

const syncTwitchArchives = async (db) => {
    console.log('[Twitch] Starting Archive Sync...');
    const members = VSPO_MEMBERS.filter(m => m.twitchId);
    let totalUpserted = 0;
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    for (const member of members) {
        // console.log(`[Twitch] Checking ${member.name} (${member.twitchId})...`);
        let videos = await fetchTwitchArchives(member.twitchId);

        // Filter by retention policy
        videos = videos.filter(v => new Date(v.created_at) > threeMonthsAgo);

        if (videos.length === 0) continue;

        const operations = videos.map(video => {
            // Twitch thumbnails need size injection
            const thumbnailUrl = video.thumbnail_url
                ? video.thumbnail_url.replace('%{width}', '640').replace('%{height}', '360')
                : 'https://placehold.co/640x360?text=No+Thumbnail';

            const streamDoc = {
                _id: video.id, // Explicitly set ID to match frontend expectation
                streamId: video.id,
                platform: 'twitch',
                title: video.title,
                thumbnail: thumbnailUrl,
                startTime: new Date(video.created_at),
                status: 'completed',
                channelId: video.user_id,
                channelTitle: video.user_name,
                memberName: member.name,
                memberId: member.ytId, // Frontend uses memberId (YouTube ID) for filtering
                viewCount: video.view_count,
                duration: video.duration,
                url: video.url,
                updatedAt: new Date()
            };

            return {
                updateOne: {
                    filter: { streamId: video.id, platform: 'twitch' },
                    update: { $set: streamDoc },
                    upsert: true
                }
            };
        });

        if (operations.length > 0) {
            const result = await db.collection('streams').bulkWrite(operations);
            totalUpserted += (result.upsertedCount + result.modifiedCount);
        }

        // Rate limit kindness
        await new Promise(r => setTimeout(r, 200));
    }
    console.log(`[Twitch] Archive Sync Completed. Processed ${totalUpserted} streams.`);
    return totalUpserted;
};

// --- Bilibili via RSSHub (Bypasses Bilibili anti-scraping) ---
async function fetchBilibiliArchives(mid) {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout for RSSHub

        // RSSHub provides Bilibili user videos as JSON
        const url = `https://rsshub.app/bilibili/user/video/${mid}?format=json`;

        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Accept': 'application/json'
            },
            signal: controller.signal
        });
        clearTimeout(timeoutId);

        if (!res.ok) {
            console.error(`[Bilibili RSSHub] Error [${mid}]: ${res.status}`);
            return [];
        }

        const data = await res.json();

        // RSSHub returns items in data.items array
        if (!data.items || data.items.length === 0) {
            console.log(`[Bilibili RSSHub] No videos found for ${mid}`);
            return [];
        }

        // Transform RSSHub format to our expected format
        return data.items.map(item => {
            // Extract BVID from URL (https://www.bilibili.com/video/BV1xxxxx)
            const bvidMatch = item.url?.match(/video\/(BV[a-zA-Z0-9]+)/);
            const bvid = bvidMatch ? bvidMatch[1] : item.id;

            // Parse pubDate to Unix timestamp
            const pubDate = new Date(item.date_published || item.pubDate || Date.now());

            return {
                bvid: bvid,
                title: item.title || 'Untitled',
                pic: item.image || '',
                author: item.authors?.[0]?.name || '',
                created: Math.floor(pubDate.getTime() / 1000), // Convert to seconds
                play: 0, // RSSHub doesn't provide view count
                length: '' // RSSHub doesn't provide duration
            };
        });
    } catch (e) {
        console.error(`[Bilibili RSSHub] Fetch Error MID=${mid}:`, e.message || e);
        return [];
    }
}

const syncBilibiliArchives = async (db) => {
    console.log('[Bilibili] Starting Archive Sync...');
    const members = VSPO_MEMBERS.filter(m => m.bilibiliId);
    let totalUpserted = 0;
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    for (const member of members) {
        // console.log(`[Bilibili] Checking ${member.name} (${member.bilibiliId})...`);
        let videos = await fetchBilibiliArchivesWbi(member.bilibiliId);

        // Filter by retention (Bilibili timestamp is seconds)
        videos = videos.filter(v => new Date(v.created * 1000) > threeMonthsAgo);

        if (videos.length === 0) continue;

        const operations = videos.map(video => {
            const streamDoc = {
                _id: video.bvid, // Explicitly set ID
                streamId: video.bvid,
                platform: 'bilibili',
                title: video.title,
                thumbnail: video.pic ? video.pic.replace('http:', 'https:') : '',
                startTime: new Date(video.created * 1000),
                status: 'completed',
                channelId: member.bilibiliId,
                channelTitle: video.author,
                memberName: member.name,
                memberId: member.ytId, // Map to YouTube ID for frontend filtering
                viewCount: video.play,
                duration: video.length,
                url: `https://www.bilibili.com/video/${video.bvid}`,
                updatedAt: new Date()
            };

            return {
                updateOne: {
                    filter: { streamId: video.bvid, platform: 'bilibili' },
                    update: { $set: streamDoc },
                    upsert: true
                }
            };
        });

        if (operations.length > 0) {
            const result = await db.collection('streams').bulkWrite(operations);
            totalUpserted += (result.upsertedCount + result.modifiedCount);
        }
        await new Promise(r => setTimeout(r, 500)); // Be gentler with Bilibili
    }
    console.log(`[Bilibili] Archive Sync Completed. Processed ${totalUpserted} streams.`);
    return totalUpserted;
};

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

            // [NEW] Multi-Link Logic
            const osis = parseAllStreamInfos(description);
            if (osis.length > 0) {
                doc.originalStreamInfo = osis[0]; // Legacy: Keep first match as primary

                // Extract all candidate IDs
                const candidateIds = osis.map(o => o.id);

                // Lookup in DB
                // Optimized: We fetch ONLY valid stream IDs from our collection
                const validStreams = await db.collection('streams').find(
                    { _id: { $in: candidateIds } },
                    { projection: { _id: 1, title: 1 } }
                ).toArray();

                if (validStreams.length > 0) {
                    doc.relatedStreamIds = validStreams.map(s => s._id); // Store Array
                    doc.relatedStreamId = validStreams[0]._id; // Store Primary (First valid match)
                }
            }

            bulkOps.push({ updateOne: { filter: { _id: videoId }, update: { $set: doc }, upsert: true } });
        }

        if (targetList && newPendingChannels.size > 0) {
            await db.collection('lists').updateOne({ _id: targetList }, { $addToSet: { items: { $each: [...newPendingChannels] } } }, { upsert: true });
        } else if (bulkOps.length > 0) {
            await db.collection('videos').bulkWrite(bulkOps);

            // [Optimization] Update Channel Last Upload Date
            const channelLatestMap = new Map();
            for (const videoId of validVideoIds) {
                const detail = videoDetailsMap.get(videoId);
                if (!detail) continue;
                const cid = detail.snippet.channelId;
                const pAt = new Date(detail.snippet.publishedAt).getTime();
                // Store title/thumbnail for metadata
                const cTitle = detail.snippet.channelTitle;
                const cThumb = channelStatsMap.get(cid)?.snippet?.thumbnails?.default?.url || '';

                if (!channelLatestMap.has(cid) || pAt > channelLatestMap.get(cid).date) {
                    channelLatestMap.set(cid, { date: pAt, title: cTitle, thumb: cThumb });
                }
            }
            if (channelLatestMap.size > 0) {
                const channelOps = [];
                for (const [cid, data] of channelLatestMap) {
                    channelOps.push({
                        updateOne: {
                            filter: { _id: cid },
                            update: { $set: { title: data.title, thumbnail: data.thumb, last_upload_at: data.date } },
                            upsert: true
                        }
                    });
                }
                await db.collection('channels').bulkWrite(channelOps);
            }
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

        // [FIX] Sync Twitch & Bilibili archives FIRST, so linking logic can find them
        console.log('[Mongo] Syncing Twitch/Bilibili archives before clip processing...');
        try { await syncTwitchArchives(db); } catch (e) { console.error('Pre-sync Twitch Failed:', e); }
        try { await syncBilibiliArchives(db); } catch (e) { console.error('Pre-sync Bilibili Failed:', e); }


        // 1. Whitelist Scan
        if (whitelist.length > 0) {
            // [Optimization] Conditional Scan for Inactive Channels
            const metaScanCtx = await db.collection('metadata').findOne({ _id: 'last_inactive_scan_cn' });
            const lastScan = metaScanCtx?.timestamp || 0;
            const shouldScanInactive = Date.now() - lastScan > INACTIVE_SCAN_INTERVAL_CN_MS;

            let targetChannels = whitelist;
            if (shouldScanInactive) {
                console.log('[Mongo] Update Strategy: FULL SCAN (Inactive Included)');
                await db.collection('metadata').updateOne({ _id: 'last_inactive_scan_cn' }, { $set: { timestamp: Date.now() } }, { upsert: true });
            } else {
                console.log('[Mongo] Update Strategy: ACTIVE ONLY');
                const channelDocs = await db.collection('channels').find({ _id: { $in: whitelist } }).toArray();
                const threshold = Date.now() - INACTIVE_THRESHOLD_CN_MS;
                targetChannels = whitelist.filter(id => {
                    const doc = channelDocs.find(c => c._id === id);
                    if (!doc || !doc.last_upload_at) return true; // Treat unknown as active
                    return doc.last_upload_at > threshold;
                });
            }

            console.log(`[Mongo] Whitelist Scan: ${targetChannels.length} channels (Total: ${whitelist.length}).`);
            let batchCount = 0;
            for (const batch of batchArray(targetChannels, 50)) {
                batchCount++;
                console.log(`[Mongo] Processing Whitelist Batch ${batchCount}...`);
                try {
                    const res = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
                    const uploads = res.items?.map(i => i.contentDetails?.relatedPlaylists?.uploads).filter(Boolean) || [];

                    // Fix: Batch playlist fetching to avoid 429 Too Many Requests
                    for (const uploadBatch of batchArray(uploads, 20)) {
                        const pResults = await Promise.all(uploadBatch.map(pid => fetchYouTube('playlistItems', { part: 'snippet', playlistId: pid, maxResults: 10 })));
                        pResults.forEach(r => r.items?.forEach(i => { if (new Date(i.snippet.publishedAt) > retentionDate) newVideoCandidates.add(i.snippet.resourceId.videoId); }));
                        await new Promise(r => setTimeout(r, 200)); // Small delay between batches
                    }
                } catch (e) {
                    if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                        console.warn('[Mongo] CRITICAL: Quota Exhausted during Whitelist Scan. Aborting.');
                        break;
                    }
                }
            }
        }

        // 2. Keyword Search
        console.log('[Mongo] Starting Keyword Search...');
        const sResults = [];
        for (const q of SEARCH_KEYWORDS) {
            try {
                // console.log(`[MongoDebug] Searching: ${q}`); // Optional Verbose
                const res = await fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter: retentionDate.toISOString() });
                if (res) sResults.push(res);
            } catch (e) {
                console.error(`[Mongo] Keyword Search Failed (${q}):`, e);
                if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                    console.warn('[Mongo] CRITICAL: Quota Exhausted during Keyword Search. Aborting.');
                    break;
                }
            }
        }

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
        console.log(`[Mongo] Found ${newVideoCandidates.size} video candidates.`);

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

        // [Optimization] JP Active/Inactive Split
        const metaScanCtx = await db.collection('metadata').findOne({ _id: 'last_inactive_scan_jp' });
        const lastScan = metaScanCtx?.timestamp || 0;
        const shouldScanInactive = Date.now() - lastScan > INACTIVE_SCAN_INTERVAL_JP_MS;

        let targetChannels = whitelist;
        if (shouldScanInactive) {
            console.log('[Mongo] JP Update Strategy: FULL SCAN (Inactive Included)');
            await db.collection('metadata').updateOne({ _id: 'last_inactive_scan_jp' }, { $set: { timestamp: Date.now() } }, { upsert: true });
        } else {
            console.log('[Mongo] JP Update Strategy: ACTIVE ONLY');
            const channelDocs = await db.collection('channels').find({ _id: { $in: whitelist } }).toArray();
            const threshold = Date.now() - INACTIVE_THRESHOLD_JP_MS;
            targetChannels = whitelist.filter(id => {
                const doc = channelDocs.find(c => c._id === id);
                if (!doc || !doc.last_upload_at) return true;
                return doc.last_upload_at > threshold;
            });
        }

        console.log(`[Mongo] JP Whitelist: ${targetChannels.length} channels (Total: ${whitelist.length}).`);
        let batchCount = 0;
        for (const batch of batchArray(targetChannels, 50)) {
            batchCount++;
            console.log(`[Mongo] Processing JP Batch ${batchCount}...`);
            try {
                const res = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
                const uploads = res.items?.map(i => i.contentDetails?.relatedPlaylists?.uploads).filter(Boolean) || [];

                // Fix: Batch playlist fetching to avoid 429 Too Many Requests
                for (const uploadBatch of batchArray(uploads, 20)) {
                    const pResults = await Promise.all(uploadBatch.map(pid => fetchYouTube('playlistItems', { part: 'snippet', playlistId: pid, maxResults: 10 })));
                    pResults.forEach(r => r.items?.forEach(i => { if (new Date(i.snippet.publishedAt) > retentionDate) newVideoCandidates.add(i.snippet.resourceId.videoId); }));
                    await new Promise(r => setTimeout(r, 200)); // Small delay between batches
                }
            } catch (e) {
                console.error(`[JP Update] Batch ${batchCount} failed:`, e);
            }
        }
        console.log(`[Mongo] Found ${newVideoCandidates.size} JP video candidates.`);

        await this.processAndStoreVideos([...newVideoCandidates], db, 'foreign', { retentionDate, blacklist: blJp?.items || [] });
        await this.verifyActiveVideos(db, 'foreign');
    },

    async updateForeignClipsKeywords(db) {
        // JP Keyword Strategy: 60 mins. Search -> Pending List.
        console.log('[Mongo] JP Keyword Update...');
        try {
            const retentionDate = new Date(); retentionDate.setDate(retentionDate.getDate() - 7); // Search recent 7 days

            // Fetch ALL lists to ensure mutual exclusion
            console.log('[MongoDebug] Fetching Exclusion Lists...');
            const [wlCn, blCn, wlJp, blJp, pendJp] = await Promise.all([
                db.collection('lists').findOne({ _id: 'whitelist_cn' }),
                db.collection('lists').findOne({ _id: 'blacklist_cn' }),
                db.collection('lists').findOne({ _id: 'whitelist_jp' }),
                db.collection('lists').findOne({ _id: 'blacklist_jp' }),
                db.collection('lists').findOne({ _id: 'pending_jp' })
            ]);
            console.log('[MongoDebug] Exclusion Lists Fetched.');

            const allExistingChannels = new Set([
                ...(wlCn?.items || []), ...(blCn?.items || []),
                ...(wlJp?.items || []), ...(blJp?.items || []),
                ...(pendJp?.items || [])
            ]);

            console.log('[Mongo] Starting JP Keyword Search...');
            const sResults = await Promise.all(FOREIGN_SEARCH_KEYWORDS.map(q => fetchYouTube('search', { part: 'snippet', type: 'video', maxResults: 50, q, publishedAfter: retentionDate.toISOString() })));
            const videoIds = new Set();
            sResults.forEach(r => r.items?.forEach(i => {
                const cid = i.snippet.channelId;
                // Strict Exclusion: Check against ALL lists
                if (!allExistingChannels.has(cid)) {
                    videoIds.add(i.id.videoId);
                }
            }));

            console.log(`[Mongo] JP Keywords found ${videoIds.size} new videos.`);
            // Store channels to 'pending_jp' list, NOT videos to DB
            await this.processAndStoreVideos([...videoIds], db, 'foreign', { retentionDate, blacklist: [], targetList: 'pending_jp' });
            console.log('[Mongo] JP Keyword Update Completed.');
        } catch (e) {
            console.error('[JP Keyword Update] Failed:', e);
        }
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
                res.items?.forEach(i => validIds.add(i.id));
            } catch (e) {
                console.error('Verify Fetch Error:', e);
                if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                    console.warn('[Verify] CRITICAL: Quota Exhausted during Verification. Aborting to prevent data loss.');
                    return; // ABORT IMMEDIATELY, do not delete anything
                }
            }
        }

        const deletedIds = ids.filter(id => !validIds.has(id));

        // Safety: If validIds is unexpectedly small (e.g. < 50%) but no error was thrown, abort deletion just in case
        if (deletedIds.length > ids.length * 0.5) {
            console.warn(`[Verify] Abnormal deletion rate detected (${deletedIds.length}/${ids.length}). Aborting for safety.`);
            return;
        }

        if (deletedIds.length > 0) {
            console.log(`[Verify] Found ${deletedIds.length} invalid/deleted videos. Removing...`);
            await db.collection('videos').deleteMany({ _id: { $in: deletedIds } });
        } else {
            console.log(`[Verify] All ${ids.length} recent videos are valid.`);
        }
    },

    async updateMemberStreams(db) {
        console.log('[Mongo] Updating Member Streams (Forward Fetch)...');

        // 1. Check Interval
        const meta = await db.collection('metadata').findOne({ _id: 'last_stream_update' });
        const lastUpdate = meta?.timestamp || 0;
        if (Date.now() - lastUpdate < STREAM_UPDATE_INTERVAL_SECONDS * 1000) {
            console.log('[Mongo] Stream Update Skipped (Interval not met).');
            return;
        }

        // 2. Update Timestamp immediately to prevent concurrent runs
        await db.collection('metadata').updateOne(
            { _id: 'last_stream_update' },
            { $set: { timestamp: Date.now() } },
            { upsert: true }
        );

        let totalStreamsFound = 0;
        const members = VSPO_MEMBERS.filter(m => m.ytId); // Filter only members with YT

        for (const member of members) {
            try {
                // A. Get Uploads Playlist ID
                const chRes = await fetchYouTube('channels', { part: 'contentDetails', id: member.ytId });
                const uploadsPlaylistId = chRes.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;

                if (!uploadsPlaylistId) continue;

                // B. Fetch Recent Uploads (Top 50 is enough for 1 hour interval)
                // We fetch 50 to ensure we catch recent streams even if they upload many clips/Shorts
                const plRes = await fetchYouTube('playlistItems', {
                    part: 'snippet',
                    playlistId: uploadsPlaylistId,
                    maxResults: 50
                });

                const videosToCheck = plRes.items?.map(i => i.snippet.resourceId.videoId) || [];
                if (videosToCheck.length === 0) continue;

                // C. Check Video Details for Live Streaming Info
                const vRes = await fetchYouTube('videos', {
                    part: 'liveStreamingDetails,snippet,statistics',
                    id: videosToCheck.join(',')
                });

                const bulkOps = [];

                for (const item of vRes.items || []) {
                    // We only want ARCHIVED streams or currently active ones if useful
                    const live = item.liveStreamingDetails;
                    if (!live) continue; // Not a stream

                    // Check if it's a VOD (has actualStartTime)
                    if (!live.actualStartTime) continue;

                    const description = item.snippet.description || "";
                    const isClipProhibited = description.includes("切り抜き禁止");

                    const doc = {
                        _id: item.id,
                        streamId: item.id,
                        memberId: member.ytId,
                        memberName: member.name,
                        title: item.snippet.title,
                        startTime: new Date(live.actualStartTime),
                        endTime: live.actualEndTime ? new Date(live.actualEndTime) : null,
                        platform: 'youtube',
                        channelTitle: item.snippet.channelTitle,
                        thumbnail: item.snippet.thumbnails.maxres?.url || item.snippet.thumbnails.high?.url,
                        status: live.actualEndTime ? 'ended' : 'live', // Simple status
                        scheduledStartTime: live.scheduledStartTime ? new Date(live.scheduledStartTime) : null,
                        isClipProhibited // [NEW] Prohibited Flag
                    };

                    bulkOps.push({
                        updateOne: {
                            filter: { _id: item.id },
                            update: { $set: doc },
                            upsert: true
                        }
                    });
                }

                if (bulkOps.length > 0) {
                    await db.collection('streams').bulkWrite(bulkOps);
                    totalStreamsFound += bulkOps.length;
                }

            } catch (e) {
                console.error(`[Stream Update] Failed for ${member.name}:`, e.message);
            }
        }

        console.log(`[Mongo] Stream Update Completed. Processed ${totalStreamsFound} streams.`);
    },

    async updateLiveStatus(db) {
        console.log('[Mongo] Updating Live Status...');
        // Set Lock
        await db.collection('metadata').updateOne(
            { _id: 'live_status' },
            { $set: { isUpdating: true, updateStartedAt: Date.now() } },
            { upsert: true }
        );

        const members = VSPO_MEMBERS;
        const liveStreams = [];

        try {

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
                                url: `https://www.twitch.tv/${s.user_login}`, // Correct URL using login
                                url: `https://www.twitch.tv/${s.user_login}`, // Correct URL using login
                                thumbnailUrl: s.thumbnail_url ? s.thumbnail_url.replace('{width}', '640').replace('{height}', '360') : '',
                                status: 'live', // Twitch is always live if returned here
                                startTime: s.started_at
                            });
                        }
                    }
                }
            } catch (e) { console.error('Twitch Check Error:', e); }

            // --- Bilibili Live Check ---
            console.log('[Debug] Starting Bilibili Live Check...');
            const biliMembers = members.filter(m => m.bilibiliId);
            for (const member of biliMembers) {
                try {
                    console.log(`[Bilibili Debug] Checking ${member.name} (${member.bilibiliId})`);

                    // 1. Status Check (Using reliable legacy API)
                    const statusUrl = `https://api.live.bilibili.com/room/v1/Room/get_info?room_id=${member.bilibiliId}`;
                    const headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Referer': `https://live.bilibili.com/${member.bilibiliId}`
                    };

                    const res = await fetch(statusUrl, { headers });
                    if (res.ok) {
                        const json = await res.json();
                        console.log(`[Bilibili Debug] ${member.name}: Code ${json.code} Status ${json.data?.live_status}`);

                        if (json.code === 0 && json.data && json.data.live_status === 1) {
                            // Find fallback avatar
                            const ytMember = members.find(m => m.bilibiliId === member.bilibiliId);
                            // Best effort to find avatar from channels DB if custom is missing
                            let avatarUrl = VSPO_MEMBERS.find(m => m.bilibiliId === member.bilibiliId)?.customAvatarUrl || '';
                            if (!avatarUrl && ytMember?.ytId) {
                                const dbCh = await db.collection('channels').findOne({ _id: ytMember.ytId });
                                if (dbCh) avatarUrl = dbCh.thumbnail;
                            }

                            liveStreams.push({
                                memberName: member.name,
                                platform: 'bilibili',
                                channelId: member.bilibiliId,
                                avatarUrl,
                                title: json.data.title,
                                url: `https://live.bilibili.com/${member.bilibiliId}`,
                                title: json.data.title,
                                url: `https://live.bilibili.com/${member.bilibiliId}`,
                                thumbnailUrl: json.data.keyframe || json.data.user_cover || '',
                                status: 'live', // Bilibili check verifies live_status === 1
                                startTime: new Date().toISOString() // Fallback
                            });
                        }
                    }

                } catch (e) {
                    console.error(`[Bilibili Error] ${member.name}:`, e);
                }
            }

            // --- YouTube Live Check ---
            // Logic: Get "Uploads" playlist ID (UC -> UU) -> Get last 3 videos -> Check if Live.

            console.log('[Debug] Starting YouTube Live Check (Playlist + RSS)...');
            const ytMembers = members.filter(m => m.ytId);
            const playlistCandidates = new Set();

            // 1. Fetch PlaylistItems (Concurrent Batches)
            const playlistBatchSize = 10;
            const parseRssVideoIds = (text) => {
                const ids = [];
                const matches = text.matchAll(/<yt:videoId>(.*?)<\/yt:videoId>/g);
                for (const m of matches) {
                    ids.push(m[1]);
                    if (ids.length >= 3) break; // Only check top 3 from RSS
                }
                return ids;
            };

            for (const batch of batchArray(ytMembers, playlistBatchSize)) {
                try {
                    await Promise.all(batch.map(async (member) => {
                        // A: API Playlist 
                        const apiPromise = (async () => {
                            try {
                                const uploadsPlaylistId = member.ytId.replace('UC', 'UU');
                                const res = await fetchYouTube('playlistItems', {
                                    part: 'snippet',
                                    playlistId: uploadsPlaylistId,
                                    maxResults: 3
                                });
                                res.items?.forEach(item => {
                                    playlistCandidates.add(item.snippet.resourceId.videoId);
                                });
                            } catch (e) {
                                if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) throw e;
                            }
                        })();

                        // B: RSS Feed 
                        const rssPromise = (async () => {
                            try {
                                const controller = new AbortController();
                                const timeout = setTimeout(() => controller.abort(), 3000); // 3s Timeout
                                const rssRes = await fetch(`https://www.youtube.com/feeds/videos.xml?channel_id=${member.ytId}`, { signal: controller.signal });
                                clearTimeout(timeout);
                                if (rssRes.ok) {
                                    const text = await rssRes.text();
                                    const rssIds = parseRssVideoIds(text);
                                    rssIds.forEach(vid => playlistCandidates.add(vid));
                                    if (rssIds.length > 0) console.log(`[RSS] ${member.name} found: ${rssIds.join(',')}`);
                                }
                            } catch (e) {
                            }
                        })();

                        await Promise.all([apiPromise, rssPromise]);
                    }));
                } catch (e) {
                    if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                        console.warn('[Live Check] CRITICAL: Quota Exhausted during Playlist Fetch. Aborting.');
                        break;
                    }
                    console.error('[Live Check] Batch Error:', e);
                }
                // Small delay between batches
                await new Promise(r => setTimeout(r, 200));
            }

            console.log(`[Debug] Found ${playlistCandidates.size} recent candidates.`);

            // 2. Merge with Smart Retention Candidates
            const currentStatusDoc = await db.collection('metadata').findOne({ _id: 'live_status' });
            const currentActiveVideoIds = (currentStatusDoc?.streams || [])
                .filter(s => s.platform === 'youtube' && s.vid)
                .map(s => s.vid);

            currentActiveVideoIds.forEach(vid => playlistCandidates.add(vid));

            const totalCandidates = [...playlistCandidates];
            console.log(`[Debug] Verifying ${totalCandidates.length} videos...`);

            // 3. Batch Verify Video Status (Is it Live?)
            for (const batch of batchArray(totalCandidates, 50)) {
                try {
                    console.log(`[Debug] Checking YT Video Status Batch: ${batch.length}`);
                    const res = await fetchYouTube('videos', {
                        part: 'snippet,liveStreamingDetails',
                        id: batch.join(',')
                    });

                    res.items?.forEach(v => {
                        const isLive = v.snippet.liveBroadcastContent === 'live';
                        const isUpcoming = v.snippet.liveBroadcastContent === 'upcoming';
                        const hasStarted = v.liveStreamingDetails?.actualStartTime;
                        const hasEnded = v.liveStreamingDetails?.actualEndTime;
                        const scheduledTime = v.liveStreamingDetails?.scheduledStartTime;

                        // CRITICAL FIX: Strictly ignore any video that has ended.
                        if (hasEnded) return;

                        // Valid if Live, Upcoming, or Started (but not ended)
                        if (isLive || isUpcoming || hasStarted) {
                            const channelId = v.snippet.channelId;
                            const member = members.find(m => m.ytId === channelId);

                            if (member) {
                                // [Fix] Require valid Start Time. Do NOT fallback to 'now' which bypasses filters.
                                const finalStartTime = scheduledTime || v.liveStreamingDetails?.actualStartTime;

                                if (finalStartTime) {
                                    liveStreams.push({
                                        memberName: member.name,
                                        platform: 'youtube',
                                        channelId: v.snippet.channelId,
                                        vid: v.id,
                                        avatarUrl: '', // Will be filled below
                                        title: v.snippet.title,
                                        url: `https://www.youtube.com/watch?v=${v.id}`,
                                        thumbnailUrl: v.snippet.thumbnails?.standard?.url || v.snippet.thumbnails?.high?.url || v.snippet.thumbnails?.maxres?.url,
                                        status: isUpcoming ? 'upcoming' : 'live',
                                        startTime: finalStartTime
                                    });
                                }
                            }
                        }
                    });
                } catch (e) {
                    console.error('[Live Check] Video Details Batch Failed:', e);
                    if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                        console.warn('[Live Check] CRITICAL: Quota Exhausted. Aborting remaining batches.');
                        break;
                    }
                }
            }
            // Fill Avatars from DB
            for (const stream of liveStreams) {
                if (!stream.avatarUrl && stream.platform === 'youtube') {
                    const dbCh = await db.collection('channels').findOne({ _id: stream.channelId });
                    if (dbCh) stream.avatarUrl = dbCh.thumbnail;
                }
            }

            // [Optimization] Deduplicate: One Upcoming Stream per Member (Keep soonest)
            // If member is Live, remove Upcoming.
            const uniqueStreamsMap = new Map();

            // First pass: Prioritize LIVE
            for (const s of liveStreams) {
                if (s.status === 'live') {
                    uniqueStreamsMap.set(s.memberName + '_live_' + s.platform, s);
                }
            }


            const memberStreamMap = new Map();
            for (const s of liveStreams) {
                if (!memberStreamMap.has(s.memberName)) {
                    memberStreamMap.set(s.memberName, []);
                }
                memberStreamMap.get(s.memberName).push(s);
            }

            const activeAndSoonestStreams = [];
            for (const [name, streams] of memberStreamMap) {
                const liveOnes = streams.filter(s => s.status === 'live');
                if (liveOnes.length > 0) {
                    activeAndSoonestStreams.push(...liveOnes);
                } else {
                    // pick soonest upcoming
                    const upcomingOnes = streams.filter(s => s.status === 'upcoming');
                    if (upcomingOnes.length > 0) {
                        upcomingOnes.sort((a, b) => new Date(a.startTime).getTime() - new Date(b.startTime).getTime());

                        // [Fix] Only show upcoming streams within 24 hours (Future)
                        // AND ignore "Zombie" streams that are scheduled > 1 hour in the past but never started.
                        const soonest = upcomingOnes[0];
                        const now = Date.now();
                        const timeDiff = new Date(soonest.startTime).getTime() - now;
                        const futureLimit = 24 * 60 * 60 * 1000; // +24 Hours
                        const pastLimit = -3 * 60 * 60 * 1000; // -3 Hour (Allow 3h delay)

                        if (timeDiff <= futureLimit && timeDiff >= pastLimit) {
                            activeAndSoonestStreams.push(soonest);
                        }
                    }
                }
            }
            // Replace list
            liveStreams.length = 0;
            liveStreams.push(...activeAndSoonestStreams);
            const memberIndexMap = new Map(VSPO_MEMBERS.map((m, i) => [m.name, i]));
            const OFFICIAL_NAME = "ぶいすぽっ!【公式】";

            liveStreams.sort((a, b) => {
                // 1. Status: Live > Upcoming
                if (a.status === 'live' && b.status !== 'live') return -1;
                if (a.status !== 'live' && b.status === 'live') return 1;

                // 2. If both Upcoming, Sort by Time (Sooner first)
                if (a.status === 'upcoming' && b.status === 'upcoming') {
                    return new Date(a.startTime).getTime() - new Date(b.startTime).getTime();
                }

                // 3. Official > Members
                if (a.memberName === OFFICIAL_NAME) return -1;
                if (b.memberName === OFFICIAL_NAME) return 1;

                const idxA = memberIndexMap.get(a.memberName) ?? 999;
                const idxB = memberIndexMap.get(b.memberName) ?? 999;
                return idxA - idxB;
            });

            // Store result

            await db.collection('metadata').updateOne({ _id: 'live_status' }, {
                $set: {
                    streams: liveStreams, // Use the processed list
                    isUpdating: false,
                    timestamp: Date.now()
                }
            }, { upsert: true });

            console.log(`[Mongo] Live Status Updated: ${liveStreams.length} active streams.`);
        } catch (e) {
            console.error('[Live Status Update] Failed:', e);
            if (e.message && (e.message.includes('Quota Exceeded') || e.message.includes('quota'))) {
                console.warn('[Live Status] Quota Error caught at top level. Skipping DB Overwrite.');
            }
        } finally {
            await db.collection('metadata').updateOne({ _id: 'live_check_lock' }, { $set: { timestamp: 0 } });
            await db.collection('metadata').updateOne({ _id: 'live_status' }, { $set: { isUpdating: false } });
        }
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

        // Backfill: Re-link clips to Twitch/Bilibili archives
        case 'backfill_links':
            try {
                // 1. Sync archives first to ensure we have latest data
                console.log('[Backfill] Syncing archives...');
                await syncTwitchArchives(db);
                await syncBilibiliArchives(db);

                // 2. Get recent videos that might need re-linking
                const lookbackDays = parseInt(body.days) || 7;
                const cutoffDate = new Date();
                cutoffDate.setDate(cutoffDate.getDate() - lookbackDays);

                const videosToCheck = await db.collection('videos').find({
                    publishedAt: { $gte: cutoffDate },
                    $or: [
                        { relatedStreamIds: { $exists: false } },
                        { relatedStreamIds: { $size: 0 } },
                        { relatedStreamIds: null }
                    ]
                }).toArray();

                console.log(`[Backfill] Found ${videosToCheck.length} videos to check.`);

                let updatedCount = 0;
                for (const video of videosToCheck) {
                    // Parse URLs from searchableText (contains description)
                    const text = video.searchableText || '';
                    const osis = parseAllStreamInfos(text);

                    if (osis.length > 0) {
                        const candidateIds = osis.map(o => o.id);
                        const validStreams = await db.collection('streams').find(
                            { _id: { $in: candidateIds } },
                            { projection: { _id: 1 } }
                        ).toArray();

                        if (validStreams.length > 0) {
                            const streamIds = validStreams.map(s => s._id);
                            await db.collection('videos').updateOne(
                                { _id: video._id },
                                {
                                    $set: {
                                        relatedStreamIds: streamIds,
                                        relatedStreamId: streamIds[0]
                                    }
                                }
                            );
                            updatedCount++;
                        }
                    }
                }

                await logAdminAction(db, 'backfill_links', { lookbackDays, checked: videosToCheck.length, updated: updatedCount });
                return res.json({ success: true, message: `Backfill complete. Checked ${videosToCheck.length} videos, updated ${updatedCount} links.` });
            } catch (e) {
                console.error('[Backfill] Error:', e);
                return res.status(500).json({ error: e.message });
            }

        // Legacy Mock Backfill (Deprecated)
        case 'backfill':
            return res.json({ success: true, message: 'Use backfill_links action instead.' });

        default: return res.status(400).json({ error: 'Invalid action' });
    }
}

// --- Handler ---
export default async function handler(req, res) {
    if (req.method === 'OPTIONS') return res.status(200).end();

    const db = await getDb();
    const url = new URL(req.url, `http://${req.headers.host}`);
    const { pathname, searchParams } = url;

    // Helper for outcome (No-op now)
    const logOutcome = async (reason) => { };

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

    if (pathname === '/api/youtube' && searchParams.get('endpoint') !== 'streams') {
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
            return (process.env.ADMIN_PASSWORD && pass === process.env.ADMIN_PASSWORD) ||
                (process.env.CRON_SECRET && pass === process.env.CRON_SECRET);
        };

        if (forceRefresh) {
            if (!authenticate()) return res.status(401).json({ error: 'Unauthorized' });
            if (isForeign) {
                await v12_logic.updateForeignClips(db);
                await v12_logic.updateForeignClipsKeywords(db); // Force refresh does both
            }
            else {
                await v12_logic.updateAndStoreYouTubeData(db);
                await v12_logic.updateLiveStatus(db);
                await v12_logic.updateMemberStreams(db);
                // Also Sync Twitch & Bilibili Archives (Best effort)
                try { await syncTwitchArchives(db); } catch (e) { console.error('Twitch Sync Failed in Main Loop:', e); }
                try { await syncBilibiliArchives(db); } catch (e) { console.error('Bilibili Sync Failed in Main Loop:', e); }
            }
            await db.collection('metadata').updateOne({ _id: metaId }, { $set: { timestamp: Date.now() } }, { upsert: true });

            didUpdate = true;
            logOutcome(`force_refresh_triggered_${lang}`);
        } else {
            const meta = await db.collection('metadata').findOne({ _id: metaId });
            let bgUpdatePromise = Promise.resolve();
            let updateStarted = false;

            // Standard Interval check (20 mins)
            if (Date.now() - (meta?.timestamp || 0) > (isForeign ? FOREIGN_UPDATE_INTERVAL_SECONDS : UPDATE_INTERVAL_SECONDS) * 1000) {
                // Optimistic Locking: Use previous timestamp as version
                const lockId = metaId + '_lock';
                const lock = await db.collection('metadata').findOne({ _id: lockId });
                const lastLockTime = lock?.timestamp || 0;

                if (Date.now() - lastLockTime > 600000) {
                    // Try to acquire lock ATOMICALLY
                    const acquireResult = await db.collection('metadata').updateOne(
                        { _id: lockId, timestamp: lastLockTime },
                        { $set: { timestamp: Date.now() } },
                        { upsert: true }
                    );

                    if (acquireResult.modifiedCount === 1 || (acquireResult.upsertedCount === 1 && !lock)) {
                        updateStarted = true;
                        logOutcome(`triggered_update_${lang}`);
                        // Lock Acquired
                        bgUpdatePromise = (async () => {
                            try {
                                // Update timestamp immediately (Start-to-Start interval)
                                await db.collection('metadata').updateOne({ _id: metaId }, { $set: { timestamp: Date.now() } }, { upsert: true });

                                if (isForeign) {
                                    await v12_logic.updateForeignClips(db);
                                    // Check if we need to run Keyword Search (60 mins)
                                    const kwMeta = await db.collection('metadata').findOne({ _id: 'last_jp_keyword_search' });
                                    if (!kwMeta || Date.now() - kwMeta.timestamp > FOREIGN_SEARCH_INTERVAL_SECONDS * 1000) {
                                        // Update keyword timestamp immediately (Start-to-Start)
                                        await db.collection('metadata').updateOne({ _id: 'last_jp_keyword_search' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                                        await v12_logic.updateForeignClipsKeywords(db);
                                    }
                                } else {
                                    await v12_logic.updateAndStoreYouTubeData(db);
                                    await v12_logic.updateMemberStreams(db);
                                    // Also Sync Twitch & Bilibili Archives
                                    try { await syncTwitchArchives(db); } catch (e) { console.error('BG Twitch Sync Failed:', e); }

                                    // Sync Bilibili Archives (Every 60 mins)
                                    try {
                                        const biliMeta = await db.collection('metadata').findOne({ _id: 'last_bilibili_sync' });
                                        if (!biliMeta || Date.now() - biliMeta.timestamp > 3600 * 1000) {
                                            await db.collection('metadata').updateOne({ _id: 'last_bilibili_sync' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                                            await syncBilibiliArchives(db);
                                        }
                                    } catch (e) { console.error('BG Bilibili Sync Failed:', e); }
                                }
                            } catch (e) {
                                console.error('BG Update:', e);
                            } finally {
                                await db.collection('metadata').updateOne({ _id: lockId }, { $set: { timestamp: 0 } });
                            }
                        })(); // Captured Promise
                        didUpdate = true;
                    } else {
                        logOutcome(`skip_lock_failed_${lang}`);
                    }
                } else {
                    logOutcome(`skip_lock_held_${lang}`);
                }
            } else {
                logOutcome(`skip_interval_recent_${lang}`);
            }

            // Independent Live Status Check (5 mins)
            let bgLivePromise = Promise.resolve();
            const liveMeta = await db.collection('metadata').findOne({ _id: 'last_live_check' });
            if (Date.now() - (liveMeta?.timestamp || 0) > LIVE_UPDATE_INTERVAL_SECONDS * 1000) {
                const liveLock = await db.collection('metadata').findOne({ _id: 'live_check_lock' });
                if (!liveLock || Date.now() - liveLock.timestamp > 300000) {
                    await db.collection('metadata').updateOne({ _id: 'live_check_lock' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                    if (!updateStarted) logOutcome('triggered_live_check');
                    bgLivePromise = (async () => {
                        try {
                            // Update timestamp immediately (Start-to-Start interval)
                            await db.collection('metadata').updateOne({ _id: 'last_live_check' }, { $set: { timestamp: Date.now() } }, { upsert: true });
                            await v12_logic.updateLiveStatus(db);
                        } catch (e) { console.error('BG Live Update:', e); }
                        finally { await db.collection('metadata').updateOne({ _id: 'live_check_lock' }, { $set: { timestamp: 0 } }); }
                    })();
                }
            }

            // [Optimization] Return minimal JSON only if explicitly requested (for Cron Jobs)
            // This prevents "Response too large" errors while keeping Frontend functional
            if (searchParams.get('trigger_only') === 'true') {
                // CRITICAL: On Serverless (Vercel), we MUST await the background promise before returning,
                // otherwise execution is frozen and the update dies.
                await Promise.all([bgUpdatePromise, bgLivePromise]);

                return res.status(200).json({
                    success: true,
                    triggered: true,
                    message: 'Background update triggered',
                    timestamp: Date.now()
                });
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

        // Cron Optimization: Return minimal response if triggered automatically
        if (searchParams.get('trigger_only') === 'true') {
            return res.status(200).json({ success: true, message: 'Update Triggered', didUpdate });
        }

        return res.status(200).json({
            videos,
            totalVisits: visits.totalVisits || 0,
            todayVisits: visits.todayVisits || 0,
            timestamp: updateMeta?.timestamp || Date.now(), // Use DB timestamp
            announcement: ann ? { content: ann.content, type: ann.type, active: ann.active === "true" } : null,
            meta: { didUpdate, timestamp: Date.now() }
        });
    }

    // --- Quota Public Endpoint ---
    if (pathname === '/api/quota') {
        // Optional: ?date=YYYY-MM-DD
        let targetDate = searchParams.get('date');
        if (!targetDate) {
            targetDate = new Intl.DateTimeFormat('en-CA', {
                timeZone: 'America/Los_Angeles',
                year: 'numeric', month: '2-digit', day: '2-digit'
            }).format(new Date());
        }

        // Trigger Cleanup (Fire and Forget)
        cleanupOldQuotaLogs(db);

        const [quotaDoc, keyDoc] = await Promise.all([
            db.collection('quota_usage').findOne({ _id: `quota_${targetDate}` }),
            db.collection('metadata').findOne({ _id: 'api_key_rotator' })
        ]);

        return res.status(200).json({
            success: true,
            date: targetDate,
            timezone: 'PT (Pacific Time)',
            totalUsed: quotaDoc?.totalUsed || 0,
            breakdown: quotaDoc?.breakdown || {},
            currentKeyIndex: keyDoc?.currentIndex || 0,
            note: 'Calculated via Dead Reckoning (Search=100, API=1). Includes failed 4xx/5xx requests.'
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
        return res.status(200).json({
            success: true,
            streams: doc?.streams || [],
            isUpdating: doc?.isUpdating || false,
            updateStartedAt: doc?.updateStartedAt || 0,
            timestamp: doc?.timestamp || 0
        });
    }

    // 11. Get Member Streams (Archives)
    if (pathname === '/api/streams' || searchParams.get('endpoint') === 'streams') {
        const page = parseInt(searchParams.get('page') || '1');
        const limit = parseInt(searchParams.get('limit') || '20');
        const platform = searchParams.get('platform'); // youtube, twitch, bilibili, all
        const memberId = searchParams.get('memberId');
        const hasClips = searchParams.get('hasClips') === 'true'; // Filter streams that have linked clips?

        const query = {};
        if (platform && platform !== 'all') query.platform = platform;
        if (memberId) query.memberId = memberId;

        // Filter out upcoming streams (Scheduled waiting rooms)
        query.status = { $ne: 'upcoming' };

        // Only show streams from the last 3 months
        const threeMonthsAgo = new Date();
        threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
        query.startTime = { $gte: threeMonthsAgo };

        const match = query;
        const skip = (page - 1) * limit;

        const pipeline = [
            { $match: match },
            { $sort: { startTime: -1 } },
            { $skip: skip },
            { $limit: limit },
            // Lookup to check for clips (Optimized)
            {
                $lookup: {
                    from: 'videos',
                    let: { sid: '$_id' },
                    pipeline: [
                        {
                            $match: {
                                $expr: {
                                    $or: [
                                        { $eq: ["$relatedStreamId", "$$sid"] },
                                        { $in: ["$$sid", { $ifNull: ["$relatedStreamIds", []] }] }
                                    ]
                                }
                            }
                        },
                        { $limit: 1 },
                        { $project: { _id: 1 } }
                    ],
                    as: 'clips_exist'
                }
            },
            { $addFields: { hasClips: { $gt: [{ $size: '$clips_exist' }, 0] } } },
            { $project: { clips_exist: 0 } }
        ];

        const [streams, totalCount] = await Promise.all([
            db.collection('streams').aggregate(pipeline).toArray(),
            db.collection('streams').countDocuments(match)
        ]);

        return res.status(200).json({
            success: true,
            streams,
            pagination: {
                page,
                limit,
                total: totalCount,
                totalPages: Math.ceil(totalCount / limit)
            }
        });
    }

    // 12. Get Member List (Public)
    if (pathname === '/api/members' || searchParams.get('endpoint') === 'members') {
        // Fetch all channels that are members
        const memberIds = VSPO_MEMBERS.map(m => m.ytId);
        const channels = await db.collection('channels').find({ _id: { $in: memberIds } }).toArray();
        const channelMap = new Map(channels.map(c => [c._id, c]));

        const members = VSPO_MEMBERS.map(m => {
            const c = channelMap.get(m.ytId);
            return {
                name: m.name,
                id: m.ytId || m.bilibiliId || m.name, // Fallback ID for members without YT
                avatarUrl: m.customAvatarUrl || c?.thumbnail || '',
                twitchId: m.twitchId
            };
        });

        return res.status(200).json({ success: true, members });
    }

    // 13. Get Stream Details + Clips
    if (pathname === '/api/stream-details' || searchParams.get('endpoint') === 'stream-details') {
        const streamId = searchParams.get('id');
        if (!streamId) return res.status(400).json({ error: 'Missing id' });

        const stream = await db.collection('streams').findOne({ _id: streamId });
        if (!stream) return res.status(404).json({ error: 'Stream not found' });

        // Find clips linking to this stream
        const clips = await db.collection('videos').find({
            $or: [
                { relatedStreamId: streamId },
                { relatedStreamIds: streamId }
            ]
        }).sort({ publishedAt: -1 }).toArray();

        return res.status(200).json({
            success: true,
            stream,
            clips: clips.map(c => ({
                id: c._id,
                title: c.title,
                thumbnail: c.thumbnail,
                channelTitle: c.channelTitle, // Changed from channelName to match createVideoCard
                channelAvatarUrl: c.channelAvatarUrl,
                channelId: c.channelId,
                viewCount: c.viewCount,
                subscriberCount: c.subscriberCount,
                videoType: c.videoType,
                source: (c.source === 'foreign' || c.source === 'jp') ? 'jp' : 'cn',
                publishedAt: c.publishedAt,
                duration: c.duration
            }))
        });
    }

    // 13. Sync Twitch Archives (Cron)
    if (pathname === '/api/cron/sync-twitch') {
        try {
            const count = await syncTwitchArchives(db);
            return res.status(200).json({ success: true, message: `Synced ${count} Twitch archives.` });
        } catch (e) {
            console.error(e);
            return res.status(500).json({ error: e.message });
        }
    }

    // 14. Sync Bilibili Archives (Cron)
    if (pathname === '/api/cron/sync-bilibili') {
        try {
            const count = await syncBilibiliArchives(db);
            return res.status(200).json({ success: true, message: `Synced ${count} Bilibili archives.` });
        } catch (e) {
            console.error(e);
            return res.status(500).json({ error: e.message });
        }
    }

    return res.status(404).json({ error: 'Not Found', path: pathname });
}
// --- Bilibili via RSSHub V2 (with Domain Fallback) ---
async function fetchBilibiliArchivesV2(mid) {
    const RSSHUB_DOMAINS = [
        'https://rsshub.app',
        'https://rsshub.feeddd.org',
        'https://rss.shab.fun'
    ];

    for (const domain of RSSHUB_DOMAINS) {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout per domain

            // RSSHub provides Bilibili user videos as JSON
            const url = `${domain}/bilibili/user/video/${mid}?format=json`;
            // console.log(`[Bilibili] Trying ${domain} for ${mid}...`);

            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'Accept': 'application/json'
                },
                signal: controller.signal
            });
            clearTimeout(timeoutId);

            if (!res.ok) {
                console.warn(`[Bilibili RSSHub] ${domain} failed for ${mid}: ${res.status}`);
                continue; // Try next domain
            }

            const data = await res.json();

            // RSSHub returns items in data.items array
            if (!data.items) {
                console.warn(`[Bilibili RSSHub] ${domain} returned invalid format for ${mid}`);
                continue;
            }

            if (data.items.length === 0) {
                // Valid response but empty, don't try other domains
                console.log(`[Bilibili RSSHub] No videos found for ${mid} via ${domain}`);
                return [];
            }

            // Success! Transform and return
            return data.items.map(item => {
                const bvidMatch = item.url?.match(/video\/(BV[a-zA-Z0-9]+)/);
                const bvid = bvidMatch ? bvidMatch[1] : item.id;
                const pubDate = new Date(item.date_published || item.pubDate || Date.now());

                return {
                    bvid: bvid,
                    title: item.title || 'Untitled',
                    pic: item.image || '',
                    author: item.authors?.[0]?.name || '',
                    created: Math.floor(pubDate.getTime() / 1000),
                    play: 0,
                    length: ''
                };
            });

        } catch (e) {
            console.warn(`[Bilibili RSSHub] Error ${domain} for ${mid}:`, e.message);
            // Continue to next domain
        }
    }

    console.error(`[Bilibili] All RSSHub instances failed for ${mid}`);
    return [];
}

// --- Bilibili Wbi Signed API Logic (Authenticated) ---
const mixinKeyEncTab = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52
];

const getMixinKey = (orig) => mixinKeyEncTab.map(n => orig[n]).join('').slice(0, 32);

async function getWbiKeys() {
    const sessdata = process.env.BILIBILI_SESSDATA;
    const headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' };
    if (sessdata) headers['Cookie'] = `SESSDATA=${sessdata}`;

    const res = await fetch('https://api.bilibili.com/x/web-interface/nav', { headers });
    const json = await res.json();
    if (!json.data || !json.data.wbi_img) {
        console.warn('[Bilibili] Failed to get Wbi Keys, response:', json);
        throw new Error('Failed to get Wbi Keys');
    }

    const img_url = json.data.wbi_img.img_url;
    const sub_url = json.data.wbi_img.sub_url;

    return {
        img_key: img_url.substring(img_url.lastIndexOf('/') + 1, img_url.lastIndexOf('.')),
        sub_key: sub_url.substring(sub_url.lastIndexOf('/') + 1, sub_url.lastIndexOf('.'))
    };
}

function encWbi(params, img_key, sub_key) {
    const mixin_key = getMixinKey(img_key + sub_key);
    const curr_time = Math.round(Date.now() / 1000);
    const chr_filter = /[!'()*]/g;
    const new_params = { ...params, wts: curr_time };
    const query = Object.keys(new_params).sort().map(key => {
        let value = new_params[key];
        if (typeof value === 'string') value = value.replace(chr_filter, '');
        return `${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
    }).join('&');
    const wbi_sign = crypto.createHash('md5').update(query + mixin_key).digest('hex');
    return query + '&w_rid=' + wbi_sign;
}

async function fetchBilibiliArchivesWbi(mid) {
    try {
        const { img_key, sub_key } = await getWbiKeys();
        const params = { mid, ps: 30, tid: 0, keyword: '', order: 'pubdate', pn: 1, web_location: 1550101, order_avoided: true };
        const query = encWbi(params, img_key, sub_key);
        const url = `https://api.bilibili.com/x/space/wbi/arc/search?${query}`;

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);

        const sessdata = process.env.BILIBILI_SESSDATA;
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': `https://space.bilibili.com/${mid}/video`,
            'Origin': 'https://space.bilibili.com'
        };
        if (sessdata) headers['Cookie'] = `SESSDATA=${sessdata}`;

        const res = await fetch(url, { headers, signal: controller.signal });
        clearTimeout(timeoutId);

        if (!res.ok) {
            console.error(`[Bilibili] HTTP Error ${res.status} for ${mid}`);
            // If 412 or 403, might need to rotate cookie or slow down
            return [];
        }

        const data = await res.json();
        if (data.code !== 0) {
            console.error(`[Bilibili] API Error ${data.code} for ${mid}: ${data.message}`);
            return [];
        }

        const vlist = data.data?.list?.vlist || [];
        if (vlist.length === 0) return [];

        return vlist.map(v => ({
            bvid: v.bvid,
            title: v.title,
            pic: v.pic,
            author: v.author,
            created: v.created, // Correct timestamp in seconds
            play: v.play,
            length: v.length // Duration string "MM:SS"
        }));

    } catch (e) {
        console.error(`[Bilibili] Fetch Error MID=${mid}:`, e.message);
        return [];
    }
}

export { v12_logic, syncTwitchArchives, syncBilibiliArchives };
