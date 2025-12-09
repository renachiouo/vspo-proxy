import { createClient } from 'redis';

// --- Redis Keys ---
const V12_PENDING_ATTRIBUTION_QUEUE = 'vspo-db:v4:attribution_queue';
const v12_VIDEO_HASH_PREFIX = 'vspo-db:v3:video:';
const v12_FOREIGN_VIDEO_HASH_PREFIX = 'vspo-db:v3:foreign_video:';

// --- Twitch Config ---
const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID;
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET;

// --- YouTube Config ---
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9,
].filter(key => key);

// --- Helpers ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.warn(`[YouTube] Key quota exceeded, trying next key...`);
                await sleep(1000);
                continue;
            }
            if (data.error) throw new Error(data.error.message);
            return data;
        } catch (e) {
            console.error(`[YouTube] Error with key:`, e);
            await sleep(500);
        }
    }
    throw new Error('All YouTube API keys exhausted.');
};

// Twitch Token Cache
let twitchAccessToken = null;
let twitchTokenExpiry = 0;

async function getTwitchToken() {
    if (twitchAccessToken && Date.now() < twitchTokenExpiry) {
        return twitchAccessToken;
    }
    if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
        console.warn('Twitch Credentials not found.');
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
        twitchTokenExpiry = Date.now() + (data.expires_in * 1000) - 60000;
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
            console.error(`[Twitch] API Error ${response.status}`);
            return null;
        }
        return await response.json();
    } catch (e) {
        console.error(`[Twitch] Network Error:`, e);
        return null;
    }
}

export default async function handler(request, response) {
    if (request.method !== 'POST') {
        return response.status(405).json({ error: 'Method Not Allowed' });
    }

    // Auth Check
    const providedPassword = request.headers.authorization?.split(' ')[1];
    const adminPassword = process.env.ADMIN_PASSWORD;
    if (!adminPassword || providedPassword !== adminPassword) {
        return response.status(401).json({ error: 'Unauthorized' });
    }

    const redisConnectionString = process.env.REDIS_URL;
    if (!redisConnectionString) {
        return response.status(500).json({ error: 'Redis URL not configured' });
    }

    const client = createClient({ url: redisConnectionString });
    await client.connect();

    try {
        // 1. Pop batch from Queue
        const BATCH_SIZE = 20;
        const videoIds = await client.sPop(V12_PENDING_ATTRIBUTION_QUEUE, BATCH_SIZE);

        if (!videoIds || videoIds.length === 0) {
            return response.status(200).json({ message: 'No pending videos in queue.', processed: 0 });
        }

        console.log(`[Enrichment] Processing ${videoIds.length} videos...`);
        const pipeline = client.multi();

        // 2. Process each video
        for (const videoId of videoIds) {
            // Check both hash prefixes (main and foreign) to find where the video lives
            let hashKey = await client.exists(`${v12_VIDEO_HASH_PREFIX}${videoId}`) ? `${v12_VIDEO_HASH_PREFIX}${videoId}` : null;
            if (!hashKey) {
                hashKey = await client.exists(`${v12_FOREIGN_VIDEO_HASH_PREFIX}${videoId}`) ? `${v12_FOREIGN_VIDEO_HASH_PREFIX}${videoId}` : null;
            }

            if (!hashKey) {
                console.warn(`[Enrichment] Video ${videoId} not found in DB, skipping.`);
                continue;
            }

            const videoData = await client.hGetAll(hashKey);
            let originalStreamInfo = [];

            try {
                if (videoData.originalStreamInfo) {
                    originalStreamInfo = JSON.parse(videoData.originalStreamInfo);
                }
            } catch (e) {
                console.error(`Error parsing originalStreamInfo for ${videoId}`, e);
            }

            if (!Array.isArray(originalStreamInfo) || originalStreamInfo.length === 0) {
                continue;
            }

            const targetChannelIds = new Set();

            for (const info of originalStreamInfo) {
                if (info.platform === 'youtube') {
                    // Optimized: In a real batch implementation, we should group these. 
                    // For now, doing it sequentially inside this worker is safer than the main loop.
                    // This worker has 10s-60s to run for just 20 videos, which is plenty.
                    try {
                        const data = await fetchYouTube('videos', { part: 'snippet', id: info.id });
                        if (data.items && data.items.length > 0) {
                            targetChannelIds.add(data.items[0].snippet.channelId);
                        }
                    } catch (e) {
                        console.error(`[Enrichment] YT Lookup failed for ${info.id}`, e);
                    }
                } else if (info.platform === 'twitch') {
                    try {
                        const data = await fetchTwitch('videos', { id: info.id });
                        if (data && data.data && data.data.length > 0) {
                            targetChannelIds.add(data.data[0].user_id);
                        } else {
                            // Fallback: If video is deleted, we can't get channel ID easily from Helix API video endpoint.
                            // Sometimes parsing channel name from URL is possible but 'twitch.tv/videos/ID' doesn't have it.
                            // Abandoning if deleted.
                        }
                    } catch (e) {
                        console.error(`[Enrichment] Twitch Lookup failed for ${info.id}`, e);
                    }
                }
            }

            if (targetChannelIds.size > 0) {
                const targetIdsArray = Array.from(targetChannelIds);
                console.log(`[Enrichment] Identified sources for ${videoId}:`, targetIdsArray);
                pipeline.hSet(hashKey, 'targetChannelIds', JSON.stringify(targetIdsArray));
            }
        }

        await pipeline.exec();
        return response.status(200).json({ message: 'Batch processed successfully', processed: videoIds.length });

    } catch (e) {
        console.error('[Enrichment] Critical Error:', e);
        return response.status(500).json({ error: e.message });
    } finally {
        await client.disconnect();
    }
}
