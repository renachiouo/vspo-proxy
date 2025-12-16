
import { MongoClient } from 'mongodb';

// --- Replicate Logic ---
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = 'vspoproxy';
const apiKeys = [
    process.env.YOUTUBE_API_KEY_1, process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3, process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5, process.env.YOUTUBE_API_KEY_6,
    process.env.YOUTUBE_API_KEY_7, process.env.YOUTUBE_API_KEY_8,
    process.env.YOUTUBE_API_KEY_9, process.env.YOUTUBE_API_KEY_10,
    process.env.YOUTUBE_API_KEY_11,
].filter(Boolean);

const fetchYouTube = async (endpoint, params) => {
    for (const apiKey of apiKeys) {
        const url = `https://www.googleapis.com/youtube/v3/${endpoint}?${new URLSearchParams(params)}&key=${apiKey}`;
        try {
            const res = await fetch(url);
            const data = await res.json();
            if (data.error && (data.error.message.toLowerCase().includes('quota') || data.error.reason === 'quotaExceeded')) {
                console.log(`[YouTube] Key ending in ...${apiKey.slice(-4)} exhausted.`);
                continue;
            }
            if (data.error) throw new Error(JSON.stringify(data.error));
            return data;
        } catch (e) {
            if (e.message.includes('quota')) continue;
            throw e;
        }
    }
    throw new Error('API Quota Exceeded (All Keys)');
};

const batchArray = (arr, size) => Array.from({ length: Math.ceil(arr.length / size) }, (v, i) => arr.slice(i * size, i * size + size));

async function run() {
    if (!MONGODB_URI) { console.error('No MONGODB_URI'); process.exit(1); }
    const client = new MongoClient(MONGODB_URI);
    try {
        await client.connect();
        const db = client.db(DB_NAME);

        console.log(`[Debug] Starting JP Whitelist Update simulation with ${apiKeys.length} keys...`);

        const wlJp = await db.collection('lists').findOne({ _id: 'whitelist_jp' });
        const whitelist = wlJp?.items || [];
        console.log(`[Debug] JP Whitelist Size: ${whitelist.length}`);

        if (whitelist.length === 0) { console.log('Whitelist empty.'); return; }

        const retentionDate = new Date(); retentionDate.setDate(retentionDate.getDate() - 90);

        for (const batch of batchArray(whitelist, 50)) {
            console.log(`[Debug] Processing batch of ${batch.length} channels...`);
            try {
                const res = await fetchYouTube('channels', { part: 'contentDetails', id: batch.join(',') });
                const uploads = res.items?.map(i => i.contentDetails?.relatedPlaylists?.uploads).filter(Boolean) || [];
                console.log(`[Debug] Found ${uploads.length} upload playlists.`);

                const pResults = await Promise.all(uploads.map(pid => fetchYouTube('playlistItems', { part: 'snippet', playlistId: pid, maxResults: 10 })));
                let vidCount = 0;
                pResults.forEach(r => r.items?.forEach(i => {
                    const date = new Date(i.snippet.publishedAt);
                    if (date > retentionDate) vidCount++;
                }));
                console.log(`[Debug] Found ${vidCount} videos in batch.`);
            } catch (e) {
                console.error('[Debug] Error in batch:', e.message);
            }
        }
        console.log('[Debug] Done.');

    } catch (e) {
        console.error('[Fatal Error]', e);
    } finally {
        await client.close();
    }
}
run();
