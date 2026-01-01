const { MongoClient } = require('mongodb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const MONGODB_URI = process.env.MONGODB_URL;
if (!MONGODB_URI) {
    console.error('請先設定 .env 中的 MONGODB_URL');
    process.exit(1);
}

async function run() {
    const client = new MongoClient(MONGODB_URI);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        const videoId = process.argv[2];
        const channelId = process.argv[3]; // Optional, strictly speaking Smart Retention only needs VID to check status

        if (!videoId) {
            console.log('用法: node manual_add_live.cjs <Video_ID>');
            return;
        }

        console.log(`正在手動將 ${videoId} 加入直播監控列表...`);

        // Structure in metadata.live_status.streams usually has { platform, vid, ... }
        // The core logic only needs 'platform' and 'vid' to include it in the candidate list.
        const entry = {
            platform: 'youtube',
            vid: videoId,
            addedAt: new Date()
        };

        const result = await db.collection('metadata').updateOne(
            { _id: 'live_status' },
            {
                $addToSet: {
                    streams: entry
                }
            },
            { upsert: true }
        );

        if (result.modifiedCount > 0 || result.upsertedCount > 0) {
            console.log('✅ 成功加入！下一次系統更新時將會檢查此影片。');
        } else {
            console.log('⚠️ 此影片可能已在列表中。');
        }

    } catch (e) {
        console.error('Error:', e);
    } finally {
        await client.close();
    }
}

run();
