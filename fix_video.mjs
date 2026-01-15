// Quick fix for a specific video's videoType in MongoDB
import 'dotenv/config';
import { MongoClient } from 'mongodb';

const MONGODB_URI = process.env.MONGODB_URL;
const DB_NAME = 'vspoproxy';

async function checkAndFixVideo(videoId) {
    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        console.log('=== Before Update ===');
        let video = await db.collection('videos').findOne({ _id: videoId });
        console.log('  videoType:', video?.videoType);

        console.log('\n=== Updating to short ===');
        const result = await db.collection('videos').updateOne(
            { _id: videoId },
            { $set: { videoType: 'short' } }
        );
        console.log('  matchedCount:', result.matchedCount);
        console.log('  modifiedCount:', result.modifiedCount);

        console.log('\n=== After Update ===');
        video = await db.collection('videos').findOne({ _id: videoId });
        console.log('  videoType:', video?.videoType);

    } finally {
        await client.close();
    }
}

checkAndFixVideo('E_EP6ywTTTk');
