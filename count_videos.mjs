// Check how many videos are actually marked as 'short' in the database
import 'dotenv/config';
import { MongoClient } from 'mongodb';

const MONGODB_URI = process.env.MONGODB_URL;
const DB_NAME = 'vspoproxy';

async function checkCounts() {
    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        const shortCount = await db.collection('videos').countDocuments({ videoType: 'short' });
        const videoCount = await db.collection('videos').countDocuments({ videoType: 'video' });
        const nullCount = await db.collection('videos').countDocuments({ videoType: { $exists: false } });
        const totalCount = await db.collection('videos').countDocuments({});

        console.log('=== Video Type Counts ===');
        console.log('  short:', shortCount);
        console.log('  video:', videoCount);
        console.log('  no videoType:', nullCount);
        console.log('  total:', totalCount);

    } finally {
        await client.close();
    }
}

checkCounts();
