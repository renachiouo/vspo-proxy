// Check latest videos to see if they have videoType
import 'dotenv/config';
import { MongoClient } from 'mongodb';

const MONGODB_URI = process.env.MONGODB_URL;
const DB_NAME = 'vspoproxy';

async function checkLatest() {
    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        console.log('--- Latest 10 Videos ---');
        const videos = await db.collection('videos')
            .find({ source: 'main' })
            .sort({ publishedAt: -1 })
            .limit(10)
            .project({ title: 1, videoType: 1, durationMinutes: 1 })
            .toArray();

        console.log(JSON.stringify(videos, null, 2));

    } finally {
        await client.close();
    }
}

checkLatest();
