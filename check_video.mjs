// Quick check for a specific video's videoType in MongoDB
import 'dotenv/config';
import { MongoClient } from 'mongodb';

const MONGODB_URI = process.env.MONGODB_URL;
const DB_NAME = 'vspoproxy';

async function checkVideo(videoId) {
    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);

        const video = await db.collection('videos').findOne({ _id: videoId });

        if (video) {
            console.log('Found video:');
            console.log('  _id:', video._id);
            console.log('  title:', video.title);
            console.log('  videoType:', video.videoType);
            console.log('  durationMinutes:', video.durationMinutes);
            console.log('  source:', video.source);
        } else {
            console.log('Video not found in database');
        }

    } finally {
        await client.close();
    }
}

checkVideo('E_EP6ywTTTk');
