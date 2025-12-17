
import { MongoClient } from 'mongodb';

// Retrieve environment variables
const uri = process.env.MONGODB_URI;
if (!uri) {
    console.error('Error: MONGODB_URI environment variable is not set.');
    process.exit(1);
}

async function createIndexes() {
    const client = new MongoClient(uri);

    try {
        await client.connect();
        // Access default database
        const db = client.db();
        console.log(`Connected to database: ${db.databaseName}`);

        console.log('Creating indexes for "videos" collection...');

        // Index for Related Clips query
        const result1 = await db.collection('videos').createIndex(
            { "originalStreamInfo.id": 1, "publishedAt": -1 },
            { background: true, name: "related_clips_idx" }
        );
        console.log(`Index created: ${result1}`);

        // Index for General Filtering/Sorting
        const result2 = await db.collection('videos').createIndex(
            { "publishedAt": -1 },
            { background: true, name: "published_at_idx" }
        );
        console.log(`Index created: ${result2}`);

        const result3 = await db.collection('videos').createIndex(
            { "videoType": 1, "publishedAt": -1 },
            { background: true, name: "video_type_idx" }
        );
        console.log(`Index created: ${result3}`);

        const result4 = await db.collection('videos').createIndex(
            { "channelId": 1, "publishedAt": -1 },
            { background: true, name: "channel_id_idx" }
        );
        console.log(`Index created: ${result4}`);

        console.log('All indexes created successfully.');

    } catch (err) {
        console.error('Error creating indexes:', err);
    } finally {
        await client.close();
    }
}

createIndexes();
