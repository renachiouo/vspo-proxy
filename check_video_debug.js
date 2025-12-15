
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspo_node');
        const id = 'quVQbyzxW5g';
        const doc = await db.collection('videos').findOne({ _id: id });

        if (doc) {
            console.log('Found:', JSON.stringify(doc, null, 2));
        } else {
            console.log('Not Found in DB.');
            // Check if it's in blacklist?
            const bl = await db.collection('lists').findOne({ _id: 'video_blacklist' });
            if (bl?.items?.includes(id)) console.log('Video is in video_blacklist');
        }

    } finally {
        await client.close();
    }
}
run();
