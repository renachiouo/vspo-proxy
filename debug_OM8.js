
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');
        const collection = db.collection('videos');

        const videoId = 'OM8bGgu-c3E';
        const doc = await collection.findOne({ _id: videoId });

        if (!doc) {
            console.log('Video NOT FOUND in DB:', videoId);
            return;
        }

        console.log('Video Found:', videoId);
        console.log('Title:', doc.title);
        console.log('OSI:', JSON.stringify(doc.originalStreamInfo, null, 2));

        if (!doc.originalStreamInfo || !doc.originalStreamInfo.id) {
            console.log('Video has no OSI ID. Cannot search.');
            return;
        }

        const streamId = doc.originalStreamInfo.id;
        console.log('Searching for StreamID:', streamId);

        // Simulate API Query
        const query = { "originalStreamInfo.id": streamId, _id: { $ne: videoId } };
        const related = await collection.find(query).toArray();

        console.log(`Query found ${related.length} related clips.`);
        related.forEach(v => {
            console.log(`- [${v._id}] ${v.title}`);
        });

    } finally {
        await client.close();
    }
}
run();
