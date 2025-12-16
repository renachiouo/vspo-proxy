
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        console.log('Syncing video sources with lists...');

        const [wlCn, wlJp] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }),
            db.collection('lists').findOne({ _id: 'whitelist_jp' })
        ]);

        const cnIds = wlCn?.items || [];
        const jpIds = wlJp?.items || [];

        console.log(`CN Whitelist: ${cnIds.length}`);
        console.log(`JP Whitelist: ${jpIds.length}`);

        // 1. Update source='main' for CN whitelist channels
        const resCn = await db.collection('videos').updateMany(
            { channelId: { $in: cnIds } },
            { $set: { source: 'main' } }
        );
        console.log(`Updated ${resCn.modifiedCount} videos to source='main' (matched CN whitelist).`);

        // 2. Update source='foreign' for JP whitelist channels
        const resJp = await db.collection('videos').updateMany(
            { channelId: { $in: jpIds } },
            { $set: { source: 'foreign' } }
        );
        console.log(`Updated ${resJp.modifiedCount} videos to source='foreign' (matched JP whitelist).`);

    } finally {
        await client.close();
    }
}
run();
