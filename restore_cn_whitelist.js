
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        console.log('Restoring deleted channels...');

        // 1. Get current whitelist
        const wlCn = await db.collection('lists').findOne({ _id: 'whitelist_cn' });
        const currentIds = new Set(wlCn?.items || []);
        console.log(`Current Whitelist Size: ${currentIds.size}`);

        // 2. Get all channels that have videos in the DB (implying they were once valid)
        // We look for videos that are NOT 'foreign' source ideally, or just all.
        // But since I deleted them from CN whitelist, they are likely Chinese channels.
        const channels = await db.collection('videos').distinct('channelId', {});
        console.log(`Total active channels in DB: ${channels.length}`);

        // 3. Find missing IDs
        const missingIds = [];
        const excludedLists = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_jp' }),
            db.collection('lists').findOne({ _id: 'blacklist_cn' }),
            db.collection('lists').findOne({ _id: 'blacklist_jp' })
        ]);

        const excludedIds = new Set();
        excludedLists.forEach(l => l?.items?.forEach(i => excludedIds.add(i)));

        for (const cid of channels) {
            // If it's NOT in current whitelist AND NOT in other lists (JP/Blacklist)
            if (!currentIds.has(cid) && !excludedIds.has(cid)) {
                missingIds.push(cid);
            }
        }

        console.log(`Found ${missingIds.length} channels missing from whitelist (but present in videos DB).`);

        if (missingIds.length > 0) {
            // Fetch titles for log
            for (const id of missingIds) {
                const vid = await db.collection('channels').findOne({ _id: id });
                const vid2 = await db.collection('videos').findOne({ channelId: id });
                const title = vid?.title || vid2?.channelTitle || 'Unknown';
                console.log(`Restoring: [${id}] ${title}`);
            }

            await db.collection('lists').updateOne(
                { _id: 'whitelist_cn' },
                { $addToSet: { items: { $each: missingIds } } }
            );
            console.log('Successfully restored channels.');
        } else {
            console.log('No channels to restore.');
        }

    } finally {
        await client.close();
    }
}
run();
