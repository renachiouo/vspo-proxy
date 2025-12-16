
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        console.log('Fetching lists...');
        const [wlCn, blCn, wlJp, blJp, pendJp] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }),
            db.collection('lists').findOne({ _id: 'blacklist_cn' }),
            db.collection('lists').findOne({ _id: 'whitelist_jp' }),
            db.collection('lists').findOne({ _id: 'blacklist_jp' }),
            db.collection('lists').findOne({ _id: 'pending_jp' })
        ]);

        const pendingItems = pendJp?.items || [];
        console.log(`Current Pending Items: ${pendingItems.length}`);

        const otherLists = new Set([
            ...(wlCn?.items || []), ...(blCn?.items || []),
            ...(wlJp?.items || []), ...(blJp?.items || [])
        ]);

        const newPending = pendingItems.filter(id => !otherLists.has(id));
        const removedCount = pendingItems.length - newPending.length;

        console.log(`Found ${removedCount} duplicates to remove.`);

        if (removedCount > 0) {
            await db.collection('lists').updateOne(
                { _id: 'pending_jp' },
                { $set: { items: newPending } }
            );
            console.log('Successfully updated pending_jp list.');
        } else {
            console.log('No duplicates found.');
        }

    } finally {
        await client.close();
    }
}
run();
