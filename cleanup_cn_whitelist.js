
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        console.log('Fetching lists...');
        const [wlCn, wlJp] = await Promise.all([
            db.collection('lists').findOne({ _id: 'whitelist_cn' }),
            db.collection('lists').findOne({ _id: 'whitelist_jp' })
        ]);

        const cnItems = wlCn?.items || [];
        const jpItems = new Set(wlJp?.items || []);

        console.log(`CN Whitelist Size: ${cnItems.length}`);
        console.log(`JP Whitelist Size: ${jpItems.size}`);

        const newCnItems = cnItems.filter(id => !jpItems.has(id));
        const removedCount = cnItems.length - newCnItems.length;

        console.log(`Found ${removedCount} JP channels in CN whitelist.`);

        if (removedCount > 0) {
            await db.collection('lists').updateOne(
                { _id: 'whitelist_cn' },
                { $set: { items: newCnItems } }
            );
            console.log('Successfully cleaned whitelist_cn.');
        } else {
            console.log('whitelist_cn is already clean.');
        }

    } finally {
        await client.close();
    }
}
run();
