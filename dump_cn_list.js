
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) { console.error('No MONGODB_URI'); process.exit(1); }

async function run() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        const wlCn = await db.collection('lists').findOne({ _id: 'whitelist_cn' });
        const ids = wlCn?.items || [];

        console.log(`Checking ${ids.length} channels in whitelist_cn...`);

        const channels = [];
        for (const id of ids) {
            // Priority: Channels Collection -> Videos Collection -> Unknown
            let title = 'Unknown';
            const chDoc = await db.collection('channels').findOne({ _id: id });
            if (chDoc) {
                title = chDoc.title;
            } else {
                const vid = await db.collection('videos').findOne({ channelId: id }, { projection: { channelTitle: 1 } });
                if (vid) title = vid.channelTitle;
            }
            channels.push({ id, title });
        }

        console.log('--- CN Whitelist Analysis ---');
        const jpRegex = /[\u3040-\u309F\u30A0-\u30FF]/;
        const potentialJp = [];

        channels.forEach(c => {
            const hasJp = jpRegex.test(c.title);
            console.log(`[${c.id}] ${c.title} ${hasJp ? '⚠️(Potential JP)' : ''}`);
            if (hasJp) potentialJp.push(c.id);
        });

        if (potentialJp.length > 0) {
            console.log(`\nFound ${potentialJp.length} Potential JP Channels. Removing them from whitelist_cn...`);
            await db.collection('lists').updateOne({ _id: 'whitelist_cn' }, { $pull: { items: { $in: potentialJp } } });
            console.log('Cleaned.');
        } else {
            console.log('\nNo obvious Japanese channels found.');
        }

    } finally {
        await client.close();
    }
}
run();
