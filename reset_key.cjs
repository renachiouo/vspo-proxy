const { MongoClient } = require('mongodb');
require('dotenv').config({ path: '.env' });

const uri = process.env.MONGODB_URL;
if (!uri) {
    console.error('Error: MONGODB_URL not found in .env');
    process.exit(1);
}

const targetIndex = process.argv[2] ? parseInt(process.argv[2]) : 0;

async function resetKey() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        console.log(`Setting API Key Index to ${targetIndex}...`);

        const res = await db.collection('metadata').updateOne(
            { _id: 'api_key_rotator' },
            { $set: { currentIndex: targetIndex, lastUpdated: Date.now() } },
            { upsert: true }
        );

        console.log('Update Result:', res);
        console.log(`SUCCESS: Global Key Index set to ${targetIndex}.`);
        console.log('Note: You may need to Redeploy (git push) for running instances to pick up this change immediately.');

    } catch (e) {
        console.error('Failed:', e);
    } finally {
        await client.close();
    }
}

resetKey();
