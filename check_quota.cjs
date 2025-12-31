require('dotenv').config({ path: './.env' });
const { MongoClient } = require('mongodb');
const fs = require('fs');

async function run() {
    const uri = process.env.MONGODB_URL;
    if (!uri) {
        fs.writeFileSync('quota_log.txt', "MONGODB_URL missing");
        process.exit(1);
    }
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('vspoproxy');

        // 1. Check Quota Usage for last 3 days
        const quotaDocs = await db.collection('quota_usage')
            .find({})
            .sort({ _id: -1 }) // Sort by date desc (id is quota_YYYY-MM-DD)
            .limit(3)
            .toArray();

        let output = "=== Last 3 Days Quota Usage ===\n";
        output += JSON.stringify(quotaDocs, null, 2);

        fs.writeFileSync('quota_log.txt', output);

    } catch (e) {
        fs.writeFileSync('quota_log.txt', "Error: " + e.toString());
    } finally {
        await client.close();
    }
}
run();
