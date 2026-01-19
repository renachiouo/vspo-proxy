// Migration script: Copy data from Taiwan cluster to Singapore cluster
import 'dotenv/config';
import { MongoClient } from 'mongodb';

const SOURCE_URI = process.env.MONGODB_URL; // Taiwan cluster
const DEST_URI = 'mongodb+srv://magirenaH:6UsIrW915szgshlo@vspoproxy-sg-test.0m1pcya.mongodb.net/vspoproxy';
const DB_NAME = 'vspoproxy';

async function migrate() {
    console.log('=== MongoDB Migration: Taiwan → Singapore ===\n');

    const sourceClient = new MongoClient(SOURCE_URI);
    const destClient = new MongoClient(DEST_URI);

    try {
        console.log('Connecting to source (Taiwan)...');
        await sourceClient.connect();
        const sourceDb = sourceClient.db(DB_NAME);
        console.log('Connected to source.\n');

        console.log('Connecting to destination (Singapore)...');
        await destClient.connect();
        const destDb = destClient.db(DB_NAME);
        console.log('Connected to destination.\n');

        // Collections to migrate
        const collections = ['videos', 'lists', 'metadata', 'channels', 'streams', 'quota_usage'];

        for (const collName of collections) {
            console.log(`--- Migrating '${collName}' ---`);

            const sourceColl = sourceDb.collection(collName);
            const destColl = destDb.collection(collName);

            // Count source documents
            const sourceCount = await sourceColl.countDocuments();
            console.log(`  Source documents: ${sourceCount}`);

            if (sourceCount === 0) {
                console.log(`  Skipping (empty)\n`);
                continue;
            }

            // Clear destination (if exists)
            await destColl.deleteMany({});
            console.log(`  Cleared destination.`);

            // Copy in batches
            const batchSize = 100;
            let copied = 0;

            const cursor = sourceColl.find({});
            let batch = [];

            while (await cursor.hasNext()) {
                const doc = await cursor.next();
                batch.push(doc);

                if (batch.length >= batchSize) {
                    await destColl.insertMany(batch);
                    copied += batch.length;
                    process.stdout.write(`\r  Copied: ${copied}/${sourceCount}`);
                    batch = [];
                }
            }

            // Insert remaining
            if (batch.length > 0) {
                await destColl.insertMany(batch);
                copied += batch.length;
            }

            console.log(`\r  Copied: ${copied}/${sourceCount} ✓`);

            // Verify
            const destCount = await destColl.countDocuments();
            if (destCount === sourceCount) {
                console.log(`  Verified: ${destCount} documents ✓\n`);
            } else {
                console.log(`  WARNING: Mismatch! Dest has ${destCount}, expected ${sourceCount}\n`);
            }
        }

        // Create indexes on destination
        console.log('--- Creating indexes on destination ---');
        await destDb.collection('videos').createIndex({ source: 1, publishedAt: -1 });
        await destDb.collection('videos').createIndex({ videoType: 1 });
        await destDb.collection('videos').createIndex({ channelId: 1 });
        console.log('Indexes created ✓\n');

        console.log('=== Migration Complete ===');

    } catch (e) {
        console.error('Migration Error:', e);
    } finally {
        await sourceClient.close();
        await destClient.close();
    }
}

migrate();
