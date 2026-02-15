// 診斷 streams collection 的 memberId 情況
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

const MONGODB_URI = process.env.MONGODB_URL;

if (!MONGODB_URI) {
    console.error('❌ 找不到 MONGODB_URL');
    process.exit(1);
}

async function diagnose() {
    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        console.log('✅ 已連接到 MongoDB\n');

        const db = client.db();
        const streams = db.collection('streams');

        // 統計
        const total = await streams.countDocuments({});
        const withMemberId = await streams.countDocuments({
            memberId: { $exists: true, $ne: null, $ne: '' }
        });

        console.log('=== Streams Collection 統計 ===');
        console.log(`總直播數：${total.toLocaleString()}`);
        console.log(`有 memberId 的直播數：${withMemberId.toLocaleString()}`);
        console.log(`比例：${((withMemberId / total) * 100).toFixed(1)}%`);

        // 抽樣檢查
        console.log('\n=== Streams 樣本（有 memberId）===');
        const samplesWithMember = await streams.find({
            memberId: { $exists: true, $ne: null, $ne: '' }
        }).limit(10).toArray();

        console.log(`找到 ${samplesWithMember.length} 個有 memberId 的 stream：`);
        samplesWithMember.forEach((s, i) => {
            console.log(`\n${i + 1}. ID: ${s._id}`);
            console.log(`   memberId: ${s.memberId}`);
            console.log(`   platform: ${s.platform}`);
            console.log(`   title: ${s.title?.substring(0, 50)}`);
        });

        console.log('\n=== Streams 樣本（無 memberId）===');
        const samplesWithoutMember = await streams.find({
            $or: [
                { memberId: { $exists: false } },
                { memberId: null },
                { memberId: '' }
            ]
        }).limit(5).toArray();

        console.log(`找到 ${samplesWithoutMember.length} 個無 memberId 的 stream：`);
        samplesWithoutMember.forEach((s, i) => {
            console.log(`\n${i + 1}. ID: ${s._id}`);
            console.log(`   memberId: ${s.memberId || 'MISSING'}`);
            console.log(`   platform: ${s.platform}`);
            console.log(`   title: ${s.title?.substring(0, 50)}`);
        });

        // 檢查遷移腳本中找到的連結
        console.log('=== 檢查遷移腳本找到的連結 ===');
        const testIds = ['axgwvatemyu', 'borjl6zqegc', 'ndnlqvea2xy', 'f_yatq8oeho', 'bgrk3ncz2zk'];

        for (const id of testIds) {
            const stream = await streams.findOne({ _id: id });
            if (stream) {
                console.log(`${id}: memberId = ${stream.memberId || 'MISSING'}, title = ${stream.title?.substring(0, 30)}`);
            } else {
                console.log(`${id}: 不存在於 streams collection`);
            }
        }

    } catch (error) {
        console.error('❌ 錯誤:', error);
    } finally {
        await client.close();
    }
}

diagnose().catch(console.error);
