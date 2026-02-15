// 診斷腳本：檢查為什麼影片都被標記為 pending_review
// 使用方法：node diagnose_streams.mjs

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

        // 1. 檢查 streams collection
        console.log('=== Streams Collection 統計 ===');
        const totalStreams = await db.collection('streams').countDocuments({});
        const streamsWithMemberId = await db.collection('streams').countDocuments({
            memberId: { $exists: true, $ne: null, $ne: '' }
        });

        console.log(`總直播數：${totalStreams.toLocaleString()}`);
        console.log(`有 memberId 的直播數：${streamsWithMemberId.toLocaleString()}`);
        console.log(`比例：${((streamsWithMemberId / totalStreams) * 100).toFixed(1)}%`);

        // 2. 抽樣檢查 streams
        console.log('\n=== Streams 樣本（前 5 筆）===');
        const sampleStreams = await db.collection('streams').find({}).limit(5).toArray();
        sampleStreams.forEach(s => {
            console.log(`ID: ${s._id}`);
            console.log(`  Platform: ${s.platform || 'N/A'}`);
            console.log(`  MemberId: ${s.memberId || 'MISSING'}`);
            console.log(`  Title: ${s.title?.substring(0, 50) || 'N/A'}`);
            console.log('');
        });

        // 3. 檢查影片（明確指定要查詢 description）
        console.log('=== Videos 樣本（前 3 筆）===');
        const sampleVideos = await db.collection('videos').find({})
            .project({ _id: 1, title: 1, description: 1 })  // 明確指定要查詢這些欄位
            .limit(3)
            .toArray();

        for (const video of sampleVideos) {
            console.log(`影片: ${video.title}`);
            console.log(`  ID: ${video._id}`);

            // 檢查描述中是否有連結
            const desc = video.description || '';
            const ytMatch = desc.match(/(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
            const twMatch = desc.match(/(?:https?:\/\/)?(?:www\.|m\.)?twitch\.tv\/videos\/(\d+)/);
            const biliMatch = desc.match(/(?:https?:\/\/)?(?:www\.)?bilibili\.com\/video\/(BV[a-zA-Z0-9]+)/);

            console.log(`  描述長度: ${desc.length}`);
            console.log(`  YouTube 連結: ${ytMatch ? ytMatch[1] : '未找到'}`);
            console.log(`  Twitch 連結: ${twMatch ? twMatch[1] : '未找到'}`);
            console.log(`  Bilibili 連結: ${biliMatch ? biliMatch[1] : '未找到'}`);

            // 如果找到連結，檢查 streams collection
            if (ytMatch) {
                const stream = await db.collection('streams').findOne({ _id: ytMatch[1] });
                console.log(`  → Streams 中是否存在: ${stream ? '是' : '否'}`);
                if (stream) {
                    console.log(`  → 有 memberId: ${stream.memberId ? '是 (' + stream.memberId + ')' : '否'}`);
                }
            }
            console.log('');
        }

    } catch (error) {
        console.error('❌ 錯誤:', error);
    } finally {
        await client.close();
    }
}

diagnose().catch(console.error);
