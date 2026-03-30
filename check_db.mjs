import 'dotenv/config';
import { MongoClient } from 'mongodb';
const c = new MongoClient(process.env.MONGODB_URL);
await c.connect();
const db = c.db('vspoproxy');
const vId = 'EeyhrliRANI';
const vidDoc = await db.collection('videos').findOne({ _id: vId });
console.log('==== VIDEO STATUS ERY ====');
if (vidDoc) {
    console.log('Found in videos collection!');
    console.log('reviewStatus:', vidDoc.reviewStatus);
    console.log('title:', vidDoc.title);
    console.log('source:', vidDoc.source);
} else {
    console.log('NOT FOUND in videos collection.');
}

const lists = await db.collection('lists').find({}).toArray();
let foundInList = false;
for (const l of lists) {
    if (l.items && l.items.includes(vId)) {
        console.log('Found in list collection:', l._id);
        foundInList = true;
    }
}
if (!foundInList) console.log('Not found in any list collections (like video_blacklist).');
await c.close();
