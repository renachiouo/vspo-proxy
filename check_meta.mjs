import 'dotenv/config';
import { MongoClient } from 'mongodb';
const c = new MongoClient(process.env.MONGODB_URL);
await c.connect();
const db = c.db('vspoproxy');
const meta = await db.collection('metadata').findOne({_id: 'last_update_jp'});
console.log('last_update_jp:', meta?.timestamp ? new Date(meta.timestamp).toISOString() : 'MISSING');
await c.close();
