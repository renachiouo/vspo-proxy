import 'dotenv/config';
import { MongoClient } from 'mongodb';
const c = new MongoClient(process.env.MONGODB_URL);
await c.connect();
const db = c.db('vspoproxy');
const ptDate = new Intl.DateTimeFormat('en-CA',{timeZone:'America/Los_Angeles',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
const q = await db.collection('quota_usage').findOne({ _id: 'quota_' + ptDate });
console.log('Total quota used today:', q?.totalUsed || 0);
await c.close();
