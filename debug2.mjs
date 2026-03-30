import 'dotenv/config';
import { MongoClient } from 'mongodb';
const c = new MongoClient(process.env.MONGODB_URL);
await c.connect();
const db = c.db('vspoproxy');
const lastCn = await db.collection('metadata').findOne({ _id: 'last_update_cn' });
console.log('Last CN sync:', lastCn?.timestamp ? new Date(lastCn.timestamp).toISOString() : 'NEVER');
const ptDate = new Intl.DateTimeFormat('en-CA',{timeZone:'America/Los_Angeles',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
const q = await db.collection('quota_usage').findOne({ _id: 'quota_' + ptDate });
console.log('Today quota used:', q?.totalUsed || 0);
if(q?.breakdown){for(const[k,v] of Object.entries(q.breakdown)){console.log(' ',k,':',v.total);}}
const yesterday = new Date(); yesterday.setDate(yesterday.getDate()-1);
const yd = new Intl.DateTimeFormat('en-CA',{timeZone:'America/Los_Angeles',year:'numeric',month:'2-digit',day:'2-digit'}).format(yesterday);
const q2 = await db.collection('quota_usage').findOne({ _id: 'quota_' + yd });
console.log('Yesterday quota:', q2?.totalUsed || 0);
await c.close();
