
// Mock VSPO_MEMBERS from worker.js
const MEMBERS = [
    { name: "小針彩", bilibiliUid: "3546695948306751", bilibiliId: "1972360561" },
    { name: "白咲露理", bilibiliUid: "3546695864421312", bilibiliId: "1842209652" },
    { name: "帕妃", bilibiliUid: "3546695946209651", bilibiliId: "1742801253" },
    { name: "千郁郁", bilibiliUid: "3546695956695430", bilibiliId: "1996441034" },
    { name: "日向晴", bilibiliUid: "3546860864146139", bilibiliId: "1833448662" }
];

async function checkBatchBilibili() {
    console.log('[Debug] Starting Bilibili Batch Live Check...');
    const uids = MEMBERS.map(m => parseInt(m.bilibiliUid));

    console.log(`Checking ${uids.length} UIDs:`, uids);

    const statusUrl = `https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids`;

    try {
        const res = await fetch(statusUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            },
            body: JSON.stringify({ uids })
        });

        console.log(`HTTP Status: ${res.status}`);

        if (res.ok) {
            const json = await res.json();
            console.log('Response Code:', json.code);
            // console.log('Full Data:', JSON.stringify(json.data, null, 2));

            if (json.code === 0 && json.data) {
                console.log('\n--- Results ---');
                for (const [uidStr, info] of Object.entries(json.data)) {
                    const member = MEMBERS.find(m => m.bilibiliUid === uidStr);
                    const name = member ? member.name : `Unknown(${uidStr})`;

                    console.log(`[${name}] Status: ${info.live_status} (1=Live) | Title: ${info.title} | Room: ${info.room_id}`);
                }
            } else {
                console.warn('API returned error code or no data.');
            }
        } else {
            console.error('Fetch failed with status:', res.statusText);
        }

    } catch (e) {
        console.error('Batch Check Failed:', e);
    }
}

checkBatchBilibili();
