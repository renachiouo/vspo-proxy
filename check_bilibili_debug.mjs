

const UID = '1742801253';

async function checkBilibiliLive() {
    console.log(`Checking Bilibili Live Status for UID: ${UID}`);

    try {
        // Replicating worker.js logic exactly
        const url = `https://api.bilibili.com/room/v1/Room/get_info?room_id=${UID}`;
        console.log(`Querying: ${url}`);

        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': `https://live.bilibili.com/${UID}`
            }
        });

        const data = await res.json();
        // console.log('API Response:', JSON.stringify(data, null, 2));

        if (data.code === 0 && data.data) {
            const liveRoom = data.data.live_room;
            if (liveRoom) {
                console.log(`\nResults for ${UID}:`);
                console.log(`Live Status: ${liveRoom.liveStatus} (1=Live)`);
                console.log(`Title: ${liveRoom.title}`);
                console.log(`URL: ${liveRoom.url}`);
                console.log(`RoomID: ${liveRoom.roomid}`);
            } else {
                console.log('No live_room info found.');
            }
        } else {
            console.log('API Error or No Data:', data.message || data);
        }

    } catch (e) {
        console.error('Error:', e);
    }
}

checkBilibiliLive();
