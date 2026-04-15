import 'dotenv/config';

async function fetchYT(endpoint, params) {
    const key = process.env.YOUTUBE_API_KEY_1;
    const url = https://www.googleapis.com/youtube/v3/?&key=;
    const res = await fetch(url);
    return await res.json();
}

async function main() {
    console.log('--- Checking EeyhrliRANI directly ---');
    const vRes = await fetchYT('videos', { part: 'snippet,contentDetails', id: 'EeyhrliRANI' });
    if (!vRes.items || vRes.items.length === 0) {
        console.log('ERROR: Video completely missing from YouTube API!');
        return;
    }
    const v = vRes.items[0];
    console.log(Video Title: );
    console.log(Published At: );

    console.log('\n--- Checking Channel UCPK8tMKReXvewGOz7d0zS9g Top Uploads ---');
    const chRes = await fetchYT('channels', { part: 'contentDetails', id: 'UCPK8tMKReXvewGOz7d0zS9g' });
    const uploadsId = chRes.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
    console.log(Uploads Playlist ID: );

    if (uploadsId) {
        const plRes = await fetchYT('playlistItems', { part: 'snippet', playlistId: uploadsId, maxResults: 15 });
        console.log('\nTop 15 videos in uploads playlist:');
        const ids = [];
        for (let i = 0; i < (plRes.items?.length || 0); i++) {
            const item = plRes.items[i];
            const vId = item.snippet.resourceId.videoId;
            ids.push(vId);
            console.log(${ i+ 1}.[](Pub: ));
    }
    console.log(\nIs EeyhrliRANI in top 10 ? -> );
    console.log(Is EeyhrliRANI in top 15 ? -> );
}
}

main().catch(console.error);
