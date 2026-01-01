async function run() {
    const channelId = 'UC-WX1CXssCtCtc2TNIRnJzg'; // Kokage
    const url = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;

    try {
        const res = await fetch(url);
        const text = await res.text();
        console.log(`Checking RSS for ${channelId}...`);

        // Check for specific Upcoming ID
        const videoId = 'q-oY2st-_qo';
        if (text.includes(videoId)) {
            console.log(`✅ FOUND in RSS: ${videoId}`);
        } else {
            console.log(`❌ NOT FOUND in RSS: ${videoId}`);
        }

    } catch (e) {
        console.error(e);
    }
}
run();
