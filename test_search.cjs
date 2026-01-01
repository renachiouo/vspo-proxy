const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const apiKey = process.env.YOUTUBE_API_KEY_1;

async function run() {
    const channelId = 'UCPkKpOHxEDcwmUAnRpIu-Ng'; // Emma
    console.log(`Checking Search for ${channelId}...`);

    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channelId}&eventType=live&type=video&key=${apiKey}`;
    try {
        const res = await fetch(url);
        const data = await res.json();

        console.log("Total Results:", data.pageInfo?.totalResults);
        if (data.items) {
            data.items.forEach(item => {
                console.log(`Found: ${item.id.videoId} - ${item.snippet.title}`);
            });
        } else {
            console.log("No items found. Raw:", JSON.stringify(data));
        }

    } catch (e) {
        console.error(e);
    }
}
run();
