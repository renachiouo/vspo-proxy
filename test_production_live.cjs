
const fetch = globalThis.fetch;
const url = 'https://vspo-proxy.vercel.app/api/youtube?endpoint=streams&page=1&limit=5';

async function testLive() {
    console.log(`Fetching: ${url}`);
    try {
        const res = await fetch(url);
        console.log(`Status: ${res.status}`);

        if (!res.ok) {
            console.log("Error body:", await res.text());
            return;
        }

        const data = await res.json();
        console.log("Response Keys:", Object.keys(data));

        if (data.streams) {
            console.log(`Streams Count: ${data.streams.length}`);
            if (data.streams.length > 0) {
                console.log("Sample Stream:", JSON.stringify(data.streams[0], null, 2));
            } else {
                console.log("STREAMS ARRAY IS EMPTY!");
            }
        } else if (data.videos) {
            console.log("FAIL: Returned 'videos'. The deployment is OLD or Rewrite/Routing is failing.");
        } else {
            console.log("FAIL: Unknown response format.");
        }

    } catch (e) {
        console.error("Fetch Error:", e);
    }
}

testLive();
