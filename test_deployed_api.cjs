
const API_BASE_URL = 'https://vspo-proxy-git-main-renas-projects-c8ce958b.vercel.app/api';

async function testEndpoint() {
    const url = `${API_BASE_URL}/youtube?endpoint=streams&page=1&limit=5`;
    console.log(`Testing URL: ${url}`);

    try {
        const res = await fetch(url);
        const data = await res.json();

        console.log("Status:", res.status);

        if (data.streams) {
            console.log("SUCCESS: API returned 'streams' field.");
            console.log("Stream count:", data.streams.length);
        } else if (data.videos) {
            console.log("FAILURE: API returned 'videos' field instead of 'streams'.");
            console.log("This indicates the backend is OLD and ignores the 'endpoint=streams' parameter.");
        } else {
            console.log("UNKNOWN RESPONSE:", Object.keys(data));
        }

    } catch (e) {
        console.error("Request failed:", e.message);
    }
}

testEndpoint();
