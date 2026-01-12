
const urls = [
    'https://vspo-proxy-git-main-renas-projects.vercel.app/api', // Likely "Latest on Main"
    'https://vspo-proxy.vercel.app/api', // Likely "Production"
];

async function testUrl(baseUrl) {
    const url = `${baseUrl}/youtube?endpoint=streams&page=1&limit=5`;
    console.log(`\nTesting: ${baseUrl}`);

    try {
        const res = await fetch(url);
        // If 404, it might be the wrong project URL
        if (!res.ok) {
            console.log(`HTTP ${res.status}: ${res.statusText}`);
            return;
        }

        const data = await res.json();

        if (data.streams) {
            console.log("✅ SUCCESS! This URL is serving the NEW code (returned 'streams').");
            console.log("Stream count:", data.streams.length);
        } else if (data.videos) {
            console.log("❌ FAILURE: This URL is serving OLD code (returned 'videos' fallback).");
        } else {
            console.log("❓ UNKNOWN: Returned keys:", Object.keys(data));
        }
    } catch (e) {
        console.error("Connection failed:", e.message);
    }
}

async function run() {
    for (const url of urls) {
        await testUrl(url);
    }
}

run();
