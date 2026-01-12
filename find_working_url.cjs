
const fetch = globalThis.fetch;

const candidates = [
    'https://vspo-proxy.vercel.app/api',
    'https://vspo-proxy-renachiouo.vercel.app/api',
    'https://vspo-proxy-git-main-renachiouo.vercel.app/api',
    'https://vspo-proxy-renas-projects.vercel.app/api',
    'https://vspo-proxy-git-main-renas-projects.vercel.app/api'
];

async function checkUrl(baseUrl) {
    const testUrl = `${baseUrl}/youtube?endpoint=streams&page=1&limit=5`;
    console.log(`Checking: ${baseUrl}`);
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);

        const res = await fetch(testUrl, { signal: controller.signal });
        clearTimeout(timeoutId);

        if (res.ok) {
            const data = await res.json();
            if (data.streams) {
                console.log(`\n🎉 FOUND WORKING URL: ${baseUrl}`);
                console.log(`Returns ${data.streams.length} streams.`);
                return baseUrl;
            } else if (data.videos) {
                console.log(`  -> Alive, but OLD version (returned videos).`);
            } else {
                console.log(`  -> Alive, but unknown response.`);
            }
        } else {
            console.log(`  -> HTTP ${res.status}`);
        }
    } catch (e) {
        console.log(`  -> Failed: ${e.message}`);
    }
    return null;
}

async function run() {
    console.log("Scanning for valid Vercel deployments...");
    for (const url of candidates) {
        const result = await checkUrl(url);
        if (result) process.exit(0); // Found it
    }
    console.log("\nNo working 'streams' endpoint found in standard patterns.");
}

run();
