
const fetch = globalThis.fetch;

// The user's URL without the hash part (c8ce958b)
// This is the Vercel "Branch URL" which always points to the latest deployment on main.
const cleanUrl = 'https://vspo-proxy-git-main-renas-projects.vercel.app/api';

async function testUrl() {
    const url = `${cleanUrl}/youtube?endpoint=streams&page=1&limit=5`;
    console.log(`Testing Clean URL: ${cleanUrl}`);

    try {
        const res = await fetch(url);
        if (!res.ok) {
            console.log(`HTTP Error: ${res.status}`);
            return;
        }

        const data = await res.json();

        if (data.streams) {
            console.log("✅ SUCCESS! This URL returns 'streams'. This is the correct Latest URL.");
        } else {
            console.log("❌ FAILURE: This URL also returns old data (videos).");
            console.log("Keys:", Object.keys(data));
        }
    } catch (e) {
        console.error("Error:", e.message);
    }
}

testUrl();
