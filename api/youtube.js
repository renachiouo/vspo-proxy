// /api/youtube.js

// 這是我們代理函式的主體
export default async function handler(request, response) {
  // 從環境變數中安全地讀取您的 API 金鑰
  // process.env.YOUTUBE_API_KEY_1 是我們稍後會在 Vercel 網站上設定的變數名稱
  const apiKey = process.env.YOUTUBE_API_KEY_1;

  if (!apiKey) {
    // 如果伺服器上沒有設定 API 金鑰，回傳錯誤
    return response.status(500).json({ error: 'API key not configured on server.' });
  }

  // 從前端請求的 URL 中獲取它想要查詢的 YouTube API 端點
  // 例如，前端會請求 /api/youtube?endpoint=search&q=...
  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const endpoint = searchParams.get('endpoint'); // "search", "videos", "channels"
  
  // 移除我們自訂的 endpoint 參數，剩下的參數直接轉發給 YouTube
  searchParams.delete('endpoint');

  // 組合出真正要發送給 YouTube 的 API 網址
  const youtubeApiUrl = `https://www.googleapis.com/youtube/v3/${endpoint}?${searchParams.toString()}&key=${apiKey}`;

  try {
    // 使用 fetch 向 YouTube API 發送請求
    const youtubeResponse = await fetch(youtubeApiUrl);
    const data = await youtubeResponse.json();

    // 在回傳給前端之前，設定一些重要的標頭
    // 允許任何來源的前端網站向這個代理發送請求
    response.setHeader('Access-Control-Allow-Origin', '*');
    // 設定快取時間為 10 分鐘，避免在短時間內重複向 YouTube 請求相同資料
    response.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');

    // 將從 YouTube 拿到的資料回傳給前端
    response.status(200).json(data);
  } catch (error) {
    // 如果發生錯誤，回傳錯誤訊息
    response.status(500).json({ error: 'Failed to fetch data from YouTube API.' });
  }
}
