// /api/youtube.js

// 這是我們代理函式的主體
export default async function handler(request, response) {
  // 從環境變數中安全地讀取您的所有 API 金鑰
  const apiKeys = [
    process.env.YOUTUBE_API_KEY_1,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
  ].filter(key => key); // 過濾掉未設定的金鑰

  if (apiKeys.length === 0) {
    return response.status(500).json({ error: 'API keys not configured on server.' });
  }

  // 從前端請求的 URL 中獲取它想要查詢的 YouTube API 端點
  const { searchParams } = new URL(request.url, `http://${request.headers.host}`);
  const endpoint = searchParams.get('endpoint'); // "search", "videos", "channels"
  
  // 移除我們自訂的 endpoint 參數，剩下的參數直接轉發給 YouTube
  searchParams.delete('endpoint');

  // 嘗試使用每一組金鑰，直到成功為止
  for (const apiKey of apiKeys) {
    const youtubeApiUrl = `https://www.googleapis.com/youtube/v3/${endpoint}?${searchParams.toString()}&key=${apiKey}`;

    try {
      const youtubeResponse = await fetch(youtubeApiUrl);
      const data = await youtubeResponse.json();

      // 檢查是否為配額錯誤
      if (data.error && data.error.message.toLowerCase().includes('quota')) {
        console.warn(`Key starting with ${apiKey.substring(0, 8)}... has exceeded its quota. Trying next key.`);
        continue; // 如果是配額錯誤，就繼續循環，嘗試下一個金鑰
      }

      // 如果是其他錯誤，直接回傳
      if (data.error) {
        throw new Error(data.error.message);
      }
      
      // 成功！設定標頭並回傳資料
      response.setHeader('Access-Control-Allow-Origin', '*');
      response.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate'); // 10分鐘快取
      return response.status(200).json(data);

    } catch (error) {
      // 捕捉網路錯誤等問題
      console.error(`Error with key starting with ${apiKey.substring(0, 8)}...`, error);
      // 繼續嘗試下一個金鑰
    }
  }

  // 如果所有金鑰都嘗試失敗了
  return response.status(503).json({ error: 'All API keys have exceeded their quotas or failed.' });
}
