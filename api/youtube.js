// /api/youtube.js (Diagnostic Version)

export default async function handler(request, response) {
  // 建立一個安全的物件來存放環境變數以供日誌記錄
  // 我們將過濾與 Vercel、Redis、KV 及我們的 YouTube 金鑰相關的鍵
  const relevantEnvVars = {};
  for (const key in process.env) {
    if (
      key.startsWith('VERCEL_') || 
      key.startsWith('KV_') || 
      key.startsWith('REDIS_') || 
      key.startsWith('UPSTASH_') || 
      key.startsWith('YOUTUBE_')
    ) {
      // 為了安全，我們只顯示值的開頭部分
      const value = process.env[key];
      relevantEnvVars[key] = value ? `${value.substring(0, 8)}...` : 'Not Set';
    }
  }

  // 新增一條訊息來指示是否找到了關鍵變數
  relevantEnvVars.DIAGNOSTIC_INFO = {
    HAS_REDIS_URL: !!process.env.REDIS_URL,
    HAS_UPSTASH_REDIS_REST_URL: !!process.env.UPSTASH_REDIS_REST_URL,
    HAS_KV_URL: !!process.env.KV_URL,
  };

  response.setHeader('Access-Control-Allow-Origin', '*');
  response.setHeader('Content-Type', 'application/json');
  
  // 將所有相關的環境變數作為 JSON 回應傳回
  return response.status(200).json({
    message: "This is a diagnostic response. Please copy this entire JSON output and paste it back to the assistant.",
    environment_variables: relevantEnvVars
  });
}
