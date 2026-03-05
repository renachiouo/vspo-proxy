# Custom Background Feature (未來功能備註)

> 建立時間：2026-03-03
> 狀態：構想階段，尚未實作

## 背景

目前 `/api/special-event` 已實作自動偵測成員直播標題中的關鍵字（誕生日、生誕、周年、birthday、anniversary），
在活動當天自動將直播封面設為暗化背景。

此備註記錄「**透過 Admin 設定自訂背景圖片**」的擴充構想，用於季節性/臨時性活動（如官方圖片等）。

## 方案概述

### 資料層
在 MongoDB `metadata` collection 新增文件：
```json
{
  "_id": "custom_background",
  "url": "https://example.com/seasonal-image.jpg",
  "label": "春季特別活動",
  "active": true
}
```

### 後端修改 (`api/youtube.js`)

1. `handleAdminAction` 新增兩個 action：
   - `set_custom_background`：設定 url、label、active=true
   - `clear_custom_background`：設定 active=false

2. `/api/special-event` 端點：在回傳前先檢查 `custom_background` 是否 active，
   若有則加入 events 陣列（可與關鍵字偵測的背景一起輪播）

### 使用方式
```
POST /api/youtube
{
  "action": "set_custom_background",
  "password": "ADMIN_PASSWORD",
  "url": "圖片網址",
  "label": "活動說明"
}
```

清除：
```
POST /api/youtube
{ "action": "clear_custom_background", "password": "ADMIN_PASSWORD" }
```

### 前端
不需修改。現有的 `checkSpecialEvent()` 會自動處理 API 回傳的新事件。
