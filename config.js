// --- VSPO Proxy Configuration Constants ---

export const SPECIAL_KEYWORDS = ["許諾番号"];
export const FOREIGN_SEARCH_KEYWORDS = ["ぶいすぽ 切り抜き"];
// Removed FOREIGN_SPECIAL_KEYWORDS as requested, using SPECIAL_KEYWORDS universally
export const SEARCH_KEYWORDS = ["VSPO中文", "VSPO精華", "VSPO剪輯"];
export const KEYWORD_BLACKLIST = ["MMD"];

export const VSPO_MEMBER_KEYWORDS = [
    "花芽すみれ", "花芽なずな", "小雀とと", "一ノ瀬うるは", "胡桃のあ", "兎咲ミミ", "空澄セナ", "橘ひなの", "英リサ", "如月れん", "神成きゅぴ", "八雲べに", "藍沢エマ", "紫宮るな", "猫汰つな", "白波らむね", "小森めと", "夢野あかり", "夜乃くろむ", "紡木こかげ", "千燈ゆうひ", "蝶屋はなび", "甘結もか", "銀城サイネ", "龍巻ちせ",
    "Remia", "Arya", "Jira", "Narin", "Riko", "Eris", "Juno",
    "小針彩", "白咲露理", "帕妃", "千郁郁", "日向晴",
    "ひなーの", "ひなの", "べに", "つな", "らむち", "らむね", "めと", "なずな", "なずぴ", "すみー", "すみれ", "ととち", "とと", "のせ", "うるは", "のあ", "ミミ", "たや", "セナ", "あしゅみ", "リサ", "れん", "きゅぴ", "エマたそ", "るな", "あかり", "あかりん", "くろむ", "こかげ", "つむお", "うひ", "ゆうひ", "はなび", "もか", "サイネ", "ちせ", "ちーたま", "ちいたま"
];

export const VSPO_MEMBERS = [
    { name: "花芽すみれ", ytId: "UCyLGcqYs7RsBb3L0SJfzGYA", twitchId: "695556933" },
    { name: "花芽なずな", ytId: "UCiMG6VdScBabPhJ1ZtaVmbw", twitchId: "790167759" },
    { name: "小雀とと", ytId: "UCgTzsBI0DIRopMylJEDqnog", twitchId: "" },
    { name: "一ノ瀬うるは", ytId: "UC5LyYg6cCA4yHEYvtUsir3g", twitchId: "582689327" },
    { name: "胡桃のあ", ytId: "UCIcAj6WkJ8vZ7DeJVgmeqKw", twitchId: "600770697" },
    { name: "兎咲ミミ", ytId: "UCnvVG9RbOW3J6Ifqo-zKLiw", twitchId: "" },
    { name: "空澄セナ", ytId: "UCF_U2GCKHvDz52jWdizppIA", twitchId: "776751504" },
    { name: "橘ひなの", ytId: "UCvUc0m317LWTTPZoBQV479A", twitchId: "568682215" },
    { name: "英リサ", ytId: "UCurEA8YoqFwimJcAuSHU0MQ", twitchId: "777700650" },
    { name: "如月れん", ytId: "UCGWa1dMU_sDCaRQjdabsVgg", twitchId: "722162135" },
    { name: "神成きゅぴ", ytId: "UCMp55EbT_ZlqiMS3lCj01BQ", twitchId: "550676410" },
    { name: "八雲べに", ytId: "UCjXBuHmWkieBApgBhDuJMMQ", twitchId: "700465409" },
    { name: "藍沢エマ", ytId: "UCPkKpOHxEDcwmUAnRpIu-Ng", twitchId: "848822715" },
    { name: "紫宮るな", ytId: "UCD5W21JqNMv_tV9nfjvF9sw", twitchId: "773185713" },
    { name: "猫汰つな", ytId: "UCIjdfjcSaEgdjwbgjxC3ZWg", twitchId: "858359105" },
    { name: "白波らむね", ytId: "UC61OwuYOVuKkpKnid-43Twg", twitchId: "858359149" },
    { name: "小森めと", ytId: "UCzUNASdzI4PV5SlqtYwAkKQ", twitchId: "801682194" },
    { name: "夢野あかり", ytId: "UCS5l_Y0oMVTjEos2LuyeSZQ", twitchId: "584184005" },
    { name: "夜乃くろむ", ytId: "UCX4WL24YEOUYd7qDsFSLDOw", twitchId: "1250148772" },
    { name: "紡木こかげ", ytId: "UC-WX1CXssCtCtc2TNIRnJzg", twitchId: "1184405770" },
    { name: "千燈ゆうひ", ytId: "UCuDY3ibSP2MFRgf7eo3cojg", twitchId: "1097252496" },
    { name: "蝶屋はなび", ytId: "UCL9hJsdk9eQa0IlWbFB2oRg", twitchId: "1361841459" },
    { name: "甘結もか", ytId: "UC8vKBjGY2HVfbW9GAmgikWw", twitchId: "" },
    { name: "銀城サイネ", ytId: "UC2xXx1m1jeL0W84_0jTg-Yw", twitchId: "1476573725" },
    { name: "龍巻ちせ", ytId: "UCoW8qQy80mKH0RJTKAK-nNA", twitchId: "" },
    { name: "ぶいすぽっ!【公式】", ytId: "UCuI5XaO-6VkOEhHao6ij7JA", twitchId: "" },
    { name: "Remia Aotsuki", ytId: "UCCra1t-eIlO3ULyXQQMD9Xw", twitchId: "1102206195" },
    { name: "Arya Kuroha", ytId: "UCLlJpxXt6L5d-XQ0cDdIyDQ", twitchId: "1102211983" },
    { name: "Jira Jisaki", ytId: "UCeCWj-SiJG9SWN6wGORiLmw", twitchId: "1102212264" },
    { name: "Narin Mikure", ytId: "UCKSpM183c85d5V2cW5qaUjA", twitchId: "1125214436" },
    { name: "Riko Solari", ytId: "UC7Xglp1fske9zmRe7Oj8YyA", twitchId: "1125216387" },
    { name: "Eris Suzukami", ytId: "UCp_3ej2br9l9L1DSoHVDZGw", twitchId: "" },
    { name: "Juno Umezono", ytId: "UCRJV_1aV4aZjFAEUn6o5arw", twitchId: "" },
    // CN Members (Bilibili Only)
    // bilibiliId = Live Room ID (for Live Status), bilibiliUid = Member ID (for Archives)
    { name: "小針彩", ytId: "", twitchId: "", bilibiliId: "1972360561", bilibiliUid: "3546695948306751", customAvatarUrl: "https://i0.hdslb.com/bfs/face/ccee4b98198a72f5de3a8174f42431bdee357270.jpg" },
    { name: "白咲露理", ytId: "", twitchId: "", bilibiliId: "1842209652", bilibiliUid: "3546695864421312", customAvatarUrl: "https://i0.hdslb.com/bfs/face/99aa887c27725e4d1dcf2ea071f04d8b29f457d4.jpg" },
    { name: "帕妃", ytId: "", twitchId: "", bilibiliId: "1742801253", bilibiliUid: "3546695946209651", customAvatarUrl: "https://i2.hdslb.com/bfs/face/b9915ddaa2d7f1b4279d516d77207bef9cc31856.jpg" },
    { name: "千郁郁", ytId: "", twitchId: "", bilibiliId: "1996441034", bilibiliUid: "3546695956695430", customAvatarUrl: "https://i1.hdslb.com/bfs/face/f9784adb001568cdc8f73f3435c0d5658af98c28.jpg" },
    { name: "日向晴", ytId: "", twitchId: "", bilibiliId: "1833448662", bilibiliUid: "3546860864146139", customAvatarUrl: "https://i2.hdslb.com/bfs/face/39e4bb7ddf7330bcf11fd6c06f8428d8ad0f0f26.jpg" },
];
