// Variational Token Helper - MV3 service worker
// 说明：
// - 监听 omni.variational.io 的请求头
// - 捕捉 Authorization: Bearer <token>（以及可选 Cookie）
// - 保存到 chrome.storage.local（仅本机，不外发）

const STORAGE_KEYS = {
  token: "variational.latest_bearer_token",
  cookie: "variational.latest_cookie_header",
  userAgent: "variational.latest_user_agent",
  lastSeenAt: "variational.last_seen_at_ms",
  lastUrl: "variational.last_seen_url"
};

function normalizeHeaderName(name) {
  return String(name || "").trim().toLowerCase();
}

function extractBearerToken(headers) {
  if (!Array.isArray(headers)) return null;
  for (const h of headers) {
    if (normalizeHeaderName(h.name) !== "authorization") continue;
    const v = String(h.value || "").trim();
    const prefix = "bearer ";
    if (v.toLowerCase().startsWith(prefix)) {
      return v.slice(prefix.length).trim();
    }
  }
  return null;
}

function extractCookieHeader(headers) {
  if (!Array.isArray(headers)) return null;
  for (const h of headers) {
    if (normalizeHeaderName(h.name) !== "cookie") continue;
    const v = String(h.value || "").trim();
    return v || null;
  }
  return null;
}

function extractVrTokenFromCookie(cookieHeader) {
  const s = String(cookieHeader || "").trim();
  if (!s) return null;
  // cookie 形如: a=1; vr-token=eyJ...; b=2
  const parts = s.split(";").map((p) => p.trim()).filter(Boolean);
  for (const p of parts) {
    const idx = p.indexOf("=");
    if (idx <= 0) continue;
    const k = p.slice(0, idx).trim().toLowerCase();
    const v = p.slice(idx + 1).trim();
    if (k === "vr-token" && v) return v;
  }
  return null;
}

function extractUserAgent(headers) {
  if (!Array.isArray(headers)) return null;
  for (const h of headers) {
    if (normalizeHeaderName(h.name) !== "user-agent") continue;
    const v = String(h.value || "").trim();
    return v || null;
  }
  return null;
}

chrome.webRequest.onBeforeSendHeaders.addListener(
  async (details) => {
    try {
      const url = String(details.url || "");
      // 只关心 Variational Omni 域名下的 API 请求（用于捕捉 token/cookie）
      if (!url.includes("omni.variational.io/")) return;
      if (!url.includes("/api/")) return;

      const headers = details.requestHeaders || [];
      const token = extractBearerToken(headers);
      const cookie = extractCookieHeader(headers);
      const userAgent = extractUserAgent(headers);
      const cookieToken = token ? null : extractVrTokenFromCookie(cookie);

      const payload = {
        [STORAGE_KEYS.lastSeenAt]: Date.now(),
        [STORAGE_KEYS.lastUrl]: url
      };
      if (token) payload[STORAGE_KEYS.token] = token;
      else if (cookieToken) payload[STORAGE_KEYS.token] = cookieToken;
      if (cookie) payload[STORAGE_KEYS.cookie] = cookie;
      if (userAgent) payload[STORAGE_KEYS.userAgent] = userAgent;

      // 只有真的捕捉到 token/cookie 才写入，避免每个 API 请求都刷 storage
      if (payload[STORAGE_KEYS.token] || payload[STORAGE_KEYS.cookie] || payload[STORAGE_KEYS.userAgent]) {
        await chrome.storage.local.set(payload);
      }
    } catch (e) {
      // service worker 里不要刷屏；必要时可打开扩展的“检查视图”看日志
    }
  },
  {
    urls: ["https://omni.variational.io/*"]
  },
  // MV3：取请求头需要 requestHeaders；部分场景需要 extraHeaders 才能读到 Authorization
  ["requestHeaders", "extraHeaders"]
);


