// Variational Funding Helper - content script（隔离世界）
// 说明：
// - 不能直接覆盖页面的 fetch/XHR（隔离世界限制）
// - 这里负责注入 page_hook.js 到页面上下文，并接收 postMessage 回传的候选，写入 storage

(() => {
  const STORAGE_KEY = "variational.funding_candidates";
  const MAX_CANDIDATES = 20;
  const TRADE_KEY = "variational.api_traces";
  const MAX_TRACES = 30;
  const WS_KEY = "variational.ws_candidates";
  const MAX_WS = 20;

  function injectPageHook() {
    try {
      const src = chrome.runtime.getURL("page_hook.js");
      const script = document.createElement("script");
      script.src = src;
      script.async = false;
      (document.documentElement || document.head || document.body).appendChild(script);
      script.parentNode && script.parentNode.removeChild(script);
    } catch {
      // ignore
    }
  }

  async function loadCandidates() {
    const data = await chrome.storage.local.get([STORAGE_KEY]);
    const arr = data[STORAGE_KEY];
    return Array.isArray(arr) ? arr : [];
  }

  async function loadTraces() {
    const data = await chrome.storage.local.get([TRADE_KEY]);
    const arr = data[TRADE_KEY];
    return Array.isArray(arr) ? arr : [];
  }

  async function loadWsCandidates() {
    const data = await chrome.storage.local.get([WS_KEY]);
    const arr = data[WS_KEY];
    return Array.isArray(arr) ? arr : [];
  }

  async function saveCandidate(candidate) {
    const list = await loadCandidates();
    const sig = JSON.stringify({
      url: candidate.url,
      method: candidate.method,
      json_path: candidate.json_path
    });
    const exists = list.some((c) => {
      try {
        return JSON.stringify({ url: c.url, method: c.method, json_path: c.json_path }) === sig;
      } catch {
        return false;
      }
    });
    if (exists) return;
    list.unshift(candidate);
    while (list.length > MAX_CANDIDATES) list.pop();
    await chrome.storage.local.set({ [STORAGE_KEY]: list });
  }

  async function saveTrace(trace) {
    const list = await loadTraces();
    const sig = JSON.stringify({
      op: trace.op,
      method: trace.method,
      url: trace.url
    });
    const exists = list.some((t) => {
      try {
        return JSON.stringify({ op: t.op, method: t.method, url: t.url }) === sig;
      } catch {
        return false;
      }
    });
    if (exists) return;
    list.unshift(trace);
    while (list.length > MAX_TRACES) list.pop();
    await chrome.storage.local.set({ [TRADE_KEY]: list });
  }

  async function saveWsCandidate(candidate) {
    const list = await loadWsCandidates();
    const sig = JSON.stringify({
      url: candidate.url,
      protocols: candidate.protocols || null,
      kind: candidate.kind || "open"
    });
    let idx = -1;
    for (let i = 0; i < list.length; i++) {
      try {
        const c = list[i];
        const s2 = JSON.stringify({ url: c.url, protocols: c.protocols || null, kind: c.kind || "open" });
        if (s2 === sig) { idx = i; break; }
      } catch {}
    }
    if (idx >= 0) {
      // 同一条 WS 连接可能会多次 send/receive；这里保留“更有信息量”的 sample
      const old = list[idx] || {};
      const oldSample = old.sample ? String(old.sample) : "";
      const newSample = candidate.sample ? String(candidate.sample) : "";
      if (!oldSample && newSample) old.sample = newSample;
      // 若新 sample 更长（且不是 <redacted>），也用新 sample 覆盖旧的
      if (newSample && newSample !== "<redacted>" && newSample.length > oldSample.length) {
        old.sample = newSample;
      }
      old.ts = candidate.ts || old.ts || Date.now();
      list[idx] = old;
    } else {
      list.unshift(candidate);
    }
    while (list.length > MAX_WS) list.pop();
    await chrome.storage.local.set({ [WS_KEY]: list });
  }

  window.addEventListener("message", async (event) => {
    try {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.__vh_type !== "VARIATIONAL_FUNDING_CANDIDATE") return;
      if (!data.payload) return;
      await saveCandidate(data.payload);
    } catch {
      // ignore
    }
  });

  window.addEventListener("message", async (event) => {
    try {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.__vh_type !== "VARIATIONAL_API_TRACE") return;
      if (!data.payload) return;
      await saveTrace(data.payload);
    } catch {
      // ignore
    }
  });

  window.addEventListener("message", async (event) => {
    try {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.__vh_type !== "VARIATIONAL_WS_CANDIDATE") return;
      if (!data.payload) return;
      await saveWsCandidate(data.payload);
    } catch {
      // ignore
    }
  });

  injectPageHook();
})();


