const STORAGE_KEYS = {
  token: "variational.latest_bearer_token",
  cookie: "variational.latest_cookie_header",
  userAgent: "variational.latest_user_agent",
  lastSeenAt: "variational.last_seen_at_ms",
  lastUrl: "variational.last_seen_url",
  fundingCandidates: "variational.funding_candidates",
  tradeTraces: "variational.api_traces",
  wsCandidates: "variational.ws_candidates"
};

function $(id) {
  return document.getElementById(id);
}

function fmtTime(ms) {
  if (!ms || typeof ms !== "number") return "-";
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return String(ms);
  }
}

async function load() {
  const data = await chrome.storage.local.get([
    STORAGE_KEYS.token,
    STORAGE_KEYS.cookie,
    STORAGE_KEYS.userAgent,
    STORAGE_KEYS.lastSeenAt,
    STORAGE_KEYS.lastUrl,
    STORAGE_KEYS.fundingCandidates,
    STORAGE_KEYS.tradeTraces,
    STORAGE_KEYS.wsCandidates
  ]);
  $("token").value = data[STORAGE_KEYS.token] || "";
  $("cookie").value = data[STORAGE_KEYS.cookie] || "";
  const uaEl = $("userAgent");
  if (uaEl) uaEl.value = data[STORAGE_KEYS.userAgent] || "";
  $("lastUrl").textContent = data[STORAGE_KEYS.lastUrl] || "-";
  $("lastSeen").textContent = fmtTime(data[STORAGE_KEYS.lastSeenAt]);

  const candidates = Array.isArray(data[STORAGE_KEYS.fundingCandidates])
    ? data[STORAGE_KEYS.fundingCandidates]
    : [];
  renderFundingCandidates(candidates);

  const traces = Array.isArray(data[STORAGE_KEYS.tradeTraces])
    ? data[STORAGE_KEYS.tradeTraces]
    : [];
  renderTradeTraces(traces);

  const wsCandidates = Array.isArray(data[STORAGE_KEYS.wsCandidates])
    ? data[STORAGE_KEYS.wsCandidates]
    : [];
  renderWsCandidates(wsCandidates);
}

function renderFundingCandidates(candidates) {
  const el = $("fundingList");
  if (!el) return;
  if (!candidates.length) {
    el.textContent = "暂无候选。请在页面里切换币种/刷新，触发资金费率相关请求。";
    return;
  }
  const lines = candidates.slice(0, 8).map((c, idx) => {
    const url = c.url || "-";
    const method = c.method || "GET";
    const path = Array.isArray(c.json_path) ? JSON.stringify(c.json_path) : "[]";
    const sample = (c.sample !== undefined) ? String(c.sample) : "-";
    return `${idx + 1}. ${method} ${url}\n   json_path: ${path}\n   sample: ${sample}`;
  });
  el.textContent = lines.join("\n\n");
}

function buildFundingYamlFromFirst(candidates) {
  if (!candidates || !candidates.length) return "";
  const c = candidates[0];
  const url = c.url || "";
  const method = (c.method || "GET").toUpperCase();
  const jsonPath = Array.isArray(c.json_path) ? c.json_path : [];
  return [
    "funding_rate_source:",
    `  method: "${method}"`,
    `  url: "${url}"`,
    "  body: null",
    "  headers: {}",
    `  json_path: ${JSON.stringify(jsonPath)}`,
    "  multiplier: 1",
    ""
  ].join("\n");
}

function renderTradeTraces(traces) {
  const el = $("tradeList");
  if (!el) return;
  if (!traces.length) {
    el.textContent = "暂无候选。请在页面里触发一次下单/撤单/打开钱包/查看仓位等操作。";
    return;
  }
  const lines = traces.slice(0, 8).map((t, idx) => {
    const op = t.op || "unknown";
    const method = t.method || "GET";
    const url = t.url || "-";
    const body = t.body ? JSON.stringify(t.body) : "null";
    return `${idx + 1}. [${op}] ${method} ${url}\n   body: ${body}`;
  });
  el.textContent = lines.join("\n\n");
}

function renderWsCandidates(candidates) {
  const el = $("wsList");
  if (!el) return;
  if (!candidates.length) {
    el.textContent = "暂无候选。请刷新页面或切换币种，看是否会建立行情 WebSocket。";
    return;
  }
  const lines = candidates.slice(0, 12).map((c, idx) => {
    const kind = c.kind || "open";
    const url = c.url || "-";
    const prot = Array.isArray(c.protocols) ? JSON.stringify(c.protocols) : (c.protocols ? String(c.protocols) : "null");
    const key = c.key ? ` key=${c.key}` : "";
    const jsonPath = Array.isArray(c.json_path) ? ` json_path=${JSON.stringify(c.json_path)}` : "";
    const sample = (c.sample !== undefined) ? ` sample=${String(c.sample)}` : "";
    return `${idx + 1}. [${kind}] ${url}\n   protocols: ${prot}${key}${jsonPath}${sample}`;
  });
  el.textContent = lines.join("\n\n");
}

function buildPortfolioWsYaml(candidates) {
  if (!candidates || !candidates.length) return "";
  // 找 portfolio 的 send（通常是 claims）
  const send = candidates.find((c) => {
    const url = String(c.url || "");
    const kind = String(c.kind || "");
    return kind === "send" && url.includes("/portfolio");
  });
  if (!send || !send.sample) return "";
  const sample = String(send.sample || "").trim();
  // sample 可能是 JSON 字符串（我们会把 claims 替换成 {{token}}）
  let msgObj = null;
  try { msgObj = JSON.parse(sample); } catch { msgObj = null; }
  const msgYaml = msgObj ? JSON.stringify(msgObj) : JSON.stringify(sample);
  return [
    "extra_params:",
    "  # portfolio WS 连接后发送的订阅/初始化消息（从扩展抓取）",
    "  ws_portfolio_on_connect_messages:",
    `    - ${msgYaml}`,
    ""
  ].join("\n");
}

function buildTradeYamlFromFirst(traces) {
  if (!traces || !traces.length) return "";
  const t = traces[0];
  const op = String(t.op || "unknown").trim() || "unknown";
  const tpl = t.template || null;
  if (!tpl) return "";
  const key = (() => {
    if (op === "order_create") {
      const u = String(tpl.url || "").toLowerCase();
      if (u.includes("/market")) return "order_create_market_template";
      if (u.includes("market")) return "order_create_market_template";
      return "order_create_market_template";
    }
    if (op === "order_cancel") return "order_cancel_template";
    if (op === "positions") return "positions_template";
    if (op === "balances") return "balances_template";
    if (op === "order_get_or_open") {
      const u = String(tpl.url || "").toLowerCase();
      // 常见：status=pending / status=open 这类更像“开放订单查询”
      if (u.includes("status=pending") || u.includes("status=open") || u.includes("status=live")) {
        return "orders_open_template";
      }
      // /api/orders/v2 通常为订单列表（更接近 open orders 查询，而不是单笔订单详情）
      if (u.startsWith("/api/orders/v2")) {
        return "orders_open_template";
      }
      return "order_get_template";
    }
    return "custom_template";
  })();
  return [
    "trade:",
    `  ${key}:`,
    `    method: "${String(tpl.method || "GET").toUpperCase()}"`,
    `    url: "${tpl.url || ""}"`,
    "    headers: " + JSON.stringify(tpl.headers || {}),
    "    cookies: {}",
    "    body: " + (tpl.body ? JSON.stringify(tpl.body) : "null"),
    "    timeout_seconds: 10",
    ""
  ].join("\n");
}

async function copyText(text) {
  await navigator.clipboard.writeText(text || "");
}

function buildEnvTwoLines(token, cookieHeader) {
  const safe = String(token || "").replace(/"/g, '\\"');
  const safeCookie = String(cookieHeader || "").replace(/"/g, '\\"');
  // 与 env.template 保持一致：两行 key="value"（不包含 export）
  return `VARIATIONAL_API_KEY="${safe}"\nVARIATIONAL_COOKIE="${safeCookie}"`;
}

function buildExportUserAgentCommand(ua) {
  const safe = String(ua || "").replace(/"/g, '\\"');
  return `export VARIATIONAL_USER_AGENT="${safe}"`;
}

document.addEventListener("DOMContentLoaded", async () => {
  await load();

  $("copyToken").addEventListener("click", async () => {
    await copyText($("token").value.trim());
  });

  $("copyEnv").addEventListener("click", async () => {
    await copyText(buildEnvTwoLines($("token").value.trim(), $("cookie").value.trim()));
  });

  $("copyCookie").addEventListener("click", async () => {
    await copyText($("cookie").value.trim());
  });

  const copyUaBtn = $("copyUserAgent");
  if (copyUaBtn) {
    copyUaBtn.addEventListener("click", async () => {
      await copyText(($("userAgent").value || "").trim());
    });
  }
  const copyUaEnvBtn = $("copyUserAgentEnv");
  if (copyUaEnvBtn) {
    copyUaEnvBtn.addEventListener("click", async () => {
      await copyText(buildExportUserAgentCommand(($("userAgent").value || "").trim()));
    });
  }

  $("clear").addEventListener("click", async () => {
    await chrome.storage.local.remove([
      STORAGE_KEYS.token,
      STORAGE_KEYS.cookie,
      STORAGE_KEYS.userAgent,
      STORAGE_KEYS.lastSeenAt,
      STORAGE_KEYS.lastUrl
    ]);
    await load();
  });

  const fundingBtn = $("copyFundingYaml");
  if (fundingBtn) {
    fundingBtn.addEventListener("click", async () => {
      const data = await chrome.storage.local.get([STORAGE_KEYS.fundingCandidates]);
      const candidates = Array.isArray(data[STORAGE_KEYS.fundingCandidates])
        ? data[STORAGE_KEYS.fundingCandidates]
        : [];
      await copyText(buildFundingYamlFromFirst(candidates));
    });
  }

  const clearFundingBtn = $("clearFunding");
  if (clearFundingBtn) {
    clearFundingBtn.addEventListener("click", async () => {
      await chrome.storage.local.remove([STORAGE_KEYS.fundingCandidates]);
      await load();
    });
  }

  const tradeBtn = $("copyTradeYaml");
  if (tradeBtn) {
    tradeBtn.addEventListener("click", async () => {
      const data = await chrome.storage.local.get([STORAGE_KEYS.tradeTraces]);
      const traces = Array.isArray(data[STORAGE_KEYS.tradeTraces]) ? data[STORAGE_KEYS.tradeTraces] : [];
      await copyText(buildTradeYamlFromFirst(traces));
    });
  }

  const clearTradeBtn = $("clearTrade");
  if (clearTradeBtn) {
    clearTradeBtn.addEventListener("click", async () => {
      await chrome.storage.local.remove([STORAGE_KEYS.tradeTraces]);
      await load();
    });
  }

  const clearWsBtn = $("clearWs");
  if (clearWsBtn) {
    clearWsBtn.addEventListener("click", async () => {
      await chrome.storage.local.remove([STORAGE_KEYS.wsCandidates]);
      await load();
    });
  }

  const copyPortfolioBtn = $("copyPortfolioWsYaml");
  if (copyPortfolioBtn) {
    copyPortfolioBtn.addEventListener("click", async () => {
      const data = await chrome.storage.local.get([STORAGE_KEYS.wsCandidates]);
      const wsCandidates = Array.isArray(data[STORAGE_KEYS.wsCandidates])
        ? data[STORAGE_KEYS.wsCandidates]
        : [];
      await copyText(buildPortfolioWsYaml(wsCandidates));
    });
  }
});


