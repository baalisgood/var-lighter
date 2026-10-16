// 注入到页面上下文（非 content-script 隔离世界）
// 目的：hook fetch/XHR，解析 JSON 响应并把 funding 候选通过 postMessage 发给 content_script

(() => {
  const FUNDING_KEY_RE = /(funding|fundingrate|current_funding_rate|predicted_funding)/i;
  const MAX_HITS_PER_RESPONSE = 5;
  const MAX_API_TRACES = 30;

  // WS 捕捉：用于发现是否存在“行情 WS”（bid/ask/mark/funding 等字段）
  const WS_NUM_KEY_RE = /(bid|ask|bbo|best_(bid|ask)|mark|index|price|mid|spread|funding|rate)/i;
  const MAX_WS_CANDIDATES = 20;
  // 多抓一些，且优先抓到“非 heartbeat”的首条有效行情消息
  const MAX_WS_MSG_PER_CONN = 30;

  // 交易/账户接口捕捉（用于生成 trade templates）
  const TRADE_HINT_URL_RE = /(\/api\/.*(order|orders|trade|position|positions|balance|balances|wallet|account|transfer|transfers|portfolio|equity|margin))/i;
  const IGNORE_TRACE_URL_RE = /(\/api\/quotes\/indicative|\/api\/metadata\/)/i;
  const ORDER_BODY_HINT_KEYS = ["side", "qty", "quantity", "size", "amount", "price", "symbol", "underlying"];

  function safeJsonParse(text) {
    try { return JSON.parse(text); } catch { return null; }
  }

  function redactWsUrl(url) {
    // 避免把 auth/token 直接写入候选
    try {
      const u = new URL(String(url || ""), window.location.origin);
      for (const k of ["auth", "token", "claims"]) {
        if (u.searchParams.has(k)) u.searchParams.set(k, "<redacted>");
      }
      return u.toString();
    } catch {
      return String(url || "");
    }
  }

  function redactText(text) {
    const s = String(text || "");
    if (!s) return s;
    // 简单处理 JWT-like：eyJ... 以及过长 token
    // - WS send 里常见 {"claims":"<jwt>"}，这里尽量保留结构但用占位符替换 token
    try {
      const j = safeJsonParse(s);
      if (j && typeof j === "object" && !Array.isArray(j)) {
        if (typeof j.claims === "string" && j.claims.includes("eyJ")) {
          return JSON.stringify({ ...j, claims: "{{token}}" });
        }
      }
    } catch {}
    if (s.includes("eyJ") && s.length > 80) return "<redacted>";
    return s.length > 240 ? (s.slice(0, 240) + "...") : s;
  }

  function toTextMaybe(data, cb) {
    try {
      if (typeof data === "string") return cb(data);
      // ArrayBuffer
      if (data && data instanceof ArrayBuffer) {
        const dec = new TextDecoder("utf-8");
        return cb(dec.decode(new Uint8Array(data)));
      }
      // Uint8Array / TypedArray
      if (data && data.buffer && data.byteLength !== undefined) {
        const dec = new TextDecoder("utf-8");
        return cb(dec.decode(new Uint8Array(data.buffer)));
      }
      // Blob
      if (data && typeof Blob !== "undefined" && data instanceof Blob) {
        const reader = new FileReader();
        reader.onload = () => cb(String(reader.result || ""));
        reader.onerror = () => cb("");
        return reader.readAsText(data);
      }
      return cb("");
    } catch {
      return cb("");
    }
  }

  function toRelativeUrl(url) {
    try {
      const u = new URL(url, window.location.origin);
      return u.pathname + (u.search || "");
    } catch {
      return String(url || "");
    }
  }

  function isLikelyNumber(v) {
    if (typeof v === "number") return Number.isFinite(v);
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return false;
      if (s.length > 64) return false;
      const n = Number(s);
      return Number.isFinite(n);
    }
    return false;
  }

  function scanNumericHints(obj, path, out) {
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) scanNumericHints(obj[i], path.concat([i]), out);
      return;
    }
    for (const [k, v] of Object.entries(obj)) {
      const nextPath = path.concat([k]);
      if (WS_NUM_KEY_RE.test(k) && isLikelyNumber(v)) out.push({ json_path: nextPath, sample: v, key: k });
      if (v && typeof v === "object") scanNumericHints(v, nextPath, out);
    }
  }

  function scanFunding(obj, path, out) {
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) scanFunding(obj[i], path.concat([i]), out);
      return;
    }
    for (const [k, v] of Object.entries(obj)) {
      const nextPath = path.concat([k]);
      if (FUNDING_KEY_RE.test(k) && isLikelyNumber(v)) out.push({ json_path: nextPath, sample: v });
      if (v && typeof v === "object") scanFunding(v, nextPath, out);
    }
  }

  function emitCandidates(meta, json) {
    const hits = [];
    scanFunding(json, [], hits);
    if (!hits.length) return;
    for (const h of hits.slice(0, MAX_HITS_PER_RESPONSE)) {
      window.postMessage({
        __vh_type: "VARIATIONAL_FUNDING_CANDIDATE",
        payload: {
          ts: Date.now(),
          url: toRelativeUrl(meta.url),
          method: meta.method || "GET",
          json_path: h.json_path,
          sample: h.sample
        }
      }, "*");
    }
  }

  function isObject(v) {
    return v && typeof v === "object" && !Array.isArray(v);
  }

  function filterSensitiveHeaders(headers) {
    const out = {};
    if (!headers) return out;
    for (const [kRaw, vRaw] of Object.entries(headers)) {
      const k = String(kRaw || "").toLowerCase();
      if (!k) continue;
      // 不把敏感字段写入候选
      if (k === "authorization") continue;
      if (k === "cookie") continue;
      if (k === "x-api-key") continue;
      if (k.includes("token")) continue;
      if (k.includes("auth")) continue;
      out[String(kRaw)] = String(vRaw);
    }
    return out;
  }

  function guessTradeOp(meta, reqBody) {
    const m = String(meta.method || "GET").toUpperCase();
    const url = toRelativeUrl(meta.url);
    const urlL = url.toLowerCase();
    if (m === "POST" && urlL.includes("order")) return "order_create";
    if ((m === "DELETE" || m === "POST") && urlL.includes("cancel")) return "order_cancel";
    if (m === "GET" && urlL.includes("order")) return "order_get_or_open";
    if (m === "GET" && urlL.includes("position")) return "positions";
    if (
      m === "GET"
      && (
        urlL.includes("balance")
        || urlL.includes("wallet")
        || urlL.includes("account")
        || urlL.includes("transfer")
        || urlL.includes("portfolio")
        || urlL.includes("equity")
        || urlL.includes("margin")
      )
    ) return "balances";
    // body 线索
    if (m === "POST" && isObject(reqBody)) {
      const keys = Object.keys(reqBody).map((k) => k.toLowerCase());
      const hasSide = keys.includes("side") || keys.includes("direction");
      const hasQty = keys.includes("qty") || keys.includes("quantity") || keys.includes("size") || keys.includes("amount");
      if (hasSide && hasQty) return "order_create";
    }
    return "unknown";
  }

  // ==========================================================================
  // WebSocket hook：捕捉页面建立的 WS 连接 + 订阅消息 + 前几条入站消息
  // ==========================================================================
  try {
    const OriginalWebSocket = window.WebSocket;
    const seenWs = new Set();
    const connMsgCount = new WeakMap();

    function emitWs(payload) {
      try {
        window.postMessage({ __vh_type: "VARIATIONAL_WS_CANDIDATE", payload }, "*");
      } catch {
        // ignore
      }
    }

    window.WebSocket = function(url, protocols) {
      const ws = protocols !== undefined
        ? new OriginalWebSocket(url, protocols)
        : new OriginalWebSocket(url);

      try {
        const safeUrl = redactWsUrl(url);
        const prot = protocols ? (Array.isArray(protocols) ? protocols : [String(protocols)]) : null;
        const sig = JSON.stringify({ url: safeUrl, protocols: prot });
        if (!seenWs.has(sig) && seenWs.size < MAX_WS_CANDIDATES) {
          seenWs.add(sig);
          emitWs({
            ts: Date.now(),
            kind: "open",
            url: safeUrl,
            protocols: prot
          });
        }
      } catch {
        // ignore
      }

      // hook send（只记录摘要，避免敏感信息）
      const origSend = ws.send;
      ws.send = function(data) {
        try {
          emitWs({
            ts: Date.now(),
            kind: "send",
            url: redactWsUrl(ws.url),
            protocols: null,
            sample: redactText(typeof data === "string" ? data : "")
          });
        } catch {
          // ignore
        }
        return origSend.apply(ws, arguments);
      };

      ws.addEventListener("message", (evt) => {
        try {
          const n = connMsgCount.get(ws) || 0;
          if (n >= MAX_WS_MSG_PER_CONN) return;
          connMsgCount.set(ws, n + 1);
          toTextMaybe(evt.data, (text) => {
            try {
              const json = safeJsonParse(text);
              if (json && typeof json === "object") {
                // 跳过 heartbeat，继续等待更有信息量的消息
                const t = String(json.type || "").toLowerCase();
                if (t === "heartbeat") return;
                const hits = [];
                scanNumericHints(json, [], hits);
                if (hits.length) {
                  const h = hits[0];
                  emitWs({
                    ts: Date.now(),
                    kind: "message",
                    url: redactWsUrl(ws.url),
                    protocols: null,
                    json_path: h.json_path,
                    sample: h.sample,
                    key: h.key
                  });
                  return;
                }
              }
              // fallback：仅记录摘要（但仍跳过纯 heartbeat 文本）
              if (text && text.includes('"type":"heartbeat"')) return;
              emitWs({
                ts: Date.now(),
                kind: "message",
                url: redactWsUrl(ws.url),
                protocols: null,
                sample: redactText(text)
              });
            } catch {
              // ignore
            }
          });
        } catch {
          // ignore
        }
      });

      return ws;
    };
    window.WebSocket.prototype = OriginalWebSocket.prototype;
    window.WebSocket.OPEN = OriginalWebSocket.OPEN;
    window.WebSocket.CLOSED = OriginalWebSocket.CLOSED;
    window.WebSocket.CLOSING = OriginalWebSocket.CLOSING;
    window.WebSocket.CONNECTING = OriginalWebSocket.CONNECTING;
  } catch {
    // ignore
  }

  function replacePlaceholders(obj) {
    if (!obj) return obj;
    if (Array.isArray(obj)) return obj.map(replacePlaceholders);
    if (!isObject(obj)) return obj;
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      const kl = String(k).toLowerCase();
      if (kl === "symbol" || kl === "market" || kl === "instrument") out[k] = "{{symbol}}";
      else if (kl === "underlying") out[k] = "{{underlying}}";
      else if (kl === "side" || kl === "direction") out[k] = "{{side}}";
      else if (kl === "qty" || kl === "quantity" || kl === "size" || kl === "amount") out[k] = "{{qty}}";
      else if (kl === "price" || kl === "limit_price") out[k] = "{{price}}";
      else if (kl === "quote_id" || kl === "quoteid") out[k] = "{{quote_id}}";
      else if (kl === "max_slippage" || kl === "maxslippage") out[k] = "{{max_slippage}}";
      else if (kl === "is_reduce_only" || kl === "reduce_only" || kl === "reduceonly") out[k] = "{{reduce_only}}";
      else out[k] = replacePlaceholders(v);
    }
    return out;
  }

  function emitApiTrace(meta, reqHeaders, reqBody, respJson) {
    const urlRel = toRelativeUrl(meta.url);
    // 只保留 API 请求，且优先捕捉交易/账户相关
    if (!urlRel.startsWith("/api/")) return;
    // 过滤行情/元数据噪音（这些会在资金费率/行情逻辑里处理）
    if (IGNORE_TRACE_URL_RE.test(urlRel)) return;
    if (!TRADE_HINT_URL_RE.test(urlRel) && !isObject(reqBody)) return;

    let hasHintKey = false;
    if (isObject(reqBody)) {
      const keys = Object.keys(reqBody).map((k) => k.toLowerCase());
      hasHintKey = ORDER_BODY_HINT_KEYS.some((x) => keys.includes(x));
    }
    if (!TRADE_HINT_URL_RE.test(urlRel) && !hasHintKey) return;

    const op = guessTradeOp(meta, reqBody);
    const template = {
      method: String(meta.method || "GET").toUpperCase(),
      url: urlRel,
      headers: filterSensitiveHeaders(reqHeaders),
      cookies: {},
      body: isObject(reqBody) || Array.isArray(reqBody) ? replacePlaceholders(reqBody) : null,
      timeout_seconds: 10
    };

    window.postMessage({
      __vh_type: "VARIATIONAL_API_TRACE",
      payload: {
        ts: Date.now(),
        op,
        method: template.method,
        url: template.url,
        headers: template.headers,
        body: template.body,
        // 响应只做轻量提示（避免过大）
        resp_has_data: !!respJson,
        template
      }
    }, "*");
  }

  // ---- hook fetch ----
  const originalFetch = window.fetch;
  window.fetch = async function(input, init) {
    const method = (init && init.method) ? String(init.method).toUpperCase() : "GET";
    const url = (typeof input === "string") ? input : (input && input.url) ? input.url : "";
    const reqHeaders = (() => {
      const h = (init && init.headers) ? init.headers : null;
      if (!h) return {};
      // headers 可能是 object / array / Headers
      try {
        if (h instanceof Headers) {
          const o = {};
          for (const [k, v] of h.entries()) o[k] = v;
          return o;
        }
        if (Array.isArray(h)) {
          const o = {};
          for (const [k, v] of h) o[k] = v;
          return o;
        }
        if (isObject(h)) return h;
      } catch {}
      return {};
    })();

    const reqBody = (() => {
      try {
        const b = init && init.body;
        if (!b) return null;
        if (typeof b === "string") return safeJsonParse(b) || null;
        // FormData/URLSearchParams 暂不处理，避免引入猜测
      } catch {}
      return null;
    })();

    const resp = await originalFetch.apply(this, arguments);
    try {
      const ct = resp.headers.get("content-type") || "";
      const clone = resp.clone();
      if (ct.includes("application/json")) {
        const json = await clone.json();
        emitCandidates({ url, method }, json);
        emitApiTrace({ url, method }, reqHeaders, reqBody, json);
      } else {
        const text = await clone.text();
        const json = safeJsonParse(text);
        if (json) {
          emitCandidates({ url, method }, json);
          emitApiTrace({ url, method }, reqHeaders, reqBody, json);
        } else {
          emitApiTrace({ url, method }, reqHeaders, reqBody, null);
        }
      }
    } catch {
      // ignore
    }
    return resp;
  };

  // ---- hook XHR ----
  const XHROpen = XMLHttpRequest.prototype.open;
  const XHRSend = XMLHttpRequest.prototype.send;
  const XHRSetHeader = XMLHttpRequest.prototype.setRequestHeader;
  XMLHttpRequest.prototype.open = function(method, url) {
    this.__vh_method = String(method || "GET").toUpperCase();
    this.__vh_url = url;
    this.__vh_headers = {};
    return XHROpen.apply(this, arguments);
  };
  XMLHttpRequest.prototype.setRequestHeader = function(name, value) {
    try {
      if (name) this.__vh_headers[String(name)] = String(value);
    } catch {}
    return XHRSetHeader.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function() {
    try {
      this.__vh_body = arguments && arguments.length ? arguments[0] : null;
    } catch {
      this.__vh_body = null;
    }
    this.addEventListener("load", () => {
      try {
        const text = this.responseText;
        if (!text || text.length < 2) return;
        const json = safeJsonParse(text);
        if (json) {
          emitCandidates({ url: this.__vh_url || "", method: this.__vh_method || "GET" }, json);
        }
        const reqBody = (typeof this.__vh_body === "string") ? (safeJsonParse(this.__vh_body) || null) : null;
        emitApiTrace(
          { url: this.__vh_url || "", method: this.__vh_method || "GET" },
          this.__vh_headers || {},
          reqBody,
          json
        );
      } catch {
        // ignore
      }
    });
    return XHRSend.apply(this, arguments);
  };
})();


