"""
Variational (Omni) - 私有交易请求重放客户端（用于“无公开API，仅能逆向前端”场景）
-----------------------------------------------------------------------------
设计目标：
- 不猜测 Variational 的真实下单接口/参数结构
- 允许用户从浏览器 DevTools 拷贝“Copy as cURL”，并转换成可重放的请求模板
- 运行时仅替换 side/qty/price 等字段，实现全自动下单

注意：
- 这里不做任何业务决策，只负责“按模板发 HTTP 请求”
- 认证方式（token/cookie/header）由模板/配置提供，避免硬编码
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Optional, Union

import aiohttp

from ..interface import ExchangeConfig


JsonLike = Union[Dict[str, Any], list, str, int, float, bool, None]


@dataclass(frozen=True)
class VariationalRequestTemplate:
    """
    通用请求模板：
    - url 支持绝对 URL 或相对 path（相对时会拼接 base_url）
    - body 支持 dict/list（作为 JSON）或 str（原样发送）
    - placeholders 采用 {{key}} 形式，递归替换
    """

    method: str
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    body: Optional[JsonLike] = None
    timeout_seconds: Optional[int] = None


class VariationalTemplateRenderer:
    """递归渲染 {{placeholders}} 的工具类。"""

    @staticmethod
    def render(value: JsonLike, ctx: Dict[str, Any]) -> JsonLike:
        if value is None:
            return None
        if isinstance(value, dict):
            return {k: VariationalTemplateRenderer.render(v, ctx) for k, v in value.items()}
        if isinstance(value, list):
            return [VariationalTemplateRenderer.render(v, ctx) for v in value]
        if isinstance(value, str):
            return VariationalTemplateRenderer._render_str_any(value, ctx)
        # 兼容 Decimal：aiohttp 的 json 序列化不支持 Decimal
        if isinstance(value, Decimal):
            try:
                return float(value)
            except Exception:
                return str(value)
        return value

    @staticmethod
    def _render_str_any(s: str, ctx: Dict[str, Any]) -> JsonLike:
        """
        渲染字符串占位符：
        - 若字符串“完全等于”某个占位符（例如 "{{reduce_only}}"），则返回 ctx 原始类型（bool/number）
        - 否则做字符串替换（返回 str）
        """
        raw = s.strip()
        if raw.startswith("{{") and raw.endswith("}}") and raw.count("{{") == 1 and raw.count("}}") == 1:
            key = raw[2:-2].strip()
            if key in ctx:
                v = ctx.get(key)
                if isinstance(v, Decimal):
                    try:
                        return float(v)
                    except Exception:
                        return str(v)
                return v

        out = s
        for k, v in ctx.items():
            if isinstance(v, Decimal):
                try:
                    vv = str(float(v))
                except Exception:
                    vv = str(v)
            else:
                vv = str(v)
            out = out.replace(f"{{{{{k}}}}}", vv)
        return out


class VariationalPrivateTradingClient:
    """
    Variational 私有交易客户端（基于请求模板重放）。
    """

    def __init__(self, config: ExchangeConfig, logger=None):
        self._config = config
        self._logger = logger
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        # 轮询类请求（positions/balances）日志节流：避免每 5 秒刷屏
        # 可通过 exchange.extra_params 覆盖（单位秒）
        try:
            self._polling_request_log_interval_s = float(
                config.extra_params.get("variational_trade_poll_log_interval_seconds", 180.0)
            )
        except Exception:
            self._polling_request_log_interval_s = 180.0
        self._last_polling_log_ts: Dict[str, float] = {}

        self._base_url = (
            (config.base_url or "").rstrip("/")
            or str(config.extra_params.get("base_url", "")).rstrip("/")
        )
        self._max_concurrency = int(config.extra_params.get("trade_max_concurrency", 1))
        self._semaphore = asyncio.Semaphore(max(1, self._max_concurrency))

        # 🔥 默认请求头：复用浏览器态 Cookie/UA/Origin/Referer，避免 WAF/风控 403
        # - 与 VariationalRestClient 的策略保持一致：优先 env，再到 extra_params
        self._default_headers: Dict[str, str] = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        env_cookie = os.getenv("VARIATIONAL_COOKIE", "").strip()
        if env_cookie:
            self._default_headers["Cookie"] = env_cookie

        # allow passing browser-ish headers via extra_params
        ua = str(config.extra_params.get("user_agent") or os.getenv("VARIATIONAL_USER_AGENT", "")).strip()
        if not ua:
            # 与 VariationalRestClient.DEFAULT_USER_AGENT 保持一致的浏览器友好默认值
            ua = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        origin = str(config.extra_params.get("origin") or os.getenv("VARIATIONAL_ORIGIN", "")).strip()
        referer = str(config.extra_params.get("referer") or os.getenv("VARIATIONAL_REFERER", "")).strip()
        if ua:
            self._default_headers["User-Agent"] = ua
        if origin:
            self._default_headers["Origin"] = origin
        if referer:
            self._default_headers["Referer"] = referer

        extra_headers = config.extra_params.get("headers")
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    # 不在这里做 auth 抑制：由调用方/模板决定
                    self._default_headers[k] = v

    async def close(self) -> None:
        async with self._session_lock:
            if self._session:
                await self._session.close()
                self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            timeout = aiohttp.ClientTimeout(total=max(1, int(self._config.request_timeout or 10)))
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self._default_headers)
            return self._session

    def _build_url(self, template_url: str) -> str:
        u = (template_url or "").strip()
        if u.startswith("http://") or u.startswith("https://"):
            return u
        base = self._base_url
        if not base:
            raise ValueError("VariationalPrivateTradingClient 缺少 base_url（请在 exchange config.base_url 或 extra_params.base_url 配置）")
        if not u.startswith("/"):
            u = "/" + u
        return f"{base}{u}"

    async def send(
        self,
        template: VariationalRequestTemplate,
        ctx: Dict[str, Any],
        template_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        按模板发送请求并返回 JSON 响应（若非 JSON，则返回 text 形式）。
        """
        async with self._semaphore:
            session = await self._ensure_session()
            # 🔥 关键：模板中 url/headers/cookies/body 均支持 {{placeholders}}，用于多币种/多方向/多数量自动化
            rendered_url = VariationalTemplateRenderer.render(template.url, ctx)
            url = self._build_url(str(rendered_url))

            # 模板 headers 覆盖默认 headers（默认 headers 包含 Cookie/UA/Origin/Referer）
            headers = dict(self._default_headers)
            for k, v in (template.headers or {}).items():
                headers[str(k)] = str(VariationalTemplateRenderer.render(v, ctx))
            cookies = {str(k): str(VariationalTemplateRenderer.render(v, ctx)) for k, v in (template.cookies or {}).items()}
            method = str(template.method or "POST").upper()

            rendered_body = VariationalTemplateRenderer.render(template.body, ctx)
            timeout_seconds = (
                int(template.timeout_seconds)
                if template.timeout_seconds is not None
                else None
            )

            if self._logger:
                keys = list(ctx.keys())
                # 避免 ctx_keys 过长刷屏：最多显示前 12 个
                key_display = keys[:12]
                more = f"...(+{len(keys) - 12})" if len(keys) > 12 else ""
                # ✅ 节流：positions/balances 属于高频轮询请求，默认每 60s 打一次；下单/撤单等仍每次都打印
                tname = str(template_name or "").strip()
                is_polling = tname in ("balances_template", "positions_template")
                if not is_polling:
                    self._logger.info(
                        "[variational-trade] 请求重放: %s %s | 超时=%ss | ctx字段=%s%s",
                        method,
                        url,
                        timeout_seconds if timeout_seconds is not None else "-",
                        key_display,
                        more,
                    )
                else:
                    now_ts = asyncio.get_event_loop().time()
                    interval = max(5.0, float(self._polling_request_log_interval_s or 60.0))
                    last_ts = float(self._last_polling_log_ts.get(tname, 0.0) or 0.0)
                    if now_ts - last_ts >= interval:
                        self._last_polling_log_ts[tname] = now_ts
                        # 正常轮询不刷文件日志：降为 DEBUG（默认 FileHandler=INFO，不会写入）
                        self._logger.debug(
                            "[variational-trade] 请求重放(%s): %s %s | 超时=%ss",
                            tname,
                            method,
                            url,
                            timeout_seconds if timeout_seconds is not None else "-",
                        )

            req_timeout = aiohttp.ClientTimeout(total=timeout_seconds) if timeout_seconds else None
            try:
                if isinstance(rendered_body, (dict, list)):
                    async with session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        cookies=cookies or None,
                        json=rendered_body,
                        timeout=req_timeout,
                    ) as resp:
                        return await self._read_response(resp)
                elif isinstance(rendered_body, str):
                    async with session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        cookies=cookies or None,
                        data=rendered_body.encode("utf-8"),
                        timeout=req_timeout,
                    ) as resp:
                        return await self._read_response(resp)
                else:
                    async with session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        cookies=cookies or None,
                        timeout=req_timeout,
                    ) as resp:
                        return await self._read_response(resp)
            except asyncio.CancelledError:
                # 上层正在取消任务（例如程序退出/策略停机），不要做重试/重放日志
                raise
            except Exception as e:
                if self._logger:
                    # aiohttp 的 TimeoutError 经常伴随底层 CancelledError（实现细节），不需要打满堆栈误导排查
                    is_timeout = isinstance(e, (asyncio.TimeoutError, TimeoutError))
                    if is_timeout:
                        self._logger.warning(
                            "[variational-trade] request timeout: %s %s (timeout=%ss)",
                            method,
                            url,
                            timeout_seconds if timeout_seconds is not None else (self._config.request_timeout or 10),
                        )
                    else:
                        self._logger.error(f"[variational-trade] request failed: {e}", exc_info=True)
                raise

    async def _read_response(self, resp: aiohttp.ClientResponse) -> Dict[str, Any]:
        status = resp.status
        content_type = (resp.headers.get("Content-Type") or "").lower()
        if status >= 400:
            body = await resp.text()
            body_preview = body[:800]
            if self._logger and status == 403:
                try:
                    cookie = self._default_headers.get("Cookie", "")
                    ua = self._default_headers.get("User-Agent", "")
                    cookie_len = len(cookie) if isinstance(cookie, str) else 0
                    ua_len = len(ua) if isinstance(ua, str) else 0
                    cookie_has_cf = isinstance(cookie, str) and ("cf_clearance=" in cookie.lower())
                    cookie_has_vr = isinstance(cookie, str) and ("vr-token=" in cookie.lower())
                    self._logger.warning(
                        "[variational-trade] 403 blocked by WAF/CF: cookie_len=%s has_cf_clearance=%s has_vr_token=%s ua_len=%s",
                        cookie_len,
                        cookie_has_cf,
                        cookie_has_vr,
                        ua_len,
                    )
                except Exception:
                    pass
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info,
                history=resp.history,
                status=status,
                message=f"{resp.reason}; body={body_preview}",
                headers=resp.headers,
            )

        if "application/json" in content_type:
            data = await resp.json()
            return {"ok": True, "status": status, "data": data}

        # 兼容：某些站点返回 text/plain 或 HTML
        text = await resp.text()
        # 尝试把 text 当 JSON 解析（有些接口 Content-Type 不规范）
        try:
            parsed = json.loads(text)
            return {"ok": True, "status": status, "data": parsed}
        except Exception:
            return {"ok": True, "status": status, "text": text}


