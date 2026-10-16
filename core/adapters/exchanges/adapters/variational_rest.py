"""
Variational (Omni) - REST 客户端

当前 Variational 前端仅展示最优买卖价（BBO），并不提供传统 orderbook 深度。
插件验证可通过 `POST /api/quotes/indicative` 获取 bid/ask/mark_price。

本模块只负责“拉取报价”，不包含任何策略/业务逻辑。
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, List, Union

import aiohttp

from ..interface import ExchangeConfig


@dataclass(frozen=True)
class VariationalIndicativeQuoteRequest:
    """Indicative quote 请求参数（与前端一致）"""

    underlying: str
    qty: str
    funding_interval_s: int = 3600
    settlement_asset: str = "USDC"
    instrument_type: str = "perpetual_future"

    def to_payload(self) -> Dict[str, Any]:
        return {
            "instrument": {
                "underlying": self.underlying,
                "funding_interval_s": self.funding_interval_s,
                "settlement_asset": self.settlement_asset,
                "instrument_type": self.instrument_type,
            },
            "qty": self.qty,
        }


class VariationalAuthError(RuntimeError):
    """
    Variational 鉴权失败（通常是 token/cookie 过期或不匹配）。
    该错误无法通过“重试/重连”自愈，必须刷新 Cookie/Token 后重启进程。
    """

    # 给通用重试器一个明确的“不可重试”信号（不影响其他异常）
    non_retryable: bool = True


class VariationalRestClient:
    """Variational REST 客户端（仅用于获取 BBO quote）"""

    DEFAULT_BASE_URL = "https://omni.variational.io"
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    def __init__(self, config: ExchangeConfig, logger=None):
        self._config = config
        self._logger = logger
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        self._base_url = (
            (config.base_url or "").rstrip("/")
            or str(config.extra_params.get("base_url", self.DEFAULT_BASE_URL)).rstrip("/")
        )

        # token：部分接口（如 /api/quotes/indicative）会要求 Authorization token
        # 约定优先级：
        # 1) extra_params.token
        # 2) config.api_key（方便在现有配置结构里复用）
        self._token: Optional[str] = None
        token = config.extra_params.get("token")
        if isinstance(token, str) and token.strip():
            self._token = self._normalize_token(token)
        elif isinstance(config.api_key, str) and config.api_key.strip():
            self._token = self._normalize_token(config.api_key)

        # 可扩展请求头：Variational 站点可能有 WAF/风控 + token 鉴权
        # 这里提供“浏览器友好”的默认值，同时允许在配置中覆盖/补充。
        self._headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            # 关键：部分站点会基于 UA/Origin/Referer 做简单风控
            "User-Agent": str(config.extra_params.get("user_agent", self.DEFAULT_USER_AGENT)),
            "Origin": str(config.extra_params.get("origin", self._base_url)),
            "Referer": str(config.extra_params.get("referer", f"{self._base_url}/")),
        }
        extra_headers = config.extra_params.get("headers")
        user_provided_authorization = False
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    self._headers[k] = v
                    if k.strip().lower() == "authorization":
                        user_provided_authorization = True

        # 🔥 从环境变量注入浏览器态（与其它交易所一致：优先 env，再到 yaml）
        # 说明：
        # - VARIATIONAL_COOKIE：直接放完整 Cookie header（不需要 "Cookie:" 前缀）
        # - VARIATIONAL_USER_AGENT / VARIATIONAL_ORIGIN / VARIATIONAL_REFERER：可选
        env_cookie = os.getenv("VARIATIONAL_COOKIE", "").strip()
        if env_cookie and not any(k.lower() == "cookie" for k in self._headers.keys()):
            self._headers["Cookie"] = env_cookie

        env_user_agent = os.getenv("VARIATIONAL_USER_AGENT", "").strip()
        if env_user_agent and not config.extra_params.get("user_agent"):
            self._headers["User-Agent"] = env_user_agent

        env_origin = os.getenv("VARIATIONAL_ORIGIN", "").strip()
        if env_origin and not config.extra_params.get("origin"):
            self._headers["Origin"] = env_origin

        env_referer = os.getenv("VARIATIONAL_REFERER", "").strip()
        if env_referer and not config.extra_params.get("referer"):
            self._headers["Referer"] = env_referer

        # 默认 Authorization：仅当用户未显式提供 Authorization 时才自动加 Bearer
        auto_added_authorization = False
        if self._token and (not user_provided_authorization) and ("Authorization" not in self._headers):
            # 绝大多数站点是 Bearer token；如果后续发现不是，可通过 extra_params.headers 覆盖
            self._headers["Authorization"] = f"Bearer {self._token}"
            auto_added_authorization = True

        # 可选 cookies：如果接口需要登录态/风控 cookie，可在 extra_params 传入
        self._cookies: Optional[Dict[str, str]] = None
        cookies = config.extra_params.get("cookies")
        if isinstance(cookies, dict):
            cleaned: Dict[str, str] = {}
            for k, v in cookies.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    cleaned[k] = v
            self._cookies = cleaned or None

        # 🔥 兼容 Variational 前端：token 可能通过 Cookie(vr-token) 传递
        # - 如果用户只提供了 token（VARIATIONAL_API_KEY / extra_params.token / config.api_key），
        #   但未显式提供 cookies / Cookie header，则自动注入 vr-token cookie，提高成功率。
        # - 如果用户已经配置了 Cookie（headers 或 cookies），则不覆盖。
        if self._token:
            has_cookie_header = any(
                k.lower() == "cookie" for k in self._headers.keys())
            has_vr_token_cookie = (
                isinstance(self._cookies, dict)
                and any(str(k).strip().lower() == "vr-token" for k in self._cookies.keys())
            )
            if (not has_cookie_header) and (not has_vr_token_cookie) and (self._cookies is None):
                self._cookies = {"vr-token": self._token}

        # 🔥 重要兼容：Variational 前端请求通常不带 Authorization，而是依赖 Cookie(vr-token/cf_clearance)
        # 某些后端会优先校验 Authorization，导致“Cookie 有效但 Authorization 走了另一条校验路径 -> Invalid token”。
        # 策略：
        # - 如果 Cookie header 中已经包含 vr-token=...，则自动移除“自动添加的 Authorization”
        # - 若用户显式提供 Authorization（extra_params.headers.Authorization），则尊重用户配置不移除
        auth_suppressed_due_to_cookie = False
        cookie_header_val = self._headers.get("Cookie", "")
        if (
            auto_added_authorization
            and isinstance(cookie_header_val, str)
            and ("vr-token=" in cookie_header_val.lower())
            and ("Authorization" in self._headers)
        ):
            self._headers.pop("Authorization", None)
            auth_suppressed_due_to_cookie = True

        # 🔍 诊断：不泄露 token，只打印是否存在/长度以及是否注入 cookie
        if self._logger:
            try:
                token_len = len(self._token) if self._token else 0
                has_auth = "Authorization" in self._headers
                has_cookie_header = any(
                    k.lower() == "cookie" for k in self._headers.keys())
                cookie_val = self._headers.get(
                    "Cookie") if has_cookie_header else ""
                cookie_len = len(cookie_val) if isinstance(
                    cookie_val, str) else 0
                has_vr_token_in_cookie_header = (
                    isinstance(cookie_val, str) and (
                        "vr-token=" in cookie_val.lower())
                )
                has_cookie_kv = bool(self._cookies)
                has_vr_cookie = (
                    isinstance(self._cookies, dict)
                    and any(str(k).strip().lower() == "vr-token" for k in self._cookies.keys())
                )
                self._logger.info(
                    "VariationalRestClient 鉴权摘要: token=%s(len=%s) | Authorization头=%s | Cookie头=%s(len=%s, 含vr-token=%s) | cookies字典=%s(vr-token=%s) | 已自动抑制Authorization=%s | 用户显式Authorization=%s",
                    "有" if bool(self._token) else "无",
                    token_len,
                    "有" if has_auth else "无",
                    "有" if has_cookie_header else "无",
                    cookie_len,
                    "是" if has_vr_token_in_cookie_header else "否",
                    "有" if has_cookie_kv else "无",
                    "是" if has_vr_cookie else "否",
                    "是" if auth_suppressed_due_to_cookie else "否",
                    "是" if user_provided_authorization else "否",
                )
            except Exception:
                pass

    @staticmethod
    def _normalize_token(raw: str) -> str:
        """
        兼容用户粘贴多种形式的 token：
        - 纯 JWT：eyJ...
        - 带前缀：Bearer eyJ...
        - 从 Cookie 直接复制：...; vr-token=eyJ...; ...
        """
        s = str(raw or "").strip().strip('"').strip("'")
        if not s:
            return ""
        lower = s.lower()
        if lower.startswith("bearer "):
            s = s.split(" ", 1)[1].strip()
            return s
        # cookie 场景：提取 vr-token=...
        if "vr-token=" in lower:
            # 按 ; 分割 cookies
            parts = [p.strip() for p in s.split(";") if p.strip()]
            for p in parts:
                if "=" not in p:
                    continue
                k, v = p.split("=", 1)
                if k.strip().lower() == "vr-token" and v.strip():
                    return v.strip().strip('"').strip("'")
        return s

    async def close(self) -> None:
        async with self._session_lock:
            if self._session:
                await self._session.close()
                self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session

            timeout = aiohttp.ClientTimeout(
                total=max(1, int(self._config.request_timeout or 10)))
            cookie_jar = aiohttp.CookieJar()
            if self._cookies:
                cookie_jar.update_cookies(self._cookies)
            self._session = aiohttp.ClientSession(
                timeout=timeout, headers=self._headers, cookie_jar=cookie_jar)
            return self._session

    async def get_indicative_quote(
        self,
        request: VariationalIndicativeQuoteRequest,
    ) -> Dict[str, Any]:
        """
        获取 BBO（bid/ask/mark_price）

        Returns:
            Dict[str, Any]: 原始响应 JSON
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/api/quotes/indicative"
        payload = request.to_payload()

        if self._logger:
            self._logger.debug(
                "Variational indicative quote request: url=%s payload=%s",
                url,
                payload,
            )

        try:
            async with session.post(url, json=payload) as resp:
                # 403/4xx 时经常会返回 WAF 的 HTML/文本说明，读取出来便于排查
                if resp.status >= 400:
                    body = await resp.text()
                    # 截断避免日志过长
                    body_preview = body[:500]
                    # 401：token/cookie 失效通常无法靠重试解决，直接抛出可读错误，避免刷屏
                    if resp.status == 401:
                        raise VariationalAuthError(
                            f"401 Unauthorized; body={body_preview}"
                        )
                    # 对于 4xx/5xx：很可能需要刷新 cookie / 重新建连；先重置 session 提高自愈概率
                    try:
                        await self.close()
                    except Exception:
                        pass
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"{resp.reason}; body={body_preview}",
                        headers=resp.headers,
                    )

                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # 断网/恢复场景下，aiohttp 连接池可能进入“坏状态”，主动重置 session 以便后续自动恢复
            if self._logger:
                try:
                    self._logger.warning(
                        "Variational quote request failed (reset session): %s",
                        str(e),
                    )
                except Exception:
                    pass
            try:
                await self.close()
            except Exception:
                pass
            raise

        if not isinstance(data, dict):
            raise ValueError(
                f"Variational quote response is not a JSON object: {type(data)}")

        return data

    # =========================================================================
    # 可选：资金费率数据源（用于“quote 不包含 funding”的场景）
    # =========================================================================

    def _render_placeholders(self, value: Any, ctx: Dict[str, Any]) -> Any:
        """
        递归替换 {{key}} 占位符，避免硬编码具体字段结构。
        """
        if value is None:
            return None
        if isinstance(value, dict):
            return {k: self._render_placeholders(v, ctx) for k, v in value.items()}
        if isinstance(value, list):
            return [self._render_placeholders(v, ctx) for v in value]
        if isinstance(value, str):
            out = value
            for k, v in ctx.items():
                out = out.replace(f"{{{{{k}}}}}", str(v))
            return out
        return value

    def _extract_json_path(self, data: Any, path: List[Union[str, int]]) -> Any:
        cur: Any = data
        for p in path:
            if isinstance(p, int):
                if not isinstance(cur, list) or p < 0 or p >= len(cur):
                    return None
                cur = cur[p]
            else:
                if not isinstance(cur, dict) or p not in cur:
                    return None
                cur = cur.get(p)
        return cur

    def _safe_decimal(self, v: Any) -> Optional[Decimal]:
        try:
            if v is None:
                return None
            if isinstance(v, Decimal):
                return v
            s = str(v).strip()
            if not s:
                return None
            return Decimal(s)
        except Exception:
            return None

    async def get_funding_info_from_source(self, underlying: str) -> tuple[Optional[Decimal], Optional[int]]:
        """
        从配置的 funding_rate_source 拉取资金费率（可选同时返回 funding_interval_s）。

        配置结构（放在 exchange_config.extra_params）：
          funding_rate_source:
            method: "GET" | "POST"
            url: "/api/xxx" 或 "https://...."
            body: {...}  # 可选，支持 {{underlying}} 等占位符
            headers: {...} # 可选，附加 header（不覆盖已有 Cookie/UA/Origin 等）
            json_path: ["data","funding_rate"]  # 必填：从响应 JSON 提取的路径
            multiplier: 1  # 可选：倍率
            interval_json_path: ["data","funding_interval_s"]  # 可选：资金费率间隔（秒）
        """
        cfg = self._config.extra_params.get("funding_rate_source")
        if not isinstance(cfg, dict):
            return None, None

        url_cfg = str(cfg.get("url") or "").strip()
        method = str(cfg.get("method") or "GET").upper()
        if not url_cfg:
            return None, None

        ctx = {
            "underlying": str(underlying).strip().upper(),
            "symbol": str(underlying).strip().upper(),
            "instrument_type": str(self._config.extra_params.get("instrument_type", "")),
            "settlement_asset": str(self._config.extra_params.get("settlement_asset", "")),
        }

        # 允许相对路径
        if url_cfg.startswith("http://") or url_cfg.startswith("https://"):
            url = self._render_placeholders(url_cfg, ctx)
        else:
            rendered_path = self._render_placeholders(url_cfg, ctx)
            path = rendered_path if str(rendered_path).startswith(
                "/") else f"/{rendered_path}"
            url = f"{self._base_url}{path}"

        json_path_raw = cfg.get("json_path")
        if not isinstance(json_path_raw, list) or not json_path_raw:
            return None, None
        json_path: List[Union[str, int]] = []
        for p in json_path_raw:
            if isinstance(p, int):
                json_path.append(p)
            elif isinstance(p, str) and p.strip():
                # 支持在 json_path 里使用 {{underlying}} 占位符
                rendered = self._render_placeholders(p, ctx)
                ps = str(rendered).strip()
                # 允许传入数字字符串作为数组下标
                if ps.isdigit():
                    json_path.append(int(ps))
                else:
                    json_path.append(ps)

        body = self._render_placeholders(cfg.get("body"), ctx)

        # 头合并：只追加，不覆盖已有浏览器态头（除非用户显式配置相同 key）
        extra_headers = cfg.get("headers")
        headers = dict(self._headers)
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    headers[k] = self._render_placeholders(v, ctx)

        session = await self._ensure_session()
        try:
            if method == "POST":
                async with session.post(url, json=body if isinstance(body, (dict, list)) else None, headers=headers) as resp:
                    data = await resp.json()
            else:
                async with session.get(url, headers=headers) as resp:
                    data = await resp.json()
        except Exception as exc:
            if self._logger:
                self._logger.debug(
                    f"[variational] funding_rate_source request failed: underlying={underlying} method={method} url={url} err={exc}"
                )
            return None, None

        value = self._extract_json_path(data, json_path)
        dec = self._safe_decimal(value)
        if dec is None:
            if self._logger:
                self._logger.debug(
                    f"[variational] funding_rate_source parse failed: underlying={underlying} url={url} json_path={json_path}"
                )
            return None, None

        mult_raw = cfg.get("multiplier", 1)
        mult = self._safe_decimal(mult_raw) or Decimal("1")
        if mult <= 0:
            mult = Decimal("1")
        funding_rate = dec * mult

        # 可选：同时解析 funding_interval_s（不同币可能 4h/8h）
        interval_s: Optional[int] = None
        interval_path_raw = cfg.get("interval_json_path")
        if isinstance(interval_path_raw, list) and interval_path_raw:
            interval_path: List[Union[str, int]] = []
            for p in interval_path_raw:
                if isinstance(p, int):
                    interval_path.append(p)
                elif isinstance(p, str) and p.strip():
                    rendered = self._render_placeholders(p, ctx)
                    ps = str(rendered).strip()
                    if ps.isdigit():
                        interval_path.append(int(ps))
                    else:
                        interval_path.append(ps)
            try:
                raw_interval = self._extract_json_path(data, interval_path)
                if raw_interval is not None:
                    interval_s = int(float(raw_interval))
                    if interval_s <= 0:
                        interval_s = None
            except Exception:
                interval_s = None

        return funding_rate, interval_s

    async def get_funding_rate_from_source(self, underlying: str) -> Optional[Decimal]:
        """
        向后兼容：只返回资金费率（Decimal，小数形式）。
        """
        rate, _interval = await self.get_funding_info_from_source(underlying)
        return rate
