"""
Variational (Omni) - WebSocket 事件流客户端

用途：
- 订阅 wss://.../events 推送的 trade / clearing_event 等事件
- 为套利执行器提供 subscribe_user_data / subscribe_order_fills / lookup_cached_order

说明：
- Variational 的前端使用 auth=<jwt> query param 建立 WS（见浏览器抓包）
- 我们不猜测更多协议：收到的每条 message 当 JSON 解析并原样回调
- 成交确认：用 trade 事件中的 source_quote（quote_id）作为“订单追踪ID”
"""

from __future__ import annotations

import asyncio
import json
from urllib.parse import quote
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from ..interface import ExchangeConfig
from ..models import (
    MarginMode,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionData,
    PositionSide,
)


@dataclass(frozen=True)
class VariationalWsEvent:
    raw: Dict[str, Any]

    @property
    def type(self) -> str:
        try:
            return str(self.raw.get("type") or "").strip()
        except Exception:
            return ""

    @property
    def data(self) -> Dict[str, Any]:
        v = self.raw.get("data")
        return v if isinstance(v, dict) else {}


class VariationalWebSocket:
    """
    最小可用 WebSocket 客户端：
    - subscribe_user_data：启动 WS 并把 raw event 推给回调
    - subscribe_order_fills：把 trade 事件转换为 OrderData 推给回调（用于成交确认）
    - lookup_cached_order：返回最近成交缓存（让执行器在轮询前先查 WS 缓存）
    """

    DEFAULT_EVENTS_URL = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events"
    DEFAULT_PORTFOLIO_URL = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"

    def __init__(self, config: ExchangeConfig, logger=None):
        self._config = config
        self._logger = logger
        # 可由服务层注入：API失败上报回调（用于 api_failure_pause）
        # 约定签名：reporter(exchange=<exchange_id>, api=<api_name>, reason=<str>)
        self._api_failure_reporter = None

        self._events_url = str(config.extra_params.get("ws_events_url") or self.DEFAULT_EVENTS_URL).strip()
        # portfolio WS：前端用它推送账户/持仓快照（positions/保证金等）
        # - ⚠️ 默认策略：仅当存在 token 时才启用（否则会出现 1006/429 的重连风暴）
        # - 你仍可通过 extra_params.ws_portfolio_enable=true 强行启用（前提是你正确配置 token/cookie/header）
        self._portfolio_url = str(
            config.extra_params.get("ws_portfolio_url") or self.DEFAULT_PORTFOLIO_URL
        ).strip()
        self._portfolio_auth_token = self._normalize_token(
            str(config.extra_params.get("ws_portfolio_auth_token") or "").strip()
        ) or ""
        self._auth_token = self._normalize_token(
            str(config.extra_params.get("ws_auth_token") or config.extra_params.get("token") or (config.api_key or "")).strip()
        )
        # 默认 portfolio token：优先独立 token，否则复用 events token
        portfolio_token = self._portfolio_auth_token or self._auth_token or ""
        if "ws_portfolio_enable" in (config.extra_params or {}):
            self._portfolio_enable = bool(config.extra_params.get("ws_portfolio_enable"))
            if self._portfolio_enable and (not portfolio_token) and self._logger:
                self._logger.warning(
                    "[variational-ws] portfolio WS 已启用但未配置 ws_auth_token/ws_portfolio_auth_token；"
                    "可能会触发 429/1006（建议先补齐 token 或关闭 ws_portfolio_enable）"
                )
        else:
            # 未显式配置时：只有在存在 token 时才启用 portfolio WS
            self._portfolio_enable = bool(portfolio_token)
        self._auth_param = str(config.extra_params.get("ws_auth_param") or "auth").strip() or "auth"
        self._protocols = config.extra_params.get("ws_protocols")
        self._on_connect_messages = config.extra_params.get("ws_on_connect_messages")
        # portfolio WS 可能需要额外订阅消息（浏览器里通常会 send 一条订阅/初始化消息）
        self._portfolio_on_connect_messages = config.extra_params.get("ws_portfolio_on_connect_messages")
        # 默认行为：若存在 token，则在连接后自动发送 {"claims": token}（与前端行为对齐）
        self._send_claims_on_connect = bool(config.extra_params.get("ws_send_claims_on_connect", True))

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._portfolio_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._portfolio_task: Optional[asyncio.Task] = None

        self._user_cbs: List[Callable[..., Any]] = []
        self._order_cbs: List[Callable[..., Any]] = []
        self._fill_cbs: List[Callable[..., Any]] = []
        self._pos_cbs: List[Callable[..., Any]] = []

        self._ws_connected = False
        self._portfolio_connected = False
        self._last_connected_log: float = 0.0
        self._last_recv_ts: float = 0.0
        self._last_portfolio_recv_ts: float = 0.0
        self._hb_task: Optional[asyncio.Task] = None
        self._portfolio_hb_task: Optional[asyncio.Task] = None
        # 轻量一次性日志：避免刷屏，同时给用户一个明确“订阅是否被调用”的信号
        self._sub_log_once: Dict[str, bool] = {}
        self._portfolio_pos_log_once: bool = False
        self._portfolio_subscribe_hint_once: bool = False
        # 持仓快照日志：限流到每分钟最多一条（用户需要持续确认订阅/解析是否正常）
        self._portfolio_positions_log_interval_s: float = float(
            config.extra_params.get("ws_portfolio_positions_log_interval_s", 180.0)
        )
        self._last_portfolio_positions_log_ts: float = 0.0

        # 成交缓存：key 为 quote_id（source_quote）或其它可识别 id
        self._filled_cache: Dict[str, OrderData] = {}
        # 持仓缓存：symbol -> PositionData（来自 /portfolio WS）
        self._positions_cache: Dict[str, PositionData] = {}
        # 权益缓存：来自 /portfolio WS 的 pool_portfolio_result.balance（Portfolio Value/Equity）
        self._portfolio_balance: Optional[Decimal] = None
        self._portfolio_balance_ts: float = 0.0
        # 部分成交聚合缓存：quote_id -> (total_qty, total_notional, last_side, symbol, trades)
        self._fill_acc: Dict[str, Dict[str, Any]] = {}
        # clearing_event: rfq_id -> quote_id（parent_quote_id）
        self._rfq_to_quote: Dict[str, str] = {}

        # 本地提交订单的“追踪ID”(local_tracking_id) 与 WS 实际 quote_id/rfq_id 的自动匹配
        # 目的：解决“请求里传的 quote_id”与 WS 推送的 parent_quote_id 不一致导致无法判定成交的问题。
        self._pending_orders: Dict[str, Dict[str, Any]] = {}
        self._quote_to_pending: Dict[str, str] = {}
        self._match_window_seconds: float = float(config.extra_params.get("ws_match_window_seconds", 15.0))
        self._pending_ttl_seconds: float = float(config.extra_params.get("ws_pending_ttl_seconds", 60.0))

        # 可选：是否把每条 WS trade/clearing_event 摘要写入日志文件（不刷终端）
        self._log_trade_events: bool = bool(config.extra_params.get("ws_log_trade_events", False))
        self._log_clearing_events: bool = bool(config.extra_params.get("ws_log_clearing_events", False))

        # 应用层 heartbeat（与前端 message 形态对齐，用于部分服务端“必须发心跳才推送事件”的场景）
        self._send_app_heartbeat = bool(config.extra_params.get("ws_send_app_heartbeat", True))
        self._app_heartbeat_seconds = int(config.extra_params.get("ws_app_heartbeat_seconds", 5))

    async def connect(self, *, timeout_seconds: float = 3.0) -> bool:
        """
        主动建立 WS 连接（用于与套利执行器的“先检查 ws_connected 再订阅”的流程对齐）。
        - 失败不抛出（返回 False），避免阻塞系统启动
        """
        try:
            await self._ensure_running()
            # 等待 _ws_connected 变为 True（最多 timeout_seconds）
            deadline = asyncio.get_event_loop().time() + max(0.1, float(timeout_seconds))
            while asyncio.get_event_loop().time() < deadline:
                if self._ws_connected:
                    return True
                await asyncio.sleep(0.05)
            return bool(self._ws_connected)
        except Exception as e:
            if self._logger:
                self._logger.warning(f"[variational-ws] connect failed: {e}")
            return False

    def register_pending_order(
        self,
        *,
        local_tracking_id: str,
        symbol: str,
        side: OrderSide,
        qty: Decimal,
        max_slippage: Optional[Decimal] = None,
    ) -> None:
        """
        在 REST 下单后登记一个待匹配订单。
        WS 收到 trade 后会按 (symbol, side, qty, 时间窗口) 自动匹配并建立 alias：
        - quote_id -> local_tracking_id
        最终 clearing_event success 时，FILLED 会同时缓存到：
        - quote_id / rfq_id / local_tracking_id
        """
        try:
            lid = str(local_tracking_id or "").strip()
            sym = str(symbol or "").strip().upper()
            if not lid or not sym:
                return
            q = Decimal(str(qty))
            if q <= 0:
                return
            rec: Dict[str, Any] = {
                "symbol": sym,
                "side": side,
                "qty": q,
                "created_mono": asyncio.get_event_loop().time(),
            }
            if max_slippage is not None:
                try:
                    ms = Decimal(str(max_slippage))
                    if ms > 0:
                        rec["max_slippage"] = str(ms)
                except Exception:
                    # 不强制：解析失败则忽略
                    pass
            self._pending_orders[lid] = rec
        except Exception:
            return

    def _cleanup_pending(self, now_mono: float) -> None:
        try:
            ttl = max(1.0, float(self._pending_ttl_seconds))
            to_del: List[str] = []
            for k, v in list(self._pending_orders.items()):
                ts = float(v.get("created_mono") or 0.0)
                if now_mono - ts > ttl:
                    to_del.append(k)
            for k in to_del:
                self._pending_orders.pop(k, None)
        except Exception:
            return

    @property
    def connected(self) -> bool:
        return bool(self._ws_connected)

    @staticmethod
    def _normalize_token(raw: str) -> str:
        s = str(raw or "").strip().strip('"').strip("'")
        if not s:
            return ""
        lower = s.lower()
        if lower.startswith("bearer "):
            return s.split(" ", 1)[1].strip()
        # cookie 场景：提取 vr-token=...
        if "vr-token=" in lower:
            parts = [p.strip() for p in s.split(";") if p.strip()]
            for p in parts:
                if "=" not in p:
                    continue
                k, v = p.split("=", 1)
                if k.strip().lower() == "vr-token" and v.strip():
                    return v.strip().strip('"').strip("'")
        return s

    def _build_ws_url(self) -> str:
        base = self._events_url
        if not self._auth_token:
            return base
        sep = "&" if "?" in base else "?"
        # token 需要 URL encode（JWT 里可能包含 '=' 等字符）
        encoded = quote(self._auth_token, safe="")
        return f"{base}{sep}{self._auth_param}={encoded}"

    def _build_portfolio_ws_url(self) -> str:
        """
        /portfolio WS 也可能使用 query param auth=<jwt>。若未单独配置 token，则复用 events token。
        """
        base = self._portfolio_url
        token = self._portfolio_auth_token or self._auth_token
        if not token:
            return base
        sep = "&" if "?" in base else "?"
        encoded = quote(token, safe="")
        return f"{base}{sep}{self._auth_param}={encoded}"

    def get_cached_positions(self) -> List[PositionData]:
        """返回最近一次从 /portfolio WS 解析到的持仓快照（用于 UI/风险控制轮询）。"""
        try:
            return list(self._positions_cache.values())
        except Exception:
            return []

    def get_cached_portfolio_balance(self) -> Optional[Decimal]:
        """返回最近一次从 /portfolio WS 解析到的权益余额（Portfolio Value）。"""
        try:
            return self._portfolio_balance
        except Exception:
            return None

    async def set_portfolio_enable(self, enabled: bool) -> None:
        """
        动态启用/禁用 /portfolio WS。

        需求背景：
        - events WS 通常稳定，但 /portfolio WS 在交易所维护后可能频繁断开/重连
        - 为了让上层可以切换“账户数据源”(WS vs REST)，需要能在运行时关闭 portfolio 连接
        """
        enabled = bool(enabled)
        if enabled == bool(getattr(self, "_portfolio_enable", False)):
            return

        # 先修改开关：后续 _ensure_running 会依据它决定是否启动 portfolio task
        self._portfolio_enable = enabled

        # 关闭时：立即停止 portfolio task / 心跳并断开 portfolio ws，避免重连风暴
        if not enabled:
            async with self._lock:
                try:
                    if self._portfolio_task:
                        self._portfolio_task.cancel()
                        self._portfolio_task = None
                except Exception:
                    pass
                try:
                    if self._portfolio_hb_task:
                        self._portfolio_hb_task.cancel()
                        self._portfolio_hb_task = None
                except Exception:
                    pass
                try:
                    if self._portfolio_ws:
                        await self._portfolio_ws.close()
                        self._portfolio_ws = None
                except Exception:
                    pass
                self._portfolio_connected = False

    @staticmethod
    def _safe_decimal(v: Any) -> Decimal:
        try:
            return Decimal(str(v))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _parse_iso_dt(v: Any) -> datetime:
        if not v:
            return datetime.now()
        try:
            s = str(v).strip()
            # 兼容 "2025-12-22T05:45:01.819030Z"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            return datetime.now()

    def _parse_portfolio_positions(self, payload: Dict[str, Any]) -> List[PositionData]:
        """
        从 /portfolio WS 推送中解析 PositionData。
        只做“能用的最小解析”：不猜测字段，缺失则填 0/None，并把原始结构保存在 raw_data。
        """
        # 兼容两种常见结构（且避免误判）：
        # 1) {"pool_portfolio_result": {"positions": [...]}}
        # 2) {"positions": [...]}  （浏览器扩展抓到的 json_path 常见为这种）
        # 注意：有些消息可能同时包含两者，但 positions 只在顶层；因此只有在
        # pool_portfolio_result.positions 确实为 list 时才优先使用它，否则回退到顶层。
        result: Dict[str, Any] = payload
        maybe = payload.get("pool_portfolio_result")
        if isinstance(maybe, dict):
            maybe_positions = maybe.get("positions")
            if isinstance(maybe_positions, list):
                result = maybe

        positions = result.get("positions")
        if not isinstance(positions, list):
            return []

        out: List[PositionData] = []
        ts = self._parse_iso_dt(result.get("published_at") or result.get("updated_at"))

        for p in positions:
            if not isinstance(p, dict):
                continue
            pos_info = p.get("position_info")
            if not isinstance(pos_info, dict):
                continue

            inst = pos_info.get("instrument")
            underlying = None
            if isinstance(inst, dict):
                underlying = inst.get("underlying")
            elif isinstance(inst, str) and inst.strip():
                # 兼容 instrument="P-ETH-USDC-3600" 这种字符串
                s = inst.strip()
                if s.startswith("P-") and "-" in s:
                    parts = s.split("-")
                    if len(parts) >= 2:
                        underlying = parts[1]
                else:
                    underlying = s
            sym = str(underlying or "").strip().upper() or "UNKNOWN"

            # qty 字段在不同版本可能不同，做最小兼容
            qty_val = (
                pos_info.get("qty")
                or pos_info.get("position_qty")
                or pos_info.get("quantity")
                or pos_info.get("size")
                or p.get("qty")
                or p.get("position_qty")
                or p.get("quantity")
                or p.get("size")
                or 0
            )
            qty = self._safe_decimal(qty_val)
            side = PositionSide.LONG
            size = qty
            # 如果 qty 为负数，按 SHORT 处理
            if qty < 0:
                side = PositionSide.SHORT
                size = abs(qty)

            entry = self._safe_decimal(pos_info.get("avg_entry_price") or 0)
            liq = pos_info.get("estimated_liquidation_price")
            liq_price = None if liq in (None, "", "null") else self._safe_decimal(liq)

            # price_info 里常见有 price（类似 mark）
            mark_price = None
            price_info = p.get("price_info") or pos_info.get("price_info")
            if isinstance(price_info, dict):
                pv = price_info.get("price")
                if pv is not None:
                    mark_price = self._safe_decimal(pv)

            upnl = self._safe_decimal(p.get("upnl") or pos_info.get("upnl") or 0)
            rpnl = self._safe_decimal(p.get("rpnl") or pos_info.get("rpnl") or 0)

            out.append(
                PositionData(
                    symbol=sym,
                    side=side,
                    size=size,
                    entry_price=entry,
                    mark_price=mark_price,
                    current_price=mark_price,
                    unrealized_pnl=upnl,
                    realized_pnl=rpnl,
                    percentage=None,
                    leverage=int(pos_info.get("leverage") or 1),
                    margin_mode=MarginMode.CROSS,
                    margin=Decimal("0"),
                    liquidation_price=liq_price,
                    timestamp=ts,
                    raw_data={"portfolio": payload, "position": p},
                )
            )

        return out

    def _parse_portfolio_balance(self, payload: Dict[str, Any]) -> Optional[Decimal]:
        """
        从 /portfolio WS 推送中提取权益余额（Portfolio Value）。
        约定：优先读取 pool_portfolio_result.balance；否则回退到顶层 balance。
        """
        try:
            v = None
            maybe = payload.get("pool_portfolio_result")
            if isinstance(maybe, dict) and maybe.get("balance") is not None:
                v = maybe.get("balance")
            elif payload.get("balance") is not None:
                v = payload.get("balance")
            if v is None:
                return None
            d = self._safe_decimal(v)
            return d if d > Decimal("0") else d
        except Exception:
            return None

    async def close(self) -> None:
        async with self._lock:
            if self._task:
                self._task.cancel()
                self._task = None
            if self._portfolio_task:
                self._portfolio_task.cancel()
                self._portfolio_task = None
            if self._hb_task:
                self._hb_task.cancel()
                self._hb_task = None
            if self._portfolio_hb_task:
                self._portfolio_hb_task.cancel()
                self._portfolio_hb_task = None
            if self._ws:
                await self._ws.close()
                self._ws = None
            if self._portfolio_ws:
                await self._portfolio_ws.close()
                self._portfolio_ws = None
            if self._session:
                await self._session.close()
                self._session = None
            self._ws_connected = False
            self._portfolio_connected = False

    async def _ensure_running(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                # events 已在跑，下面只需要确认 portfolio 任务即可
                pass
            else:
                self._task = asyncio.create_task(self._run_forever())

            if self._portfolio_enable:
                if not self._portfolio_task or self._portfolio_task.done():
                    self._portfolio_task = asyncio.create_task(self._run_portfolio_forever())

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        timeout = aiohttp.ClientTimeout(total=max(1, int(self._config.request_timeout or 10)))
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _connect(self) -> None:
        session = await self._ensure_session()
        url = self._build_ws_url()

        # 前端通常会带 Origin/Referer/User-Agent；部分服务端会校验这些头。
        # 这里给出“浏览器友好”的默认头，并允许通过 extra_params.ws_headers 覆盖/补充。
        ws_headers: Dict[str, str] = {}
        try:
            origin = str(self._config.extra_params.get("origin") or "https://omni.variational.io").strip()
            referer = str(self._config.extra_params.get("referer") or "https://omni.variational.io/").strip()
            user_agent = str(self._config.extra_params.get("user_agent") or "").strip()
            if origin:
                ws_headers["Origin"] = origin
            if referer:
                ws_headers["Referer"] = referer
            if user_agent:
                ws_headers["User-Agent"] = user_agent
        except Exception:
            ws_headers = {}

        extra = self._config.extra_params.get("ws_headers")
        if isinstance(extra, dict):
            for k, v in extra.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    ws_headers[k] = v

        # heartbeat 会定期发 ping，避免服务端空闲断连；也能更快感知断开
        heartbeat_s = int(self._config.extra_params.get("ws_heartbeat_seconds", 20))
        protocols = None
        if isinstance(self._protocols, list):
            protocols = [str(p).strip() for p in self._protocols if str(p).strip()]

        self._ws = await session.ws_connect(
            url,
            headers=ws_headers or None,
            heartbeat=max(5, heartbeat_s),
            autoping=True,
            protocols=protocols,
        )
        self._ws_connected = True
        self._last_recv_ts = asyncio.get_event_loop().time()
        if self._logger:
            now = asyncio.get_event_loop().time()
            # 避免频繁重连刷屏：至少间隔 2 秒才打印一次 connected
            if now - self._last_connected_log >= 2.0:
                self._last_connected_log = now
                self._logger.info(
                    "[variational-ws] WS已连接(events): 鉴权=%s 心跳=%ss Origin=%s Protocols=%s 参数名=%s",
                    "是" if bool(self._auth_token) else "否",
                    max(5, heartbeat_s),
                    ws_headers.get("Origin"),
                    protocols,
                    self._auth_param,
                )

        # 可选：连接后发送订阅/握手消息（某些服务端需要，否则会很快断开 -> close_code=1006）
        await self._send_on_connect_messages()
        self._start_app_heartbeat()

    async def _connect_portfolio(self) -> None:
        """
        连接 /portfolio WS：用于接收账户/持仓推送。
        """
        session = await self._ensure_session()
        url = self._build_portfolio_ws_url()

        ws_headers: Dict[str, str] = {}
        try:
            origin = str(self._config.extra_params.get("origin") or "https://omni.variational.io").strip()
            referer = str(self._config.extra_params.get("referer") or "https://omni.variational.io/").strip()
            user_agent = str(self._config.extra_params.get("user_agent") or "").strip()
            if origin:
                ws_headers["Origin"] = origin
            if referer:
                ws_headers["Referer"] = referer
            if user_agent:
                ws_headers["User-Agent"] = user_agent
        except Exception:
            ws_headers = {}

        extra = self._config.extra_params.get("ws_headers")
        if isinstance(extra, dict):
            for k, v in extra.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    ws_headers[k] = v

        heartbeat_s = int(self._config.extra_params.get("ws_heartbeat_seconds", 20))
        protocols = None
        if isinstance(self._protocols, list):
            protocols = [str(p).strip() for p in self._protocols if str(p).strip()]

        self._portfolio_ws = await session.ws_connect(
            url,
            headers=ws_headers or None,
            heartbeat=max(5, heartbeat_s),
            autoping=True,
            protocols=protocols,
        )
        self._portfolio_connected = True
        self._last_portfolio_recv_ts = asyncio.get_event_loop().time()

        # 默认：自动发送 claims 握手（与 events 对齐）
        token = self._portfolio_auth_token or self._auth_token
        if token and self._send_claims_on_connect:
            try:
                await self._portfolio_ws.send_str(json.dumps({"claims": token}, ensure_ascii=False))
                if self._logger:
                    self._logger.info("[variational-ws] 已发送 claims 握手(portfolio)")
            except Exception as e:
                if self._logger:
                    self._logger.warning(f"[variational-ws] claims 握手发送失败(portfolio): {e}")

        self._start_portfolio_app_heartbeat()
        await self._send_portfolio_on_connect_messages()

    def _start_portfolio_app_heartbeat(self) -> None:
        if not self._send_app_heartbeat:
            return
        if self._portfolio_hb_task and not self._portfolio_hb_task.done():
            return

        async def _loop() -> None:
            interval = max(1, int(self._app_heartbeat_seconds))
            while True:
                try:
                    if not self._portfolio_ws or not self._portfolio_connected:
                        await asyncio.sleep(0.2)
                        continue
                    payload = {
                        "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                        "type": "heartbeat",
                    }
                    await self._portfolio_ws.send_str(json.dumps(payload, ensure_ascii=False))
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self._logger:
                        self._logger.debug(f"[variational-ws] portfolio app heartbeat send failed: {e}")
                    await asyncio.sleep(min(2.0, interval))

        self._portfolio_hb_task = asyncio.create_task(_loop())

    async def _send_portfolio_on_connect_messages(self) -> None:
        """
        portfolio WS 可能不会主动推送，需要发送订阅消息（来源：浏览器抓包）。
        这里不猜测消息内容，只允许通过配置注入。
        """
        if not self._portfolio_ws or not self._portfolio_connected:
            return

        msgs = self._portfolio_on_connect_messages
        if not msgs:
            # 只提示一次，避免刷屏
            if self._logger and not self._portfolio_subscribe_hint_once:
                self._portfolio_subscribe_hint_once = True
                self._logger.info(
                    "[variational-ws] 提示：portfolio WS 未配置订阅消息，可能收不到持仓推送。"
                    "请从浏览器抓包复制 WebSocket send 内容，填入 extra_params.ws_portfolio_on_connect_messages"
                )
            return

        if not isinstance(msgs, list):
            msgs = [msgs]

        token = self._portfolio_auth_token or self._auth_token or ""
        for m in msgs:
            try:
                payload = m
                if isinstance(payload, str):
                    payload = (
                        payload.replace("{{auth}}", token).replace("{{token}}", token)
                    )
                    await self._portfolio_ws.send_str(payload)
                elif isinstance(payload, dict):
                    s = json.dumps(payload, ensure_ascii=False)
                    s = s.replace("{{auth}}", token).replace("{{token}}", token)
                    await self._portfolio_ws.send_str(s)
                else:
                    continue
                if self._logger:
                    self._logger.info("[variational-ws] 已发送 on_connect 消息(portfolio)")
            except Exception as e:
                if self._logger:
                    self._logger.warning(f"[variational-ws] on_connect 消息发送失败(portfolio): {e}")

    def _start_app_heartbeat(self) -> None:
        if not self._send_app_heartbeat:
            return
        if self._hb_task and not self._hb_task.done():
            return

        async def _loop() -> None:
            interval = max(1, int(self._app_heartbeat_seconds))
            while True:
                try:
                    if not self._ws or not self._ws_connected:
                        await asyncio.sleep(0.2)
                        continue
                    payload = {
                        "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                        "type": "heartbeat",
                    }
                    await self._ws.send_str(json.dumps(payload, ensure_ascii=False))
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # 发送失败不抛出，交给 recv loop / 连接循环处理
                    if self._logger:
                        self._logger.debug(f"[variational-ws] app heartbeat send failed: {e}")
                    await asyncio.sleep(min(2.0, interval))

        self._hb_task = asyncio.create_task(_loop())

    async def _send_on_connect_messages(self) -> None:
        if not self._ws or not self._ws_connected:
            return
        msgs = self._on_connect_messages
        if not msgs:
            # 默认：自动发送 claims 握手（不需要额外配置）
            if self._send_claims_on_connect and self._auth_token:
                try:
                    await self._ws.send_str(json.dumps({"claims": self._auth_token}, ensure_ascii=False))
                    if self._logger:
                        self._logger.info("[variational-ws] 已发送 claims 握手(events)")
                except Exception as e:
                    if self._logger:
                        self._logger.warning(f"[variational-ws] claims 握手发送失败(events): {e}")
            return
        if not isinstance(msgs, list):
            msgs = [msgs]
        for m in msgs:
            try:
                payload = m
                # 支持简单占位符
                if isinstance(payload, str):
                    payload = (
                        payload
                        .replace("{{auth}}", self._auth_token or "")
                        .replace("{{token}}", self._auth_token or "")
                    )
                    await self._ws.send_str(payload)
                elif isinstance(payload, dict):
                    # dict -> json
                    s = json.dumps(payload, ensure_ascii=False)
                    s = (
                        s.replace("{{auth}}", self._auth_token or "")
                        .replace("{{token}}", self._auth_token or "")
                    )
                    await self._ws.send_str(s)
                else:
                    continue
                if self._logger:
                    self._logger.info("[variational-ws] 已发送 on_connect 消息(events)")
            except Exception as e:
                if self._logger:
                    self._logger.warning(f"[variational-ws] on_connect 消息发送失败(events): {e}")

    async def _run_forever(self) -> None:
        backoff = 0.5
        while True:
            try:
                await self._connect()
                backoff = 0.5
                await self._recv_loop()
                # 🔥 recv loop 正常返回也意味着“已断开”，需要退避避免重连风暴
                await asyncio.sleep(min(10.0, backoff))
                backoff = min(10.0, backoff * 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws_connected = False
                if self._logger:
                    self._logger.warning(f"[variational-ws] events 循环异常: {e}")
                await asyncio.sleep(min(10.0, backoff))
                backoff = min(10.0, backoff * 2)

    async def _run_portfolio_forever(self) -> None:
        backoff = 0.5
        while True:
            try:
                await self._connect_portfolio()
                backoff = 0.5
                await self._recv_portfolio_loop()
                # 🔥 recv loop 正常返回也意味着“已断开”，需要退避避免重连风暴
                await asyncio.sleep(min(10.0, backoff))
                backoff = min(10.0, backoff * 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._portfolio_connected = False
                if self._logger:
                    self._logger.warning(f"[variational-ws] portfolio 循环异常: {e}")
                await asyncio.sleep(min(10.0, backoff))
                backoff = min(10.0, backoff * 2)

    async def _recv_portfolio_loop(self) -> None:
        assert self._portfolio_ws is not None
        try:
            async for msg in self._portfolio_ws:
                self._last_portfolio_recv_ts = asyncio.get_event_loop().time()
                payload: Optional[Dict[str, Any]] = None
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = self._parse_portfolio_payload(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    try:
                        payload = self._parse_portfolio_payload(
                            msg.data.decode("utf-8", errors="ignore")
                        )
                    except Exception:
                        payload = None
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break

                if payload:
                    self._handle_portfolio_payload(payload)
        finally:
            try:
                code = getattr(self._portfolio_ws, "close_code", None)
            except Exception:
                code = None
            if self._portfolio_hb_task:
                self._portfolio_hb_task.cancel()
                self._portfolio_hb_task = None
            if self._logger:
                self._logger.warning("[variational-ws] WS已断开(portfolio): close_code=%s", code)
            # 🔥 WS断连计入 api_failure_pause（避免网络抖动/交易所故障导致持续运行）
            # - 正常关闭（1000）通常是主动 stop，不应触发风控
            if code not in (1000,):
                reporter = getattr(self, "_api_failure_reporter", None)
                if callable(reporter):
                    try:
                        reporter(
                            exchange=str(getattr(self._config, "exchange_id", "") or ""),
                            api="ws_disconnect_portfolio",
                            reason=f"close_code={code}",
                        )
                    except Exception:
                        pass
        self._portfolio_connected = False

    @staticmethod
    def _parse_portfolio_payload(text: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _handle_portfolio_payload(self, payload: Dict[str, Any]) -> None:
        try:
            # 一次性轻量日志：确认 portfolio WS 实际收到的消息结构（只打印 keys，避免刷屏）
            if self._logger and not self._sub_log_once.get("portfolio_first_payload"):
                self._sub_log_once["portfolio_first_payload"] = True
                try:
                    top_keys = list(payload.keys())[:30]
                except Exception:
                    top_keys = []
                self._logger.info(
                    "[variational-ws] portfolio 首包已收到: keys=%s",
                    top_keys,
                )

            # 🔥 权益余额（Portfolio Value）：用于 UI 余额展示与风控
            bal = self._parse_portfolio_balance(payload)
            if bal is not None:
                self._portfolio_balance = bal
                self._portfolio_balance_ts = asyncio.get_event_loop().time()

            positions = self._parse_portfolio_positions(payload)
            # 这是“快照”语义：如果消息里明确带 positions 列表，就应当以本次快照为准重建缓存；
            # 否则只增量更新会导致“已平仓的代币仍残留在缓存/UI”。
            snapshot_has_positions = False
            try:
                if isinstance(payload.get("positions"), list):
                    snapshot_has_positions = True
                else:
                    maybe = payload.get("pool_portfolio_result")
                    if isinstance(maybe, dict) and isinstance(maybe.get("positions"), list):
                        snapshot_has_positions = True
            except Exception:
                snapshot_has_positions = False

            # 一次性诊断：消息里有 positions，但我们解析为空 -> 说明字段名不匹配（便于你贴日志我再对齐）
            if self._logger and not positions and not self._sub_log_once.get("portfolio_positions_parse_empty"):
                try:
                    maybe = payload.get("pool_portfolio_result")
                    result = maybe if isinstance(maybe, dict) else payload
                    raw_positions = result.get("positions")
                    if isinstance(raw_positions, list) and len(raw_positions) > 0:
                        self._sub_log_once["portfolio_positions_parse_empty"] = True
                        p0 = raw_positions[0] if isinstance(raw_positions[0], dict) else {}
                        pi0 = p0.get("position_info") if isinstance(p0, dict) else None
                        self._logger.warning(
                            "[variational-ws] portfolio 收到 positions=%s 但解析为0；请核对字段名。positions[0].keys=%s position_info.keys=%s",
                            len(raw_positions),
                            list(p0.keys())[:40] if isinstance(p0, dict) else [],
                            list(pi0.keys())[:40] if isinstance(pi0, dict) else [],
                        )
                except Exception:
                    pass

            if snapshot_has_positions:
                # 快照：用本次 positions 重建缓存（允许为空 -> 清空）
                new_cache: Dict[str, PositionData] = {}
                for p in positions or []:
                    sym = str(getattr(p, "symbol", "") or "").strip().upper()
                    if sym:
                        new_cache[sym] = p
                self._positions_cache = new_cache
            elif positions:
                # 非快照：保留原逻辑，做增量更新
                for p in positions:
                    self._positions_cache[str(p.symbol or "").upper()] = p

            # 🔥 持仓快照摘要：每分钟最多打印一次（用于持续确认订阅/解析正常）
            # - 对于 snapshot_has_positions：即使 positions 为空也打印，避免 UI/风控误以为没更新
            if self._logger and (positions or snapshot_has_positions):
                now_mono = asyncio.get_event_loop().time()
                interval = max(10.0, float(self._portfolio_positions_log_interval_s))
                if now_mono - float(self._last_portfolio_positions_log_ts or 0.0) >= interval:
                    self._last_portfolio_positions_log_ts = now_mono
                    syms = ",".join(
                        sorted({str(p.symbol) for p in (positions or []) if getattr(p, "symbol", None)})
                    )
                    self._logger.info(
                        "[variational-ws] 持仓快照(portfolio): 数量=%s 代币=%s",
                        len(positions or []),
                        syms or "-",
                    )

            # 推给 subscribe_positions 的回调（同步/异步都兼容）
            # - snapshot_has_positions 时必须推送（即使为空），否则上层无法清理旧持仓
            if positions or snapshot_has_positions:
                for cb in list(self._pos_cbs):
                    try:
                        r = cb(list(positions or []))
                        if asyncio.iscoroutine(r):
                            asyncio.create_task(r)
                    except Exception:
                        if self._logger:
                            self._logger.error("[variational-ws] positions callback error", exc_info=True)
        except Exception:
            if self._logger:
                self._logger.error("[variational-ws] portfolio payload handle error", exc_info=True)

    async def _recv_loop(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                self._last_recv_ts = asyncio.get_event_loop().time()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    event = self._parse_event(msg.data)
                    if event:
                        await self._dispatch_event(event)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    # 有些服务可能用二进制帧传 JSON
                    try:
                        text = msg.data.decode("utf-8", errors="ignore")
                        event = self._parse_event(text)
                        if event:
                            await self._dispatch_event(event)
                    except Exception:
                        pass
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            # 记录断开原因，便于诊断为什么“连上但没数据”
            try:
                code = getattr(self._ws, "close_code", None)
            except Exception:
                code = None
            # 停止应用层 heartbeat
            if self._hb_task:
                self._hb_task.cancel()
                self._hb_task = None
            if self._logger:
                self._logger.warning("[variational-ws] WS已断开(events): close_code=%s", code)
            # 🔥 WS断连计入 api_failure_pause（避免网络抖动/交易所故障导致持续运行）
            # - 正常关闭（1000）通常是主动 stop，不应触发风控
            if code not in (1000,):
                reporter = getattr(self, "_api_failure_reporter", None)
                if callable(reporter):
                    try:
                        reporter(
                            exchange=str(getattr(self._config, "exchange_id", "") or ""),
                            api="ws_disconnect_events",
                            reason=f"close_code={code}",
                        )
                    except Exception:
                        pass
        self._ws_connected = False

    def _parse_event(self, text: str) -> Optional[VariationalWsEvent]:
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return VariationalWsEvent(raw=data)
        except Exception:
            return None
        return None

    async def _dispatch_event(self, event: VariationalWsEvent) -> None:
        # 1) raw user events
        for cb in list(self._user_cbs):
            try:
                r = cb(event.raw)
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                if self._logger:
                    self._logger.error("[variational-ws] user callback error", exc_info=True)

        # 2) order/fill events -> OrderData
        if event.type == "trade":
            if self._logger and self._log_trade_events:
                try:
                    d = event.data
                    qid = str(d.get("source_quote") or d.get("parent_quote_id") or d.get("quote_id") or "").strip()
                    rfq = str(d.get("source_rfq") or d.get("rfq_id") or "").strip()
                    side = str(d.get("side") or "").strip()
                    qty = str(d.get("qty") or "").strip()
                    price = str(d.get("price") or "").strip()
                    self._logger.info(
                        "[variational-ws] trade: quote_id=%s rfq_id=%s side=%s qty=%s price=%s",
                        qid,
                        rfq,
                        side,
                        qty,
                        price,
                    )
                except Exception:
                    pass
            order_update, maybe_final = self._accumulate_trade_event(event)
            if order_update:
                # 订单更新（包含部分成交累加）
                for cb in list(self._order_cbs):
                    try:
                        r = cb(order_update)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception:
                        if self._logger:
                            self._logger.error("[variational-ws] order callback error", exc_info=True)

            # 仅当明确“最终成交”时才触发 fills 回调（避免第一笔部分成交就被当成 fully-filled）
            if maybe_final:
                self._cache_filled_order(maybe_final)
                for cb in list(self._fill_cbs):
                    try:
                        r = cb(maybe_final)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception:
                        if self._logger:
                            self._logger.error("[variational-ws] fill callback error", exc_info=True)

        if event.type == "clearing_event":
            if self._logger and self._log_clearing_events:
                try:
                    d = event.data
                    rfq = str(d.get("rfq_id") or "").strip()
                    pq = str(d.get("parent_quote_id") or d.get("source_quote") or d.get("quote_id") or "").strip()
                    st = str(d.get("clearing_status") or "").strip()
                    self._logger.info(
                        "[variational-ws] clearing_event: rfq_id=%s parent_quote_id=%s status=%s",
                        rfq,
                        pq,
                        st,
                    )
                except Exception:
                    pass
            # clearing_event 可作为“最终确认”信号：把累计成交标记为 FILLED 并通知 fill 回调
            final = self._finalize_on_clearing_event(event)
            if final:
                self._cache_filled_order(final)
                # 也发一次订单更新（FILLED）
                for cb in list(self._order_cbs):
                    try:
                        r = cb(final)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception:
                        if self._logger:
                            self._logger.error("[variational-ws] order callback error", exc_info=True)
                for cb in list(self._fill_cbs):
                    try:
                        r = cb(final)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception:
                        if self._logger:
                            self._logger.error("[variational-ws] fill callback error", exc_info=True)

        # 3) positions (暂不强制解析，先把 raw 发出去即可)
        if event.type in ("positions", "position", "portfolio"):
            for cb in list(self._pos_cbs):
                try:
                    r = cb(event.raw)
                    if asyncio.iscoroutine(r):
                        await r
                except Exception:
                    if self._logger:
                        self._logger.error("[variational-ws] positions callback error", exc_info=True)

    def _cache_filled_order(self, order: OrderData) -> None:
        """
        将最终成交缓存到多个 key 下，便于上层用不同ID（quote_id / rfq_id）查找。
        """
        try:
            self._filled_cache[str(order.id)] = order
            raw = getattr(order, "raw_data", {}) or {}
            # ✅ 可验证日志：最终成交时打印 max_slippage 与 avg_fill_price
            if self._logger:
                try:
                    max_slip = None
                    confirmed_by = None
                    ws_quote_id = None
                    if isinstance(raw, dict):
                        max_slip = raw.get("max_slippage") or raw.get("request_max_slippage")
                        confirmed_by = raw.get("confirmed_by")
                        ws_quote_id = raw.get("ws_quote_id")
                    self._logger.info(
                        "🧾 [Variational-Frontend] FILL compare | id=%s quote_id=%s symbol=%s side=%s qty=%s | "
                        "max_slippage=%s | avg_fill_price=%s | confirmed_by=%s",
                        str(getattr(order, "id", "")),
                        str(ws_quote_id) if ws_quote_id is not None else "-",
                        str(getattr(order, "symbol", "")),
                        str(getattr(getattr(order, "side", None), "value", None) or ""),
                        str(getattr(order, "amount", "")),
                        str(max_slip) if max_slip is not None else "None",
                        str(getattr(order, "average", None)) if getattr(order, "average", None) is not None else "None",
                        str(confirmed_by) if confirmed_by is not None else "-",
                    )
                except Exception:
                    pass

            # 兼容：如果对外 emit 使用了 pending_id 作为 order.id，则 raw_data 里会保留 ws_quote_id
            quote_id = None
            if isinstance(raw, dict):
                q = raw.get("ws_quote_id")
                if q and str(q).strip():
                    quote_id = str(q).strip()
                    self._filled_cache[quote_id] = order

            # 若存在 quote_id -> pending_id 映射，也缓存到 pending_id 下（双向兜底）
            if quote_id:
                pending_id = self._quote_to_pending.get(quote_id)
                if pending_id:
                    self._filled_cache[str(pending_id)] = order
            else:
                pending_id = self._quote_to_pending.get(str(order.id))
                if pending_id:
                    self._filled_cache[str(pending_id)] = order
            ws_event = raw.get("ws_event") if isinstance(raw, dict) else None
            data = None
            if isinstance(ws_event, dict):
                data = ws_event.get("data") if isinstance(ws_event.get("data"), dict) else None
            if isinstance(data, dict):
                # trade: source_rfq / rfq_id
                rfq = data.get("source_rfq") or data.get("rfq_id")
                if rfq and str(rfq).strip():
                    self._filled_cache[str(rfq).strip()] = order
        except Exception:
            return

    def _parse_trade_core(self, d: Dict[str, Any]) -> Optional[Tuple[str, OrderSide, Decimal, Decimal, str, str]]:
        """
        从 trade event.data 提取核心字段：
        - quote_id（source_quote）
        - rfq_id（source_rfq，可用于更稳定追踪）
        - side
        - qty
        - price
        - symbol（underlying）
        """
        source_quote = d.get("source_quote") or d.get("parent_quote_id") or d.get("quote_id")
        if not source_quote:
            return None
        quote_id = str(source_quote).strip()
        if not quote_id:
            return None

        rfq = str(d.get("source_rfq") or d.get("rfq_id") or "").strip()

        side_raw = str(d.get("side") or "").strip().lower()
        side = OrderSide.BUY if side_raw == "buy" else OrderSide.SELL if side_raw == "sell" else OrderSide.BUY

        try:
            qty = Decimal(str(d.get("qty") or "0"))
            price = Decimal(str(d.get("price") or "0"))
        except Exception:
            return None

        sym = ""
        inst = d.get("instrument")
        if isinstance(inst, dict):
            sym = str(inst.get("underlying") or "").strip().upper()

        return quote_id, side, qty, price, (sym or "UNKNOWN"), rfq

    def _accumulate_trade_event(self, event: VariationalWsEvent) -> Tuple[Optional[OrderData], Optional[OrderData]]:
        """
        处理 trade：
        - 累加同一 quote_id 的成交 qty / notional，计算加权均价
        - 返回 order_update（status=OPEN，表示有成交更新）
        - maybe_final：默认不在 trade 就标记 FILLED（避免部分成交误判），除非服务端明确给出“终态信号”
        """
        d = event.data
        core = self._parse_trade_core(d)
        if not core:
            return None, None
        quote_id, side, qty, price, sym, rfq = core
        if qty <= 0:
            return None, None

        # 默认用 quote_id 作为内部聚合 key；但对外（发给执行器）尽量用本地 pending_id（下单 tracking_id）
        pending_id: Optional[str] = None

        # 尝试把 WS quote_id 关联到本地 pending tracking_id（解决“请求 quote_id ≠ WS quote_id”）
        try:
            now_mono = asyncio.get_event_loop().time()
            self._cleanup_pending(now_mono)
            if quote_id not in self._quote_to_pending and self._pending_orders:
                best_id: Optional[str] = None
                best_dt: float = 10**9
                win = max(1.0, float(self._match_window_seconds))
                for pid, pv in list(self._pending_orders.items()):
                    if str(pv.get("symbol") or "").upper() != sym.upper():
                        continue
                    if pv.get("side") != side:
                        continue
                    pqty: Decimal = pv.get("qty") or Decimal("0")
                    # 允许部分成交：trade.qty <= pending.qty + epsilon
                    eps = max(Decimal("1e-12"), pqty * Decimal("0.0001"))
                    if qty > pqty + eps:
                        continue
                    ts = float(pv.get("created_mono") or 0.0)
                    dt = now_mono - ts
                    if dt < 0 or dt > win:
                        continue
                    if dt < best_dt:
                        best_dt = dt
                        best_id = pid
                if best_id:
                    self._quote_to_pending[quote_id] = best_id
                    if rfq:
                        self._rfq_to_quote[rfq] = quote_id
                    pending_id = best_id
                    if self._logger:
                        self._logger.info(
                            "[variational-ws] 成交匹配: tracking_id=%s quote_id=%s rfq_id=%s 代币=%s 方向=%s 成交量=%s 匹配耗时=%.3fs",
                            best_id,
                            quote_id,
                            (rfq or ""),
                            sym,
                            str(side.value),
                            str(qty),
                            float(best_dt),
                        )
        except Exception:
            pass

        # 如果之前已匹配过，也补上 pending_id 方便后续 emit
        if not pending_id:
            pending_id = self._quote_to_pending.get(quote_id)

        acc = self._fill_acc.get(quote_id)
        if not acc:
            acc = {"qty": Decimal("0"), "notional": Decimal("0"), "side": side, "symbol": sym, "trades": []}
            self._fill_acc[quote_id] = acc
        acc["qty"] = (acc.get("qty") or Decimal("0")) + qty
        # 若价格缺失则不累 notional
        if price > 0:
            acc["notional"] = (acc.get("notional") or Decimal("0")) + (price * qty)
        acc["side"] = side
        acc["symbol"] = sym
        trades = acc.get("trades")
        if isinstance(trades, list):
            trades.append(d)
        else:
            acc["trades"] = [d]

        total_qty: Decimal = acc["qty"]
        total_notional: Decimal = acc.get("notional") or Decimal("0")
        avg = (total_notional / total_qty) if total_qty > 0 and total_notional > 0 else (price if price > 0 else None)
        cost = total_notional if total_notional > 0 else (price * total_qty if price > 0 else Decimal("0"))

        now = datetime.now()

        # 从 pending 里拿“目标下单量”，用于判断是否已完全成交（trade 累计已足够时可直接标记 FILLED）
        expected_qty: Optional[Decimal] = None
        if pending_id:
            pv = self._pending_orders.get(str(pending_id)) if isinstance(self._pending_orders, dict) else None
            if isinstance(pv, dict):
                try:
                    expected_qty = Decimal(str(pv.get("qty") or "0"))
                except Exception:
                    expected_qty = None
                # 透传下单时的 max_slippage（用于最终 FILL compare 日志）
                try:
                    ms = pv.get("max_slippage")
                    if ms not in (None, ""):
                        event.raw.setdefault("_meta", {})  # 不影响原始语义，仅用于内部观测
                        if isinstance(event.raw.get("_meta"), dict):
                            event.raw["_meta"]["max_slippage"] = str(ms)
                except Exception:
                    pass

        emit_id = str(pending_id) if pending_id else quote_id
        # 允许一个很小的 epsilon，避免精度差导致“0.1 != 0.1000000001”
        eps = Decimal("1e-10")
        reached_expected = bool(expected_qty and expected_qty > Decimal("0") and total_qty + eps >= expected_qty)
        final_qty = expected_qty if reached_expected and expected_qty else total_qty

        order_update = OrderData(
            id=emit_id,
            client_id=emit_id,  # 让执行器可以用 order_id 或 client_id 都能匹配到
            symbol=sym,
            side=side,
            type=OrderType.MARKET,
            amount=final_qty,
            price=None,
            filled=min(total_qty, final_qty) if final_qty > Decimal("0") else total_qty,
            remaining=max(final_qty - total_qty, Decimal("0")) if final_qty > Decimal("0") else Decimal("0"),
            cost=cost,
            average=avg,
            status=OrderStatus.FILLED if reached_expected else OrderStatus.OPEN,
            timestamp=now,
            updated=now,
            fee=None,
            trades=list(acc.get("trades") or []),
            params={},
            raw_data={
                "ws_event": event.raw,
                "ws_quote_id": quote_id,
                "ws_rfq_id": rfq,
                "confirmed_by": "trade_full_qty" if reached_expected else "trade_partial",
                "expected_qty": str(expected_qty) if expected_qty is not None else None,
                "agg": {"qty": str(total_qty), "notional": str(total_notional)},
                "max_slippage": (event.raw.get("_meta") or {}).get("max_slippage") if isinstance(event.raw.get("_meta"), dict) else None,
            },
        )

        # 若服务端显式给出终态字段（兼容），可直接标记 FILLED
        status_raw = str(d.get("status") or "").strip().lower()
        if status_raw in ("filled", "closed") and total_qty > 0:
            final = OrderData(**{**order_update.__dict__, "status": OrderStatus.FILLED})
            return final, final

        # 🔥 关键：如果 trade 累计已达到“下单数量”，直接视为成交（避免 clearing_event 丢失导致永远确认不了）
        if reached_expected:
            return order_update, order_update

        return order_update, None

    def _finalize_on_clearing_event(self, event: VariationalWsEvent) -> Optional[OrderData]:
        d = event.data
        rfq_id = d.get("rfq_id")
        parent_quote_id = d.get("parent_quote_id") or d.get("source_quote") or d.get("quote_id")
        clearing_status = str(d.get("clearing_status") or "").strip().lower()

        quote_id = str(parent_quote_id).strip() if parent_quote_id else ""
        if not quote_id:
            return None
        if isinstance(rfq_id, str) and rfq_id.strip():
            self._rfq_to_quote[rfq_id.strip()] = quote_id

        # 只有成功的清算才作为“最终成交确认”
        if "success" not in clearing_status:
            return None

        acc = self._fill_acc.get(quote_id)
        if not acc:
            return None
        total_qty: Decimal = acc.get("qty") or Decimal("0")
        total_notional: Decimal = acc.get("notional") or Decimal("0")
        side: OrderSide = acc.get("side") or OrderSide.BUY
        sym: str = acc.get("symbol") or "UNKNOWN"
        avg = (total_notional / total_qty) if total_qty > 0 and total_notional > 0 else None
        now = datetime.now()
        pending_id = self._quote_to_pending.get(quote_id)
        emit_id = str(pending_id) if pending_id else quote_id
        max_slippage = None
        if pending_id:
            pv = self._pending_orders.get(str(pending_id)) if isinstance(self._pending_orders, dict) else None
            if isinstance(pv, dict):
                max_slippage = pv.get("max_slippage")
        return OrderData(
            id=emit_id,
            client_id=emit_id,
            symbol=sym,
            side=side,
            type=OrderType.MARKET,
            amount=total_qty,
            price=None,
            filled=total_qty,
            remaining=Decimal("0"),
            cost=total_notional if total_notional > 0 else Decimal("0"),
            average=avg,
            status=OrderStatus.FILLED,
            timestamp=now,
            updated=now,
            fee=None,
            trades=list(acc.get("trades") or []),
            params={},
            raw_data={
                "ws_event": event.raw,
                "ws_quote_id": quote_id,
                "confirmed_by": "clearing_event",
                "agg": {"qty": str(total_qty), "notional": str(total_notional)},
                "max_slippage": str(max_slippage) if max_slippage not in (None, "") else None,
            },
        )

    # ===== public subscription APIs (compatible with existing executor) =====

    async def subscribe_user_data(self, callback: Callable[..., Any]) -> None:
        if callback not in self._user_cbs:
            self._user_cbs.append(callback)
        if self._logger and not self._sub_log_once.get("user_data"):
            self._sub_log_once["user_data"] = True
            self._logger.info(
                "[variational-ws] subscribe_user_data registered (callbacks=%s, url=%s, token=%s, on_connect_msgs=%s)",
                len(self._user_cbs),
                self._events_url,
                "yes" if bool(self._auth_token) else "no",
                "yes" if bool(self._on_connect_messages) else "no",
            )
        await self._ensure_running()

    async def subscribe_order_fills(self, callback: Callable[..., Any], symbol: Optional[str] = None) -> None:
        # 只接收“最终成交”（clearing_event success 或明确 filled）
        if callback not in self._fill_cbs:
            self._fill_cbs.append(callback)
        if self._logger and not self._sub_log_once.get("order_fills"):
            self._sub_log_once["order_fills"] = True
            self._logger.info(
                "[variational-ws] subscribe_order_fills registered (callbacks=%s, url=%s)",
                len(self._fill_cbs),
                self._events_url,
            )
        await self._ensure_running()

    async def subscribe_orders(self, callback: Callable[..., Any], symbol: Optional[str] = None) -> None:
        # 用 trade 事件做“成交进度更新”（OPEN），clearing_event 做“最终成交确认”（FILLED）
        if callback not in self._order_cbs:
            self._order_cbs.append(callback)
        if self._logger and not self._sub_log_once.get("orders"):
            self._sub_log_once["orders"] = True
            self._logger.info(
                "[variational-ws] subscribe_orders registered (callbacks=%s, url=%s)",
                len(self._order_cbs),
                self._events_url,
            )
        await self._ensure_running()

    async def subscribe_positions(self, callback: Callable[..., Any]) -> None:
        if callback not in self._pos_cbs:
            self._pos_cbs.append(callback)
        if self._logger and not self._sub_log_once.get("positions"):
            self._sub_log_once["positions"] = True
            self._logger.info(
                "[variational-ws] subscribe_positions 已注册: callbacks=%s | portfolio_enable=%s | portfolio_url=%s",
                len(self._pos_cbs),
                "是" if bool(self._portfolio_enable) else "否",
                self._portfolio_url,
            )
        await self._ensure_running()

    def lookup_cached_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[OrderData]:
        if not order_id:
            return None
        oid = str(order_id)
        hit = self._filled_cache.get(oid)
        if hit:
            return hit
        # 若传入的是 rfq_id，则尝试映射到 quote_id
        qid = self._rfq_to_quote.get(oid)
        if qid:
            hit = self._filled_cache.get(str(qid))
            if hit:
                return hit
        # 若传入的是本地 tracking_id（pending），且已经建立映射，则返回 quote_id 的缓存
        for quote_id, pending_id in list(self._quote_to_pending.items()):
            if pending_id == oid:
                return self._filled_cache.get(str(quote_id))
        return None


