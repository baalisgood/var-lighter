"""
Variational (Omni) 交易所适配器 - 行情(BBO)版

背景：
- Variational 前端页面仅展示最优买/卖价（BBO），不提供传统 orderbook 深度
- 通过 `POST /api/quotes/indicative` 可获取 bid/ask/mark_price（插件已验证）

本适配器当前只实现“行情读取（get_ticker/get_tickers）”，
其余交易/账户能力后续如需可再扩展（遵循分层架构与 DI 规范）。
"""

from __future__ import annotations

import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Callable
import time
import uuid

import aiohttp

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    OHLCVData,
    OrderBookData,
    PositionData,
    TickerData,
    TradeData,
    OrderData,
    OrderSide,
    OrderType,
)

from .variational_rest import (
    VariationalAuthError,
    VariationalIndicativeQuoteRequest,
    VariationalRestClient,
)
from .variational_trade_client import (
    VariationalPrivateTradingClient,
    VariationalRequestTemplate,
)
from ..models import OrderStatus, PositionSide, MarginMode
from .variational_websocket import VariationalWebSocket


class VariationalAdapter(ExchangeAdapter):
    """Variational 适配器（只提供 BBO 行情）"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)
        # 🔥 重要：Variational 的 WS events 会产生较多 INFO 日志。
        # 在终端 UI 场景下，我们只希望写入日志文件，不希望输出到终端。
        # core.logging 的 BaseLogger 默认会挂一个 StreamHandler，这里仅对 variational 移除它。
        try:
            base_logger = getattr(self, "logger", None)
            # BaseLogger.logger -> logging.Logger
            inner = getattr(base_logger, "logger", None)
            if inner and hasattr(inner, "handlers"):
                for h in list(inner.handlers):
                    # 只移除“控制台输出”的 StreamHandler，保留 FileHandler（FileHandler 也是 StreamHandler 的子类）
                    if isinstance(h, logging.StreamHandler) and (not isinstance(h, logging.FileHandler)):
                        inner.removeHandler(h)

                # 若之前版本误删了文件 handler，这里兜底补回，确保仍写入 logs/ExchangeAdapter.variational.log
                has_file_handler = any(isinstance(
                    h, logging.FileHandler) for h in inner.handlers)
                if not has_file_handler:
                    file_handler = RotatingFileHandler(
                        "logs/ExchangeAdapter.variational.log",
                        maxBytes=5 * 1024 * 1024,
                        backupCount=3,
                        encoding="utf-8",
                    )
                    file_handler.setLevel(logging.INFO)
                    formatter = logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
                    )
                    file_handler.setFormatter(formatter)
                    inner.addHandler(file_handler)
                    inner.setLevel(logging.INFO)
                    inner.propagate = False
        except Exception:
            pass

        self._rest = VariationalRestClient(config=config, logger=self.logger)
        self._connected = False
        # 🔥 UI/风控会读取 adapter._position_cache（参考 LighterWebSocket 实现）
        # Variational 优先通过 WS /portfolio 获取实时持仓，并写入该缓存
        self._position_cache: Dict[str, Any] = {}
        self._position_cache_ts: float = 0.0

        # 余额缓存：用于 rest 模式下避免 UI/风控每次都打 REST
        self._balance_cache: List[BalanceData] = []
        self._balance_cache_ts: float = 0.0

        # 私有交易客户端（可选）：用于“无公开 API，仅能逆向前端”的下单能力
        self._trade: Optional[VariationalPrivateTradingClient] = None
        self._trade_templates: Dict[str, VariationalRequestTemplate] = {}
        self._load_trade_templates()

        # WebSocket events（可选）：用于成交/订单事件推送（替代单笔查单接口）
        self._websocket: Optional[VariationalWebSocket] = None
        if bool(config.extra_params.get("ws_enable", True)):
            self._websocket = VariationalWebSocket(
                config=config, logger=self.logger)

        # ===== 账户数据源（余额/持仓）=====
        # ws:       默认行为，使用 /portfolio WS 推送（更实时）
        # rest:     不依赖 /portfolio WS，使用 REST 轮询 balances_template/positions_template 并写入缓存
        # rest+ws:  同时启用 /portfolio WS 与 REST 轮询，谁拿到数据谁更新缓存（更稳：任意一路正常即可）
        raw_src = str((config.extra_params or {}).get("account_data_source", "ws") or "")
        self._account_data_source: str = self._normalize_account_data_source(raw_src)
        self._account_rest_poll_interval_s: float = float(
            (config.extra_params or {}).get("account_rest_poll_interval_seconds", 5.0)
        )
        self._account_rest_cache_ttl_s: float = float(
            (config.extra_params or {}).get("account_rest_cache_ttl_seconds", 10.0)
        )
        self._account_poll_task: Optional[asyncio.Task] = None
        self._account_poll_error_log_ts: float = 0.0
        self._account_poll_ok_log_ts: float = 0.0
        # REST 轮询“OK摘要”日志节流（秒）：让日志能体现 rest 在跑，但不刷屏
        # 默认 180s（3分钟），可通过 extra_params.account_rest_ok_log_interval_seconds 覆盖
        try:
            self._account_poll_ok_log_interval_s: float = float(
                (config.extra_params or {}).get("account_rest_ok_log_interval_seconds", 180.0)
            )
        except Exception:
            self._account_poll_ok_log_interval_s = 180.0
        self._account_last_positions_http_status: Optional[int] = None
        self._account_last_balances_http_status: Optional[int] = None
        # positions_template 的“结果摘要日志”限流：避免每 5 秒轮询刷屏
        self._positions_result_log_interval_s: float = float(
            (config.extra_params or {}).get("positions_result_log_interval_seconds", 180.0)
        )
        self._last_positions_result_log_ts: float = 0.0
        # 🔥 仅启动提示一次：用于确认 account_data_source 是否启用 rest+ws 混合模式
        self._account_source_mode_log_once: bool = False

        # 订阅（轮询）任务：Variational 无公开 WS orderbook，这里用轮询实现 subscribe_ticker
        self._ticker_tasks: Dict[str, asyncio.Task] = {}
        # 记录“订阅意图”（用于基类自动重连后恢复轮询任务）
        # key=underlying(normalized) -> callback
        self._ticker_callbacks: Dict[str, Callable[[TickerData], None]] = {}
        # 🔧 轮询任务退出自愈：记录低频日志时间，避免重启时刷屏
        self._ticker_task_restart_log_ts: Dict[str, float] = {}

        # 默认参数均可通过 config.extra_params 覆盖，避免硬编码
        self._funding_interval_s = int(
            config.extra_params.get("funding_interval_s", 3600))
        self._settlement_asset = str(
            config.extra_params.get("settlement_asset", "USDC"))
        self._instrument_type = str(config.extra_params.get(
            "instrument_type", "perpetual_future"))

        # 指定一个“询价数量”，用于得到接近你实际下单量的 bid/ask
        # 说明：Variational 的 quote 是“按 qty 询价”，不同 qty 会有不同滑点
        self._default_quote_qty = str(
            config.extra_params.get("indicative_quote_qty", "0.001"))

        # 可选：健康检查/心跳用的默认币种
        self._health_symbol = str(config.extra_params.get(
            "health_check_symbol", "BTC")).upper()

        # ticker 轮询间隔（毫秒），可在配置中覆盖
        # 默认 1000ms：与前端“约 1 秒一轮”的行为更接近，也更不容易触发限流
        self._ticker_poll_interval_ms = int(
            config.extra_params.get("ticker_poll_interval_ms", 1000))

        # 资金费率：低频缓存（避免每次 get_ticker 都额外请求）
        self._funding_refresh_seconds = int(
            config.extra_params.get("funding_refresh_seconds", 60))
        self._funding_cache: Dict[str, Dict[str, Any]] = {}
        self._funding_locks: Dict[str, asyncio.Lock] = {}

        # 🔥 下单提交兜底：记录最近一次“提交但可能未拿到HTTP回包”的本地下单上下文
        # key=local_client_id -> {symbol, side, qty, created_mono}
        self._submit_ctx_cache: Dict[str, Dict[str, Any]] = {}

    def _load_trade_templates(self) -> None:
        """
        从 config.extra_params 读取交易/账户模板（请求重放）。
        原则：不猜测字段结构；用户从前端抓包后把模板贴进配置即可。
        """
        trade_cfg = self.config.extra_params.get("trade")
        if not isinstance(trade_cfg, dict):
            return

        def _build_template(cfg: Any) -> Optional[VariationalRequestTemplate]:
            if not isinstance(cfg, dict):
                return None
            method = str(cfg.get("method") or "GET").upper()
            url = str(cfg.get("url") or "").strip()
            if not url:
                return None
            headers = cfg.get("headers") if isinstance(
                cfg.get("headers"), dict) else {}
            cookies = cfg.get("cookies") if isinstance(
                cfg.get("cookies"), dict) else {}
            body = cfg.get("body")
            timeout_seconds = cfg.get("timeout_seconds")
            try:
                timeout_int = int(
                    timeout_seconds) if timeout_seconds is not None else None
            except Exception:
                timeout_int = None
            return VariationalRequestTemplate(
                method=method,
                url=url,
                headers={str(k): str(v) for k, v in (headers or {}).items()},
                cookies={str(k): str(v) for k, v in (cookies or {}).items()},
                body=body,
                timeout_seconds=timeout_int,
            )

        # 模板命名约定：优先使用更细粒度 key；执行引擎只需要这些最小集合即可闭环
        # 注意：Variational 很多查询接口是“列表型”（/api/orders/v2、/api/trades），
        # 因此我们额外支持 history 模板，用于 WS 超时后的 REST 兜底确认成交。
        for name in (
            "order_create_market_template",
            "order_create_limit_template",   # 占位：未来支持限价单
            "order_cancel_template",
            "order_get_template",
            "orders_open_template",
            "orders_history_template",
            "trades_history_template",
            "balances_template",
            "positions_template",
            "set_leverage_template",
            "set_margin_mode_template",
        ):
            tpl = _build_template(trade_cfg.get(name))
            if tpl:
                self._trade_templates[name] = tpl

        # 兼容旧配置：order_create_template -> order_create_market_template
        legacy = _build_template(trade_cfg.get("order_create_template"))
        if legacy and "order_create_market_template" not in self._trade_templates:
            self._trade_templates["order_create_market_template"] = legacy

        if self._trade_templates:
            self._trade = VariationalPrivateTradingClient(
                config=self.config, logger=self.logger)

    def _has_trade_template(self, name: str) -> bool:
        return bool(self._trade and self._trade_templates.get(name))

    async def _trade_send(self, name: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
        if not self._trade:
            raise NotImplementedError(
                "VariationalAdapter 未初始化私有交易客户端（缺少 trade 模板配置）")
        tpl = self._trade_templates.get(name)
        if not tpl:
            raise NotImplementedError(f"VariationalAdapter 缺少交易模板: {name}")
        resp = await self._trade.send(tpl, ctx, template_name=name)

        # positions_template 结果摘要：
        # - 正常情况不打印（保持日志清洁）
        # - 仅异常/无法解析数量时打印 WARNING（节流）
        try:
            if str(name).strip() == "positions_template":
                now_ts = time.time()
                interval = max(5.0, float(self._positions_result_log_interval_s or 60.0))
                if (now_ts - float(self._last_positions_result_log_ts)) >= interval:
                    self._last_positions_result_log_ts = now_ts
                    status = resp.get("status") if isinstance(resp, dict) else None
                    data = resp.get("data") if isinstance(resp, dict) else None

                    def _count_positions(d: Any) -> Optional[int]:
                        if isinstance(d, list):
                            # /api/positions 典型返回：list[dict]
                            return len([x for x in d if isinstance(x, dict)])
                        if isinstance(d, dict):
                            # 兼容 {"positions":[...]} 或 {"BTC": {...}} 形式
                            if isinstance(d.get("positions"), list):
                                return len([x for x in (d.get("positions") or []) if isinstance(x, dict)])
                            # dict-of-dict
                            try:
                                if d and all(isinstance(v, dict) for v in d.values()):
                                    return len(d)
                            except Exception:
                                return None
                        return None

                    cnt = _count_positions(data)
                    try:
                        st_int = int(status) if status is not None else None
                    except Exception:
                        st_int = None
                    if (st_int is not None and st_int != 200) or (cnt is None):
                        self.logger.warning(
                            "[variational-trade] positions_template abnormal: status=%s positions=%s",
                            status,
                            cnt if cnt is not None else "N/A",
                        )
        except Exception:
            pass

        return resp

    def _normalize_underlying_ctx(self, symbol: str) -> Dict[str, str]:
        normalized = self._normalize_symbol(symbol)
        return {
            "symbol": normalized,
            "underlying": normalized,
        }

    def _build_instrument(self, underlying: str) -> str:
        """
        Variational 前端常见 instrument 形式：P-ETH-USDC-3600
        - 这里不强依赖固定格式，允许在 config.extra_params 覆盖：
          orders_instrument_format: "P-{{underlying}}-{{settlement_asset}}-{{funding_interval_s}}"
        """
        fmt = str(
            self.config.extra_params.get(
                "orders_instrument_format",
                "P-{{underlying}}-{{settlement_asset}}-{{funding_interval_s}}",
            )
        )
        ctx = {
            "underlying": str(underlying).strip().upper(),
            "settlement_asset": str(self._settlement_asset or "USDC").strip().upper(),
            "funding_interval_s": str(int(self._funding_interval_s)),
        }
        out = fmt
        for k, v in ctx.items():
            out = out.replace(f"{{{{{k}}}}}", str(v))
        return out

    @staticmethod
    def _extract_list_items(data: Any) -> List[Dict[str, Any]]:
        """
        从 Variational 常见返回结构中提取“列表项”：
        - raw = {"ok":True,"status":200,"data": ...}
        - data 可能是 list，也可能是 dict 包裹（result/orders/data）
        """
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if not isinstance(data, dict):
            return []
        for k in ("result", "orders", "data", "items"):
            v = data.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
            if isinstance(v, dict):
                # 继续向内探一层（常见 result.data / data.result）
                vv = v.get("result") or v.get("orders")
                if isinstance(vv, list):
                    return [x for x in vv if isinstance(x, dict)]
        return []

    @staticmethod
    def _match_order_id_in_item(item: Dict[str, Any], order_id: str) -> bool:
        """
        Variational 的订单/成交可能同时出现：
        - rfq_id（更稳定）
        - quote_id / parent_quote_id / source_quote
        - order_id / id
        这里做“弱匹配”，只做字符串等值比较，不猜测业务语义。
        """
        oid = str(order_id or "").strip()
        if not oid:
            return False
        keys = (
            "rfq_id",
            "rfqId",
            "source_rfq",
            "quote_id",
            "quoteId",
            "parent_quote_id",
            "source_quote",
            "order_id",
            "orderId",
            "id",
        )
        for k in keys:
            v = item.get(k)
            if v is None:
                continue
            if str(v).strip() == oid:
                return True
        return False

    def _parse_status_from_item(self, it: Dict[str, Any]) -> OrderStatus:
        """
        /api/orders/v2 常见 status：
        - pending -> OPEN
        - cleared -> FILLED
        - canceled -> CANCELED
        - rejected -> REJECTED
        """
        v = it.get("status") or it.get("state") or it.get(
            "order_status") or it.get("orderState")
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("cleared", "filled", "closed", "done"):
                return OrderStatus.FILLED
            if s in ("pending", "open", "live", "working", "partially_filled", "partial"):
                return OrderStatus.OPEN
            if s in ("canceled", "cancelled", "expired"):
                return OrderStatus.CANCELED
            if s in ("rejected", "error", "failed"):
                return OrderStatus.REJECTED
        return OrderStatus.PENDING

    def _pick_first(self, data: Any, keys: List[str]) -> Optional[Any]:
        if not isinstance(data, dict):
            return None
        for k in keys:
            if k in data and data.get(k) is not None:
                return data.get(k)
        return None

    def _parse_order_id(self, raw: Dict[str, Any]) -> str:
        data = raw.get("data") if isinstance(raw, dict) else None
        if isinstance(data, dict):
            # Variational 常见：rfq_id / quote_id 可能才是执行器追踪ID
            v = self._pick_first(
                data,
                [
                    "rfq_id",
                    "rfqId",
                    "source_rfq",
                    "quote_id",
                    "quoteId",
                    "parent_quote_id",
                    "source_quote",
                    "order_id",
                    "orderId",
                    "id",
                    "orderID",
                ],
            )
            if isinstance(v, (str, int)) and str(v).strip():
                return str(v).strip()
        # fallback：某些接口直接返回字符串/数字
        if isinstance(data, (str, int)) and str(data).strip():
            return str(data).strip()
        return "unknown"

    def _extract_response_field(self, raw: Dict[str, Any], keys: List[str]) -> Optional[str]:
        """
        从 VariationalPrivateTradingClient 返回的 raw 中提取字段（弱假设）：
        raw 结构通常为 {"ok":True,"status":200,"data":{...}}
        支持在 data / data.result / data.data 中寻找指定 keys。
        """
        try:
            data = raw.get("data") if isinstance(raw, dict) else None
            candidates: List[Any] = []
            if isinstance(data, dict):
                candidates.append(data)
                inner = data.get("data")
                if isinstance(inner, dict):
                    candidates.append(inner)
                result = data.get("result")
                if isinstance(result, dict):
                    candidates.append(result)
                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, dict):
                            candidates.append(item)
                            break
            for c in candidates:
                for k in keys:
                    v = c.get(k) if isinstance(c, dict) else None
                    if isinstance(v, (str, int)) and str(v).strip():
                        return str(v).strip()
        except Exception:
            return None
        return None

    def _parse_order_status(self, raw: Dict[str, Any]) -> OrderStatus:
        # 尽量不假设字段；只做弱推断
        data = raw.get("data") if isinstance(raw, dict) else None
        if isinstance(data, dict):
            v = self._pick_first(
                data, ["status", "state", "order_status", "orderState"])
            if isinstance(v, str):
                s = v.strip().lower()
                if s in ("filled", "closed", "done"):
                    return OrderStatus.FILLED
                if s in ("open", "live", "partially_filled", "partial", "working"):
                    return OrderStatus.OPEN
                if s in ("canceled", "cancelled"):
                    return OrderStatus.CANCELED
                if s in ("rejected", "error", "failed"):
                    return OrderStatus.REJECTED
        return OrderStatus.PENDING

    def _parse_decimal_any(self, v: Any) -> Optional[Decimal]:
        try:
            if v is None:
                return None
            if isinstance(v, Decimal):
                return v
            if isinstance(v, (int, float)):
                return Decimal(str(v))
            if isinstance(v, str) and v.strip():
                return Decimal(v.strip())
        except Exception:
            return None
        return None

    @staticmethod
    def _parse_iso_dt_safe(v: Any) -> Optional[datetime]:
        if not v:
            return None
        try:
            s = str(v).strip()
            if not s:
                return None
            # 兼容 Z
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            return None

    # ===== 账户数据源切换（portfolio WS vs REST轮询）=====

    def _is_account_data_source_rest(self) -> bool:
        return str(self._account_data_source or "").strip().lower() == "rest"

    def _is_account_data_source_rest_enabled(self) -> bool:
        """rest 或 rest+ws 都视为启用 REST 轮询。"""
        v = str(self._account_data_source or "").strip().lower()
        return v in ("rest", "rest+ws")

    def _is_account_data_source_ws_enabled(self) -> bool:
        """ws 或 rest+ws 都视为启用 portfolio WS。"""
        v = str(self._account_data_source or "").strip().lower()
        return v in ("ws", "rest+ws")

    @staticmethod
    def _normalize_account_data_source(value: str) -> str:
        """
        规范化 account_data_source：
        - 允许: ws / rest / rest+ws
        - 兼容别名: ws+rest / both / hybrid
        """
        v = str(value or "").strip().lower().replace(" ", "")
        if v in ("rest+ws", "ws+rest", "both", "hybrid"):
            return "rest+ws"
        if v == "rest":
            return "rest"
        return "ws"

    def _account_rest_poll_interval_seconds(self) -> float:
        try:
            return max(0.5, float(self._account_rest_poll_interval_s or 0.0))
        except Exception:
            return 5.0

    def _account_rest_cache_ttl_seconds(self) -> float:
        try:
            return max(0.5, float(self._account_rest_cache_ttl_s or 0.0))
        except Exception:
            return 10.0

    def _is_account_cache_fresh(self, ts: float) -> bool:
        if ts <= 0:
            return False
        return (time.time() - ts) <= self._account_rest_cache_ttl_seconds()

    async def _start_account_rest_polling_if_needed(self) -> None:
        """
        rest 模式下启动轮询：
        - positions_template -> 写入 self._position_cache
        - balances_template  -> 写入 self._balance_cache
        """
        if not self._is_account_data_source_rest_enabled():
            return
        if self._account_poll_task and (not self._account_poll_task.done()):
            return

        async def _loop() -> None:
            interval = self._account_rest_poll_interval_seconds()
            while True:
                try:
                    if not self._connected:
                        await asyncio.sleep(0.2)
                        continue
                    await self._refresh_positions_cache_via_rest()
                    await self._refresh_balance_cache_via_rest()

                    # 成功摘要日志（限流）：让你能快速确认“REST是否真的在超时/失败”
                    now_ts = time.time()
                    interval_s = max(10.0, float(getattr(self, "_account_poll_ok_log_interval_s", 180.0) or 180.0))
                    if now_ts - float(self._account_poll_ok_log_ts) >= interval_s:
                        self._account_poll_ok_log_ts = now_ts
                        try:
                            pos_n = len(getattr(self, "_position_cache", {}) or {})
                        except Exception:
                            pos_n = -1
                        equity = None
                        try:
                            if self._balance_cache:
                                equity = self._balance_cache[0].total
                        except Exception:
                            equity = None
                        # ✅ 状态摘要：写入日志文件（INFO）但节流到每3分钟最多一条
                        # 目的：你希望在 log 里能明确看到 rest 在工作
                        self.logger.info(
                            "[variational-account] REST轮询OK: positions=%s equity=%s positions_status=%s balances_status=%s",
                            pos_n,
                            str(equity) if equity is not None else "N/A",
                            self._account_last_positions_http_status,
                            self._account_last_balances_http_status,
                        )
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # 限流日志：避免 REST 不稳定时刷屏
                    now = time.time()
                    if now - self._account_poll_error_log_ts >= 30.0:
                        self._account_poll_error_log_ts = now
                        self.logger.warning(
                            "[variational-account] REST轮询异常（已限流）：%s", e
                        )
                    await asyncio.sleep(min(5.0, interval))

        self._account_poll_task = asyncio.create_task(_loop())

    async def _stop_account_rest_polling(self) -> None:
        if not self._account_poll_task:
            return
        try:
            self._account_poll_task.cancel()
        except Exception:
            pass
        self._account_poll_task = None

    async def _refresh_positions_cache_via_rest(self) -> None:
        if not self._has_trade_template("positions_template"):
            return
        raw = await self._trade_send("positions_template", ctx={})
        if isinstance(raw, dict):
            self._account_last_positions_http_status = raw.get("status")
        data = raw.get("data") if isinstance(raw, dict) else None
        now_dt = datetime.now()

        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            if "positions" in data and isinstance(data.get("positions"), list):
                items = [x for x in data.get("positions") if isinstance(x, dict)]
            else:
                for k, v in data.items():
                    if isinstance(v, dict):
                        items.append({"symbol": k, **v})

        out: List[PositionData] = []
        for it in items:
            # 兼容 Var 前端结构：list[{"position_info":{"instrument":{"underlying":...}, "qty":...}, "price_info":{...}, ...}]
            sym_raw = it.get("symbol") or it.get("market") or it.get("underlying")
            pos_info = it.get("position_info") if isinstance(it, dict) else None
            if (not sym_raw) and isinstance(pos_info, dict):
                inst = pos_info.get("instrument")
                if isinstance(inst, dict):
                    sym_raw = inst.get("underlying") or inst.get("symbol") or inst.get("market")
                if not sym_raw:
                    sym_raw = pos_info.get("underlying") or pos_info.get("symbol") or pos_info.get("market")
            if not sym_raw:
                continue
            sym = self._normalize_symbol(str(sym_raw))
            # qty/size 可能出现在 position_info.qty
            size = self._parse_decimal_any(
                it.get("size")
                or it.get("position_size")
                or it.get("qty")
                or it.get("amount")
                or (pos_info.get("qty") if isinstance(pos_info, dict) else None)
                or (pos_info.get("size") if isinstance(pos_info, dict) else None)
            ) or Decimal("0")
            side = PositionSide.LONG if size >= 0 else PositionSide.SHORT
            size_abs = abs(size)
            entry = self._parse_decimal_any(
                it.get("entry_price")
                or it.get("entryPrice")
                or it.get("avg_price")
                or it.get("average_price")
                or (pos_info.get("avg_entry_price") if isinstance(pos_info, dict) else None)
                or (pos_info.get("avgEntryPrice") if isinstance(pos_info, dict) else None)
            ) or Decimal("0")
            # mark/price 常见在 price_info.price
            price_info = it.get("price_info") if isinstance(it, dict) else None
            mark = self._parse_decimal_any(it.get("mark_price") or it.get("markPrice"))
            if (mark is None) and isinstance(price_info, dict):
                mark = self._parse_decimal_any(price_info.get("price") or price_info.get("underlying_price") or price_info.get("underlyingPrice"))
            liq = self._parse_decimal_any(it.get("liquidation_price") or it.get("liquidationPrice"))
            if (liq is None) and isinstance(it, dict):
                liq = self._parse_decimal_any(it.get("estimated_liquidation_price") or it.get("estimatedLiquidationPrice"))
            pnl = self._parse_decimal_any(it.get("unrealized_pnl") or it.get("unrealizedPnl"))
            if pnl is None and isinstance(it, dict):
                pnl = self._parse_decimal_any(it.get("upnl") or it.get("uPnl"))
            rpnl = None
            if isinstance(it, dict):
                rpnl = self._parse_decimal_any(it.get("realized_pnl") or it.get("realizedPnl") or it.get("rpnl") or it.get("rPnl"))

            # 过滤 0 仓位（避免 UI 显示“空仓条目”）
            if size_abs <= Decimal("0"):
                continue

            out.append(
                PositionData(
                    symbol=sym,
                    side=side,
                    size=size_abs,
                    entry_price=entry,
                    mark_price=mark,
                    current_price=mark,
                    liquidation_price=liq,
                    unrealized_pnl=pnl or Decimal("0"),
                    realized_pnl=rpnl or Decimal("0"),
                    percentage=None,
                    leverage=1,
                    margin_mode=MarginMode.CROSS,
                    margin=Decimal("0"),
                    timestamp=now_dt,
                    raw_data={"source": "rest", "response": raw, "item": it},
                )
            )

        new_cache: Dict[str, PositionData] = {}
        for p in out:
            sym = str(getattr(p, "symbol", "") or "").strip().upper()
            if sym:
                new_cache[sym] = p
        self._position_cache = new_cache
        self._position_cache_ts = time.time()

    async def _refresh_balance_cache_via_rest(self) -> None:
        if not self._has_trade_template("balances_template"):
            return
        raw = await self._trade_send("balances_template", ctx={})
        if isinstance(raw, dict):
            self._account_last_balances_http_status = raw.get("status")
        data = raw.get("data") if isinstance(raw, dict) else None
        now_dt = datetime.now()
        cur = str(self._settlement_asset or "USDC").strip() or "USDC"

        equity: Optional[Decimal] = None
        if isinstance(data, dict):
            equity_keys = [
                "balance",
                "portfolio_value",
                "portfolioValue",
                "portfolio",
                "equity",
                "account_equity",
                "accountEquity",
                "net_value",
                "netValue",
                "total_equity",
                "totalEquity",
            ]

            def _iter_equity_dicts(d: Dict[str, Any]) -> List[Dict[str, Any]]:
                out_dicts: List[Dict[str, Any]] = [d]
                for k in ("result", "data", "portfolio", "account", "summary", "pool_portfolio_result"):
                    v = d.get(k)
                    if isinstance(v, dict):
                        out_dicts.append(v)
                if isinstance(d.get("result"), dict) and isinstance(d["result"].get("summary"), dict):
                    out_dicts.append(d["result"]["summary"])
                return out_dicts

            for cand in _iter_equity_dicts(data):
                equity = self._parse_decimal_any(self._pick_first(cand, equity_keys))
                if equity is not None:
                    break

        if equity is None:
            # 没解析到权益则不覆盖旧缓存（避免一次异常把权益清空）
            return

        self._balance_cache = [
            BalanceData(
                currency=cur,
                free=equity,
                used=Decimal("0"),
                total=equity,
                usd_value=equity,
                timestamp=now_dt,
                raw_data={"source": "rest", "note": "portfolio_value/equity", "response": raw},
            )
        ]
        self._balance_cache_ts = time.time()

    def _match_recent_by_params(
        self,
        items: List[Dict[str, Any]],
        *,
        symbol: str,
        side: OrderSide,
        qty: Decimal,
        now_mono: float,
        window_s: float,
    ) -> Optional[Dict[str, Any]]:
        """
        用“参数近似匹配”来识别一次下单（用于 HTTP 超时后确认是否已下单/已成交）。
        - 不猜测业务语义，只做结构弱匹配：
          1) underlying/symbol/market/instrument 中包含目标币种
          2) side 匹配
          3) qty 匹配（允许极小误差）
          4) created_at 处于最近窗口内（若字段存在）
        """
        sym = str(symbol or "").strip().upper()
        if not sym:
            return None
        q = Decimal(str(qty))
        if q <= 0:
            return None
        eps = max(Decimal("1e-12"), q * Decimal("0.0001"))
        best: Optional[Dict[str, Any]] = None
        best_dt: float = 10**9
        win = max(1.0, float(window_s))

        for it in items:
            if not isinstance(it, dict):
                continue

            sraw = str(it.get("side") or it.get(
                "direction") or "").strip().lower()
            if sraw and sraw not in ("buy", "sell"):
                # 不识别的值直接跳过
                continue
            if sraw:
                if (side == OrderSide.BUY and sraw != "buy") or (side == OrderSide.SELL and sraw != "sell"):
                    continue

            # symbol/instrument 粗匹配
            cand_sym = (
                it.get("underlying")
                or it.get("symbol")
                or it.get("market")
                or (it.get("instrument") if isinstance(it.get("instrument"), str) else None)
            )
            if cand_sym is None:
                # 有些结构在 instrument.underlying
                if isinstance(it.get("instrument"), dict):
                    cand_sym = it["instrument"].get("underlying")
            cand_sym_s = str(cand_sym or "").strip().upper()
            if cand_sym_s and (sym not in cand_sym_s):
                continue

            # qty 匹配
            qv = (
                it.get("qty")
                or it.get("quantity")
                or it.get("amount")
                or it.get("size")
                or it.get("filled")   # trades 里可能只有 qty/filled
            )
            dq = self._parse_decimal_any(qv)
            if dq is None:
                continue
            if dq < q - eps or dq > q + eps:
                continue

            # created_at 过滤（若存在）
            created = self._parse_iso_dt_safe(
                it.get("created_at") or it.get("createdAt") or it.get("timestamp"))
            if created:
                try:
                    # 用 wall clock 估算 mono 差值：窗口只做过滤，不做绝对精确
                    dt = abs((datetime.now(created.tzinfo) -
                             created).total_seconds())
                except Exception:
                    dt = 0.0
                if dt > win:
                    continue
                if dt < best_dt:
                    best_dt = dt
                    best = it
            else:
                # 无 created_at：退化为第一条符合的（但优先选择更“近”的：无法比较则取第一条）
                if best is None:
                    best = it
        return best

    async def _confirm_after_submit_timeout(
        self,
        *,
        local_client_id: str,
        symbol: str,
        side: OrderSide,
        qty: Decimal,
    ) -> Optional[OrderData]:
        """
        方案A核心：
        - HTTP 下单超时不代表没成交。这里先用 WS/REST 确认“是否已成交/已生成订单”。
        - 返回 OrderData（尽量带 filled/status），用于上层继续流程，避免重复下单。
        """
        lid = str(local_client_id or "").strip()
        sym = self._normalize_symbol(symbol)
        if not lid:
            return None

        confirm_seconds = float(self.config.extra_params.get(
            "trade_submit_timeout_confirm_seconds", 3.0))
        rest_window_s = float(self.config.extra_params.get(
            "trade_submit_timeout_rest_window_seconds", 20.0))
        now_mono = asyncio.get_event_loop().time()

        # 1) 优先：WS 缓存
        try:
            if self._websocket:
                deadline = now_mono + max(0.0, confirm_seconds)
                while asyncio.get_event_loop().time() <= deadline:
                    hit = self._websocket.lookup_cached_order(lid, symbol=sym)
                    if hit:
                        return hit
                    await asyncio.sleep(0.05)
        except Exception:
            pass

        # 2) 兜底：REST 列表（open/history/trades），按参数近似匹配
        ctx = {
            "symbol": sym,
            "underlying": sym,
            "instrument": self._build_instrument(sym),
            "limit": int(self.config.extra_params.get("orders_lookup_limit", 50)),
            "offset": 0,
        }

        try:
            # open
            if self._has_trade_template("orders_open_template"):
                ro = await self._trade_send("orders_open_template", ctx)
                items = self._extract_list_items(
                    ro.get("data") if isinstance(ro, dict) else None)
                it = self._match_recent_by_params(
                    items, symbol=sym, side=side, qty=qty, now_mono=now_mono, window_s=rest_window_s
                )
                if it:
                    status = self._parse_status_from_item(it)
                    filled = self._parse_decimal_any(it.get("filled") or it.get(
                        "executed") or it.get("qty")) or Decimal("0")
                    now = datetime.now()
                    return OrderData(
                        id=lid,
                        client_id=lid,
                        symbol=sym,
                        side=side,
                        type=OrderType.MARKET,
                        amount=qty,
                        price=None,
                        filled=filled,
                        remaining=max(qty - filled, Decimal("0")),
                        cost=Decimal("0"),
                        average=self._parse_decimal_any(
                            it.get("average") or it.get("avg_price") or it.get("price")),
                        status=status,
                        timestamp=now,
                        updated=now,
                        fee=None,
                        trades=[],
                        params={},
                        raw_data={"confirm_by": "rest_orders_open",
                                  "item": it, "ctx": ctx},
                    )
        except Exception:
            pass

        try:
            # history
            if self._has_trade_template("orders_history_template"):
                rh = await self._trade_send("orders_history_template", ctx)
                items = self._extract_list_items(
                    rh.get("data") if isinstance(rh, dict) else None)
                it = self._match_recent_by_params(
                    items, symbol=sym, side=side, qty=qty, now_mono=now_mono, window_s=rest_window_s
                )
                if it:
                    status = self._parse_status_from_item(it)
                    filled = self._parse_decimal_any(it.get("filled") or it.get("executed") or it.get("qty")) or (
                        qty if status == OrderStatus.FILLED else Decimal("0")
                    )
                    now = datetime.now()
                    return OrderData(
                        id=lid,
                        client_id=lid,
                        symbol=sym,
                        side=side,
                        type=OrderType.MARKET,
                        amount=qty,
                        price=None,
                        filled=filled,
                        remaining=max(qty - filled, Decimal("0")),
                        cost=Decimal("0"),
                        average=self._parse_decimal_any(
                            it.get("average") or it.get("avg_price") or it.get("price")),
                        status=status,
                        timestamp=now,
                        updated=now,
                        fee=None,
                        trades=[],
                        params={},
                        raw_data={"confirm_by": "rest_orders_history",
                                  "item": it, "ctx": ctx},
                    )
        except Exception:
            pass

        try:
            # trades
            if self._has_trade_template("trades_history_template"):
                rt = await self._trade_send("trades_history_template", ctx)
                items = self._extract_list_items(
                    rt.get("data") if isinstance(rt, dict) else None)
                it = self._match_recent_by_params(
                    items, symbol=sym, side=side, qty=qty, now_mono=now_mono, window_s=rest_window_s
                )
                if it:
                    filled = self._parse_decimal_any(
                        it.get("qty") or it.get("filled") or it.get("amount")) or qty
                    now = datetime.now()
                    return OrderData(
                        id=lid,
                        client_id=lid,
                        symbol=sym,
                        side=side,
                        type=OrderType.MARKET,
                        amount=qty,
                        price=None,
                        filled=filled,
                        remaining=max(qty - filled, Decimal("0")),
                        cost=Decimal("0"),
                        average=self._parse_decimal_any(it.get("price")),
                        status=OrderStatus.FILLED if filled > Decimal(
                            "0") else OrderStatus.PENDING,
                        timestamp=now,
                        updated=now,
                        fee=None,
                        trades=[],
                        params={},
                        raw_data={"confirm_by": "rest_trades",
                                  "item": it, "ctx": ctx},
                    )
        except Exception:
            pass

        return None

    # ===== 生命周期 / 连接 =====

    async def _do_connect(self) -> bool:
        self._connected = True
        # 🔥 主动连接 WS events：确保套利执行器初始化订阅时 ws_connected=True
        # 失败不影响行情/REST 功能
        try:
            if self._websocket:
                # rest 模式下明确关闭 portfolio WS（只保留 events WS）
                # 仅 rest-only 才关闭；rest+ws 需要保留 portfolio
                if not self._is_account_data_source_ws_enabled():
                    try:
                        await self._websocket.set_portfolio_enable(False)
                    except Exception:
                        pass
                await self._websocket.connect(timeout_seconds=3.0)
                # 🔥 订阅 portfolio 持仓推送，并写入 _position_cache 供 UI/风控读取
                if self._is_account_data_source_ws_enabled():
                    async def _on_positions(positions: Any) -> None:
                        try:
                            # portfolio 是“快照”语义：用本次列表重建缓存（允许为空 -> 清空）
                            if not isinstance(positions, list):
                                return
                            new_cache: Dict[str, PositionData] = {}
                            for p in positions:
                                sym = str(getattr(p, "symbol", "") or "").strip().upper()
                                if sym:
                                    new_cache[sym] = p
                            self._position_cache = new_cache
                            self._position_cache_ts = time.time()
                        except Exception:
                            return

                    await self._websocket.subscribe_positions(_on_positions)
        except Exception as e:
            self.logger.warning(
                f"[variational-ws] connect on adapter.connect failed: {e}")

        # rest 模式：启动账户轮询（positions/balances），替代 portfolio WS
        try:
            await self._start_account_rest_polling_if_needed()
        except Exception:
            pass
        try:
            if self._is_account_data_source_rest_enabled() and (not self._is_account_data_source_ws_enabled()):
                # 仅展示用：降为 DEBUG（默认不写入日志文件）
                self.logger.debug(
                    "[variational-account] account_data_source=rest：已禁用 portfolio WS，启用REST轮询 interval=%ss ttl=%ss",
                    self._account_rest_poll_interval_seconds(),
                    self._account_rest_cache_ttl_seconds(),
                )
            elif self._is_account_data_source_rest_enabled() and self._is_account_data_source_ws_enabled():
                self.logger.debug(
                    "[variational-account] account_data_source=rest+ws：已启用 portfolio WS + REST轮询 interval=%ss ttl=%ss",
                    self._account_rest_poll_interval_seconds(),
                    self._account_rest_cache_ttl_seconds(),
                )
                # ✅ 仅打印一条 INFO（写入日志文件）：让你从日志快速确认“rest+ws 混合模式已启用”
                if not bool(getattr(self, "_account_source_mode_log_once", False)):
                    self._account_source_mode_log_once = True
                    self.logger.info(
                        "[variational-account] 已启用账户数据源混合模式: account_data_source=rest+ws portfolio_ws=ON rest_poll=ON interval=%ss ttl=%ss",
                        self._account_rest_poll_interval_seconds(),
                        self._account_rest_cache_ttl_seconds(),
                    )
            else:
                # 仅展示用：降为 DEBUG（默认不写入日志文件）
                self.logger.debug(
                    "[variational-account] account_data_source=ws：余额/持仓优先使用 portfolio WS 推送"
                )
        except Exception:
            pass

        # 🔥 关键修复：基类遇到网络错误会触发 disconnect->connect 的自动重连。
        # Variational 的行情来自 subscribe_ticker 的轮询任务；断开连接会取消任务。
        # 这里在 connect 成功后，自动恢复之前的 ticker 订阅（避免“重连后无行情”）。
        try:
            if self._ticker_callbacks:
                for underlying, cb in list(self._ticker_callbacks.items()):
                    try:
                        await self.subscribe_ticker(underlying, cb)
                    except Exception:
                        # 恢复失败不影响整体 connect；轮询任务内部也有退避
                        continue
        except Exception:
            pass
        return True

    async def _do_disconnect(self) -> None:
        self._connected = False
        await self._stop_account_rest_polling()
        # 🔥 关键修复：断开连接时取消 ticker 轮询任务，但不清空订阅意图；
        # 这样基类自动重连成功后可以恢复订阅，避免“3点后不再更新行情”的问题。
        try:
            tasks = list(self._ticker_tasks.values())
            self._ticker_tasks.clear()
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
        except Exception:
            pass
        await self._rest.close()
        if self._trade:
            await self._trade.close()
        if self._websocket:
            await self._websocket.close()

    async def _do_authenticate(self) -> bool:
        # 仅行情：不需要 API key；交易能力后续扩展再做鉴权
        return True

    async def _do_health_check(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"connected": self._connected}
        try:
            ticker = await self.get_ticker(self._health_symbol)
            data["ticker_ok"] = True
            data["bid"] = float(ticker.bid) if ticker.bid is not None else None
            data["ask"] = float(ticker.ask) if ticker.ask is not None else None
            data["mark_price"] = float(
                ticker.mark_price) if ticker.mark_price is not None else None
        except Exception as e:
            data["ticker_ok"] = False
            data["error"] = str(e)
        return data

    async def _do_heartbeat(self) -> None:
        # 用一次轻量的 quote 作为心跳
        await self.get_ticker(self._health_symbol)

    # ===== 市场数据接口 =====

    async def get_exchange_info(self) -> ExchangeInfo:
        return ExchangeInfo(
            name="Variational Omni",
            id="variational",
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "ticker",
                "bbo_quote",
            ],
            rate_limits=self.config.rate_limits if self.config else {},
            precision=self.config.precision if self.config else {},
            fees={},
            markets={},
            status="operational" if self._connected else "disconnected",
            timestamp=datetime.now(),
        )

    async def get_ticker(self, symbol: str) -> TickerData:
        """
        获取单个交易对行情（BBO）

        说明：
        - Variational 不是传统 orderbook 模式
        - bid/ask 来自 quote（按 qty 询价）
        """

        normalized = self._normalize_symbol(symbol)
        qty = self._get_quote_qty_for_symbol(normalized)
        request = VariationalIndicativeQuoteRequest(
            underlying=normalized,
            qty=qty,
            funding_interval_s=self._funding_interval_s,
            settlement_asset=self._settlement_asset,
            instrument_type=self._instrument_type,
        )

        async def _do_call() -> Dict[str, Any]:
            return await self._rest.get_indicative_quote(request)

        raw = await self._execute_with_retry(_do_call, operation_name="get_ticker")

        bid = self._safe_decimal(raw.get("bid"))
        ask = self._safe_decimal(raw.get("ask"))
        mark_price = self._safe_decimal(raw.get("mark_price"))

        # 🔥 Variational 资金费率：
        # 1) 先尝试从 quote 响应里提取（如果后端提供）
        # 2) 若 quote 不含 funding，则走“可配置 funding_rate_source”低频拉取
        funding_rate = self._extract_funding_rate(raw)
        # 尝试从 quote 里读取 funding_interval_s，并将 funding_rate 归一到系统内部 8h 口径
        quote_interval_s = None
        try:
            if isinstance(raw.get("instrument"), dict) and raw["instrument"].get("funding_interval_s") is not None:
                quote_interval_s = int(
                    float(raw["instrument"]["funding_interval_s"]))
            elif raw.get("funding_interval_s") is not None:
                quote_interval_s = int(float(raw.get("funding_interval_s")))
        except Exception:
            quote_interval_s = None
        funding_rate_raw = funding_rate
        funding_rate_interval_s = quote_interval_s
        funding_rate = self._normalize_funding_rate_to_8h(
            funding_rate, quote_interval_s)
        if funding_rate is None:
            funding_rate, funding_rate_raw, funding_rate_interval_s = await self._get_funding_rate_cached(normalized)

        # 统一输出：last 取 mark_price（若无则取中间价）
        last = mark_price
        if last == Decimal("0") and bid > 0 and ask > 0:
            last = (bid + ask) / Decimal("2")

        now = datetime.now()
        return TickerData(
            symbol=normalized,
            timestamp=now,
            bid=bid if bid > 0 else None,
            ask=ask if ask > 0 else None,
            last=last if last > 0 else None,
            mark_price=mark_price if mark_price > 0 else None,
            funding_rate=funding_rate,
            # 系统内部统一 funding_rate 口径为 8h，因此这里对外也标记为 8h
            funding_interval=28800,
            base_currency=normalized,
            quote_currency=self._settlement_asset,
            received_timestamp=now,
            raw_data={
                "quote_qty": qty,
                # 记录资金费率来源信息，便于排查“4h/8h 不一致”的问题
                "funding_rate_raw": str(funding_rate_raw) if isinstance(funding_rate_raw, Decimal) else None,
                "funding_interval_s_raw": funding_rate_interval_s,
                **raw,
            },
        )

    def _extract_funding_rate(self, raw: Dict[str, Any]) -> Optional[Decimal]:
        """
        从 quote 响应中提取资金费率（如果存在）。
        可通过 config.extra_params 覆盖字段名/倍率，避免硬编码：
        - variational_funding_rate_keys: ["funding_rate_8h", "funding_rate", ...]
        - variational_funding_rate_multiplier: 例如 8（把每小时转成8小时）/ 1（已是8小时）
        """
        if not isinstance(raw, dict):
            return None

        keys = self.config.extra_params.get("variational_funding_rate_keys")
        if not isinstance(keys, list) or not keys:
            # 默认不猜测：只尝试少数常见命名（存在才读）
            keys = ["funding_rate_8h", "fundingRate8h",
                    "funding_rate", "fundingRate"]

        picked: Optional[Decimal] = None
        for k in keys:
            if not isinstance(k, str) or not k.strip():
                continue
            if k in raw and raw.get(k) is not None:
                picked = self._safe_decimal(raw.get(k))
                break

        if picked is None or picked == Decimal("0"):
            return None

        mult_raw = self.config.extra_params.get(
            "variational_funding_rate_multiplier", 1)
        try:
            mult = Decimal(str(mult_raw))
        except Exception:
            mult = Decimal("1")
        if mult <= 0:
            mult = Decimal("1")

        return picked * mult

    def _normalize_funding_rate_to_8h(self, rate: Optional[Decimal], interval_s: Optional[int]) -> Optional[Decimal]:
        """
        将“每 interval 的资金费率”统一换算为系统内部 8h 口径（28800s）。
        - 若 interval_s=4h(14400)，则 *2
        - 若 interval_s=8h(28800)，则 *1
        """
        if rate is None:
            return None
        # 注意：部分数据源（如 supported_assets）可能“显示口径已是 8h”，但同时给出结算周期=4h。
        # 这种情况下若再按 interval 换算会导致数值放大一倍。
        # 通过配置开关控制：默认不按 interval 换算，只使用 interval 做标注。
        cfg = self.config.extra_params.get(
            "funding_rate_source") if self.config else None
        is_per_interval = False
        if isinstance(cfg, dict):
            is_per_interval = bool(cfg.get("is_rate_per_interval", False))
        if not is_per_interval:
            return rate
        try:
            if interval_s is None or interval_s <= 0:
                return rate
            target = Decimal("28800")
            cur = Decimal(str(int(interval_s)))
            if cur <= 0:
                return rate
            return rate * (target / cur)
        except Exception:
            return rate

    async def _get_funding_rate_cached(self, underlying: str) -> tuple[Optional[Decimal], Optional[Decimal], Optional[int]]:
        """
        低频刷新 funding rate，避免影响 BBO 高频轮询。
        """
        key = str(underlying).strip().upper()
        lock = self._funding_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._funding_locks[key] = lock

        now = time.time()
        cached = self._funding_cache.get(key)
        if cached:
            ts = float(cached.get("ts", 0.0) or 0.0)
            if now - ts < max(5, self._funding_refresh_seconds):
                val = cached.get("value")
                raw_val = cached.get("raw")
                interval_s = cached.get("interval_s")
                out = val if isinstance(val, Decimal) else None
                raw_out = raw_val if isinstance(raw_val, Decimal) else None
                interval_out = interval_s if isinstance(
                    interval_s, int) else None
                return out, raw_out, interval_out

        async with lock:
            # double check
            cached = self._funding_cache.get(key)
            if cached:
                ts = float(cached.get("ts", 0.0) or 0.0)
                if now - ts < max(5, self._funding_refresh_seconds):
                    val = cached.get("value")
                    raw_val = cached.get("raw")
                    interval_s = cached.get("interval_s")
                    out = val if isinstance(val, Decimal) else None
                    raw_out = raw_val if isinstance(raw_val, Decimal) else None
                    interval_out = interval_s if isinstance(
                        interval_s, int) else None
                    return out, raw_out, interval_out

            try:
                raw_val, interval_s = await self._rest.get_funding_info_from_source(key)
                val_8h = self._normalize_funding_rate_to_8h(
                    raw_val if isinstance(raw_val, Decimal) else None, interval_s)
                if isinstance(val_8h, Decimal):
                    self._funding_cache[key] = {
                        "ts": time.time(), "value": val_8h, "raw": raw_val, "interval_s": interval_s}
                    return val_8h, raw_val if isinstance(raw_val, Decimal) else None, interval_s if isinstance(interval_s, int) else None
                self._funding_cache[key] = {
                    "ts": time.time(), "value": None, "raw": raw_val, "interval_s": interval_s}
                return None, raw_val if isinstance(raw_val, Decimal) else None, interval_s if isinstance(interval_s, int) else None
            except Exception:
                # 不阻塞行情；失败就维持 None
                self._funding_cache[key] = {
                    "ts": time.time(), "value": None, "raw": None, "interval_s": None}
                return None, None, None

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        # Variational “全市场列表”不在本适配器范围内，必须显式给 symbols
        if not symbols:
            raise ValueError(
                "VariationalAdapter.get_tickers 需要显式提供 symbols 列表")

        tasks = [self.get_ticker(s) for s in symbols]
        return await self._gather_safely(tasks)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """
        Variational 不提供深度 orderbook：返回空结构，避免上层误用。
        """
        normalized = self._normalize_symbol(symbol)
        now = datetime.now()
        return OrderBookData(
            symbol=normalized,
            bids=[],
            asks=[],
            timestamp=now,
            nonce=None,
            received_timestamp=now,
            raw_data={
                "note": "Variational does not provide orderbook depth; use get_ticker() for bid/ask"},
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[OHLCVData]:
        raise NotImplementedError("VariationalAdapter 暂不支持 K线数据（仅实现 BBO 行情）")

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[TradeData]:
        raise NotImplementedError("VariationalAdapter 暂不支持成交数据（仅实现 BBO 行情）")

    # ===== 账户与交易接口（基于模板重放，按需启用） =====

    async def get_balances(self) -> List[BalanceData]:
        # 任意模式：优先返回最近更新的缓存（rest/ws 都会写入缓存），避免 UI/风控每次都打外部请求
        if self._is_account_cache_fresh(self._balance_cache_ts):
            return list(self._balance_cache)

        if not self._has_trade_template("balances_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 balances_template（未启用余额查询）")

        now = datetime.now()
        out: List[BalanceData] = []
        cur = str(self._settlement_asset or "USDC").strip() or "USDC"

        # 0) 优先：portfolio WS 推送的权益余额（pool_portfolio_result.balance）
        try:
            if self._websocket and self._is_account_data_source_ws_enabled():
                ws_bal = self._websocket.get_cached_portfolio_balance()
                if ws_bal is not None:
                    out.append(
                        BalanceData(
                            currency=cur,
                            free=ws_bal,
                            used=Decimal("0"),
                            total=ws_bal,
                            usd_value=ws_bal,
                            timestamp=now,
                            raw_data={"source": "ws",
                                      "note": "portfolio_ws_balance"},
                        )
                    )
                    # ✅ 写入统一缓存：让 rest+ws 模式下“谁拿到谁更新”
                    try:
                        self._balance_cache = list(out)
                        self._balance_cache_ts = time.time()
                    except Exception:
                        pass
                    return out
        except Exception:
            pass

        # 1) 备用：REST /api/portfolio?compute_margin=true（balances_template）
        raw = await self._trade_send("balances_template", ctx={})
        data = raw.get("data") if isinstance(raw, dict) else None

        # 1) 优先识别“账户权益/净值”（Portfolio Value / Equity），用于风控与实盘规模控制
        # 说明：不同后端字段命名不同，这里做“弱解析”，不做强假设；同时兼容常见的包裹结构（result/portfolio/summary）。
        if isinstance(data, dict):
            equity_keys = [
                # Variational /api/portfolio 常见字段：balance 即权益余额
                "balance",
                "portfolio_value",
                "portfolioValue",
                "portfolio",
                "equity",
                "account_equity",
                "accountEquity",
                "net_value",
                "netValue",
                "total_equity",
                "totalEquity",
            ]

            def _iter_equity_dicts(d: Dict[str, Any]) -> List[Dict[str, Any]]:
                """按常见层级展开，尽量不猜测业务语义，只做结构容错。"""
                out_dicts: List[Dict[str, Any]] = [d]
                for k in ("result", "data", "portfolio", "account", "summary", "pool_portfolio_result"):
                    v = d.get(k)
                    if isinstance(v, dict):
                        out_dicts.append(v)
                # 有些接口会把核心字段放在 data/summary 里再嵌一层
                if isinstance(d.get("result"), dict) and isinstance(d["result"].get("summary"), dict):
                    out_dicts.append(d["result"]["summary"])
                return out_dicts

            equity: Optional[Decimal] = None
            for cand in _iter_equity_dicts(data):
                equity = self._parse_decimal_any(
                    self._pick_first(cand, equity_keys))
                if equity is not None:
                    break

            if equity is not None:
                # 用结算资产作为“权益币种”（通常 USDC）
                out.append(
                    BalanceData(
                        currency=cur,
                        free=equity,          # “剩余权益”对外表现为可用权益
                        used=Decimal("0"),
                        total=equity,
                        usd_value=equity,     # 结算资产本身等同 USD 计价（对 USDC）
                        timestamp=now,
                        raw_data={
                            "source": "rest", "note": "portfolio_value/equity", "response": raw},
                    )
                )
                try:
                    self._balance_cache = list(out)
                    self._balance_cache_ts = time.time()
                except Exception:
                    pass
                return out

            # 一次性诊断：接口返回了 dict 但没解析到权益字段，打印 keys 便于你贴日志我对齐字段
            if not getattr(self, "_balances_parse_log_once", False):
                setattr(self, "_balances_parse_log_once", True)
                try:
                    self.logger.info(
                        "[variational] balances 未识别到权益字段：data.keys=%s",
                        list(data.keys())[:50],
                    )
                except Exception:
                    pass

        # 容错解析：支持 list[dict] 或 dict[asset]->dict
        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            # 如果是 {"USDC": {...}} 形式
            if all(isinstance(k, str) for k in data.keys()):
                for k, v in data.items():
                    if isinstance(v, dict):
                        items.append({"currency": k, **v})

        for it in items:
            cur = str(it.get("currency") or it.get("asset")
                      or it.get("symbol") or "").strip()
            if not cur:
                continue
            free = self._parse_decimal_any(it.get("free") or it.get(
                "available") or it.get("available_balance"))
            used = self._parse_decimal_any(
                it.get("used") or it.get("locked") or it.get("frozen"))
            total = self._parse_decimal_any(
                it.get("total") or it.get("balance"))
            if total is None:
                if free is not None and used is not None:
                    total = free + used
                else:
                    total = Decimal("0")
            out.append(
                BalanceData(
                    currency=cur,
                    free=free or Decimal("0"),
                    used=used or Decimal("0"),
                    total=total,
                    usd_value=self._parse_decimal_any(
                        it.get("usd_value") or it.get("usdValue")),
                    timestamp=now,
                    raw_data={"response": raw, "item": it},
                )
            )
        try:
            self._balance_cache = list(out)
            self._balance_cache_ts = time.time()
        except Exception:
            pass
        return out

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        # 任意模式：优先返回最近更新的缓存（rest/ws 都会写入缓存）
        if self._is_account_cache_fresh(self._position_cache_ts):
            cached_list = list(self._position_cache.values())
            if symbols:
                normalized_filter = {
                    self._normalize_symbol(s) for s in symbols if isinstance(s, str)
                }
                return [p for p in cached_list if self._normalize_symbol(p.symbol) in normalized_filter]
            return cached_list

        # 🔥 优先使用 WS /portfolio 推送缓存（更实时，也避免必须配置 positions_template）
        try:
            if self._websocket and self._is_account_data_source_ws_enabled():
                cached = self._websocket.get_cached_positions()
                if cached:
                    # ✅ 写入统一缓存：让 rest+ws 模式下“谁拿到谁更新”
                    try:
                        new_cache: Dict[str, PositionData] = {}
                        for p in cached:
                            sym = str(getattr(p, "symbol", "") or "").strip().upper()
                            if sym:
                                new_cache[sym] = p
                        self._position_cache = new_cache
                        self._position_cache_ts = time.time()
                    except Exception:
                        pass
                    if symbols:
                        normalized_filter = {
                            self._normalize_symbol(s) for s in symbols if isinstance(s, str)
                        }
                        return [p for p in cached if self._normalize_symbol(p.symbol) in normalized_filter]
                    return cached
        except Exception:
            pass

        if not self._has_trade_template("positions_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 positions_template（未启用持仓查询），且 WS 未提供持仓缓存"
            )

        ctx: Dict[str, Any] = {}
        raw = await self._trade_send("positions_template", ctx=ctx)
        data = raw.get("data") if isinstance(raw, dict) else None
        now = datetime.now()
        out: List[PositionData] = []

        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            # 允许 {"BTC": {...}} 或 {"positions":[...]} 这种
            if "positions" in data and isinstance(data.get("positions"), list):
                items = [x for x in data.get(
                    "positions") if isinstance(x, dict)]
            else:
                for k, v in data.items():
                    if isinstance(v, dict):
                        items.append({"symbol": k, **v})

        normalized_filter = None
        if symbols:
            normalized_filter = {self._normalize_symbol(
                s) for s in symbols if isinstance(s, str)}

        for it in items:
            # 兼容 Var 前端结构：list[{"position_info":{"instrument":{"underlying":...}, "qty":...}, "price_info":{...}, ...}]
            sym_raw = it.get("symbol") or it.get("market") or it.get("underlying")
            pos_info = it.get("position_info") if isinstance(it, dict) else None
            if (not sym_raw) and isinstance(pos_info, dict):
                inst = pos_info.get("instrument")
                if isinstance(inst, dict):
                    sym_raw = inst.get("underlying") or inst.get("symbol") or inst.get("market")
                if not sym_raw:
                    sym_raw = pos_info.get("underlying") or pos_info.get("symbol") or pos_info.get("market")
            if not sym_raw:
                continue
            sym = self._normalize_symbol(str(sym_raw))
            if normalized_filter and sym not in normalized_filter:
                continue
            size = self._parse_decimal_any(
                it.get("size")
                or it.get("position_size")
                or it.get("qty")
                or it.get("amount")
                or (pos_info.get("qty") if isinstance(pos_info, dict) else None)
                or (pos_info.get("size") if isinstance(pos_info, dict) else None)
            ) or Decimal("0")
            side = PositionSide.LONG if size >= 0 else PositionSide.SHORT
            size_abs = abs(size)
            entry = self._parse_decimal_any(
                it.get("entry_price")
                or it.get("entryPrice")
                or it.get("avg_price")
                or it.get("average_price")
                or (pos_info.get("avg_entry_price") if isinstance(pos_info, dict) else None)
                or (pos_info.get("avgEntryPrice") if isinstance(pos_info, dict) else None)
            ) or Decimal("0")
            price_info = it.get("price_info") if isinstance(it, dict) else None
            mark = self._parse_decimal_any(it.get("mark_price") or it.get("markPrice"))
            if (mark is None) and isinstance(price_info, dict):
                mark = self._parse_decimal_any(price_info.get("price") or price_info.get("underlying_price") or price_info.get("underlyingPrice"))
            upnl = self._parse_decimal_any(it.get("unrealized_pnl") or it.get(
                "unrealizedPnl") or it.get("uPnl")) or Decimal("0")
            # Var positions REST 里常见字段：upnl/rpnl/value/cum_funding 等在顶层
            rpnl = self._parse_decimal_any(
                it.get("realized_pnl") or it.get("realizedPnl") or it.get("rPnl") or it.get("rpnl")
            ) or Decimal("0")
            lev = int(it.get("leverage") or (pos_info.get("leverage") if isinstance(pos_info, dict) else 1) or 1)
            mm = str(
                it.get("margin_mode")
                or it.get("marginMode")
                or (pos_info.get("margin_mode") if isinstance(pos_info, dict) else None)
                or "cross"
            ).lower()
            margin_mode = MarginMode.ISOLATED if mm == "isolated" else MarginMode.CROSS
            margin = self._parse_decimal_any(it.get("margin") or it.get(
                "initial_margin") or it.get("initialMargin")) or Decimal("0")
            liq = self._parse_decimal_any(
                it.get("liquidation_price")
                or it.get("liq_price")
                or it.get("liquidationPrice")
                or it.get("estimated_liquidation_price")
                or it.get("estimatedLiquidationPrice")
            )

            # 过滤 0 仓位（避免 UI 显示“空仓条目”）
            if size_abs <= Decimal("0"):
                continue

            out.append(
                PositionData(
                    symbol=sym,
                    side=side,
                    size=size_abs,
                    entry_price=entry,
                    mark_price=mark,
                    current_price=None,
                    unrealized_pnl=upnl,
                    realized_pnl=rpnl,
                    percentage=None,
                    leverage=lev,
                    margin_mode=margin_mode,
                    margin=margin,
                    liquidation_price=liq,
                    timestamp=now,
                    raw_data={"response": raw, "item": it},
                )
            )
        # 写入缓存：供 UI/风控快速读取（尤其在 rest 模式下）
        try:
            new_cache: Dict[str, PositionData] = {}
            for p in out:
                sym = str(getattr(p, "symbol", "") or "").strip().upper()
                if sym:
                    new_cache[sym] = p
            self._position_cache = new_cache
            self._position_cache_ts = time.time()
        except Exception:
            pass
        return out

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> OrderData:
        # 先只实现市价；限价保留占位符
        if order_type == OrderType.LIMIT:
            if not self._has_trade_template("order_create_limit_template"):
                raise NotImplementedError(
                    "VariationalAdapter 暂不支持限价单：缺少 order_create_limit_template（预留占位）")
        elif order_type != OrderType.MARKET:
            raise NotImplementedError(
                f"VariationalAdapter 暂不支持该订单类型: {order_type.value}")

        if order_type == OrderType.MARKET and not self._has_trade_template("order_create_market_template"):
            raise NotImplementedError(
                "VariationalAdapter 暂不支持下单：缺少 order_create_market_template")

        normalized = self._normalize_symbol(symbol)
        reduce_only_flag = False
        max_slippage = None
        # 🔥 本地 client_id：用于 WS 事件匹配与 OrderMonitor 等待（避免“WS 推送早于 pending 注册”导致匹配到旧单）
        # - 若上层传入了 client_id/tracking_id，则复用；否则生成 UUID
        local_client_id = None
        if isinstance(params, dict):
            for k in ("client_id", "clientId", "tracking_id", "trackingId"):
                v = params.get(k)
                if isinstance(v, str) and v.strip():
                    local_client_id = v.strip()
                    break
        if not local_client_id:
            local_client_id = str(uuid.uuid4())

        ctx = {
            "symbol": normalized,
            "underlying": normalized,
            "side": side.value,
            "qty": str(amount),
            "amount": str(amount),
            "order_type": order_type.value,
            "price": "" if price is None else str(price),
            "client_id": local_client_id,
        }
        if isinstance(params, dict):
            # 允许把额外上下文塞进模板（例如 reduce_only / tif / client_id 等）
            for k, v in params.items():
                if k and v is not None:
                    # ⚠️ Variational API 对 max_slippage 的类型很严格：必须是数值（f64），不能是字符串
                    if str(k) == "max_slippage":
                        try:
                            ctx[str(k)] = float(Decimal(str(v)))
                        except Exception:
                            ctx[str(k)] = v
                    else:
                        ctx[str(k)] = v
            reduce_only_flag = bool(params.get(
                "reduce_only") or params.get("is_reduce_only") or False)
            max_slippage = params.get(
                "max_slippage") if "max_slippage" in params else None

        # 兼容前端常见字段命名
        ctx.setdefault("reduce_only", reduce_only_flag)
        ctx.setdefault("is_reduce_only", reduce_only_flag)

        # 可选：默认 max_slippage 由配置提供（避免硬编码）
        if max_slippage is None:
            max_slippage = self.config.extra_params.get(
                "trade_default_max_slippage")
        if max_slippage is not None:
            try:
                ctx.setdefault("max_slippage", float(Decimal(str(max_slippage))))
            except Exception:
                ctx.setdefault("max_slippage", max_slippage)

        # 🔥 市价单：若模板需要 quote_id，则先调用 /api/quotes/indicative 获取 quote_id（全自动）
        if order_type == OrderType.MARKET and "quote_id" not in ctx:
            try:
                qty_str = str(amount)
                req = VariationalIndicativeQuoteRequest(
                    underlying=normalized,
                    qty=qty_str,
                    funding_interval_s=self._funding_interval_s,
                    settlement_asset=self._settlement_asset,
                    instrument_type=self._instrument_type,
                )
                quote_raw = await self._rest.get_indicative_quote(req)
                # 尽量不假设字段名，但避免使用泛化的 "id"（很可能不是 quote_id，导致 WS 匹配失败）
                quote_id = None
                if isinstance(quote_raw, dict):
                    for k in ("quote_id", "quoteId", "source_quote", "parent_quote_id"):
                        v = quote_raw.get(k)
                        if isinstance(v, (str, int)) and str(v).strip():
                            quote_id = str(v).strip()
                            break
                if quote_id:
                    ctx["quote_id"] = quote_id
            except Exception as e:
                # 允许继续走模板（若模板不需要 quote_id）；需要时会在下单时报错，便于排查
                self.logger.warning(
                    f"[variational-trade] pre-quote failed (symbol={normalized} qty={amount}): {e}")

        # ✅ 下单前日志：可验证 max_slippage 是否随单进入模板（不含 token/body）
        if self.logger and order_type == OrderType.MARKET:
            try:
                self.logger.info(
                    "🧾 [Variational-Frontend] MARKET submit | symbol=%s side=%s qty=%s | quote_id=%s | max_slippage=%s | client_id=%s",
                    normalized,
                    side.value,
                    str(amount),
                    str(ctx.get("quote_id") or "-"),
                    str(ctx.get("max_slippage")) if ctx.get("max_slippage") is not None else "None",
                    str(local_client_id),
                )
            except Exception:
                pass

        # 🔥 下单前就登记 pending（用本地 client_id），避免 WS trade 来得太快导致匹配到旧单
        try:
            if self._websocket:
                self._websocket.register_pending_order(
                    local_tracking_id=str(local_client_id),
                    symbol=normalized,
                    side=side,
                    qty=amount,
                    max_slippage=(
                        Decimal(str(ctx.get("max_slippage")))
                        if ctx.get("max_slippage") not in (None, "")
                        else None
                    ),
                )
        except Exception:
            pass

        # 记录提交上下文：用于 HTTP 超时后的“按参数确认”
        try:
            self._submit_ctx_cache[str(local_client_id)] = {
                "symbol": normalized,
                "side": side,
                "qty": Decimal(str(amount)),
                "created_mono": asyncio.get_event_loop().time(),
            }
        except Exception:
            pass

        tpl_name = "order_create_market_template" if order_type == OrderType.MARKET else "order_create_limit_template"
        try:
            raw = await self._trade_send(tpl_name, ctx)
        except Exception as e:
            # =========================
            # 方案A：HTTP 超时不等于下单失败
            # - 不抛出异常给执行器（否则会触发“网络异常重试”导致重复下单）
            # - 先用 WS/REST 确认是否已经成交/已生成订单
            # =========================
            # 只把“真正的网络/超时问题”当成“可能已成交”，避免把 400/403 这类响应错误误判为超时
            is_timeout = isinstance(e, (asyncio.TimeoutError, TimeoutError))
            if isinstance(e, aiohttp.ClientResponseError):
                is_timeout = False
            elif (not is_timeout) and isinstance(e, aiohttp.ClientError):
                # 连接断开/网络抖动：可能请求已到达服务器但回包丢失
                is_timeout = True
            if is_timeout:
                self.logger.warning(
                    "[variational-trade] 下单HTTP超时(可能已成交)：symbol=%s side=%s qty=%s client_id=%s，将进入确认流程避免重复下单",
                    normalized,
                    side.value,
                    str(amount),
                    str(local_client_id),
                )
                confirmed = await self._confirm_after_submit_timeout(
                    local_client_id=str(local_client_id),
                    symbol=normalized,
                    side=side,
                    qty=amount,
                )
                if confirmed:
                    # 确认到了就直接返回（相当于成功）
                    try:
                        if not confirmed.raw_data:
                            confirmed.raw_data = {}
                        confirmed.raw_data.update(
                            {
                                "submit_timeout": True,
                                "submit_timeout_error": str(e),
                                "request_ctx": ctx,
                                "local_client_id": str(local_client_id),
                            }
                        )
                    except Exception:
                        pass
                    return confirmed

                # 没确认到：返回一个“待确认订单”，让上层等待 WS（而不是重发）
                now = datetime.now()
                return OrderData(
                    id=str(local_client_id),
                    client_id=str(local_client_id),
                    symbol=normalized,
                    side=side,
                    type=order_type,
                    amount=amount,
                    price=price,
                    filled=Decimal("0"),
                    remaining=amount,
                    cost=Decimal("0"),
                    average=None,
                    status=OrderStatus.PENDING,
                    timestamp=now,
                    updated=now,
                    fee=None,
                    trades=[],
                    params=params or {},
                    raw_data={
                        "submit_timeout": True,
                        "submit_timeout_error": str(e),
                        "request_ctx": ctx,
                        "local_client_id": str(local_client_id),
                    },
                )

            # 非超时：保持原行为抛出（例如 400/参数错误）
            raise

        # 1) 先从响应提取“交易所订单ID”（若存在）
        response_order_id = self._parse_order_id(raw)

        # 2) 再从响应提取“quote_id”（优先，用于 WS 成交匹配）
        response_quote_id = self._extract_response_field(
            raw,
            ["quote_id", "quoteId", "source_quote", "parent_quote_id"],
        )
        response_rfq_id = self._extract_response_field(
            raw,
            ["rfq_id", "rfqId", "source_rfq"],
        )

        # 🔥 Variational：WS 成交事件用 source_quote/parent_quote_id（即 quote_id）关联。
        # 因此市价单的 OrderData.id 用 quote_id 做追踪ID；若响应未给 quote_id，则退化使用请求 ctx 的 quote_id。
        # 优先使用 rfq_id 作为追踪ID（更稳定；clearing_event 里一定有 rfq_id）
        order_id = response_rfq_id or response_quote_id or response_order_id
        if order_type == OrderType.MARKET:
            qid = ctx.get("quote_id")
            if isinstance(qid, str) and qid.strip():
                # 如果没拿到 rfq_id，则用响应 quote_id，否则退化到请求 quote_id
                order_id = response_rfq_id or response_quote_id or qid.strip() or order_id

        now = datetime.now()
        order = OrderData(
            id=order_id,
            client_id=local_client_id,
            symbol=normalized,
            side=side,
            type=order_type,
            amount=amount,
            price=price,
            filled=Decimal("0"),
            remaining=amount,
            cost=Decimal("0"),
            average=None,
            status=self._parse_order_status(raw),
            timestamp=now,
            updated=now,
            fee=None,
            trades=[],
            params=params or {},
            raw_data={
                "request_ctx": ctx,
                "response": raw,
                "exchange_order_id": response_order_id,
                "response_quote_id": response_quote_id,
                "response_rfq_id": response_rfq_id,
                "local_client_id": local_client_id,
            },
        )

        # 将本地 tracking_id(client_id) 登记到 WS（兜底：即使上面提前登记失败，这里也再登记一次）
        try:
            if self._websocket:
                self._websocket.register_pending_order(
                    local_tracking_id=str(local_client_id),
                    symbol=normalized,
                    side=side,
                    qty=amount,
                    max_slippage=(
                        Decimal(str(ctx.get("max_slippage")))
                        if ctx.get("max_slippage") not in (None, "")
                        else None
                    ),
                )
        except Exception:
            pass

        # ✅ 下单结果日志：此时通常 still pending，成交均价需等 WS（见 VariationalWebSocket 的 FILL compare）
        if self.logger and order_type == OrderType.MARKET:
            try:
                self.logger.info(
                    "🧾 [Variational-Frontend] MARKET result | symbol=%s side=%s qty=%s | order_id=%s status=%s | max_slippage=%s",
                    normalized,
                    side.value,
                    str(amount),
                    str(order_id),
                    str(order.status.value if getattr(order, "status", None) else ""),
                    str(ctx.get("max_slippage")) if ctx.get("max_slippage") is not None else "None",
                )
            except Exception:
                pass

        return order

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        if not self._has_trade_template("order_cancel_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 order_cancel_template（未启用撤单）")

        base = self._normalize_underlying_ctx(symbol)
        ctx = {"order_id": str(order_id), "id": str(order_id), **base}
        raw = await self._trade_send("order_cancel_template", ctx)
        now = datetime.now()
        return OrderData(
            id=str(order_id),
            client_id=None,
            symbol=base["symbol"],
            side=OrderSide.BUY,  # 取消不关心方向；给个占位
            type=OrderType.MARKET,
            amount=Decimal("0"),
            price=None,
            filled=Decimal("0"),
            remaining=Decimal("0"),
            cost=Decimal("0"),
            average=None,
            status=OrderStatus.CANCELED,
            timestamp=now,
            updated=now,
            fee=None,
            trades=[],
            params={},
            raw_data={"request_ctx": ctx, "response": raw},
        )

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        base = self._normalize_underlying_ctx(symbol)
        ctx = {
            "order_id": str(order_id),
            "id": str(order_id),
            **base,
            # 方便模板做 instrument 过滤（若模板未使用该字段，不影响）
            "instrument": self._build_instrument(base["symbol"]),
            "funding_interval_s": int(self._funding_interval_s),
            "settlement_asset": str(self._settlement_asset),
            # 列表查单常见分页参数（若模板未使用，不影响）
            "limit": int(self.config.extra_params.get("orders_lookup_limit", 50)),
            "offset": 0,
        }

        # 1) 优先：若存在单笔查单模板，直接使用
        raw: Dict[str, Any]
        data: Any
        if self._has_trade_template("order_get_template"):
            raw = await self._trade_send("order_get_template", ctx)
            data = raw.get("data") if isinstance(raw, dict) else None
        else:
            # 2) 兜底：用“列表接口”模拟单笔查单（用于 WS 超时后确认成交）
            #    - 先查 open(pending)，再查 history(cleared/canceled/rejected)
            found: Optional[Dict[str, Any]] = None
            raw = {"ok": True, "status": 0,
                   "data": None, "note": "list_fallback"}
            data = None
            try:
                if self._has_trade_template("orders_open_template"):
                    ro = await self._trade_send("orders_open_template", ctx)
                    od = ro.get("data") if isinstance(ro, dict) else None
                    for it in self._extract_list_items(od):
                        if self._match_order_id_in_item(it, str(order_id)):
                            found = it
                            raw = ro
                            data = it
                            break
            except Exception:
                pass
            if found is None:
                try:
                    if self._has_trade_template("orders_history_template"):
                        rh = await self._trade_send("orders_history_template", ctx)
                        hd = rh.get("data") if isinstance(rh, dict) else None
                        for it in self._extract_list_items(hd):
                            if self._match_order_id_in_item(it, str(order_id)):
                                found = it
                                raw = rh
                                data = it
                                break
                except Exception:
                    pass

            # 3) 可选：再用 trades 列表兜底（部分情况下订单列表不全，但成交列表能确认）
            #    - 这里只用作“确认有成交”，不强行推断剩余/均价
            if found is None:
                try:
                    if self._has_trade_template("trades_history_template"):
                        rt = await self._trade_send("trades_history_template", ctx)
                        td = rt.get("data") if isinstance(rt, dict) else None
                        for it in self._extract_list_items(td):
                            if self._match_order_id_in_item(it, str(order_id)):
                                # trades item：用最小结构构造一个“已成交”结果
                                found = {"status": "cleared", "filled": it.get("qty") or it.get(
                                    "filled") or None, "average": it.get("price") or None, "raw_trade": it}
                                raw = rt
                                data = found
                                break
                except Exception:
                    pass

            if found is None:
                raise NotImplementedError(
                    "VariationalAdapter 缺少 order_get_template 且无法通过 orders_open/history/trades 模板匹配该订单"
                )

        filled = Decimal("0")
        remaining = Decimal("0")
        avg = None
        price = None
        side = OrderSide.BUY
        otype = OrderType.MARKET
        if isinstance(data, dict):
            filled = self._parse_decimal_any(self._pick_first(
                data, ["filled", "filled_qty", "filledQty", "executed", "executed_qty"])) or Decimal("0")
            remaining = self._parse_decimal_any(self._pick_first(
                data, ["remaining", "remaining_qty", "remainingQty", "leaves", "leaves_qty"])) or Decimal("0")
            avg = self._parse_decimal_any(self._pick_first(
                data, ["average", "avg_price", "avgPrice", "average_price"]))
            price = self._parse_decimal_any(self._pick_first(
                data, ["price", "limit_price", "limitPrice"]))
            s = self._pick_first(data, ["side", "direction"])
            if isinstance(s, str) and s.strip().lower() in ("buy", "sell"):
                side = OrderSide.BUY if s.strip().lower() == "buy" else OrderSide.SELL
            t = self._pick_first(data, ["type", "order_type", "orderType"])
            if isinstance(t, str) and t.strip().lower() in ("market", "limit"):
                otype = OrderType.MARKET if t.strip().lower() == "market" else OrderType.LIMIT

        # 单笔接口与列表接口的 status 命名不同：这里优先用“列表型 status 解析”
        status = (
            self._parse_status_from_item(data)
            if isinstance(data, dict)
            else self._parse_order_status(raw)
        )
        now = datetime.now()
        return OrderData(
            id=str(order_id),
            client_id=None,
            symbol=base["symbol"],
            side=side,
            type=otype,
            amount=(filled + remaining) if (filled +
                                            remaining) > 0 else Decimal("0"),
            price=price,
            filled=filled,
            remaining=remaining,
            cost=Decimal("0"),
            average=avg,
            status=status,
            timestamp=now,
            updated=now,
            fee=None,
            trades=[],
            params={},
            raw_data={"request_ctx": ctx, "response": raw},
        )

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        if not self._has_trade_template("orders_open_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 orders_open_template（未启用开放订单查询）")

        ctx: Dict[str, Any] = {}
        if symbol:
            base = self._normalize_underlying_ctx(symbol)
            ctx.update(base)
            ctx.setdefault(
                "instrument", self._build_instrument(base["symbol"]))
            ctx.setdefault("funding_interval_s", int(self._funding_interval_s))
            ctx.setdefault("settlement_asset", str(self._settlement_asset))
        # 给列表接口一个通用分页默认值（模板没用到也不影响）
        ctx.setdefault("limit", int(
            self.config.extra_params.get("orders_lookup_limit", 50)))
        ctx.setdefault("offset", 0)
        raw = await self._trade_send("orders_open_template", ctx)
        data = raw.get("data") if isinstance(raw, dict) else None
        now = datetime.now()
        out: List[OrderData] = []
        items = self._extract_list_items(data)

        for it in items:
            oid = self._parse_order_id({"data": it})
            sym_raw = it.get("symbol") or it.get(
                "market") or it.get("underlying") or (symbol or "")
            sym = self._normalize_symbol(str(sym_raw)) if sym_raw else (
                self._normalize_symbol(symbol) if symbol else "")
            s = (it.get("side") or it.get("direction") or "").strip().lower()
            side = OrderSide.BUY if s == "buy" else OrderSide.SELL if s == "sell" else OrderSide.BUY
            t = (it.get("type") or it.get("order_type")
                 or it.get("orderType") or "").strip().lower()
            otype = OrderType.MARKET if t == "market" else OrderType.LIMIT if t == "limit" else OrderType.MARKET
            amt = self._parse_decimal_any(it.get("amount") or it.get(
                "qty") or it.get("quantity") or it.get("size")) or Decimal("0")
            filled = self._parse_decimal_any(it.get("filled") or it.get(
                "filled_qty") or it.get("executed")) or Decimal("0")
            remaining = self._parse_decimal_any(it.get("remaining") or it.get(
                "remaining_qty") or it.get("leaves")) or max(Decimal("0"), amt - filled)
            price = self._parse_decimal_any(
                it.get("price") or it.get("limit_price"))
            status = self._parse_status_from_item(it)
            out.append(
                OrderData(
                    id=oid,
                    client_id=None,
                    symbol=sym,
                    side=side,
                    type=otype,
                    amount=amt,
                    price=price,
                    filled=filled,
                    remaining=remaining,
                    cost=Decimal("0"),
                    average=self._parse_decimal_any(
                        it.get("average") or it.get("avg_price")),
                    status=status,
                    timestamp=now,
                    updated=now,
                    fee=None,
                    trades=[],
                    params={},
                    raw_data={"response": raw, "item": it},
                )
            )
        return out

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        raise NotImplementedError("VariationalAdapter 暂不支持取消全部订单（当前只实现A：行情）")

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[OrderData]:
        # 用于 OrderMonitor 的 WS 超时兜底：通过 /api/orders/v2 查询最近已关闭订单（cleared/canceled/rejected）
        if not self._has_trade_template("orders_history_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 orders_history_template（未启用历史订单查询）")

        ctx: Dict[str, Any] = {}
        if symbol:
            base = self._normalize_underlying_ctx(symbol)
            ctx.update(base)
            ctx.setdefault(
                "instrument", self._build_instrument(base["symbol"]))
            ctx.setdefault("funding_interval_s", int(self._funding_interval_s))
            ctx.setdefault("settlement_asset", str(self._settlement_asset))
        if limit is not None:
            ctx["limit"] = int(limit)
        ctx.setdefault("offset", 0)
        if since is not None:
            # 允许模板使用 created_at_gte 占位符（扩展抓到的接口就是这个参数）
            ctx["created_at_gte"] = since.isoformat(
                timespec="milliseconds") + "Z"

        raw = await self._trade_send("orders_history_template", ctx)
        data = raw.get("data") if isinstance(raw, dict) else None
        now = datetime.now()
        out: List[OrderData] = []
        for it in self._extract_list_items(data):
            oid = self._parse_order_id({"data": it})
            sym_raw = it.get("symbol") or it.get(
                "market") or it.get("underlying") or (symbol or "")
            sym = self._normalize_symbol(str(sym_raw)) if sym_raw else (
                self._normalize_symbol(symbol) if symbol else "")
            s = (it.get("side") or it.get("direction") or "").strip().lower()
            side = OrderSide.BUY if s == "buy" else OrderSide.SELL if s == "sell" else OrderSide.BUY
            t = (it.get("type") or it.get("order_type")
                 or it.get("orderType") or "").strip().lower()
            otype = OrderType.MARKET if t == "market" else OrderType.LIMIT if t == "limit" else OrderType.MARKET
            amt = self._parse_decimal_any(it.get("amount") or it.get(
                "qty") or it.get("quantity") or it.get("size")) or Decimal("0")
            filled = self._parse_decimal_any(it.get("filled") or it.get(
                "filled_qty") or it.get("executed")) or Decimal("0")
            remaining = self._parse_decimal_any(it.get("remaining") or it.get(
                "remaining_qty") or it.get("leaves")) or max(Decimal("0"), amt - filled)
            price = self._parse_decimal_any(
                it.get("price") or it.get("limit_price"))
            status = self._parse_status_from_item(it)
            out.append(
                OrderData(
                    id=oid,
                    client_id=None,
                    symbol=sym,
                    side=side,
                    type=otype,
                    amount=amt,
                    price=price,
                    filled=filled,
                    remaining=remaining,
                    cost=Decimal("0"),
                    average=self._parse_decimal_any(
                        it.get("average") or it.get("avg_price")),
                    status=status,
                    timestamp=now,
                    updated=now,
                    fee=None,
                    trades=[],
                    params={},
                    raw_data={"response": raw, "item": it},
                )
            )
        return out

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        if not self._has_trade_template("set_leverage_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 set_leverage_template（预留占位）")
        base = self._normalize_underlying_ctx(symbol)
        ctx = {**base, "leverage": int(leverage)}
        return await self._trade_send("set_leverage_template", ctx)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        if not self._has_trade_template("set_margin_mode_template"):
            raise NotImplementedError(
                "VariationalAdapter 缺少 set_margin_mode_template（预留占位）")
        base = self._normalize_underlying_ctx(symbol)
        ctx = {**base, "margin_mode": str(margin_mode)}
        return await self._trade_send("set_margin_mode_template", ctx)

    # ===== 实时数据流接口（ticker 用轮询实现；orderbook/trades/user_data 暂不支持）=====

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """
        订阅 ticker：
        - Variational 无公开 orderbook，BBO 来自 /api/quotes/indicative
        - 用轮询实现实时推送给上层
        """
        normalized = self._normalize_symbol(symbol)
        # 保存订阅意图：用于断线重连后恢复
        try:
            self._ticker_callbacks[normalized] = callback
        except Exception:
            pass
        if normalized in self._ticker_tasks and not self._ticker_tasks[normalized].done():
            return

        # 一次性提示：确认 ticker 轮询任务确实被启动（否则用户只能从 UI 间接判断）
        try:
            once_key = f"_ticker_poll_started_{normalized}"
            if not getattr(self, once_key, False):
                setattr(self, once_key, True)
                self.logger.info(
                    "[variational] ticker轮询已启动: underlying=%s interval_ms=%s",
                    normalized,
                    int(self._ticker_poll_interval_ms),
                )
        except Exception:
            pass

        async def _loop() -> None:
            interval = max(50, self._ticker_poll_interval_ms) / 1000.0
            backoff = 0.2
            auth_last_log_ts = 0.0
            # 鉴权失败处理（避免硬编码，允许通过配置调参）
            try:
                auth_log_interval_s = float(
                    self.config.extra_params.get("variational_auth_log_interval_s", 60.0)
                )
            except Exception:
                auth_log_interval_s = 60.0
            try:
                auth_sleep_s = float(
                    self.config.extra_params.get("variational_auth_sleep_s", 60.0)
                )
            except Exception:
                auth_sleep_s = 60.0
            while True:
                try:
                    ticker = await self.get_ticker(normalized)
                    # 一次性提示：确认首次成功拿到 BBO（若一直失败，UI 会长期为空）
                    try:
                        ok_key = f"_ticker_first_ok_{normalized}"
                        if not getattr(self, ok_key, False):
                            setattr(self, ok_key, True)
                            self.logger.info(
                                "[variational] ticker首次成功: underlying=%s bid=%s ask=%s",
                                normalized,
                                str(getattr(ticker, "bid", None)),
                                str(getattr(ticker, "ask", None)),
                            )
                    except Exception:
                        pass
                    await self._invoke_callback(callback, ticker)
                    backoff = 0.2
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except VariationalAuthError as e:
                    # token/cookie 过期：不做快速重试（无意义），节流日志 + 长等待，避免刷屏
                    try:
                        now_ts = time.time()
                        if (now_ts - float(auth_last_log_ts or 0.0)) >= auth_log_interval_s:
                            auth_last_log_ts = now_ts
                            self.logger.error(
                                "[variational] 鉴权失败(401/Invalid token)，请刷新 VARIATIONAL_COOKIE 或 token 后重启进程: underlying=%s err=%s",
                                normalized,
                                str(e),
                            )
                    except Exception:
                        pass
                    await asyncio.sleep(max(1.0, auth_sleep_s))
                except Exception as e:
                    # 避免刷屏，简单退避
                    self.logger.warning(
                        f"Variational ticker poll error ({normalized}): {e}")
                    await asyncio.sleep(min(5.0, backoff))
                    backoff = min(5.0, backoff * 2)

        self._ticker_tasks[normalized] = asyncio.create_task(_loop())
        # 🔧 自愈：若轮询任务意外结束（异常/被取消），且仍处于连接状态，则自动重启订阅
        # 说明：这比常驻“看门狗循环”更轻量，也能覆盖“静默停更”的问题。

        def _on_task_done(task: asyncio.Task, underlying: str = normalized) -> None:
            try:
                # task 结束后先从表里移除，避免 subscribe_ticker 认为仍在运行
                self._ticker_tasks.pop(underlying, None)
            except Exception:
                pass

            # 如果是正常断开连接导致的 cancel，不要重启
            if not getattr(self, "_connected", False):
                return

            cb = self._ticker_callbacks.get(underlying)
            if not cb:
                return

            # 尝试提取异常原因（CancelledError 通常无异常）
            reason = None
            try:
                exc = task.exception()
                reason = str(exc) if exc else None
            except Exception:
                reason = None

            # 低频告警 + 异步重启（不要在 done_callback 里 await）
            try:
                now_ts = time.time()
                last = float(self._ticker_task_restart_log_ts.get(
                    underlying, 0.0) or 0.0)
                if (now_ts - last) >= 60.0:
                    self._ticker_task_restart_log_ts[underlying] = now_ts
                    self.logger.warning(
                        "[variational] ticker轮询任务已退出，自动重启订阅: underlying=%s reason=%s",
                        underlying,
                        reason or "cancelled_or_unknown",
                    )
            except Exception:
                pass

            try:
                asyncio.create_task(self.subscribe_ticker(underlying, cb))
            except Exception:
                return

        try:
            self._ticker_tasks[normalized].add_done_callback(_on_task_done)
        except Exception:
            pass

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """
        Variational 不提供深度 orderbook。
        为了不让上层订阅流程直接失败，这里会推送一次“空 orderbook”，并在 raw_data 里注明原因。
        """
        ob = await self.get_orderbook(symbol)
        await self._invoke_callback(callback, ob)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        raise NotImplementedError("VariationalAdapter 暂不支持订阅成交（当前只实现A：行情）")

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        # 兼容执行器：如果存在 _websocket，则走 events 流；否则报错
        if not self._websocket:
            raise NotImplementedError(
                "VariationalAdapter 未启用 WebSocket（请检查 extra_params.ws_enable / ws_events_url / ws_auth_token）")
        try:
            # 轻量一次性日志：确认“订阅入口确实被调用”（否则用户只能从 arbitrage_executor.log 间接判断）
            if not getattr(self, "_ws_subscribe_log_once", False):
                setattr(self, "_ws_subscribe_log_once", True)
                self.logger.info(
                    "[variational-ws] subscribe_user_data called (ws_connected=%s, events_url=%s)",
                    getattr(self._websocket, "_ws_connected", False),
                    getattr(self._websocket, "_events_url", ""),
                )
        except Exception:
            pass
        await self._websocket.subscribe_user_data(callback)

    async def subscribe_orders(self, callback: Callable[..., Any]) -> None:
        """
        订阅订单/成交进度（OrderData）。

        说明：
        - Variational 的 `subscribe_user_data` 目前保持“原始 WS event(dict)”语义（兼容现有工具脚本）。
        - 刷量/执行类服务通常需要 OrderData（OPEN/FILLED）来做成交确认与滑点失败检测，
          因此提供 `subscribe_orders` 作为更高层语义的订阅入口。
        """
        if not self._websocket:
            raise NotImplementedError(
                "VariationalAdapter 未启用 WebSocket（请检查 extra_params.ws_enable / ws_events_url / ws_auth_token）"
            )
        await self._websocket.subscribe_orders(callback)

    async def subscribe_positions(self, callback: Callable[..., Any]) -> None:
        """
        订阅持仓快照/更新。

        说明：当前 Variational portfolio WS 推送结构是 raw event(dict)；
        上层若只用于 UI 展示/风控缓存，可直接消费该回调。
        """
        if not self._websocket:
            raise NotImplementedError(
                "VariationalAdapter 未启用 WebSocket（请检查 extra_params.ws_enable / ws_portfolio_url / ws_auth_token）"
            )
        await self._websocket.subscribe_positions(callback)

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
        slippage_multiplier: Decimal = Decimal("1.0"),
        **kwargs,
    ) -> OrderData:
        """
        兼容刷量服务的市价下单入口（对齐 LighterRest.place_market_order 的常用参数子集）。

        约定：
        - `slippage_multiplier` 用于放大 max_slippage（基础值来自：
            1) kwargs['max_slippage'] / params['max_slippage']
            2) config.extra_params.trade_default_max_slippage
          ）
        - Variational 实际交易通过 `create_order(OrderType.MARKET)` 实现（含 quote_id 自动获取）。
        """
        params: Dict[str, Any] = {"reduce_only": bool(reduce_only)}
        if client_order_id:
            # create_order 会识别 client_id/tracking_id
            params["client_id"] = str(client_order_id)

        # 基础 max_slippage：优先显式传入，其次用交易所配置默认值
        base_slippage = kwargs.get("max_slippage")
        if base_slippage is None:
            base_slippage = self.config.extra_params.get("trade_default_max_slippage")
        try:
            if base_slippage is not None:
                base_dec = Decimal(str(base_slippage))
                mult = Decimal(str(slippage_multiplier or "1"))
                if base_dec > 0 and mult > 0:
                    # ⚠️ Variational API 期望数值类型（f64），不要传字符串
                    params["max_slippage"] = float(base_dec * mult)
        except Exception:
            # 参数解析失败时：不强行覆盖，让 create_order 使用其内部默认值/模板默认值
            pass

        return await self.create_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            amount=Decimal(str(quantity)),
            params=params,
        )

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """
        取消订阅：
        - symbol=None：取消所有 ticker 轮询任务
        - 否则取消指定币种 ticker 轮询
        """
        if symbol is None:
            tasks = list(self._ticker_tasks.values())
            self._ticker_tasks.clear()
            self._ticker_callbacks.clear()
            for t in tasks:
                t.cancel()
            return

        normalized = self._normalize_symbol(symbol)
        try:
            self._ticker_callbacks.pop(normalized, None)
        except Exception:
            pass
        task = self._ticker_tasks.pop(normalized, None)
        if task:
            task.cancel()

    # ===== 内部工具 =====

    def _normalize_symbol(self, symbol: str) -> str:
        # Variational 页面与插件都按 underlying（如 BTC/ETH）工作
        # 这里兼容传入 BTC/USDC、BTC-USDC、BTC/USDC:PERP 等格式
        s = (symbol or "").strip().upper()
        if not s:
            raise ValueError("symbol 不能为空")
        base = s.split(":")[0]
        base = base.split("/")[0].split("-")[0]
        return base

    def _get_quote_qty_for_symbol(self, symbol: str) -> str:
        per_symbol = self.config.extra_params.get(
            "indicative_quote_qty_by_symbol")
        if isinstance(per_symbol, dict):
            v = per_symbol.get(symbol)
            if isinstance(v, (int, float, str)) and str(v).strip():
                return str(v)
        return self._default_quote_qty

    async def _gather_safely(self, tasks: List[Any]) -> List[Any]:
        # 避免一个 symbol 失败导致全部失败；失败的任务会被记录并跳过
        results: List[Any] = []
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for r in gathered:
            if isinstance(r, Exception):
                self.logger.warning(f"Variational get_ticker failed: {r}")
                continue
            results.append(r)
        return results

    async def _invoke_callback(self, callback: Callable, data: Any) -> None:
        """兼容同步/异步回调"""
        try:
            result = callback(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            self.logger.error(f"Variational callback error: {e}")
