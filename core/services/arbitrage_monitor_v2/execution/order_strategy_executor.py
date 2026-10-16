from __future__ import annotations

"""
订单策略执行器模块
------------------
负责承接 `ArbitrageExecutor` 中所有与“下单模式选择 + 执行流程”有关的纯逻辑：
- 解析执行计划（限价/市价/双限价/批量WS）
- 调度各执行模式并处理部分成交、fallback、市价补单
- 保持原有日志与异常格式，确保外层行为一致

此模块不做依赖注入或状态管理，所有外部依赖通过 `executor` 传入，做到“可读、可复用、不改功能”。
"""

import asyncio
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import logging
from typing import Any, Dict, Optional, Type, TYPE_CHECKING, Tuple, List

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderData, OrderSide, OrderType
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

from ..risk_control.network_state import notify_network_recovered

if TYPE_CHECKING:
    from .arbitrage_executor import (
        ArbitrageExecutor,
        ExecutionRequest,
        ExecutionResult,
        ReduceOnlyRestrictionError,
    )


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class OrderStrategyExecutor:
    """封装下单模式选择与执行逻辑，降低 ArbitrageExecutor 体积。"""

    def __init__(
        self,
        executor: "ArbitrageExecutor",
        execution_result_cls: Type["ExecutionResult"],
        reduce_only_exception_cls: Type["ReduceOnlyRestrictionError"],
    ) -> None:
        self.executor = executor
        self.ExecutionResult = execution_result_cls
        self.ReduceOnlyRestrictionError = reduce_only_exception_cls

    def _mark_symbol_waiting(self, request: Optional["ExecutionRequest"], reason: str) -> None:
        """通知执行器将符号标记为等待状态。"""
        if not request:
            return
        self.executor._mark_symbol_waiting(request, reason)

    # ⚠️ 旧“需人工介入”机制已废弃：统一改为“连续失败暂停（全局）”风控。
    # 这里不再对符号做人工等待标记，避免掩盖真实对账/校验差异。

    def _apply_second_leg_overrides(
        self,
        request: "ExecutionRequest",
        plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        针对限价+市价模式的第二腿做额外配置覆写，
        例如 lighter 设置 force_limit_when_second_leg 时改用激进限价。
        """
        if plan.get("mode") != "limit_market":
            return plan
        if plan.get("second_order_mode") != "market":
            return plan

        second_exchange = plan.get("second_exchange")
        if not second_exchange:
            return plan

        second_symbol = (
            (request.buy_symbol or request.symbol)
            if plan.get("second_is_buy")
            else (request.sell_symbol or request.symbol)
        )
        exchange_config = self.executor._get_exchange_mode_config(
            second_exchange,
            second_symbol,
        )
        if exchange_config and getattr(exchange_config, "force_limit_when_second_leg", False):
            updated_plan = dict(plan)
            updated_plan["second_order_mode"] = "limit"
            logger.info(
                f"[执行计划] {second_exchange} 启用 force_limit_when_second_leg，"
                "第二腿改为激进限价顺序执行。"
            )
            return updated_plan
        return plan

    async def _wait_fill_after_cancel_failure(
        self,
        order: Optional[OrderData],
        adapter: ExchangeInterface,
        symbol: str,
        target_quantity: Decimal,
        exchange_tag: str,
        *,
        wait_seconds: float = 2.0,
        interval_seconds: float = 0.2,
    ) -> Optional[Decimal]:
        """
        撤单失败后额外等待 WebSocket/REST 更新，以捕捉“刚成交但尚未推送”的情况。
        返回检测到的最新成交量（若无更新则返回 None）。
        """
        if not order or not getattr(order, "id", None):
            return None

        epsilon = Decimal("0.00000001")
        interval = max(0.05, interval_seconds)
        remaining = max(0.0, wait_seconds)

        refreshed_total = await self._resolve_filled_quantity(
            order=order,
            adapter=adapter,
            symbol=symbol,
            reported_filled=None,
        )
        best_fill = self.executor._to_decimal_value(
            refreshed_total or Decimal("0"))
        if best_fill + epsilon >= target_quantity:
            return best_fill

        loop = asyncio.get_running_loop()
        deadline = loop.time() + remaining
        while loop.time() < deadline:
            await asyncio.sleep(interval)
            refreshed_total = await self._resolve_filled_quantity(
                order=order,
                adapter=adapter,
                symbol=symbol,
                reported_filled=None,
            )
            current_fill = self.executor._to_decimal_value(
                refreshed_total or Decimal("0"))
            if current_fill > best_fill + epsilon:
                logger.debug(
                    f"⏳ [套利执行] [{exchange_tag}] 撤单失败后检测到新增成交 {current_fill}"
                )
            best_fill = max(best_fill, current_fill)
            if best_fill + epsilon >= target_quantity:
                return best_fill

        return best_fill if best_fill > Decimal("0") else None

    async def _retry_limit_submission(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        price: Decimal,
        quantity: Decimal,
        is_buy: bool,
        absolute_offset: Optional[Decimal],
        request: "ExecutionRequest",
        exchange_name: str,
    ) -> Optional[OrderData]:
        logger.warning(
            "🔁 [套利执行] %s 限价订单提交失败，启动失败腿重试（方向=%s，数量=%s）",
            exchange_name,
            "买入" if is_buy else "卖出",
            quantity,
        )
        refreshed_price = self.executor._get_live_market_price(
            exchange_name,
            symbol,
            is_buy,
        )
        effective_price = refreshed_price or price
        if refreshed_price and refreshed_price != price:
            logger.debug(
                f"[套利执行] [{self.executor._format_exchange_tag(exchange_name)}] "
                f"限价重试使用最新盘口价: {effective_price} (原价={price})"
            )
        return await self.executor._place_limit_order(
            adapter,
            symbol,
            effective_price,
            quantity,
            is_buy=is_buy,
            absolute_offset=absolute_offset,
            request=request,
        )

    async def _handle_first_leg_min_qty_failure(
        self,
        *,
        request: "ExecutionRequest",
        execution_plan: Dict[str, Any],
        first_exchange: str,
        first_adapter: ExchangeInterface,
        first_symbol: str,
        accumulated_fill: Decimal,
        submit_quantity: Decimal,
        min_first_leg_qty: Decimal,
    ) -> "ExecutionResult":
        """
        处理第一腿限价单因剩余数量低于最小下单量而无法提交的场景。
        """
        executor = self.executor
        Result = self.ExecutionResult
        epsilon = Decimal("0.00000001")

        qty_text = f"{submit_quantity:.8f}"
        min_text = f"{min_first_leg_qty:.8f}"

        logger.error(
            f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
            f"剩余数量 {qty_text} 低于最小下单量 {min_text}，限价单提交失败，触发紧急平仓流程。"
        )

        close_success = True
        if accumulated_fill > epsilon:
            try:
                close_success = await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=accumulated_fill,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
            except Exception as exc:
                logger.error(
                    f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                    f"紧急平仓异常: {exc}",
                    exc_info=True,
                )
                close_success = False

        if close_success:
            logger.warning(
                f"⚠️ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                "已撤回已成交部分，等待重新调整最小下单量后再尝试。"
            )
        else:
            logger.error(
                f"🚨 [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                "紧急平仓失败，需要人工处理剩余仓位。"
            )
            # 不再进入“需人工介入等待”，交由上层连续失败暂停统一处理

        return self._result_failure(
            request,
            f"{first_exchange} 限价单剩余数量低于最小下单量，已停止本次执行",
            failure_code="limit_min_qty_violation",
        )

    async def _retry_market_submission(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        request: "ExecutionRequest",
        exchange_name: str,
        slippage_override: Optional[Decimal] = None,
    ) -> Optional[OrderData]:
        logger.warning(
            "🔁 [套利执行] %s 市价订单提交失败，启动失败腿重试（方向=%s，数量=%s）",
            exchange_name,
            "买入" if is_buy else "卖出",
            quantity,
        )
        return await self.executor._place_market_order(
            adapter,
            symbol,
            quantity,
            is_buy=is_buy,
            reduce_only=reduce_only,
            request=request,
            slippage_override=slippage_override,
        )

    async def _place_aggressive_limit_retry_order(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool = False,
        request: Optional["ExecutionRequest"],
        exchange_name: str,
        slippage_pct: Optional[Decimal],
        base_slippage: Optional[Decimal],
    ) -> Optional[OrderData]:
        """市价多次失败后，改用最新盘口价 + 加倍偏移的激进限价单吃单。"""
        if not request:
            return None

        executor = self.executor

        base_price_raw = request.price_buy if is_buy else request.price_sell
        fallback_price_raw = request.price_sell if is_buy else request.price_buy

        live_price = executor._get_live_market_price(
            exchange_name, symbol, is_buy)
        if live_price is not None:
            price_decimal = live_price
        else:
            price_source = base_price_raw if base_price_raw not in (
                None, 0) else fallback_price_raw
            price_decimal = executor._to_decimal_value(
                price_source or Decimal("0"))

        if price_decimal <= Decimal("0"):
            return None

        multiplier = Decimal("2")

        absolute_offset: Optional[Decimal] = None
        raw_absolute_offset = None
        if request:
            raw_absolute_offset = (
                request.limit_price_offset_buy if is_buy else request.limit_price_offset_sell
            )
        if raw_absolute_offset is not None:
            absolute_offset = executor._to_decimal_value(raw_absolute_offset)
            if absolute_offset <= Decimal("0"):
                absolute_offset = None
        if absolute_offset is not None:
            absolute_offset *= multiplier

        offset_pct_raw = slippage_pct or base_slippage or Decimal("0.001")
        if offset_pct_raw <= Decimal("0"):
            offset_pct_raw = Decimal("0.001")
        offset_pct = executor._to_decimal_value(offset_pct_raw) * multiplier

        # 🔥 Paradex(frontend/auto+frontend模板) 不支持限价单。
        # 这里将“激进限价兜底”替换为“放宽保护价的市价补单”（通过 slippage_override 覆盖 market_slippage）。
        try:
            if exchange_name and exchange_name.lower() == "paradex":
                mode = str(getattr(adapter, "_trade_execution_mode", "") or "").strip().lower()
                has_frontend_tpl = False
                try:
                    if hasattr(adapter, "_has_trade_template"):
                        has_frontend_tpl = bool(getattr(adapter, "_has_trade_template")(
                            "order_create_market_template"
                        ))
                except Exception:
                    has_frontend_tpl = False
                is_frontend_market_only = (mode == "frontend") or (mode == "auto" and has_frontend_tpl)
                if is_frontend_market_only:
                    # 放宽系数：×2（保守兜底），避免“保护价太紧 -> 0成交/取消”导致单腿风险
                    paradex_mult = Decimal("2")
                    paradex_slippage_override = executor._to_decimal_value(offset_pct_raw) * paradex_mult
                    exchange_tag = executor._format_exchange_tag(exchange_name)
                    leg_label = "买腿" if is_buy else "卖腿"
                    logger.warning(
                        "🔁 [套利执行] [%s] Paradex 前端模式不支持限价兜底，改用放宽滑点的市价补单[%s]: "
                        "slippage_override=%.4f%% (原计划走限价兜底)",
                        exchange_tag,
                        leg_label,
                        float(paradex_slippage_override * Decimal("100")),
                    )
                    return await executor._place_market_order(
                        adapter=adapter,
                        symbol=symbol,
                        quantity=quantity,
                        is_buy=is_buy,
                        reduce_only=reduce_only,
                        request=request,
                        slippage_override=paradex_slippage_override,
                    )
        except Exception:
            pass

        # 🔥 Variational(frontend模板) 常见只启用市价模板：未启用限价模板时不支持“激进限价兜底”。
        # 这里同样改为“放宽 max_slippage 的市价补单”，避免直接 NotImplementedError。
        try:
            if exchange_name and exchange_name.lower() == "variational":
                has_limit_tpl = False
                try:
                    if hasattr(adapter, "_has_trade_template"):
                        has_limit_tpl = bool(getattr(adapter, "_has_trade_template")(
                            "order_create_limit_template"
                        ))
                except Exception:
                    has_limit_tpl = False
                if not has_limit_tpl:
                    exchange_tag = executor._format_exchange_tag(exchange_name)
                    leg_label = "买腿" if is_buy else "卖腿"
                    logger.warning(
                        "🔁 [套利执行] [%s] Variational 未启用限价模板，跳过限价兜底，改用放宽滑点的市价补单[%s]: "
                        "slippage_override=%.4f%% (原计划走限价兜底)",
                        exchange_tag,
                        leg_label,
                        float(offset_pct * Decimal("100")),
                    )
                    return await executor._place_market_order(
                        adapter=adapter,
                        symbol=symbol,
                        quantity=quantity,
                        is_buy=is_buy,
                        reduce_only=reduce_only,
                        request=request,
                        slippage_override=offset_pct,
                    )
        except Exception:
            pass

        if is_buy:
            if absolute_offset:
                target_price = price_decimal + absolute_offset
            else:
                target_price = price_decimal * (Decimal("1") + offset_pct)
        else:
            if absolute_offset:
                target_price = price_decimal - absolute_offset
            else:
                target_price = price_decimal * (Decimal("1") - offset_pct)
            if target_price <= Decimal("0"):
                target_price = price_decimal * Decimal("0.999")

        step = executor._infer_price_step_from_orderbook(
            exchange=exchange_name,
            symbol=symbol,
            fallback_price=price_decimal,
        )
        if step > Decimal("0"):
            rounding_mode = ROUND_UP if is_buy else ROUND_DOWN
            target_price = target_price.quantize(step, rounding=rounding_mode)

        exchange_tag = executor._format_exchange_tag(exchange_name)
        leg_label = "买腿" if is_buy else "卖腿"
        offset_hint = (
            f"{absolute_offset} (abs)"
            if absolute_offset
            else f"{float((offset_pct * Decimal('100'))):.4f}%"
        )
        logger.info(
            f"[套利执行] [{exchange_tag}] 激进限价补仓[{leg_label}]: 基准价={price_decimal}, 目标价={target_price}, 偏移={offset_hint}"
        )

        return await executor._place_limit_order(
            adapter=adapter,
            symbol=symbol,
            price=price_decimal,
            quantity=quantity,
            is_buy=is_buy,
            absolute_offset=None,
            request=request,
            override_price=target_price,
        )

    async def _confirm_market_fill_with_retries(
        self,
        *,
        initial_order: Optional[OrderData],
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        request: Optional["ExecutionRequest"],
        exchange_name: str,
        allow_resubmit: bool = True,
        max_attempts: Optional[int] = None,
        enable_internal_aggressive_retry: bool = True,
    ) -> Tuple[Optional[OrderData], Decimal]:
        """
        等待市价订单成交，必要时重新提交，避免长期无成交造成单腿风险。
        enable_internal_aggressive_retry=False 时禁用内部激进限价兜底（供双限价补仓自定义流程使用）。
        """
        executor = self.executor
        leg_label = "买腿" if is_buy else "卖腿"
        epsilon = Decimal("0.00000001")
        limit_timeout_cfg = getattr(
            executor.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60
        try:
            limit_timeout = max(1, int(limit_timeout_cfg))
        except (ValueError, TypeError):
            limit_timeout = 60
        lighter_market_timeout_cfg = getattr(
            executor.config.order_execution,
            "lighter_market_order_timeout",
            None,
        )
        lighter_market_timeout: Optional[int] = None
        if lighter_market_timeout_cfg not in (None, 0):
            try:
                lighter_market_timeout = max(
                    1, int(lighter_market_timeout_cfg))
            except (ValueError, TypeError):
                lighter_market_timeout = None
        configured_attempt_limit = max_attempts or getattr(
            executor.config.order_execution,
            "max_retry_count",
            3,
        )
        # 市价单允许多次补齐（用于处理“部分成交/剩余仍OPEN”的场景，避免残留挂单导致仓位漂移）
        try:
            market_attempt_limit = max(1, int(configured_attempt_limit))
        except (ValueError, TypeError):
            market_attempt_limit = 3

        # 统计“市价单提交次数”（包含 initial_order 对应的一次提交）
        market_submits_used = 1 if initial_order is not None else 0
        current_order = initial_order
        current_is_market = True
        last_order = initial_order
        requested_total = executor._to_decimal_value(quantity)
        remaining_quantity = requested_total
        total_filled = Decimal("0")
        base_slippage = executor._get_slippage_percent(request)
        lighter_retry_mode = bool(
            exchange_name and exchange_name.lower() == "lighter")
        aggressive_limit_attempted = False

        while True:
            if current_order is None:
                if not allow_resubmit and not aggressive_limit_attempted:
                    break
                slippage_override: Optional[Decimal] = None
                if market_submits_used < market_attempt_limit:
                    # 逐次放大滑点：首次补齐=1x，之后=2x/4x/...
                    if base_slippage is not None:
                        initial_offset = 1 if initial_order is not None else 0
                        resubmit_idx = max(0, market_submits_used - initial_offset)
                        multiplier = Decimal(2) ** Decimal(resubmit_idx)
                        slippage_override = base_slippage * multiplier
                    slip_hint = (
                        f"，滑点={float(slippage_override * Decimal('100')):.4f}%"
                        if slippage_override is not None
                        else ""
                    )
                    logger.warning(
                        "⏱️ [套利执行] %s %s 市价补单/补齐尝试(%s/%s)%s，目标剩余=%s",
                        exchange_name,
                        leg_label,
                        market_submits_used + 1,
                        market_attempt_limit,
                        slip_hint,
                        remaining_quantity,
                    )
                    try:
                        current_order = await executor._place_market_order(
                            adapter,
                            symbol,
                            remaining_quantity,
                            is_buy=is_buy,
                            reduce_only=reduce_only,
                            request=request,
                            slippage_override=slippage_override,
                        )
                        current_is_market = True
                        market_submits_used += 1
                    except self.ReduceOnlyRestrictionError:
                        raise

                    if not current_order:
                        current_order = await self._retry_market_submission(
                            adapter=adapter,
                            symbol=symbol,
                            quantity=remaining_quantity,
                            is_buy=is_buy,
                            reduce_only=reduce_only,
                            request=request,
                            exchange_name=exchange_name,
                            slippage_override=slippage_override,
                        )
                        current_is_market = True
                        if not current_order:
                            continue
                elif enable_internal_aggressive_retry and not aggressive_limit_attempted and total_filled <= epsilon:
                    logger.warning(
                        "🔁 [套利执行] %s %s 市价补单多次失败，尝试激进限价吃单",
                        exchange_name,
                        leg_label,
                    )
                    current_order = await self._place_aggressive_limit_retry_order(
                        adapter=adapter,
                        symbol=symbol,
                        quantity=remaining_quantity,
                        is_buy=is_buy,
                        reduce_only=reduce_only,
                        request=request,
                        exchange_name=exchange_name,
                        slippage_pct=base_slippage,
                        base_slippage=base_slippage,
                    )
                    aggressive_limit_attempted = True
                    # 兜底可能返回“市价单”（Paradex/Variational 前端模式），也可能返回“限价单”（其他交易所）
                    try:
                        current_is_market = bool(
                            current_order
                            and getattr(current_order, "type", None) == OrderType.MARKET
                        )
                    except Exception:
                        current_is_market = False
                    if not current_order:
                        break
                else:
                    break

                last_order = current_order

            wait_timeout = (
                lighter_market_timeout
                if lighter_retry_mode and current_is_market and lighter_market_timeout
                else limit_timeout
            )
            filled = await executor.order_monitor.wait_for_order_fill(
                current_order,
                adapter,
                timeout=wait_timeout,
                is_market_order=current_is_market,
            )
            filled_decimal = executor._to_decimal_value(filled or Decimal("0"))
            if filled_decimal > epsilon:
                price_info = getattr(current_order, "average", None) or getattr(
                    current_order, "price", None)
                price_text = (
                    f"{executor._to_decimal_value(price_info):f}"
                    if price_info is not None
                    else "--"
                )
                # 🔄 同步订单对象的最新成交信息，便于后续统计
                current_order.filled = filled_decimal
                normalized_amount = executor._to_decimal_value(
                    getattr(current_order, "amount", None) or filled_decimal
                )
                current_order.amount = normalized_amount
                current_order.remaining = max(
                    normalized_amount - filled_decimal, Decimal("0"))
                price_decimal = executor._to_decimal_value(
                    price_info) if price_info is not None else None
                if price_decimal is not None:
                    current_order.average = price_decimal
                    current_order.price = price_decimal
                    current_cost = executor._to_decimal_value(
                        getattr(current_order, "cost", None))
                    if current_cost <= Decimal("0"):
                        current_order.cost = price_decimal * filled_decimal

                # 累加成交并判断是否已补齐目标
                total_filled += filled_decimal
                remaining_quantity = max(Decimal("0"), requested_total - total_filled)

                if remaining_quantity <= epsilon:
                    logger.info(
                        "✅ [套利执行] %s %s 市价补单已补齐: 总成交=%s/%s, 最后价格=%s",
                        exchange_name,
                        leg_label,
                        total_filled,
                        requested_total,
                        price_text,
                    )
                    return current_order, total_filled

                # 部分成交：继续用市价补齐剩余数量
                logger.warning(
                    "⚠️ [套利执行] %s %s 市价单部分成交: 本次=%s, 累计=%s/%s, 剩余=%s，将继续市价补齐",
                    exchange_name,
                    leg_label,
                    filled_decimal,
                    total_filled,
                    requested_total,
                    remaining_quantity,
                )

                # 若订单仍处于 OPEN 且有 remaining，先尽力撤单，避免后续继续成交导致超额
                try:
                    status = getattr(current_order, "status", None)
                    status_str = str(getattr(status, "value", status) or "").lower()
                    rem = executor._to_decimal_value(getattr(current_order, "remaining", None))
                    if rem > epsilon and status_str not in ("filled", "canceled", "cancelled", "closed"):
                        await executor._cancel_order(adapter, str(current_order.id), symbol)
                except Exception:
                    pass

                current_order = None
                current_is_market = True
                continue

            if executor._is_slippage_cancel(current_order):
                logger.error(
                    "❌ [套利执行] %s %s 市价补单因滑点保护被取消: order_id=%s",
                    exchange_name,
                    leg_label,
                    current_order.id if current_order else "-",
                )
                if aggressive_limit_attempted:
                    break
                current_order = None
                current_is_market = True
                continue

            logger.warning(
                "%s [套利执行] %s %s 补单未确认成交: order_id=%s",
                "⚠️" if current_is_market else "❌",
                exchange_name,
                leg_label,
                current_order.id if current_order else "-",
            )
            if aggressive_limit_attempted:
                break
            current_order = None
            current_is_market = True

        # 失败退出：返回累计成交量（可能为0），由上层决定是否进入失败处理/紧急平仓
        return last_order, total_filled

    def _result_failure(
        self,
        request: Optional["ExecutionRequest"],
        message: str,
        *,
        mark_waiting: bool = True,
        **kwargs: Any,
    ) -> "ExecutionResult":
        if mark_waiting:
            self._mark_symbol_waiting(request, message)
        return self.ExecutionResult(
            success=False,
            error_message=message,
            **kwargs,
        )

    def determine_execution_plan(self, request: "ExecutionRequest") -> Dict[str, Any]:
        executor = self.executor
        dual_limit_enabled = getattr(
            executor.config.order_execution,
            "enable_dual_limit_mode",
            False,
        )
        forced_mode = str(getattr(request, "force_order_mode", "") or "").strip().lower()
        if forced_mode == "market_market":
            return {
                "mode": "market_market",
                "first_exchange": None,
                "first_is_buy": None,
                "first_order_mode": None,
                "second_exchange": None,
                "second_is_buy": None,
                "second_order_mode": None,
                "parallel": True,
            }

        buy_exchange_config = executor._get_exchange_mode_config(
            request.exchange_buy,
            request.buy_symbol or request.symbol,
        )
        buy_mode = buy_exchange_config.order_mode if buy_exchange_config else "market"
        buy_priority = buy_exchange_config.priority if buy_exchange_config else 0

        sell_exchange_config = executor._get_exchange_mode_config(
            request.exchange_sell,
            request.sell_symbol or request.symbol,
        )
        sell_mode = sell_exchange_config.order_mode if sell_exchange_config else "market"
        sell_priority = sell_exchange_config.priority if sell_exchange_config else 0

        logger.debug(
            f"[执行计划] {request.exchange_buy}(买入): mode={buy_mode}, priority={buy_priority} | "
            f"{request.exchange_sell}(卖出): mode={sell_mode}, priority={sell_priority}"
        )

        if buy_mode == "market" and sell_mode == "market":
            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "market_market",
                    "first_exchange": None,
                    "first_is_buy": None,
                    "first_order_mode": None,
                    "second_exchange": None,
                    "second_is_buy": None,
                    "second_order_mode": None,
                    "parallel": True,
                },
            )

        if buy_mode == "limit" and sell_mode == "limit":
            # 🔥 检查是否有任一方启用了 force_sequential_limit
            buy_force_sequential = getattr(
                buy_exchange_config, "force_sequential_limit", False) if buy_exchange_config else False
            sell_force_sequential = getattr(
                sell_exchange_config, "force_sequential_limit", False) if sell_exchange_config else False

            logger.debug(
                f"[执行计划] 双限价检查: {request.exchange_buy}(buy_force={buy_force_sequential}) | "
                f"{request.exchange_sell}(sell_force={sell_force_sequential})"
            )

            # 如果任一方启用了强制顺序执行，采用 limit+market 模式
            if buy_force_sequential or sell_force_sequential:
                # 优先执行没有启用 force_sequential_limit 的一方（让它先挂限价单）
                # 启用 force_sequential_limit 的一方后执行（用激进限价单吃单）
                if buy_force_sequential and not sell_force_sequential:
                    # buy方强制顺序，先执行sell（限价），再执行buy（激进限价吃单）
                    first_exchange = request.exchange_sell
                    first_is_buy = False
                    second_exchange = request.exchange_buy
                    second_is_buy = True
                    logger.info(
                        f"[执行计划] {request.exchange_buy} 启用force_sequential_limit，"
                        f"采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )
                elif sell_force_sequential and not buy_force_sequential:
                    # sell方强制顺序，先执行buy（限价），再执行sell（激进限价吃单）
                    first_exchange = request.exchange_buy
                    first_is_buy = True
                    second_exchange = request.exchange_sell
                    second_is_buy = False
                    logger.info(
                        f"[执行计划] {request.exchange_sell} 启用force_sequential_limit，"
                        f"采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )
                else:
                    # 双方都启用了强制顺序，根据priority决定
                    if buy_priority >= sell_priority:
                        first_exchange = request.exchange_buy
                        first_is_buy = True
                        second_exchange = request.exchange_sell
                        second_is_buy = False
                    else:
                        first_exchange = request.exchange_sell
                        first_is_buy = False
                        second_exchange = request.exchange_buy
                        second_is_buy = True
                    logger.info(
                        f"[执行计划] 双方均启用force_sequential_limit，"
                        f"根据priority采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )

                return self._apply_second_leg_overrides(
                    request,
                    {
                        "mode": "limit_market",
                        "first_exchange": first_exchange,
                        "first_is_buy": first_is_buy,
                        "first_order_mode": "limit",
                        "second_exchange": second_exchange,
                        "second_is_buy": second_is_buy,
                        "second_order_mode": "limit",  # 🔥 第二腿也是limit，但会用激进价格偏移
                        "parallel": False,
                    },
                )

            # 没有启用强制顺序，走原有的双限价或limit+market逻辑
            if dual_limit_enabled:
                return {
                    "mode": "limit_limit",
                    "first_exchange": None,
                    "first_is_buy": None,
                    "first_order_mode": "limit",
                    "second_exchange": None,
                    "second_is_buy": None,
                    "second_order_mode": "limit",
                    "parallel": True,
                }
            if buy_priority >= sell_priority:
                first_exchange = request.exchange_buy
                first_is_buy = True
                second_exchange = request.exchange_sell
                second_is_buy = False
            else:
                first_exchange = request.exchange_sell
                first_is_buy = False
                second_exchange = request.exchange_buy
                second_is_buy = True

            logger.warning(
                f"[执行计划] {request.exchange_buy}/{request.exchange_sell} 均为limit，"
                f"根据priority调整为先限价({first_exchange})后市价({second_exchange})"
            )

            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "limit_market",
                    "first_exchange": first_exchange,
                    "first_is_buy": first_is_buy,
                    "first_order_mode": "limit",
                    "second_exchange": second_exchange,
                    "second_is_buy": second_is_buy,
                    "second_order_mode": "market",
                    "parallel": False,
                },
            )

        if buy_mode == "limit":
            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "limit_market",
                    "first_exchange": request.exchange_buy,
                    "first_is_buy": True,
                    "first_order_mode": "limit",
                    "second_exchange": request.exchange_sell,
                    "second_is_buy": False,
                    "second_order_mode": "market",
                    "parallel": False,
                },
            )

        return self._apply_second_leg_overrides(
            request,
            {
                "mode": "limit_market",
                "first_exchange": request.exchange_sell,
                "first_is_buy": False,
                "first_order_mode": "limit",
                "second_exchange": request.exchange_buy,
                "second_is_buy": True,
                "second_order_mode": "market",
                "parallel": False,
            },
        )

    async def execute_limit_market_mode(
        self,
        request: "ExecutionRequest",
        execution_plan: Dict[str, Any],
    ) -> "ExecutionResult":
        from .order_modes.limit_market import (
            execute_limit_market_mode as _execute_limit_market_mode,
        )

        return await _execute_limit_market_mode(
            self,
            logger=logger,
            request=request,
            execution_plan=execution_plan,
        )

    async def execute_limit_limit_mode(
        self,
        request: "ExecutionRequest",
    ) -> "ExecutionResult":
        from .order_modes.limit_limit import (
            execute_limit_limit_mode as _execute_limit_limit_mode,
        )

        return await _execute_limit_limit_mode(
            self,
            logger=logger,
            request=request,
        )

    async def _fail_limit_market_second_leg(
        self,
        *,
        request: "ExecutionRequest",
        limit_order: OrderData,
        market_order: Optional[OrderData],
        execution_plan: Dict[str, Any],
        first_adapter: ExchangeInterface,
        first_exchange: str,
        first_symbol: str,
        filled_quantity: Optional[Decimal],
        failed_exchange: str,
        error_message: str,
    ) -> "ExecutionResult":
        executor = self.executor
        epsilon = Decimal("0.00000001")

        first_leg_qty = executor._to_decimal_value(
            filled_quantity or Decimal("0"))
        second_leg_qty = Decimal("0")
        if market_order is not None:
            try:
                second_leg_qty = executor._to_decimal_value(
                    getattr(market_order, "filled", Decimal("0"))
                )
            except (InvalidOperation, TypeError, ValueError):
                second_leg_qty = Decimal("0")

        close_first_success = True
        if first_leg_qty > epsilon:
            try:
                close_first_success = await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=first_leg_qty,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                    f"紧急平仓异常: {exc}",
                    exc_info=True,
                )
                close_first_success = False

        close_second_success = True
        if second_leg_qty > epsilon:
            second_exchange = execution_plan.get("second_exchange")
            second_symbol = (
                (request.buy_symbol or request.symbol)
                if execution_plan.get("second_is_buy")
                else (request.sell_symbol or request.symbol)
            )
            second_adapter = (
                executor.exchange_adapters.get(second_exchange)
                if second_exchange else None
            )

            if second_adapter:
                try:
                    close_second_success = await executor._emergency_close_position(
                        exchange=second_exchange,
                        adapter=second_adapter,
                        symbol=second_symbol,
                        quantity=second_leg_qty,
                        is_buy_to_close=not execution_plan.get(
                            "second_is_buy", False),
                        request=request,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    logger.error(
                        f"❌ [套利执行] [{executor._format_exchange_tag(second_exchange)}] "
                        f"第二腿紧急平仓异常: {exc}",
                        exc_info=True,
                    )
                    close_second_success = False
            else:
                close_second_success = False
                logger.error(
                    f"❌ [套利执行] 第二腿适配器不存在，无法回滚 {second_exchange}"
                )

        if not close_first_success or not close_second_success:
            reason_hint = []
            if not close_first_success:
                reason_hint.append(f"{first_exchange}紧急平仓失败")
            if not close_second_success and second_leg_qty > epsilon:
                reason_hint.append(
                    f"{execution_plan.get('second_exchange')}第二腿回滚失败")
            # 不再进入“需人工介入等待”，交由上层连续失败暂停统一处理（本次仍会返回 partial_failure）

        result_order_buy = limit_order if execution_plan["first_is_buy"] else market_order
        result_order_sell = limit_order if not execution_plan["first_is_buy"] else market_order

        return self._result_failure(
            request,
            error_message,
            order_buy=result_order_buy,
            order_sell=result_order_sell,
            partial_failure=True,
            failed_exchange=failed_exchange,
            success_exchange=first_exchange,
            success_quantity=first_leg_qty,
        )

    async def _handle_market_market_submission_failure(
        self,
        *,
        request: "ExecutionRequest",
        buy_order: Optional[OrderData],
        sell_order: Optional[OrderData],
        buy_adapter: ExchangeInterface,
        sell_adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
    ) -> "ExecutionResult":
        executor = self.executor
        if not buy_order and not sell_order:
            return self._result_failure(
                request,
                "两个交易所订单都提交失败",
            )

        buy_failed = buy_order is None
        reduce_only_flag = not request.is_open
        success_order = sell_order if buy_failed else buy_order
        success_adapter = sell_adapter if buy_failed else buy_adapter
        success_symbol = sell_symbol if buy_failed else buy_symbol
        success_exchange = request.exchange_sell if buy_failed else request.exchange_buy
        failed_exchange = request.exchange_buy if buy_failed else request.exchange_sell
        failed_adapter = buy_adapter if buy_failed else sell_adapter
        failed_symbol = buy_symbol if buy_failed else sell_symbol
        failed_is_buy = bool(buy_failed)

        # ===== 新增策略：先重试失败腿一次（市价），仍失败才平掉成功腿 =====
        # - 重试仍使用市价单
        # - 若有滑点配置，则对失败腿重试时放大 10 倍（部分交易所不支持则会被忽略）
        base_slippage = executor._get_slippage_percent(request)
        retry_slippage = (base_slippage * Decimal("50")
                          ) if base_slippage is not None else None
        retry_order: Optional[OrderData] = None
        try:
            retry_order = await self._retry_market_submission(
                adapter=failed_adapter,
                symbol=failed_symbol,
                quantity=request.quantity,
                is_buy=failed_is_buy,
                reduce_only=reduce_only_flag,
                request=request,
                exchange_name=failed_exchange,
                slippage_override=retry_slippage,
            )
        except self.ReduceOnlyRestrictionError:
            executor.reduce_only_handler.register_reduce_only_event(
                request,
                failed_exchange,
                failed_symbol,
                closing_issue=reduce_only_flag,
                reason="失败腿重试触发 reduce-only",
            )
            retry_order = None

        # 如果重试成功：此时两腿都有订单 -> 走正常的“确认成交”流程
        if retry_order:
            if buy_failed:
                buy_order = retry_order
            else:
                sell_order = retry_order

            try:
                buy_order, buy_filled_dec = await self._confirm_market_fill_with_retries(
                    initial_order=buy_order,
                    adapter=buy_adapter,
                    symbol=buy_symbol,
                    quantity=request.quantity,
                    is_buy=True,
                    reduce_only=reduce_only_flag,
                    request=request,
                    exchange_name=request.exchange_buy,
                )
            except self.ReduceOnlyRestrictionError:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    request.exchange_buy,
                    buy_symbol,
                    closing_issue=reduce_only_flag,
                    reason="买单触发 reduce-only",
                )
                return self._result_failure(
                    request,
                    f"{request.exchange_buy} 市价订单因 reduce-only 受限",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            try:
                sell_order, sell_filled_dec = await self._confirm_market_fill_with_retries(
                    initial_order=sell_order,
                    adapter=sell_adapter,
                    symbol=sell_symbol,
                    quantity=request.quantity,
                    is_buy=False,
                    reduce_only=reduce_only_flag,
                    request=request,
                    exchange_name=request.exchange_sell,
                )
            except self.ReduceOnlyRestrictionError:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    request.exchange_sell,
                    sell_symbol,
                    closing_issue=reduce_only_flag,
                    reason="卖单触发 reduce-only",
                )
                return self._result_failure(
                    request,
                    f"{request.exchange_sell} 市价订单因 reduce-only 受限",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            epsilon = Decimal("0.00000001")
            if buy_filled_dec > epsilon and sell_filled_dec > epsilon:
                buy_order.filled = buy_filled_dec
                sell_order.filled = sell_filled_dec
                slip = executor._log_execution_summary(
                    request=request,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    is_open=request.is_open,
                    is_last_split=request.is_last_split,
                )
                actual_quantity = self._calculate_success_quantity_from_orders(
                    buy_order,
                    sell_order,
                    fallback=request.quantity,
                )
                return self.ExecutionResult(
                    success=True,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    success_quantity=actual_quantity,
                    slippage_pct=(slip or {}).get("slippage_pct"),
                    high_slippage_threshold_pct=(slip or {}).get(
                        "high_slippage_threshold_pct"),
                )

            # 重试后仍出现“单腿成交/另一腿未成交” -> 平掉已成交腿，返回失败
            filled_exchange = request.exchange_sell if buy_filled_dec <= epsilon else request.exchange_buy
            unfilled_exchange = request.exchange_buy if buy_filled_dec <= epsilon else request.exchange_sell
            filled_quantity = sell_filled_dec if buy_filled_dec <= epsilon else buy_filled_dec
            filled_adapter = sell_adapter if buy_filled_dec <= epsilon else buy_adapter
            filled_symbol = sell_symbol if buy_filled_dec <= epsilon else buy_symbol
            is_buy_unfilled = (buy_filled_dec <= epsilon)
            await executor._emergency_close_position(
                exchange=filled_exchange,
                adapter=filled_adapter,
                symbol=filled_symbol,
                quantity=Decimal(str(filled_quantity)),
                is_buy_to_close=is_buy_unfilled,
                request=request,
            )
            return self._result_failure(
                request,
                f"{unfilled_exchange} 订单未成交，已平掉 {filled_exchange} 订单，等待人工介入",
                order_buy=buy_order,
                order_sell=sell_order,
                partial_failure=True,
                failed_exchange=unfilled_exchange,
                success_exchange=filled_exchange,
                success_quantity=Decimal(str(filled_quantity)),
            )

        # 重试仍失败：保持原策略——确认成功腿是否成交，若成交则平掉成功腿，最后返回失败
        filled_decimal = Decimal("0")
        if success_order:
            _, filled_decimal = await self._confirm_market_fill_with_retries(
                initial_order=success_order,
                adapter=success_adapter,
                symbol=success_symbol,
                quantity=request.quantity,
                is_buy=(not buy_failed),
                reduce_only=reduce_only_flag,
                request=request,
                exchange_name=success_exchange,
                allow_resubmit=False,
                max_attempts=1,
            )
            if filled_decimal > Decimal("0"):
                await executor._emergency_close_position(
                    exchange=success_exchange,
                    adapter=success_adapter,
                    symbol=success_symbol,
                    quantity=filled_decimal,
                    is_buy_to_close=buy_failed,
                    request=request,
                )

        return self._result_failure(
            request,
            f"{failed_exchange} 市价订单提交失败",
            order_buy=buy_order,
            order_sell=sell_order,
            partial_failure=bool(success_order),
            failed_exchange=failed_exchange,
            success_exchange=success_exchange if success_order else None,
            success_quantity=filled_decimal,
        )

    async def execute_market_market_mode(
        self,
        request: "ExecutionRequest",
    ) -> "ExecutionResult":
        from .order_modes.market_market import (
            execute_market_market_mode as _execute_market_market_mode,
        )

        return await _execute_market_market_mode(
            self,
            logger=logger,
            request=request,
        )

    def _should_use_lighter_ws_batch(
        self,
        request: "ExecutionRequest",
        buy_adapter: Optional[ExchangeInterface],
        sell_adapter: Optional[ExchangeInterface],
    ) -> bool:
        if not buy_adapter or not sell_adapter:
            return False
        buy_exchange = (request.exchange_buy or "").lower()
        sell_exchange = (request.exchange_sell or "").lower()
        if buy_exchange != sell_exchange:
            return False
        if buy_exchange != "lighter":
            return False
        if buy_adapter is not sell_adapter:
            return False
        return hasattr(buy_adapter, "place_market_orders_ws_batch")

    async def _execute_lighter_ws_batch(
        self,
        request: "ExecutionRequest",
        adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
    ) -> "ExecutionResult":
        from .lighter_batch_executor import execute_lighter_ws_batch
        return await execute_lighter_ws_batch(
            executor_obj=self,
            request=request,
            adapter=adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
        )

    async def _handle_reduce_only_submission_failure(
        self,
        *,
        request: "ExecutionRequest",
        failing_exchange: str,
        failing_symbol: str,
        failing_is_buy: bool,
        other_order: Optional[OrderData],
        other_adapter: ExchangeInterface,
        other_exchange: str,
        other_symbol: str,
        other_is_buy: bool,
        epsilon: Decimal,
        limit_timeout: float,
    ) -> "ExecutionResult":
        executor = self.executor
        logger.warning(
            "⚠️ [reduce-only] %s %s 限价下单被拒绝，仅允许平仓，开始处理对腿",
            failing_exchange,
            failing_symbol,
        )

        filled_dec = Decimal("0")
        reported = None
        if other_order:
            try:
                reported = await executor.order_monitor.wait_for_order_fill(
                    other_order,
                    other_adapter,
                    timeout=limit_timeout,
                )
            except Exception:
                reported = None
            filled_dec = await self._resolve_filled_quantity(
                order=other_order,
                adapter=other_adapter,
                symbol=other_symbol,
                reported_filled=reported,
            )

        if filled_dec > epsilon and executor.reduce_only_handler:
            await executor.reduce_only_handler.handle_reduce_only_after_partial(
                request=request,
                completed_exchange=other_exchange,
                completed_adapter=other_adapter,
                completed_symbol=other_symbol,
                completed_quantity=filled_dec,
                completed_was_buy=other_is_buy,
                failing_exchange=failing_exchange,
                failing_symbol=failing_symbol,
                closing_issue=(not request.is_open),
            )
        elif other_order:
            await executor._cancel_order(other_adapter, other_order.id, other_symbol)

        message = (
            f"{failing_exchange} reduce-only 限制，已处理{('买腿' if other_is_buy else '卖腿')}并暂停等待探针"
        )
        return self._result_failure(
            request,
            message,
            order_buy=other_order if other_is_buy else None,
            order_sell=other_order if not other_is_buy else None,
            partial_failure=True,
        )

    async def _handle_single_leg_shortfall(
        self,
        *,
        request: "ExecutionRequest",
        buy_order: OrderData,
        sell_order: OrderData,
        buy_adapter: ExchangeInterface,
        sell_adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
        missing_buy: bool,
        shortfall: Decimal,
        target_quantity: Decimal,
        epsilon: Decimal,
        cancel_opposite_order: bool = False,
        result_quantity: Optional[Decimal] = None,
        market_retry_max_attempts: Optional[int] = None,
    ) -> "ExecutionResult":
        executor = self.executor
        Result = self.ExecutionResult

        fallback_exchange = request.exchange_buy if missing_buy else request.exchange_sell
        fallback_adapter = buy_adapter if missing_buy else sell_adapter
        fallback_symbol = buy_symbol if missing_buy else sell_symbol
        fallback_is_buy = missing_buy
        failing_order = buy_order if missing_buy else sell_order
        opposite_adapter = sell_adapter if missing_buy else buy_adapter
        opposite_symbol = sell_symbol if missing_buy else buy_symbol
        opposite_order = sell_order if missing_buy else buy_order
        reported_quantity = result_quantity or (
            target_quantity if not cancel_opposite_order else shortfall)

        leg_label = "买腿" if fallback_is_buy else "卖腿"
        completed_leg_label = "卖腿" if missing_buy else "买腿"

        logger.warning(
            f"⚠️ [套利执行] 双限价模式{'仅' if cancel_opposite_order else ''}"
            f"{completed_leg_label} 完成，{leg_label} 缺口 {shortfall}，"
            f"将在 {fallback_exchange} 启动激进限价补仓"
        )

        await executor._cancel_order(fallback_adapter, failing_order.id, fallback_symbol)
        if cancel_opposite_order:
            await executor._cancel_order(opposite_adapter, opposite_order.id, opposite_symbol)

        shortfall_dec = executor._to_decimal_value(shortfall)
        if shortfall_dec <= epsilon:
            return self._result_failure(
                request,
                "补单数量异常（<=0），已终止执行",
                order_buy=buy_order,
                order_sell=sell_order,
            )

        min_qty_limit = executor._resolve_min_order_qty(
            request, fallback_exchange)
        aggressive_allowed = shortfall_dec > epsilon
        if min_qty_limit and shortfall_dec < min_qty_limit:
            aggressive_allowed = False
            logger.info(
                f"ℹ️ [套利执行] {fallback_exchange} {leg_label} 补单量 {shortfall_dec} < 最小下单量 {min_qty_limit}，跳过激进限价"
            )

        supplemental_filled = Decimal("0")
        remaining_to_fill = shortfall_dec
        final_fallback_order: Optional[OrderData] = None
        aggressive_order: Optional[OrderData] = None

        if aggressive_allowed:
            slippage_hint = executor._get_slippage_percent(request)
            logger.info(
                f"⚡ [套利执行] {fallback_exchange} {leg_label} 缺口 {shortfall_dec}，尝试激进限价补齐"
            )
            aggressive_order = await self._place_aggressive_limit_retry_order(
                adapter=fallback_adapter,
                symbol=fallback_symbol,
                quantity=remaining_to_fill,
                is_buy=fallback_is_buy,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=fallback_exchange,
                slippage_pct=slippage_hint,
                base_slippage=slippage_hint,
            )
            if aggressive_order:
                _, aggressive_filled = await self._confirm_market_fill_with_retries(
                    initial_order=aggressive_order,
                    adapter=fallback_adapter,
                    symbol=fallback_symbol,
                    quantity=remaining_to_fill,
                    is_buy=fallback_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=fallback_exchange,
                    allow_resubmit=False,
                    max_attempts=1,
                    enable_internal_aggressive_retry=False,
                )
                if aggressive_filled > epsilon:
                    supplemental_filled += aggressive_filled
                    remaining_to_fill = max(
                        Decimal("0"), shortfall_dec - supplemental_filled)
                    final_fallback_order = aggressive_order

        if remaining_to_fill > epsilon:
            if aggressive_allowed and aggressive_order and getattr(aggressive_order, "id", None):
                try:
                    await executor._cancel_order(fallback_adapter, aggressive_order.id, fallback_symbol)
                except Exception as cancel_exc:
                    logger.warning(
                        "⚠️ [套利执行] 取消 %s 激进限价补单失败 (order_id=%s): %s",
                        fallback_exchange,
                        aggressive_order.id,
                        cancel_exc,
                    )
                else:
                    logger.info(
                        "↩️ [套利执行] [%s] 激进限价补单未全成，已先取消原单（剩余 %.6f）",
                        fallback_exchange,
                        float(remaining_to_fill),
                    )
                try:
                    latest = await fallback_adapter.get_order(str(aggressive_order.id), fallback_symbol)
                except Exception as refresh_exc:
                    logger.warning(
                        "⚠️ [套利执行] 查询 %s 激进限价补单剩余失败: %s",
                        fallback_exchange,
                        refresh_exc,
                    )
                else:
                    refreshed_remaining = executor._to_decimal_value(
                        getattr(latest, "remaining", None)
                    )
                    if refreshed_remaining is not None:
                        # REST 在撤单后通常返回 0，但我们仍然保留本地剩余，避免误以为已补满
                        remaining_to_fill = max(
                            Decimal("0"),
                            refreshed_remaining,
                            remaining_to_fill,
                        )
            if remaining_to_fill <= epsilon:
                logger.info(
                    "✅ [套利执行] %s %s 激进限价补单取消后已确认无剩余，跳过市价补单",
                    fallback_exchange,
                    leg_label,
                )
                final_fallback_order = aggressive_order or final_fallback_order
            else:
                if aggressive_allowed:
                    logger.warning(
                        f"⚠️ [套利执行] {fallback_exchange} {leg_label} 激进限价补单未完全成交，剩余 {remaining_to_fill} 改用市价补齐"
                    )
                else:
                    logger.warning(
                        f"⚠️ [套利执行] {fallback_exchange} {leg_label} 补单量不足最小门槛或禁用激进模式，直接市价补齐 {remaining_to_fill}"
                    )

                try:
                    market_order = await executor._place_market_order(
                        fallback_adapter,
                        fallback_symbol,
                        remaining_to_fill,
                        is_buy=fallback_is_buy,
                        reduce_only=(not request.is_open),
                        request=request,
                    )
                except self.ReduceOnlyRestrictionError:
                    completed_exchange = request.exchange_sell if missing_buy else request.exchange_buy
                    completed_adapter = sell_adapter if missing_buy else buy_adapter
                    completed_symbol = sell_symbol if missing_buy else buy_symbol
                    await executor.reduce_only_handler.handle_reduce_only_after_partial(
                        request=request,
                        completed_exchange=completed_exchange,
                        completed_adapter=completed_adapter,
                        completed_symbol=completed_symbol,
                        completed_quantity=shortfall,
                        completed_was_buy=(not missing_buy),
                        failing_exchange=fallback_exchange,
                        failing_symbol=fallback_symbol,
                        closing_issue=(not request.is_open),
                    )
                    return self._result_failure(
                        request,
                        f"{fallback_exchange} reduce-only 限制，市价补齐被拒绝",
                        order_buy=buy_order,
                        order_sell=sell_order,
                        partial_failure=True,
                        failed_exchange=fallback_exchange,
                        success_exchange=completed_exchange,
                        success_quantity=reported_quantity,
                    )

                if not market_order:
                    success_exchange = request.exchange_sell if missing_buy else request.exchange_buy
                    success_adapter = sell_adapter if missing_buy else buy_adapter
                    success_symbol = sell_symbol if missing_buy else buy_symbol
                    await executor._emergency_close_position(
                        exchange=success_exchange,
                        adapter=success_adapter,
                        symbol=success_symbol,
                        quantity=shortfall,
                        is_buy_to_close=missing_buy,
                        request=request,
                    )
                    return self._result_failure(
                        request,
                        "市价补齐订单提交失败，已触发紧急平仓",
                        order_buy=buy_order,
                        order_sell=sell_order,
                        partial_failure=True,
                        failed_exchange=fallback_exchange,
                        success_exchange=success_exchange,
                        success_quantity=reported_quantity,
                    )

                market_order, market_filled = await self._confirm_market_fill_with_retries(
                    initial_order=market_order,
                    adapter=fallback_adapter,
                    symbol=fallback_symbol,
                    quantity=remaining_to_fill,
                    is_buy=fallback_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=fallback_exchange,
                    max_attempts=market_retry_max_attempts,
                    enable_internal_aggressive_retry=False,
                )
                if market_filled > epsilon:
                    supplemental_filled += market_filled
                    remaining_to_fill = max(
                        Decimal("0"), shortfall_dec - supplemental_filled)
                    final_fallback_order = market_order

        if supplemental_filled + epsilon < shortfall_dec:
            success_exchange = request.exchange_sell if missing_buy else request.exchange_buy
            success_adapter = sell_adapter if missing_buy else buy_adapter
            success_symbol = sell_symbol if missing_buy else buy_symbol
            await executor._emergency_close_position(
                exchange=success_exchange,
                adapter=success_adapter,
                symbol=success_symbol,
                quantity=shortfall_dec - supplemental_filled,
                is_buy_to_close=missing_buy,
                request=request,
            )
            return self._result_failure(
                request,
                "补单成交量不足目标，已触发紧急平仓",
                order_buy=buy_order,
                order_sell=sell_order,
                partial_failure=True,
                failed_exchange=fallback_exchange,
                success_exchange=success_exchange,
                success_quantity=reported_quantity,
            )

        # 🔥 获取未补齐腿的原始部分成交数量
        failing_order_partial_filled = executor._to_decimal_value(
            getattr(failing_order, "filled", Decimal("0"))
        )

        # 🔥 获取对面腿（已完成）的成交数量
        opposite_filled = executor._to_decimal_value(
            getattr(opposite_order, "filled", Decimal("0"))
        )

        # 🔥 该腿总成交 = 原始订单部分成交 + 补单累计成交
        total_failing_leg_filled = failing_order_partial_filled + supplemental_filled

        # 🔥 调试日志：记录成交明细
        logger.info(
            f"📊 [成交明细] {fallback_exchange} | "
            f"限价部分成交={failing_order_partial_filled} + 补单合计={supplemental_filled} = 总成交={total_failing_leg_filled} | "
            f"对面腿成交={opposite_filled} | "
            f"计算数量=min({total_failing_leg_filled}, {opposite_filled}) | "
            f"缺口目标={shortfall}"
        )

        if not final_fallback_order:
            final_fallback_order = failing_order

        final_buy_order = final_fallback_order if missing_buy else buy_order
        final_sell_order = sell_order if missing_buy else final_fallback_order

        # 🔥 更新最终订单对象的 filled 字段为总成交数
        if final_fallback_order:
            final_fallback_order.filled = total_failing_leg_filled

        slip = executor._log_execution_summary(
            request=request,
            order_buy=final_buy_order,
            order_sell=final_sell_order,
            is_open=request.is_open,
            is_last_split=request.is_last_split,
        )

        # 🔥 正确计算成交数量：两腿取最小值
        actual_quantity = min(opposite_filled, total_failing_leg_filled)

        return Result(
            success=True,
            order_buy=final_buy_order,
            order_sell=final_sell_order,
            success_quantity=actual_quantity,
            slippage_pct=(slip or {}).get("slippage_pct"),
            high_slippage_threshold_pct=(slip or {}).get(
                "high_slippage_threshold_pct"),
        )

    async def _fill_leg_to_target(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        exchange_name: str,
        is_buy: bool,
        remaining_quantity: Decimal,
        request: "ExecutionRequest",
        epsilon: Decimal,
        market_retry_max_attempts: Optional[int] = None,
    ) -> Decimal:
        """
        使用激进限价 + 市价顺序补齐单个腿至拆单目标数量。
        """
        executor = self.executor
        leg_label = "买腿" if is_buy else "卖腿"
        target_quantity = executor._to_decimal_value(remaining_quantity)
        if target_quantity <= epsilon:
            return Decimal("0")

        min_qty_limit = executor._resolve_min_order_qty(request, exchange_name)
        aggressive_allowed = target_quantity > epsilon
        if min_qty_limit and target_quantity < min_qty_limit:
            aggressive_allowed = False
            logger.info(
                f"ℹ️ [套利执行] {exchange_name} remaining={target_quantity} < 最小下单量 {min_qty_limit}，跳过激进限价补齐"
            )

        supplemental_filled = Decimal("0")
        remaining_to_fill = target_quantity

        if aggressive_allowed:
            slippage_hint = executor._get_slippage_percent(request)
            logger.info(
                f"⚡ [套利执行] {exchange_name} {leg_label} 剩余 {remaining_to_fill}，优先尝试激进限价补齐"
            )
            aggressive_order = await self._place_aggressive_limit_retry_order(
                adapter=adapter,
                symbol=symbol,
                quantity=remaining_to_fill,
                is_buy=is_buy,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=exchange_name,
                slippage_pct=slippage_hint,
                base_slippage=slippage_hint,
            )
            if aggressive_order:
                _, aggressive_filled = await self._confirm_market_fill_with_retries(
                    initial_order=aggressive_order,
                    adapter=adapter,
                    symbol=symbol,
                    quantity=remaining_to_fill,
                    is_buy=is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=exchange_name,
                    allow_resubmit=False,
                    max_attempts=1,
                    enable_internal_aggressive_retry=False,
                )
                if aggressive_filled > epsilon:
                    supplemental_filled += aggressive_filled
                    remaining_to_fill = max(
                        Decimal("0"), target_quantity - supplemental_filled)

        if remaining_to_fill > epsilon:
            if aggressive_allowed:
                logger.warning(
                    "⚠️ [套利执行] %s %s 激进限价补单未完全成交，改用市价补单 (remaining=%s)",
                    exchange_name,
                    leg_label,
                    remaining_to_fill,
                )
            else:
                logger.warning(
                    "⚠️ [套利执行] %s %s 剩余量不足最小限额，直接使用市价补单 (remaining=%s)",
                    exchange_name,
                    leg_label,
                    remaining_to_fill,
                )

            try:
                market_order = await executor._place_market_order(
                    adapter,
                    symbol,
                    remaining_to_fill,
                    is_buy=is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                )
            except self.ReduceOnlyRestrictionError:
                logger.warning(
                    f"⚠️ [套利执行] {exchange_name} {leg_label} 市价补单触发 reduce-only 限制，remaining={remaining_to_fill}"
                )
                raise

            if not market_order:
                raise RuntimeError(
                    f"{exchange_name} {leg_label} 市价补单提交失败，无法补齐剩余 {remaining_to_fill}"
                )

            market_order, market_filled = await self._confirm_market_fill_with_retries(
                initial_order=market_order,
                adapter=adapter,
                symbol=symbol,
                quantity=remaining_to_fill,
                is_buy=is_buy,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=exchange_name,
                max_attempts=market_retry_max_attempts,
                enable_internal_aggressive_retry=False,
            )
            if market_filled > epsilon:
                supplemental_filled += market_filled
                remaining_to_fill = max(
                    Decimal("0"), target_quantity - supplemental_filled)

        if supplemental_filled + epsilon < target_quantity:
            raise RuntimeError(
                f"{exchange_name} {leg_label} 激进限价/市价补单仍未成交，剩余 {remaining_to_fill}"
            )

        return supplemental_filled

    def _calculate_success_quantity_from_orders(
        self,
        order_buy: Optional[OrderData],
        order_sell: Optional[OrderData],
        fallback: Optional[Decimal] = None,
    ) -> Decimal:
        """从买卖订单中计算实际成交数量（取两腿最小值）"""
        executor = self.executor
        quantities: List[Decimal] = []
        if order_buy and getattr(order_buy, "filled", None) is not None:
            buy_qty = executor._to_decimal_value(
                order_buy.filled or Decimal("0"))
            if buy_qty > Decimal("0"):
                quantities.append(buy_qty)
        if order_sell and getattr(order_sell, "filled", None) is not None:
            sell_qty = executor._to_decimal_value(
                order_sell.filled or Decimal("0"))
            if sell_qty > Decimal("0"):
                quantities.append(sell_qty)
        if len(quantities) == 2:
            return min(quantities)
        if len(quantities) == 1:
            return quantities[0]
        return executor._to_decimal_value(fallback or Decimal("0"))

    async def _resolve_filled_quantity(
        self,
        *,
        order: OrderData,
        adapter: ExchangeInterface,
        symbol: str,
        reported_filled: Optional[Decimal],
    ) -> Decimal:
        """
        确认限价订单的真实成交数量，支持部分成交场景。
        优先使用等待阶段返回的数量；若无，则尝试读取 order.filled，
        仍为空时触发一次 REST 查询刷新状态。
        """
        executor = self.executor
        decimal_value = executor._to_decimal_value(
            reported_filled or Decimal("0"))
        if decimal_value > Decimal("0"):
            return decimal_value

        if order and getattr(order, "filled", None):
            existing = executor._to_decimal_value(order.filled or Decimal("0"))
            if existing > Decimal("0"):
                return existing

        if not adapter or not hasattr(adapter, "get_order"):
            return Decimal("0")

        try:
            latest = await adapter.get_order(str(order.id), symbol)
        except Exception as exc:  # pragma: no cover - 仅日志提示
            logger.warning(
                "⚠️ [套利执行] 查询 %s 订单 %s 状态失败: %s",
                executor._get_exchange_name(adapter),
                order.id,
                exc,
            )
            return Decimal("0")

        if not latest:
            return Decimal("0")

        try:
            executor.order_monitor._merge_order_state(
                order, latest)  # type: ignore[attr-defined]
        except AttributeError:
            order.filled = latest.filled or order.filled
            if latest.status:
                order.status = latest.status
            if latest.average:
                order.average = latest.average
            if latest.price:
                order.price = latest.price
            if latest.remaining is not None:
                order.remaining = latest.remaining

        refreshed = executor._to_decimal_value(order.filled or Decimal("0"))
        if refreshed > Decimal("0"):
            logger.info(
                "ℹ️ [套利执行] 刷新成交数量: %s %s -> %s",
                executor._get_exchange_name(adapter),
                order.id,
                refreshed,
            )
        return refreshed
