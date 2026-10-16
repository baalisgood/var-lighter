from __future__ import annotations

"""
限价 + 市价（limit_market）执行链路

约束：
- 仅做代码搬迁：逻辑/日志/异常处理必须与原 `OrderStrategyExecutor.execute_limit_market_mode` 一致
- 不引入新的公共抽象；仍通过 `self.xxx(...)` 调用原有 helper
"""

import asyncio
from decimal import Decimal
from typing import Any, Dict, Optional

from core.adapters.exchanges.models import OrderData


async def execute_limit_market_mode(
    self: Any,
    *,
    logger: Any,
    request: Any,
    execution_plan: Dict[str, Any],
) -> Any:
    executor = self.executor
    Result = self.ExecutionResult
    epsilon = Decimal("0.00000001")

    # 🔥 获取限价单超时配置
    limit_timeout_cfg = getattr(
        executor.config.order_execution,
        "limit_order_timeout",
        60,
    ) or 60
    try:
        limit_timeout = max(1, int(limit_timeout_cfg))
    except (ValueError, TypeError):
        limit_timeout = 60

    buy_adapter = executor.exchange_adapters.get(request.exchange_buy)
    sell_adapter = executor.exchange_adapters.get(request.exchange_sell)

    buy_symbol = request.buy_symbol or request.symbol
    sell_symbol = request.sell_symbol or request.symbol

    if not buy_adapter or not sell_adapter:
        return self._result_failure(
            request,
            f"交易所适配器不存在: {request.exchange_buy} 或 {request.exchange_sell}",
            mark_waiting=False,
        )

    try:
        # 限价挂单“盘口变动→快速撤单”的最小检查间隔（秒）
        # - 只作用于 limit_market 第一腿限价等待
        # - 其他模式不读取该配置，因此不会受影响
        reprice_interval_raw = getattr(
            executor.config.order_execution,
            "limit_order_reprice_min_interval_seconds",
            2.0,
        )
        try:
            reprice_interval_seconds = float(reprice_interval_raw)
        except (TypeError, ValueError):
            reprice_interval_seconds = 2.0
        # 保护下限：
        # - 本处“盘口/价差检查”主要读取本地 orderbook 快照缓存（DataProcessor），不是每次都主动请求交易所；
        # - 但检查过于频繁会更容易触发“撤单/补单/成交确认(可能走REST)”等真实API动作，增加被限流风险。
        if reprice_interval_seconds < 2.0:
            reprice_interval_seconds = 2.0

        first_adapter = buy_adapter if execution_plan["first_is_buy"] else sell_adapter
        second_adapter = sell_adapter if execution_plan["first_is_buy"] else buy_adapter
        first_exchange = execution_plan["first_exchange"]
        second_exchange = execution_plan["second_exchange"]
        first_tag = self.executor._format_exchange_tag(first_exchange)
        second_tag = self.executor._format_exchange_tag(second_exchange)
        first_price = request.price_buy if execution_plan["first_is_buy"] else request.price_sell
        first_offset = (
            request.limit_price_offset_buy
            if execution_plan["first_is_buy"]
            else request.limit_price_offset_sell
        )

        first_symbol = buy_symbol if execution_plan["first_is_buy"] else sell_symbol
        first_exchange_config = self.executor._get_exchange_mode_config(
            first_exchange,
            first_symbol,
        )
        first_uses_tick_precision = bool(
            getattr(first_exchange_config, "use_tick_precision", False)
        )
        first_absolute_offset = None if first_uses_tick_precision else first_offset
        requested_quantity = executor._to_decimal_value(
            request.quantity or Decimal('0'))
        if requested_quantity <= epsilon:
            return self._result_failure(
                request,
                f"限价订单数量异常 ({first_exchange})",
                mark_waiting=False,
            )

        # 预取第二腿上下文，避免回调路径重复计算
        second_is_buy = execution_plan["second_is_buy"]
        second_symbol = buy_symbol if second_is_buy else sell_symbol
        second_exchange_config = self.executor._get_exchange_mode_config(
            second_exchange,
            second_symbol,
        )
        second_uses_tick_precision = bool(
            getattr(second_exchange_config, "use_tick_precision", False)
        )
        prefetched_second_ctx = {
            "symbol": second_symbol,
            "is_buy": second_is_buy,
            "tag": second_tag,
            "uses_tick_precision": second_uses_tick_precision,
        }

        min_second_leg_qty = executor._resolve_min_order_qty(
            request, second_exchange)
        min_first_leg_qty = executor._resolve_min_order_qty(
            request, first_exchange)
        accumulated_fill = Decimal("0")
        first_leg_complete_by_min_qty = False
        first_leg_min_qty_threshold = (
            executor._to_decimal_value(min_first_leg_qty)
            if min_first_leg_qty is not None
            else None
        )
        total_fill_cost = Decimal("0")
        total_fill_qty = Decimal("0")

        def _accumulate_fill_cost(fill_qty: Decimal, order_obj: Any) -> None:
            nonlocal total_fill_cost, total_fill_qty
            if fill_qty <= epsilon:
                return
            fill_price = executor._resolve_actual_price(order_obj)
            if fill_price is None or fill_price <= epsilon:
                price_fallback = getattr(order_obj, "price", None)
                fill_price = executor._to_decimal_value(price_fallback)
            if fill_price is None or fill_price <= epsilon:
                return
            total_fill_cost += fill_price * fill_qty
            total_fill_qty += fill_qty
        # 第一腿限价补单轮次：使用 limit_market 专用次数，避免与“市价重试次数”强绑定
        retry_cfg = getattr(
            executor.config.order_execution,
            "limit_market_first_leg_max_retry_count",
            None,
        )
        if retry_cfg is None:
            retry_cfg = getattr(executor.config.order_execution, "max_retry_count", 3)
        max_partial_attempts = max(1, int(retry_cfg or 1))
        limit_attempt = 0
        limit_order: Optional[OrderData] = None
        zero_fill_abort = False
        zero_fill_order_id: Optional[str] = None

        # 订单簿快照新鲜度：
        # - 优先使用按交易所覆盖（orchestrator.system_mode.data_freshness_seconds_by_exchange）
        # - 否则使用全局 data_freshness_seconds
        def _resolve_snapshot_max_age_seconds(exchange_name: str) -> float:
            try:
                ex = str(exchange_name or "").strip().lower()
                by_ex = getattr(executor, "data_freshness_seconds_by_exchange", None) or {}
                if isinstance(by_ex, dict):
                    v = by_ex.get(ex)
                    if v is not None:
                        return float(v)
            except Exception:
                pass
            try:
                return float(getattr(executor, "data_freshness_seconds", 2.0))
            except Exception:
                return 2.0

        snapshot_max_age_seconds = _resolve_snapshot_max_age_seconds(first_exchange)

        def _get_best_bid_ask() -> tuple[Optional[Decimal], Optional[Decimal]]:
            """
            读取第一腿交易所的买一/卖一（用于判断盘口是否变化）。
            注意：只看第一腿限价交易所，不看另一边（市价腿）交易所。
            """
            data_processor = getattr(executor, "data_processor", None)
            if not data_processor:
                return None, None
            try:
                # ⚠️ 统一 key：DataReceiver/Processor 通常使用小写 exchange、标准化 symbol 存储
                ex_key = str(first_exchange or "").strip().lower()
                sym_key = str(first_symbol or "").strip().upper()
                orderbook = data_processor.get_orderbook(
                    ex_key,
                    sym_key,
                    max_age_seconds=_resolve_snapshot_max_age_seconds(first_exchange),
                )
            except Exception:
                return None, None
            if not orderbook:
                return None, None
            bid_price = getattr(getattr(orderbook, "best_bid", None), "price", None)
            ask_price = getattr(getattr(orderbook, "best_ask", None), "price", None)
            bid_dec: Optional[Decimal]
            ask_dec: Optional[Decimal]
            try:
                bid_dec = executor._to_decimal_value(bid_price) if bid_price else None
            except Exception:
                bid_dec = None
            try:
                ask_dec = executor._to_decimal_value(ask_price) if ask_price else None
            except Exception:
                ask_dec = None
            return bid_dec, ask_dec

        def _get_best_prices(exchange: str, symbol: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
            """
            返回 (best_ask, best_bid)。
            若盘口不可用/过期/解析失败，则返回 (None, None)。
            """
            data_processor = getattr(executor, "data_processor", None)
            if not data_processor:
                return None, None
            try:
                ex_key = str(exchange or "").strip().lower()
                sym_key = str(symbol or "").strip().upper()
                orderbook = data_processor.get_orderbook(
                    ex_key,
                    sym_key,
                    max_age_seconds=_resolve_snapshot_max_age_seconds(exchange),
                )
            except Exception:
                return None, None
            if not orderbook:
                return None, None
            ask_price = getattr(getattr(orderbook, "best_ask", None), "price", None)
            bid_price = getattr(getattr(orderbook, "best_bid", None), "price", None)
            ask_dec: Optional[Decimal]
            bid_dec: Optional[Decimal]
            try:
                ask_dec = executor._to_decimal_value(ask_price) if ask_price else None
            except Exception:
                ask_dec = None
            try:
                bid_dec = executor._to_decimal_value(bid_price) if bid_price else None
            except Exception:
                bid_dec = None
            return ask_dec, bid_dec

        def _is_arbitrage_condition_still_valid() -> Optional[bool]:
            """
            检查本轮套利条件是否仍满足（使用实时盘口重新计算价差）。
            - 仅在 request.grid_threshold_pct 存在时才做严格判断；否则返回 None 表示“不判断/不触发撤单”
            - 开仓：要求 当前价差% >= 阈值%
            - 平仓：要求 当前价差% <= 阈值%
              （平仓请求在上层已经做了 exchange_buy/sell 的反向处理，因此同一公式依旧成立）
            """
            threshold_pct = getattr(request, "grid_threshold_pct", None)
            if threshold_pct is None:
                return None
            try:
                threshold = executor._to_decimal_value(threshold_pct)
            except Exception:
                return None
            if threshold <= Decimal("0"):
                return None

            # 以“买入腿卖一/卖出腿买一”作为可成交的保守价差估计
            buy_ex = str(getattr(request, "exchange_buy", "") or "")
            sell_ex = str(getattr(request, "exchange_sell", "") or "")
            buy_sym = str(getattr(request, "buy_symbol", None) or getattr(request, "symbol", "") or "")
            sell_sym = str(getattr(request, "sell_symbol", None) or getattr(request, "symbol", "") or "")
            if not buy_ex or not sell_ex or not buy_sym or not sell_sym:
                return None

            buy_ask, _buy_bid = _get_best_prices(buy_ex, buy_sym)
            _sell_ask, sell_bid = _get_best_prices(sell_ex, sell_sym)
            if buy_ask is None or sell_bid is None or buy_ask <= Decimal("0"):
                return None

            spread_pct = (sell_bid - buy_ask) / buy_ask * Decimal("100")
            action = str(getattr(request, "grid_action", "") or "").strip().lower()
            is_close = (action == "close") or (not bool(getattr(request, "is_open", True)))
            if is_close:
                return spread_pct <= threshold
            return spread_pct >= threshold

        while accumulated_fill + epsilon < requested_quantity:
            # ⚠️ 重要：残差截断判断必须早于“补单次数上限”判断。
            # 否则当 max_retry_count 配得很小（例如=1）时：
            # - 第1次下单出现部分成交
            # - 进入下一轮循环时会先触发“次数用尽 break”，导致根本没机会执行 remaining<min_qty 的截断逻辑
            remaining_quantity = max(
                Decimal("0"), requested_quantity - accumulated_fill)
            submit_quantity = remaining_quantity

            # ✅ 新方案（仅限 limit_market 第一腿）：
            # - 已经出现部分成交后：优先完成本轮，不受决策引擎信号影响
            # - 若剩余缺口量 < 交易所最小下单量，则无法继续补单：
            #   -> 直接把第一腿视为“完成”（以实际成交量为准），进入第二腿
            if (
                accumulated_fill > epsilon
                and first_leg_min_qty_threshold is not None
                and first_leg_min_qty_threshold > Decimal("0")
                and submit_quantity + epsilon < first_leg_min_qty_threshold
            ):
                first_leg_complete_by_min_qty = True
                logger.warning(
                    f"ℹ️ [套利执行] [{first_tag}] 第一腿已部分成交 {accumulated_fill}，"
                    f"剩余缺口 {submit_quantity} < 最小下单量 {first_leg_min_qty_threshold}，"
                    "无法继续补单：本轮将以实际成交量进入第二腿（残差留给后续总量驱动/网格归并）"
                )
                break

            if limit_attempt >= max_partial_attempts:
                break
            limit_attempt += 1

            refreshed_price = self.executor._get_live_market_price(
                first_exchange,
                first_symbol,
                execution_plan["first_is_buy"],
            )
            if refreshed_price and refreshed_price > Decimal("0"):
                if first_price != refreshed_price:
                    logger.debug(
                        f"[套利执行] [{first_tag}] 第{limit_attempt}次限价重试使用最新盘口价: "
                        f"{refreshed_price} (原价={first_price})"
                    )
                    first_price = refreshed_price

            logger.debug(
                f"[套利执行] [{first_tag}] 提交限价订单: "
                f"方向={'买入' if execution_plan['first_is_buy'] else '卖出'}, "
                f"价格={first_price}, 数量={submit_quantity} (第{limit_attempt}次)"
            )

            limit_order = await executor._place_limit_order(
                first_adapter,
                first_symbol,
                first_price,
                submit_quantity,
                is_buy=execution_plan["first_is_buy"],
                absolute_offset=first_absolute_offset,
            )
            if not limit_order:
                limit_order = await self._retry_limit_submission(
                    adapter=first_adapter,
                    symbol=first_symbol,
                    price=first_price,
                    quantity=submit_quantity,
                    is_buy=execution_plan["first_is_buy"],
                    absolute_offset=first_absolute_offset,
                    request=request,
                    exchange_name=first_exchange,
                )

            if not limit_order:
                if (
                    min_first_leg_qty
                    and submit_quantity + epsilon < min_first_leg_qty
                ):
                    return await self._handle_first_leg_min_qty_failure(
                        request=request,
                        execution_plan=execution_plan,
                        first_exchange=first_exchange,
                        first_adapter=first_adapter,
                        first_symbol=first_symbol,
                        accumulated_fill=accumulated_fill,
                        submit_quantity=submit_quantity,
                        min_first_leg_qty=min_first_leg_qty,
                    )
                return self._result_failure(
                    request,
                    f"限价订单提交失败 ({first_exchange})",
                )

            submitted_price = getattr(
                limit_order, "price", None) or first_price
            logger.info(
                f"[套利执行] [{first_tag}] 限价订单已提交: order_id={limit_order.id}, "
                f"价格={submitted_price}, 数量={submit_quantity}"
            )

            # ----------------------------
            # 第一腿限价等待（改造点）：
            # - 原逻辑：一直等到 limit_timeout，然后根据成交情况撤单/补单
            # - 新逻辑：挂单期间按配置的最小间隔检查一次第一腿盘口买一/卖一
            #   - 若“盘口变化”且“该订单无新增成交”：
            #       - 若该订单累计仍为 0 成交：撤单并结束本轮（交给决策引擎下一次信号决定是否再挂）
            #       - 若已部分成交：撤单并进入下一轮补单（必须把剩余量做完）
            #   - 若盘口未变化：仍可等待到 limit_timeout
            # ----------------------------
            loop = asyncio.get_running_loop()
            deadline = loop.time() + float(limit_timeout)
            last_progress_fill = executor._to_decimal_value(getattr(limit_order, "filled", None) or Decimal("0"))
            baseline_bid, baseline_ask = _get_best_bid_ask()
            cancel_by_price_change = False
            cancel_by_arbitrage_invalid = False
            last_orderbook_missing_log_ts = 0.0

            while loop.time() < deadline:
                remaining_wait = max(0.0, deadline - loop.time())
                wait_slice = min(reprice_interval_seconds, remaining_wait)
                if wait_slice <= 0.0:
                    break
                # 使用短窗口等待，以便周期性检查盘口
                slice_timeout = max(1, int(wait_slice))
                slice_filled = await executor.order_monitor.wait_for_order_fill(
                    limit_order,
                    first_adapter,
                    timeout=slice_timeout,
                    require_full_fill=False,
                    cancel_on_ws_timeout=False,
                )
                if slice_filled is not None:
                    try:
                        slice_filled_dec = executor._to_decimal_value(slice_filled)
                    except Exception:
                        slice_filled_dec = Decimal("0")
                    if slice_filled_dec > last_progress_fill + epsilon:
                        last_progress_fill = slice_filled_dec
                        # 若出现成交进度，刷新基线盘口（后续只在“无新增成交”时才触发快速撤单）
                        baseline_bid, baseline_ask = _get_best_bid_ask()
                        if last_progress_fill + epsilon >= submit_quantity:
                            break

                # 检查盘口是否变化（买一/卖一任意变化都算）
                current_bid, current_ask = _get_best_bid_ask()

                # 🔎 订单簿快照不可用（过期/缺失）时：
                # - 无法判断“盘口变化”与“套利条件是否仍满足”（两者都依赖快照）
                # - 这种情况下本轮会更倾向于走“等待超时”路径
                if current_bid is None and current_ask is None:
                    now_ts = loop.time()
                    if now_ts - last_orderbook_missing_log_ts >= 10.0:
                        last_orderbook_missing_log_ts = now_ts
                        logger.warning(
                            f"⚠️ [套利执行] [{first_tag}] 订单簿快照不可用/过期，暂无法执行盘口变化/套利条件检查；"
                            f"将继续等待（interval={reprice_interval_seconds}s, max_age={snapshot_max_age_seconds}s）"
                        )
                    continue

                # ✅ 修复：如果基线盘口一开始没拿到（None），但后续拿到了快照，需要立刻建立基线，
                # 否则“盘口变化”永远不会触发（只能走等待超时）。
                if baseline_bid is None and current_bid is not None:
                    baseline_bid = current_bid
                if baseline_ask is None and current_ask is not None:
                    baseline_ask = current_ask

                price_changed = False
                if (
                    baseline_bid is not None
                    and current_bid is not None
                    and baseline_bid != current_bid
                ):
                    price_changed = True
                if (
                    baseline_ask is not None
                    and current_ask is not None
                    and baseline_ask != current_ask
                ):
                    price_changed = True
                # 仅在“该订单无新增成交”时触发快速撤单
                if price_changed:
                    cancel_by_price_change = True
                    break

                # 同步检查：套利条件是否仍满足
                # 说明：只在“本轮还没有任何成交”时才用它作为“结束本轮”的依据；
                # 一旦已有部分成交（accumulated_fill>0），需要把剩余量补齐，不应因信号消失直接结束。
                if accumulated_fill <= epsilon and last_progress_fill <= epsilon:
                    arb_ok = _is_arbitrage_condition_still_valid()
                    if arb_ok is False:
                        cancel_by_arbitrage_invalid = True
                        break

            # 到这里：要么已吃满，要么超时，要么盘口变化触发提前撤单
            # 仅在“未完全吃满”时撤单（避免对已完全成交订单做无意义撤单请求）
            if (
                limit_order
                and limit_order.id
                and last_progress_fill + epsilon < submit_quantity
            ):
                reason_parts = []
                if cancel_by_price_change:
                    reason_parts.append("盘口变化")
                if cancel_by_arbitrage_invalid:
                    reason_parts.append("套利条件不满足")
                if not reason_parts:
                    reason_parts.append("等待超时")
                cancel_reason = "+".join(reason_parts)
                cancel_success = False
                extra_wait_fill: Optional[Decimal] = None
                if not executor.order_monitor.consume_ws_cancelled(limit_order.id):
                    cancel_success = await executor._cancel_order(
                        first_adapter,
                        limit_order.id,
                        first_symbol,
                        reason=cancel_reason,
                    )
                    if not cancel_success:
                        extra_wait_fill = await self._wait_fill_after_cancel_failure(
                            order=limit_order,
                            adapter=first_adapter,
                            symbol=first_symbol,
                            target_quantity=submit_quantity,
                            exchange_tag=first_tag,
                        )
                else:
                    cancel_success = True
                if extra_wait_fill is not None and extra_wait_fill > last_progress_fill + epsilon:
                    last_progress_fill = extra_wait_fill
                # 撤单失败也不强行报错：后续会按成交量与风控逻辑继续处理

            # 最终只做一次“成交量确认”（必要时单次 REST 刷新）
            filled_decimal = await self._resolve_filled_quantity(
                order=limit_order,
                adapter=first_adapter,
                symbol=first_symbol,
                reported_filled=last_progress_fill if last_progress_fill > epsilon else None,
            )

            if filled_decimal > epsilon:
                _accumulate_fill_cost(filled_decimal, limit_order)
                accumulated_fill += filled_decimal
                has_reached_target = accumulated_fill + epsilon >= requested_quantity
                if has_reached_target:
                    logger.info(
                        f"✅ [套利执行] [{first_tag}] 限价腿已累计成交 {accumulated_fill}，达到拆单目标 {requested_quantity}"
                    )
                    break
                logger.info(
                    f"[套利执行] [{first_tag}] 限价订单成交: order_id={limit_order.id}, "
                    f"成交数量={filled_decimal} (累计={accumulated_fill})"
                )
            else:
                # 0成交：如果本轮累计仍为0，则结束本轮（交给决策引擎决定是否再触发）
                if accumulated_fill <= epsilon:
                    zero_fill_abort = True
                    zero_fill_order_id = limit_order.id
                    if cancel_by_price_change and cancel_by_arbitrage_invalid:
                        logger.warning(
                            f"⚠️ [套利执行] [{first_tag}] 限价订单无成交且盘口变化&套利条件不满足，结束本轮套利 (order_id={zero_fill_order_id})"
                        )
                    elif cancel_by_price_change:
                        logger.warning(
                            f"⚠️ [套利执行] [{first_tag}] 限价订单无成交且盘口已变化，结束本轮套利 (order_id={zero_fill_order_id})"
                        )
                    elif cancel_by_arbitrage_invalid:
                        logger.warning(
                            f"⚠️ [套利执行] [{first_tag}] 限价订单无成交且套利条件不满足，结束本轮套利 (order_id={zero_fill_order_id})"
                        )
                    else:
                        logger.warning(
                            f"⚠️ [套利执行] [{first_tag}] 限价订单等待超时仍无任何成交，结束本轮套利 (order_id={zero_fill_order_id})"
                        )
                    break
                logger.warning(
                    f"[套利执行] [{first_tag}] 本次限价单未追加成交: order_id={limit_order.id}, "
                    f"累计成交仍为 {accumulated_fill}"
                )

            remaining_quantity = max(
                Decimal("0"), requested_quantity - accumulated_fill)
            logger.warning(
                f"⚠️ [套利执行] [{first_tag}] 限价订单未完全成交，剩余 {remaining_quantity}，准备重新挂单"
            )

            # 注意：本轮已在等待阶段主动撤单，这里不再重复撤单

            # 🔁 撤单过程中可能又吃满，需要重新确认累计成交量
            if (
                limit_order
                and accumulated_fill + epsilon < requested_quantity
            ):
                refreshed_total = await self._resolve_filled_quantity(
                    order=limit_order,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    reported_filled=None,  # 强制刷新
                )
                if refreshed_total > accumulated_fill + epsilon:
                    extra_fill = refreshed_total - accumulated_fill
                    accumulated_fill = refreshed_total
                    _accumulate_fill_cost(extra_fill, limit_order)
                    logger.info(
                        f"ℹ️ [套利执行] [{first_tag}] 限价订单在撤单流程中新增成交 {extra_fill} (累计={accumulated_fill})"
                    )
                    if accumulated_fill + epsilon >= requested_quantity:
                        logger.info(
                            f"✅ [套利执行] [{first_tag}] 撤单期间已吃满拆单目标 {requested_quantity}，结束补单"
                        )
                        break

            if limit_attempt < max_partial_attempts:
                logger.info(
                    f"[套利执行] [{first_tag}] 累计成交 {accumulated_fill} 未达到拆单目标，继续限价补单 (剩余 {remaining_quantity})"
                )
            else:
                logger.warning(
                    f"[套利执行] [{first_tag}] 限价补单次数已用尽，累计成交 {accumulated_fill}，剩余 {remaining_quantity}"
                )

        if zero_fill_abort:
            return self._result_failure(
                request,
                f"限价订单未获得有效成交 ({first_exchange})",
                mark_waiting=False,
                failure_code="limit_zero_fill",
            )

        filled_decimal = accumulated_fill

        # ✅ 新方案（仅限 limit_market 第一腿）：若因“剩余<最小下单量”提前完成第一腿，
        # 则以实际成交量作为本轮第一腿完成量，避免触发“累计不足拆单目标→紧急平仓”的旧失败路径。
        if first_leg_complete_by_min_qty and filled_decimal > epsilon:
            requested_quantity = filled_decimal

        # ✅ 汇总本轮第一腿的加权均价（多次补单/部分成交更准确）
        if (
            filled_decimal > epsilon
            and total_fill_qty > epsilon
            and total_fill_cost > epsilon
            and limit_order is not None
        ):
            limit_order.filled = total_fill_qty
            limit_order.cost = total_fill_cost
            limit_order.average = (total_fill_cost / total_fill_qty)

        if filled_decimal + epsilon < requested_quantity:
            remaining_quantity = max(
                Decimal("0"), requested_quantity - filled_decimal)
            logger.warning(
                f"[套利执行] [{first_tag}] 累计成交 {filled_decimal} 仍低于拆单目标 {requested_quantity}，"
                f"剩余 {remaining_quantity}，执行紧急平仓"
            )
            await executor._emergency_close_position(
                exchange=first_exchange,
                adapter=first_adapter,
                symbol=first_symbol,
                quantity=filled_decimal,
                is_buy_to_close=not execution_plan["first_is_buy"],
                request=request,
            )
            return self._result_failure(
                request,
                f"限价订单累计成交不足拆单目标 ({first_exchange})",
                partial_failure=True,
                failed_exchange=second_exchange,
                success_exchange=first_exchange,
                success_quantity=filled_decimal,
                failure_code="limit_incomplete_fill",
            )

        if (
            min_second_leg_qty
            and min_second_leg_qty > Decimal("0")
            and filled_decimal < min_second_leg_qty
        ):
            logger.warning(
                f"[套利执行] [{first_tag}] 累计成交 {filled_decimal} 低于 {second_exchange} 最小下单量 "
                f"{min_second_leg_qty}，取消本次拆单"
            )
            await executor._emergency_close_position(
                exchange=first_exchange,
                adapter=first_adapter,
                symbol=first_symbol,
                quantity=filled_decimal,
                is_buy_to_close=not execution_plan["first_is_buy"],
                request=request,
            )
            return self._result_failure(
                request,
                f"限价订单累计成交不足最小市价量 ({first_exchange})",
                partial_failure=True,
                failed_exchange=second_exchange,
                success_exchange=first_exchange,
                success_quantity=filled_decimal,
                failure_code="limit_min_qty_unmet",
            )

        second_order_mode = execution_plan.get(
            "second_order_mode", "market")
        order_type_label = "激进限价吃单" if second_order_mode == "limit" else "市价订单"

        second_symbol = sell_symbol if execution_plan["first_is_buy"] else buy_symbol
        second_exchange_config = self.executor._get_exchange_mode_config(
            second_exchange,
            second_symbol,
        )
        second_uses_tick_precision = bool(
            getattr(second_exchange_config, "use_tick_precision", False)
        )
        smart_second_leg_price = bool(
            getattr(second_exchange_config,
                    "smart_second_leg_price", False)
        )
        first_leg_clean_fill = limit_attempt == 1
        reuse_signal_price_second_leg = bool(
            getattr(second_exchange_config,
                    "reuse_signal_price_when_second_leg", False)
        )
        if smart_second_leg_price:
            reuse_signal_price_second_leg = first_leg_clean_fill
            logger.info(
                f"[套利执行] [{second_tag}] 智能价格模式 -> "
                f"{'信号价' if reuse_signal_price_second_leg else '最新盘口价'} "
                f"(第一腿尝试次数={limit_attempt})"
            )
        if smart_second_leg_price:
            price_mode_label = f"智能-{'信号价' if reuse_signal_price_second_leg else '最新盘口'}"
        elif reuse_signal_price_second_leg:
            price_mode_label = "固定-信号价"
        else:
            price_mode_label = "最新盘口"
        effective_second_offset: Optional[Decimal] = None
        signal_price_for_second_leg = executor._to_decimal_value(
            request.price_buy if execution_plan["second_is_buy"] else request.price_sell
        )

        try:
            if second_order_mode == "limit":
                # 🔥 使用激进限价单吃单（force_sequential_limit 模式）
                second_price: Optional[Decimal]
                if reuse_signal_price_second_leg:
                    second_price = signal_price_for_second_leg
                    if not second_price or second_price <= Decimal("0"):
                        second_price = executor._get_live_market_price(
                            second_exchange,
                            second_symbol,
                            execution_plan["second_is_buy"],
                        ) or signal_price_for_second_leg
                else:
                    second_price = executor._get_live_market_price(
                        second_exchange,
                        second_symbol,
                        execution_plan["second_is_buy"],
                    )
                    if not second_price or second_price <= Decimal("0"):
                        # 如果获取不到实时价格，使用请求中的价格
                        second_price = signal_price_for_second_leg
                second_price = executor._to_decimal_value(
                    second_price or Decimal("0"))

                second_offset = (
                    request.limit_price_offset_buy if execution_plan["second_is_buy"]
                    else request.limit_price_offset_sell
                )
                effective_second_offset = (
                    None if second_uses_tick_precision else second_offset
                )

                # 🔥 计算实际下单价格（用于日志显示）
                # 获取交易所配置
                exchange_config = second_exchange_config or executor.config.exchange_order_modes.get(
                    second_exchange)
                limit_price_offset_pct = (
                    getattr(exchange_config, 'limit_price_offset', 0.001)
                    if exchange_config else 0.001
                )

                # 计算激进价格（与 _place_limit_order 中的逻辑一致）
                if effective_second_offset and effective_second_offset > Decimal('0'):
                    # 如果有绝对偏移，使用绝对偏移
                    if execution_plan["second_is_buy"]:
                        aggressive_price = second_price + effective_second_offset
                    else:
                        aggressive_price = second_price - effective_second_offset
                else:
                    # 否则使用百分比偏移
                    if execution_plan["second_is_buy"]:
                        aggressive_price = second_price * \
                            (Decimal('1') + Decimal(str(limit_price_offset_pct)))
                    else:
                        aggressive_price = second_price * \
                            (Decimal('1') - Decimal(str(limit_price_offset_pct)))

                second_best_bid: Optional[Decimal] = None
                second_best_ask: Optional[Decimal] = None
                data_processor = getattr(executor, "data_processor", None)
                snapshot_max_age_seconds = float(getattr(executor, "data_freshness_seconds", 2.0))
                if data_processor:
                    try:
                        second_orderbook = data_processor.get_orderbook(
                            second_exchange,
                            second_symbol,
                            max_age_seconds=snapshot_max_age_seconds,
                        )
                        if second_orderbook:
                            if getattr(second_orderbook, "best_bid", None):
                                second_best_bid = executor._to_decimal_value(
                                    second_orderbook.best_bid.price
                                )
                            if getattr(second_orderbook, "best_ask", None):
                                second_best_ask = executor._to_decimal_value(
                                    second_orderbook.best_ask.price
                                )
                    except Exception as snapshot_err:  # pragma: no cover - snapshot仅用于日志
                        logger.debug(
                            f"[套利执行] [{second_tag}] 记录盘口快照失败: {snapshot_err}"
                        )
                bid_display = (
                    f"{second_best_bid:.5f}"
                    if isinstance(second_best_bid, Decimal)
                    else "-"
                )
                ask_display = (
                    f"{second_best_ask:.5f}"
                    if isinstance(second_best_ask, Decimal)
                    else "-"
                )

                logger.info(
                    f"[套利执行] [{second_tag}] 提交{order_type_label}: "
                    f"方向={'买入' if second_is_buy else '卖出'}, "
                    f"盘口Snapshot=Ask:{ask_display}/Bid:{bid_display}, "
                    f"max_age={snapshot_max_age_seconds:.1f}s, "
                    f"市场价={second_price}, 下单价={aggressive_price}, "
                    f"价格模式={price_mode_label}, "
                    f"数量={filled_decimal} (根据第一腿实际成交数量)"
                )

                market_order = await executor._place_limit_order(
                    second_adapter,
                    second_symbol,
                    second_price,
                    filled_decimal,
                    is_buy=second_is_buy,
                    absolute_offset=effective_second_offset,
                    request=request,
                )
            else:
                logger.info(
                    f"[套利执行] [{second_tag}] 提交{order_type_label}: "
                    f"方向={'买入' if second_is_buy else '卖出'}, "
                    f"数量={filled_decimal} (根据第一腿实际成交数量)"
                )
                # 原有的市价单逻辑
                market_order = await executor._place_market_order(
                    second_adapter,
                    second_symbol,
                    filled_decimal,
                    is_buy=second_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                )
        except self.ReduceOnlyRestrictionError:
            await executor.reduce_only_handler.handle_reduce_only_after_partial(
                request=request,
                completed_exchange=first_exchange,
                completed_adapter=first_adapter,
                completed_symbol=first_symbol,
                completed_quantity=filled_decimal,
                completed_was_buy=execution_plan["first_is_buy"],
                failing_exchange=second_exchange,
                failing_symbol=second_symbol,
                closing_issue=(not request.is_open),
            )
            return self._result_failure(
                request,
                f"{second_exchange} reduce-only 限制，已撤销已成交腿",
                partial_failure=True,
                order_buy=limit_order if execution_plan["first_is_buy"] else None,
                order_sell=limit_order if not execution_plan["first_is_buy"] else None,
            )

        if not market_order:
            if second_order_mode == "limit":
                # 限价单重试
                if reuse_signal_price_second_leg:
                    second_price_retry = signal_price_for_second_leg
                else:
                    second_price_retry = executor._get_live_market_price(
                        second_exchange,
                        second_symbol,
                        second_is_buy,
                    ) or signal_price_for_second_leg
                second_price_retry = executor._to_decimal_value(
                    second_price_retry or Decimal("0"))

                retry_market = await self._retry_limit_submission(
                    adapter=second_adapter,
                    symbol=second_symbol,
                    price=second_price_retry,
                    quantity=filled_decimal,
                    is_buy=second_is_buy,
                    absolute_offset=effective_second_offset,
                    request=request,
                    exchange_name=second_exchange,
                )
            else:
                # 市价单重试
                retry_market = await self._retry_market_submission(
                    adapter=second_adapter,
                    symbol=second_symbol,
                    quantity=filled_decimal,
                    is_buy=second_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=second_exchange,
                )
            market_order = retry_market

        if not market_order:
            return await self._fail_limit_market_second_leg(
                request=request,
                limit_order=limit_order,
                market_order=None,
                execution_plan=execution_plan,
                first_adapter=first_adapter,
                first_exchange=first_exchange,
                first_symbol=first_symbol,
                filled_quantity=filled_decimal,
                failed_exchange=second_exchange,
                error_message=f"{order_type_label}提交失败 ({second_exchange})",
            )

        logger.info(
            f"[套利执行] [{second_tag}] {order_type_label}已提交: order_id={market_order.id}, 数量={filled_decimal}"
        )
        limit_completion_logged = False
        try:
            if second_order_mode == "limit":
                # 🔥 激进限价单使用不同的等待逻辑
                await executor.order_monitor.wait_for_order_fill(
                    market_order,
                    second_adapter,
                    limit_timeout,
                )
                market_filled_dec = executor._to_decimal_value(
                    market_order.filled)

                # 🔥 检查是否完全成交
                if market_filled_dec >= filled_decimal - epsilon:
                    # 完全成交，成功
                    logger.info(
                        f"[套利执行] [{second_tag}] 激进限价单完全成交: order_id={market_order.id}, 成交数量={market_filled_dec}"
                    )
                    limit_completion_logged = True
                else:
                    # 🔥 未吃满（包含0成交）：先撤单，再用市价补齐，按市价重试策略逐步放大滑点
                    shortfall = max(
                        Decimal("0"), filled_decimal - market_filled_dec)
                    if shortfall <= epsilon:
                        shortfall = Decimal("0")
                    warn_prefix = "部分成交" if market_filled_dec > epsilon else "未成交"
                    logger.warning(
                        f"⚠️ [套利执行] [{second_tag}] 激进限价单{warn_prefix}，缺口 {shortfall}，将改走市价补齐。"
                    )

                    need_market_supplement = True
                    extra_wait_fill: Optional[Decimal] = None

                    if not executor.order_monitor.consume_ws_cancelled(market_order.id):
                        cancel_success = await executor._cancel_order(
                            second_adapter,
                            market_order.id,
                            second_symbol,
                        )
                        if not cancel_success:
                            extra_wait_fill = await self._wait_fill_after_cancel_failure(
                                order=market_order,
                                adapter=second_adapter,
                                symbol=second_symbol,
                                target_quantity=filled_decimal,
                                exchange_tag=second_tag,
                            )
                    else:
                        cancel_success = True

                    if extra_wait_fill is not None:
                        market_filled_dec = max(
                            market_filled_dec, extra_wait_fill)
                        shortfall = max(
                            Decimal("0"), filled_decimal - market_filled_dec)
                        if market_order:
                            market_order.filled = market_filled_dec
                        if shortfall <= epsilon:
                            need_market_supplement = False
                            limit_completion_logged = True
                            logger.info(
                                f"✅ [套利执行] [{second_tag}] 撤单失败后的额外等待检测到成交，"
                                f"order_id={market_order.id}, 成交数量={market_filled_dec}"
                            )

                    if need_market_supplement:
                        try:
                            supplement_order, supplement_filled_dec = await self._confirm_market_fill_with_retries(
                                initial_order=None,
                                adapter=second_adapter,
                                symbol=second_symbol,
                                quantity=shortfall,
                                is_buy=execution_plan["second_is_buy"],
                                reduce_only=(not request.is_open),
                                request=request,
                                exchange_name=second_exchange,
                                enable_internal_aggressive_retry=True,
                            )
                            if supplement_filled_dec + epsilon < shortfall:
                                logger.error(
                                    f"❌ [套利执行] [{second_tag}] 市价补齐多次失败，"
                                    f"仅成交 {supplement_filled_dec} / 目标 {shortfall}"
                                )
                                return await self._fail_limit_market_second_leg(
                                    request=request,
                                    limit_order=limit_order,
                                    market_order=market_order,
                                    execution_plan=execution_plan,
                                    first_adapter=first_adapter,
                                    first_exchange=first_exchange,
                                    first_symbol=first_symbol,
                                    filled_quantity=market_filled_dec,
                                    failed_exchange=second_exchange,
                                    error_message=f"激进限价缺口市价补单失败 ({second_exchange})",
                                )

                            market_filled_dec += supplement_filled_dec
                            if supplement_order:
                                market_order = supplement_order
                            if market_order:
                                market_order.filled = market_filled_dec
                            logger.info(
                                f"✅ [套利执行] [{second_tag}] 市价补单成功: 补单量={supplement_filled_dec}, "
                                f"总成交={market_filled_dec}"
                            )
                            limit_completion_logged = True

                        except self.ReduceOnlyRestrictionError:
                            logger.error(
                                f"❌ [套利执行] [{second_tag}] 市价补单遇到 reduce-only 限制")
                            raise
                        except Exception as market_err:
                            logger.error(
                                f"❌ [套利执行] [{second_tag}] 市价补单异常: {market_err}",
                                exc_info=True,
                            )
                            return await self._fail_limit_market_second_leg(
                                request=request,
                                limit_order=limit_order,
                                market_order=market_order,
                                execution_plan=execution_plan,
                                first_adapter=first_adapter,
                                first_exchange=first_exchange,
                                first_symbol=first_symbol,
                                filled_quantity=market_filled_dec,
                                failed_exchange=second_exchange,
                                error_message=f"激进限价缺口市价补单异常 ({second_exchange})",
                            )
            else:
                # 原有的市价单确认逻辑
                market_order, market_filled_dec = await self._confirm_market_fill_with_retries(
                    initial_order=market_order,
                    adapter=second_adapter,
                    symbol=second_symbol,
                    quantity=filled_decimal,
                    is_buy=execution_plan["second_is_buy"],
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=second_exchange,
                )
        except self.ReduceOnlyRestrictionError:
            await executor.reduce_only_handler.handle_reduce_only_after_partial(
                request=request,
                completed_exchange=first_exchange,
                completed_adapter=first_adapter,
                completed_symbol=first_symbol,
                completed_quantity=filled_decimal,
                completed_was_buy=execution_plan["first_is_buy"],
                failing_exchange=second_exchange,
                failing_symbol=second_symbol,
                closing_issue=(not request.is_open),
            )
            return self._result_failure(
                request,
                f"{second_exchange} reduce-only 限制，已撤销已成交腿",
                partial_failure=True,
                order_buy=limit_order if execution_plan["first_is_buy"] else market_order,
                order_sell=limit_order if not execution_plan["first_is_buy"] else market_order,
            )

        if market_filled_dec <= epsilon:
            order_type_display = "激进限价单" if second_order_mode == "limit" else "市价订单"
            logger.warning(
                f"[套利执行] [{second_tag}] {order_type_display}未在等待窗口内确认成交: "
                f"order_id={market_order.id if market_order else 'unknown'}"
            )
            return await self._fail_limit_market_second_leg(
                request=request,
                limit_order=limit_order,
                market_order=market_order,
                execution_plan=execution_plan,
                first_adapter=first_adapter,
                first_exchange=first_exchange,
                first_symbol=first_symbol,
                filled_quantity=filled_decimal,
                failed_exchange=second_exchange,
                error_message=f"{order_type_display}超时未成交 ({second_exchange})",
            )

        result_order_buy = limit_order if execution_plan["first_is_buy"] else market_order
        result_order_sell = limit_order if not execution_plan["first_is_buy"] else market_order

        order_type_display = "激进限价单" if second_order_mode == "limit" else "市价订单"
        if not (second_order_mode == "limit" and limit_completion_logged):
            logger.info(
                f"[套利执行] [{second_tag}] {order_type_display}成交确认: "
                f"order_id={market_order.id if market_order else 'unknown'}, "
                f"成交数量={market_filled_dec}"
            )
        slip = executor._log_execution_summary(
            request=request,
            order_buy=result_order_buy,
            order_sell=result_order_sell,
            is_open=request.is_open,
            is_last_split=request.is_last_split,
        )

        return Result(
            success=True,
            order_buy=result_order_buy,
            order_sell=result_order_sell,
            success_quantity=filled_decimal,
            slippage_pct=(slip or {}).get("slippage_pct"),
            high_slippage_threshold_pct=(slip or {}).get(
                "high_slippage_threshold_pct"),
        )

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f"[套利执行] 限价+市场模式执行失败: {exc}", exc_info=True)
        return self._result_failure(
            request,
            str(exc),
        )

