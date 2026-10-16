from __future__ import annotations

"""
限价 + 限价（limit_limit）执行链路

约束：
- 仅做代码搬迁：逻辑/日志/异常处理必须与原 `OrderStrategyExecutor.execute_limit_limit_mode` 一致
- 不引入新的公共抽象；仍通过 `self.xxx(...)` 调用原有 helper
"""

import asyncio
from decimal import Decimal
from typing import Any, Optional

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderData


async def execute_limit_limit_mode(
    self: Any,
    *,
    logger: Any,
    request: Any,
) -> Any:
    executor = self.executor
    Result = self.ExecutionResult

    buy_adapter = executor.exchange_adapters.get(request.exchange_buy)
    sell_adapter = executor.exchange_adapters.get(request.exchange_sell)
    if not buy_adapter or not sell_adapter:
        return self._result_failure(
            request,
            f"交易所适配器不存在: {request.exchange_buy} 或 {request.exchange_sell}",
            mark_waiting=False,
        )

    buy_symbol = request.buy_symbol or request.symbol
    sell_symbol = request.sell_symbol or request.symbol
    quantity = request.quantity or Decimal('0')
    epsilon = Decimal("0.00000001")
    limit_timeout = getattr(
        executor.config.order_execution,
        "limit_order_timeout",
        60,
    ) or 60
    # 双限价（limit_limit）模式独立重试次数：只影响本模式内部补齐链路
    limit_limit_retry_cfg = getattr(
        executor.config.order_execution,
        "limit_limit_max_retry_count",
        None,
    )
    try:
        limit_limit_retry_max_attempts = int(limit_limit_retry_cfg) if limit_limit_retry_cfg is not None else None
    except (TypeError, ValueError):
        limit_limit_retry_max_attempts = None
    if limit_limit_retry_max_attempts is not None:
        limit_limit_retry_max_attempts = max(1, limit_limit_retry_max_attempts)

    try:
        logger.info(
            f"[套利执行] 双限价模式：同时提交限价订单，"
            f"买入={request.exchange_buy}@{request.price_buy}, "
            f"卖出={request.exchange_sell}@{request.price_sell}, 数量={quantity}"
        )
        buy_offset = request.limit_price_offset_buy
        sell_offset = request.limit_price_offset_sell

        buy_order_task = asyncio.create_task(
            executor._place_limit_order(
                buy_adapter,
                buy_symbol,
                request.price_buy,
                quantity,
                is_buy=True,
                absolute_offset=buy_offset,
            )
        )
        sell_order_task = asyncio.create_task(
            executor._place_limit_order(
                sell_adapter,
                sell_symbol,
                request.price_sell,
                quantity,
                is_buy=False,
                absolute_offset=sell_offset,
            )
        )

        buy_order_raw, sell_order_raw = await asyncio.gather(
            buy_order_task,
            sell_order_task,
            return_exceptions=True,
        )

        buy_error = buy_order_raw if isinstance(
            buy_order_raw, self.ReduceOnlyRestrictionError) else None
        sell_error = sell_order_raw if isinstance(
            sell_order_raw, self.ReduceOnlyRestrictionError) else None

        buy_order = buy_order_raw if not isinstance(
            buy_order_raw, Exception) else None
        sell_order = sell_order_raw if not isinstance(
            sell_order_raw, Exception) else None

        if buy_error:
            return await self._handle_reduce_only_submission_failure(
                request=request,
                failing_exchange=request.exchange_buy,
                failing_symbol=buy_symbol,
                failing_is_buy=True,
                other_order=sell_order,
                other_adapter=sell_adapter,
                other_exchange=request.exchange_sell,
                other_symbol=sell_symbol,
                other_is_buy=False,
                epsilon=epsilon,
                limit_timeout=limit_timeout,
            )

        if sell_error:
            return await self._handle_reduce_only_submission_failure(
                request=request,
                failing_exchange=request.exchange_sell,
                failing_symbol=sell_symbol,
                failing_is_buy=False,
                other_order=buy_order,
                other_adapter=buy_adapter,
                other_exchange=request.exchange_buy,
                other_symbol=buy_symbol,
                other_is_buy=True,
                epsilon=epsilon,
                limit_timeout=limit_timeout,
            )

        if isinstance(buy_order_raw, Exception):
            raise buy_order_raw
        if isinstance(sell_order_raw, Exception):
            raise sell_order_raw

        if not buy_order:
            buy_order = await self._retry_limit_submission(
                adapter=buy_adapter,
                symbol=buy_symbol,
                price=request.price_buy,
                quantity=quantity,
                is_buy=True,
                absolute_offset=buy_offset,
                request=request,
                exchange_name=request.exchange_buy,
            )

        if not sell_order:
            sell_order = await self._retry_limit_submission(
                adapter=sell_adapter,
                symbol=sell_symbol,
                price=request.price_sell,
                quantity=quantity,
                is_buy=False,
                absolute_offset=sell_offset,
                request=request,
                exchange_name=request.exchange_sell,
            )

        if not buy_order and not sell_order:
            return self._result_failure(
                request,
                "双限价订单均提交失败",
            )

        if not buy_order:
            if sell_order:
                await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
            return self._result_failure(
                request,
                f"买入限价订单提交失败 ({request.exchange_buy})",
            )

        if not sell_order:
            await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
            return self._result_failure(
                request,
                f"卖出限价订单提交失败 ({request.exchange_sell})",
            )

        buy_fill_task = asyncio.create_task(
            executor.order_monitor.wait_for_order_fill(
                buy_order, buy_adapter, timeout=limit_timeout)
        )
        sell_fill_task = asyncio.create_task(
            executor.order_monitor.wait_for_order_fill(
                sell_order, sell_adapter, timeout=limit_timeout)
        )
        buy_filled, sell_filled = await asyncio.gather(buy_fill_task, sell_fill_task)

        buy_filled_dec = await self._resolve_filled_quantity(
            order=buy_order,
            adapter=buy_adapter,
            symbol=buy_symbol,
            reported_filled=buy_filled,
        )
        sell_filled_dec = await self._resolve_filled_quantity(
            order=sell_order,
            adapter=sell_adapter,
            symbol=sell_symbol,
            reported_filled=sell_filled,
        )

        def _is_completed(filled_dec: Decimal) -> bool:
            return filled_dec + epsilon >= quantity

        buy_completed = _is_completed(buy_filled_dec)
        sell_completed = _is_completed(sell_filled_dec)

        if buy_completed and sell_completed:
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
                fallback=quantity,
            )
            return Result(
                success=True,
                order_buy=buy_order,
                order_sell=sell_order,
                success_quantity=actual_quantity,
                slippage_pct=(slip or {}).get("slippage_pct"),
                high_slippage_threshold_pct=(slip or {}).get(
                    "high_slippage_threshold_pct"),
            )

        # ✅ 只有一条腿成交（另一条腿0成交）→ 立即取消未成交腿并启动补仓流程
        if buy_completed ^ sell_completed:
            missing_buy = not buy_completed
            shortfall = (
                sell_filled_dec - buy_filled_dec
                if missing_buy else
                buy_filled_dec - sell_filled_dec
            )
            if shortfall > epsilon:
                logger.warning(
                    f"⚠️ [套利执行] 双限价模式出现单腿成交，"
                    f"{'买腿' if missing_buy else '卖腿'} 缺口 {shortfall}，"
                    "先激进限价补仓，剩余再市价兜底"
                )
                return await self._handle_single_leg_shortfall(
                    request=request,
                    buy_order=buy_order,
                    sell_order=sell_order,
                    buy_adapter=buy_adapter,
                    sell_adapter=sell_adapter,
                    buy_symbol=buy_symbol,
                    sell_symbol=sell_symbol,
                    missing_buy=missing_buy,
                    shortfall=shortfall,
                    target_quantity=quantity,
                    epsilon=epsilon,
                    cancel_opposite_order=False,
                    result_quantity=Decimal(str(quantity)),
                    market_retry_max_attempts=limit_limit_retry_max_attempts,
                )

        if not buy_completed and not sell_completed:
            has_buy_fill = buy_filled_dec > epsilon
            has_sell_fill = sell_filled_dec > epsilon

            if not has_buy_fill and not has_sell_fill:
                await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
                await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
                logger.info(
                    "↩️ [套利执行] 双限价腿均 0 成交，已取消 buy=%s / sell=%s",
                    buy_order.id,
                    sell_order.id,
                )
                refreshed_buy = await self._resolve_filled_quantity(
                    order=buy_order,
                    adapter=buy_adapter,
                    symbol=buy_symbol,
                    reported_filled=None,
                )
                refreshed_sell = await self._resolve_filled_quantity(
                    order=sell_order,
                    adapter=sell_adapter,
                    symbol=sell_symbol,
                    reported_filled=None,
                )
                if refreshed_buy > epsilon or refreshed_sell > epsilon:
                    buy_filled_dec = refreshed_buy
                    sell_filled_dec = refreshed_sell
                    has_buy_fill = buy_filled_dec > epsilon
                    has_sell_fill = sell_filled_dec > epsilon
                    logger.info(
                        "ℹ️ [套利执行] 取消双限价后检测到延迟成交: buy=%s, sell=%s",
                        buy_filled_dec,
                        sell_filled_dec,
                    )
                if not has_buy_fill and not has_sell_fill:
                    return self._result_failure(
                        request,
                        "双限价订单均未成交",
                        order_buy=buy_order,
                        order_sell=sell_order,
                        failure_code="dual_limit_no_fill",
                        mark_waiting=False,
                    )

            if has_buy_fill ^ has_sell_fill:
                missing_buy = not has_buy_fill
                fallback_quantity = (
                    sell_filled_dec - buy_filled_dec
                    if missing_buy else
                    buy_filled_dec - sell_filled_dec
                )
                if fallback_quantity > epsilon:
                    return await self._handle_single_leg_shortfall(
                        request=request,
                        buy_order=buy_order,
                        sell_order=sell_order,
                        buy_adapter=buy_adapter,
                        sell_adapter=sell_adapter,
                        buy_symbol=buy_symbol,
                        sell_symbol=sell_symbol,
                        missing_buy=missing_buy,
                        shortfall=fallback_quantity,
                        target_quantity=quantity,
                        epsilon=epsilon,
                        cancel_opposite_order=True,
                        result_quantity=fallback_quantity,
                        market_retry_max_attempts=limit_limit_retry_max_attempts,
                    )
                return self._result_failure(
                    request,
                    "单边成交缺口无法识别",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            buy_remaining = (quantity - buy_filled_dec).copy_abs()
            sell_remaining = (quantity - sell_filled_dec).copy_abs()
            fill_failed = False

            async def _complete_leg(remaining: Decimal, *, is_buy: bool) -> Decimal:
                exchange_name = request.exchange_buy if is_buy else request.exchange_sell
                adapter = buy_adapter if is_buy else sell_adapter
                symbol = buy_symbol if is_buy else sell_symbol
                leg_label = "买腿" if is_buy else "卖腿"
                if remaining <= epsilon:
                    return Decimal("0")
                logger.warning(
                    f"⚠️ [套利执行] 双腿部分成交，{leg_label} 剩余 {remaining} 未到达拆单目标，启动补单流程（激进限价优先）"
                )
                added = await self._fill_leg_to_target(
                    adapter=adapter,
                    symbol=symbol,
                    exchange_name=exchange_name,
                    is_buy=is_buy,
                    remaining_quantity=remaining,
                    request=request,
                    epsilon=epsilon,
                    market_retry_max_attempts=limit_limit_retry_max_attempts,
                )
                return added

            try:
                if buy_remaining > epsilon:
                    buy_filled_dec += await _complete_leg(buy_remaining, is_buy=True)
                if sell_remaining > epsilon:
                    sell_filled_dec += await _complete_leg(sell_remaining, is_buy=False)
            except self.ReduceOnlyRestrictionError:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    request.exchange_buy if buy_remaining > sell_remaining else request.exchange_sell,
                    buy_symbol if buy_remaining > sell_remaining else sell_symbol,
                    closing_issue=(not request.is_open),
                    reason="fill_to_target_reduce_only",
                )
                fill_failed = True
            except Exception as fill_exc:
                logger.warning(
                    f"⚠️ [套利执行] 双腿部分成交补齐至拆单量失败，将降级为差值补齐: {fill_exc}",
                    exc_info=True,
                )
                fill_failed = True

            still_missing_buy = (quantity - buy_filled_dec) > epsilon
            still_missing_sell = (quantity - sell_filled_dec) > epsilon
            if not fill_failed and not (still_missing_buy or still_missing_sell):
                slip = executor._log_execution_summary(
                    request=request,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    is_open=request.is_open,
                    is_last_split=request.is_last_split,
                )
                return Result(
                    success=True,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    success_quantity=min(
                        buy_filled_dec, sell_filled_dec, quantity),
                    slippage_pct=(slip or {}).get("slippage_pct"),
                    high_slippage_threshold_pct=(slip or {}).get(
                        "high_slippage_threshold_pct"),
                )

            # 若无法补齐拆单目标，则终止本次执行避免进一步混乱
            await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
            await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)

            reason = "双腿部分成交补齐至拆单量失败"
            if fill_failed:
                reason += "（补单阶段异常或受限）"
            elif still_missing_buy or still_missing_sell:
                reason += "（市价补单成交量不足）"

            return self._result_failure(
                request,
                reason,
                order_buy=buy_order,
                order_sell=sell_order,
                failure_code="dual_partial_fill_incomplete",
            )

        missing_buy = not buy_completed
        shortfall = (
            sell_filled_dec - buy_filled_dec
            if missing_buy else
            buy_filled_dec - sell_filled_dec
        )
        fallback_quantity = shortfall if shortfall > epsilon else Decimal(
            '0')

        if fallback_quantity <= epsilon:
            await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
            await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
            return self._result_failure(
                request,
                "限价订单未产生需要补齐的成交差额",
                order_buy=buy_order,
                order_sell=sell_order,
            )

        return await self._handle_single_leg_shortfall(
            request=request,
            buy_order=buy_order,
            sell_order=sell_order,
            buy_adapter=buy_adapter,
            sell_adapter=sell_adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
            missing_buy=missing_buy,
            shortfall=fallback_quantity,
            target_quantity=quantity,
            epsilon=epsilon,
            cancel_opposite_order=False,
            result_quantity=Decimal(str(quantity)),
            market_retry_max_attempts=limit_limit_retry_max_attempts,
        )

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f"[套利执行] 双限价模式执行失败: {exc}", exc_info=True)
        return self._result_failure(
            request,
            str(exc),
        )

