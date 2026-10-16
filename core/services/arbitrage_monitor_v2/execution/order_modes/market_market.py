from __future__ import annotations

"""
市价 + 市价（market_market）执行链路

约束：
- 仅做代码搬迁：逻辑/日志/异常处理必须与原 `OrderStrategyExecutor.execute_market_market_mode` 一致
- 不引入新的公共抽象；仍通过 `self.xxx(...)` 调用原有 helper
"""

import asyncio
from decimal import Decimal
from typing import Any


async def execute_market_market_mode(
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

    if self._should_use_lighter_ws_batch(request, buy_adapter, sell_adapter):
        return await self._execute_lighter_ws_batch(
            request=request,
            adapter=buy_adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
        )

    try:
        buy_order_task = asyncio.create_task(
            executor._place_market_order(
                buy_adapter,
                buy_symbol,
                request.quantity,
                is_buy=True,
                reduce_only=(not request.is_open),
                request=request,
            )
        )

        sell_order_task = asyncio.create_task(
            executor._place_market_order(
                sell_adapter,
                sell_symbol,
                request.quantity,
                is_buy=False,
                reduce_only=(not request.is_open),
                request=request,
            )
        )

        buy_order, sell_order = await asyncio.gather(buy_order_task, sell_order_task)

        if not buy_order:
            buy_order = await self._retry_market_submission(
                adapter=buy_adapter,
                symbol=buy_symbol,
                quantity=request.quantity,
                is_buy=True,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=request.exchange_buy,
            )
        if not sell_order:
            sell_order = await self._retry_market_submission(
                adapter=sell_adapter,
                symbol=sell_symbol,
                quantity=request.quantity,
                is_buy=False,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=request.exchange_sell,
            )

        if not buy_order and not sell_order:
            logger.error(
                f"❌ [风险控制] 两个交易所订单都失败: {request.exchange_buy}, {request.exchange_sell}"
            )
            return self._result_failure(
                request,
                "两个交易所订单都提交失败",
            )

        if not buy_order or not sell_order:
            return await self._handle_market_market_submission_failure(
                request=request,
                buy_order=buy_order,
                sell_order=sell_order,
                buy_adapter=buy_adapter,
                sell_adapter=sell_adapter,
                buy_symbol=buy_symbol,
                sell_symbol=sell_symbol,
            )

        logger.info(
            f"[套利执行] 两个市价订单已提交: "
            f"买入订单={buy_order.id} 卖出订单={sell_order.id}"
        )

        try:
            buy_order, buy_filled_dec = await self._confirm_market_fill_with_retries(
                initial_order=buy_order,
                adapter=buy_adapter,
                symbol=buy_symbol,
                quantity=request.quantity,
                is_buy=True,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=request.exchange_buy,
            )
        except self.ReduceOnlyRestrictionError:
            executor.reduce_only_handler.register_reduce_only_event(
                request,
                request.exchange_buy,
                buy_symbol,
                closing_issue=(not request.is_open),
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
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=request.exchange_sell,
            )
        except self.ReduceOnlyRestrictionError:
            executor.reduce_only_handler.register_reduce_only_event(
                request,
                request.exchange_sell,
                sell_symbol,
                closing_issue=(not request.is_open),
                reason="卖单触发 reduce-only",
            )
            return self._result_failure(
                request,
                f"{request.exchange_sell} 市价订单因 reduce-only 受限",
                order_buy=buy_order,
                order_sell=sell_order,
            )

        epsilon = Decimal("0.00000001")
        target_qty = executor._to_decimal_value(request.quantity)

        if buy_filled_dec <= epsilon and sell_filled_dec <= epsilon:
            logger.error("❌ [风险控制] 两个订单都未成交")
            return self._result_failure(
                request,
                "两个订单都未成交",
                order_buy=buy_order,
                order_sell=sell_order,
            )

        # 🔥 关键修复：市价部分成交但未补齐 -> 先走“下单失败重试”路径：放大滑点再补齐；
        # 若仍失败，再走“平仓成功腿”路径（准确说：平掉超额那条腿，消除净敞口）。
        if (buy_filled_dec + epsilon < target_qty) or (sell_filled_dec + epsilon < target_qty):
            base_slip = executor._get_slippage_percent(request)
            retry_slip = (base_slip * Decimal("50")) if base_slip is not None else None
            max_retry = getattr(executor.config.order_execution, "max_retry_count", 3) or 3

            logger.warning(
                "⚠️ [套利执行] 市价补齐未达目标：buy=%s/%s sell=%s/%s，将放大滑点重试补齐（retry_slip=%s, max_retry=%s）",
                str(buy_filled_dec),
                str(target_qty),
                str(sell_filled_dec),
                str(target_qty),
                str(retry_slip) if retry_slip is not None else "None",
                str(max_retry),
            )

            # 临时放大 request 的 slippage_tolerance_pct，让 _confirm_market_fill_with_retries 使用更宽滑点
            orig_slip = getattr(request, "slippage_tolerance_pct", None)
            try:
                if retry_slip is not None:
                    try:
                        request.slippage_tolerance_pct = retry_slip  # type: ignore[attr-defined]
                    except Exception:
                        pass

                # 失败腿补齐：买腿/卖腿分别补齐剩余数量
                if buy_filled_dec + epsilon < target_qty:
                    remaining = max(Decimal("0"), target_qty - buy_filled_dec)
                    retry_buy_order, extra = await self._confirm_market_fill_with_retries(
                        initial_order=None,
                        adapter=buy_adapter,
                        symbol=buy_symbol,
                        quantity=remaining,
                        is_buy=True,
                        reduce_only=(not request.is_open),
                        request=request,
                        exchange_name=request.exchange_buy,
                        max_attempts=max_retry,
                        enable_internal_aggressive_retry=True,
                    )
                    buy_filled_dec += executor._to_decimal_value(extra)
                    # 🔧 更新订单引用，确保成交统计使用正确的订单信息
                    if retry_buy_order is not None:
                        buy_order = retry_buy_order

                if sell_filled_dec + epsilon < target_qty:
                    remaining = max(Decimal("0"), target_qty - sell_filled_dec)
                    retry_sell_order, extra = await self._confirm_market_fill_with_retries(
                        initial_order=None,
                        adapter=sell_adapter,
                        symbol=sell_symbol,
                        quantity=remaining,
                        is_buy=False,
                        reduce_only=(not request.is_open),
                        request=request,
                        exchange_name=request.exchange_sell,
                        max_attempts=max_retry,
                        enable_internal_aggressive_retry=True,
                    )
                    sell_filled_dec += executor._to_decimal_value(extra)
                    # 🔧 更新订单引用，确保成交统计使用正确的订单信息
                    if retry_sell_order is not None:
                        sell_order = retry_sell_order
            finally:
                # 恢复原始滑点设置，避免影响后续流程
                try:
                    request.slippage_tolerance_pct = orig_slip  # type: ignore[attr-defined]
                except Exception:
                    pass

            # 如果补齐成功：继续走正常成功路径
            if (buy_filled_dec + epsilon >= target_qty) and (sell_filled_dec + epsilon >= target_qty):
                logger.info(
                    "✅ [套利执行] 放大滑点重试补齐成功：buy=%s/%s sell=%s/%s",
                    str(buy_filled_dec),
                    str(target_qty),
                    str(sell_filled_dec),
                    str(target_qty),
                )
            else:
                # 仍补不齐：平掉“超额成交”的那条腿，消除净敞口，然后返回失败（交由上层单腿风险逻辑/暂停处理）
                actual_qty = min(buy_filled_dec, sell_filled_dec)
                excess_buy = max(Decimal("0"), buy_filled_dec - actual_qty)
                excess_sell = max(Decimal("0"), sell_filled_dec - actual_qty)

                if excess_buy > epsilon:
                    logger.error(
                        "❌ [风险控制] 市价补齐重试仍失败：buy=%s sell=%s，将平掉 buy 腿超额=%s 以消除净敞口",
                        str(buy_filled_dec),
                        str(sell_filled_dec),
                        str(excess_buy),
                    )
                    await executor._emergency_close_position(
                        exchange=request.exchange_buy,
                        adapter=buy_adapter,
                        symbol=buy_symbol,
                        quantity=Decimal(str(excess_buy)),
                        is_buy_to_close=False,  # 平掉多头
                        request=request,
                    )
                if excess_sell > epsilon:
                    logger.error(
                        "❌ [风险控制] 市价补齐重试仍失败：buy=%s sell=%s，将平掉 sell 腿超额=%s 以消除净敞口",
                        str(buy_filled_dec),
                        str(sell_filled_dec),
                        str(excess_sell),
                    )
                    await executor._emergency_close_position(
                        exchange=request.exchange_sell,
                        adapter=sell_adapter,
                        symbol=sell_symbol,
                        quantity=Decimal(str(excess_sell)),
                        is_buy_to_close=True,  # 平掉空头
                        request=request,
                    )

                return self._result_failure(
                    request,
                    "市价补齐失败：已平掉超额腿以消除净敞口（剩余对齐仓位保留）",
                    order_buy=buy_order,
                    order_sell=sell_order,
                    partial_failure=True,
                    failed_exchange="market_fill_incomplete",
                    success_exchange=None,
                    success_quantity=actual_qty,
                )

        if buy_filled_dec <= epsilon or sell_filled_dec <= epsilon:
            buy_order.filled = buy_filled_dec
            sell_order.filled = sell_filled_dec
            filled_exchange = request.exchange_sell if buy_filled_dec <= epsilon else request.exchange_buy
            unfilled_exchange = request.exchange_buy if buy_filled_dec <= epsilon else request.exchange_sell
            filled_quantity = sell_filled_dec if buy_filled_dec <= epsilon else buy_filled_dec
            filled_adapter = sell_adapter if buy_filled_dec <= epsilon else buy_adapter
            filled_symbol = sell_symbol if buy_filled_dec <= epsilon else buy_symbol
            is_buy_unfilled = (buy_filled_dec <= epsilon)

            logger.error(
                f"❌ [风险控制] 部分成交: {filled_exchange} 成交 {filled_quantity}, "
                f"{unfilled_exchange} 未成交。将平掉 {filled_exchange} 订单"
            )

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

        return Result(
            success=True,
            order_buy=buy_order,
            order_sell=sell_order,
            success_quantity=actual_quantity,
            slippage_pct=(slip or {}).get("slippage_pct"),
            high_slippage_threshold_pct=(slip or {}).get(
                "high_slippage_threshold_pct"),
        )

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f"[套利执行] 市价+市价模式执行失败: {exc}", exc_info=True)
        return self._result_failure(
            request,
            str(exc),
        )

