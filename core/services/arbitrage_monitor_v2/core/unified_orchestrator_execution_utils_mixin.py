"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：执行结果解析工具（成交量/成交价推断、数量安全转换等）。

来源：`core/services/arbitrage_monitor_v2/core/unified_orchestrator.py`
"""

from __future__ import annotations

import asyncio
import time
import logging
import time
import re
from typing import Dict, Optional, Set, List, Tuple, Any
from collections import defaultdict
from pathlib import Path
from decimal import Decimal, InvalidOperation
from dataclasses import asdict
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
import yaml

from ..config.symbol_config import SegmentedConfigManager
from ..config.monitor_config import ConfigManager as MonitorConfigManager
from ..config.debug_config import DebugConfig
from ..config.unified_config_manager import UnifiedConfigManager
from ..config.arbitrage_config import RiskControlConfig, ExecutionConfig, ExchangeFeeConfig
from ..config.multi_leg_pairs_config import MultiLegPairsConfigManager, MultiLegPairConfig
from ..config.multi_exchange_config import (
    MultiExchangeArbitrageConfigManager,
    TradingPair,
)
# 🔥 使用V2统一决策引擎（总量驱动算法）
from ..decision.unified_decision_engine import UnifiedDecisionEngine
from ..execution.arbitrage_executor import ArbitrageExecutor, ExecutionRequest, ExecutionResult
from ..analysis.spread_calculator import SpreadCalculator, SpreadData
from ..guards.reduce_only_guard import ReduceOnlyGuard
from ..data.data_receiver import DataReceiver
from ..data.data_processor import DataProcessor
from ..data.latest_store import LatestStore
from ..display.ui_manager import UIManager, UIMode
from ..display.realtime_scroller import RealtimeScroller
from ..utils.orchestrator_utils import ThrottledLogger, LiquidityFailureLogger
from ..utils.risk_control_utils import RiskControlUtils
from ..utils.ema_spread_gate import EmaSpreadGateState, EmaUpdateResult
from .orchestrator_ui_controller import OrchestratorUIController
from .spread_pipeline import SpreadPipeline
from .reduce_only_probe_service import ReduceOnlyProbeService
from .orchestrator_bootstrap import OrchestratorBootstrap
from ..models import FundingRateData, PositionSegment, SegmentedPosition
from ..risk_control.global_risk_controller import GlobalRiskController
from ..risk_control.open_only_close_guard import (
    OpenOnlyCloseGuard,
    OpenOnlyCloseConfig,
)
from .debug_state_printer import DebugStatePrinter
from ..state.symbol_state_manager import SymbolStateManager

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderBookData

from .unified_orchestrator_logging import logger


class UnifiedOrchestratorExecutionUtilsMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _extract_filled_quantity(
            self,
            execution_result: ExecutionResult,
            fallback: Decimal
        ) -> Decimal:
            """根据执行结果推断实际成交数量"""
            qty_decimal = self._safe_decimal(getattr(execution_result, "success_quantity", None))
            if qty_decimal > Decimal("0"):
                return qty_decimal

            derived = self._derive_quantity_from_orders(execution_result)
            if derived > Decimal("0"):
                return derived

            if fallback > Decimal("0"):
                logger.warning(
                    "⚠️ [统一调度] 执行结果未返回实际成交量，已按0处理 (预期=%s)",
                    fallback,
                )
            return Decimal("0")

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _derive_quantity_from_orders(self, execution_result: ExecutionResult) -> Decimal:
            quantities: List[Decimal] = []
            for order in (execution_result.order_buy, execution_result.order_sell):
                if order is None:
                    continue
                filled_value = getattr(order, "filled", None)
                filled_decimal = self._safe_decimal(filled_value)
                if filled_decimal > Decimal("0"):
                    quantities.append(filled_decimal)
            if len(quantities) == 2:
                return min(quantities)
            if len(quantities) == 1:
                return quantities[0]
            return Decimal("0")

    def _resolve_execution_price(
            self,
            order,
            fallback: Decimal
        ) -> Decimal:
            """
            获取订单的实际成交价，若不存在则回退到理论价格
            """
            from typing import Any, Optional

            def _safe_decimal(value: Any) -> Optional[Decimal]:
                """安全地将任意值转换为 Decimal"""
                if value is None:
                    return None
                if isinstance(value, Decimal):
                    return value
                try:
                    return Decimal(str(value))
                except:
                    return None

            price_candidates: List[Optional[Decimal]] = []

            if order:
                price_candidates.extend([
                    order.average,
                    order.price,
                ])

                # 兼容 trades / raw_data 中的价格
                for trade in order.trades or []:
                    price_candidates.append(_safe_decimal(trade.get("price")))

                if order.raw_data:
                    price_candidates.append(_safe_decimal(order.raw_data.get("price")))

            for candidate in price_candidates:
                if candidate is not None and candidate > Decimal("0"):
                    return candidate

            return fallback
