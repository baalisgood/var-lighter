"""
套利决策模块

职责：
- 实现开仓条件判断（价差套利、资金费率套利）
- 实现平仓条件判断
- 持仓状态管理
- 调用历史数据储存模块和数据分析模块的接口

注意：此模块只负责决策逻辑，不负责具体计算
"""

import logging
import time
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from ..config.arbitrage_config import (
    DecisionConfig,
    DecisionThresholdsConfig,
    ExchangeOrderModeConfig,
    ExchangeFeeConfig,
)
from ..analysis.spread_calculator import SpreadData
from ..history.history_calculator import HistoryDataCalculator
from ..models import (
    PositionInfo,
    FundingRateData,
    PositionLeg,
    ClosePositionContext,
)

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_decision.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)


class ArbitrageDecisionEngine:
    """套利决策引擎"""
    
    def __init__(
        self,
        decision_config: DecisionConfig,
        history_calculator: HistoryDataCalculator,
        exchange_order_modes: Optional[Dict[str, ExchangeOrderModeConfig]] = None,
        exchange_fee_config: Optional[Dict[str, ExchangeFeeConfig]] = None,
    ):
        """
        初始化套利决策引擎
        
        Args:
            decision_config: 决策配置
            history_calculator: 历史数据计算器
        """
        self.config = decision_config
        self.thresholds = decision_config.thresholds
        self.history_calculator = history_calculator
        self.exchange_order_modes: Dict[str, ExchangeOrderModeConfig] = dict(
            exchange_order_modes or {}
        )
        self.exchange_fee_config: Dict[str, ExchangeFeeConfig] = dict(
            exchange_fee_config or {}
        )
        if 'default' not in self.exchange_fee_config:
            self.exchange_fee_config['default'] = ExchangeFeeConfig()
        
        # 持仓管理
        self.positions: Dict[str, PositionInfo] = {}  # {symbol: PositionInfo}
        
        # 资金费率不利持续时间跟踪
        self.funding_rate_unfavorable_duration: Dict[str, datetime] = {}  # {symbol: 开始时间}
        
        # 🔥 资金费率警告日志频率控制（避免日志文件过大）
        self._funding_rate_warning_log_times: Dict[str, float] = {}  # {symbol: 最后打印时间}
        self._funding_rate_warning_log_interval = 60  # 每60秒最多打印一次
        
        # 🔥 决策状态日志节流（避免相同原因刷屏）
        self._decision_status_log_times: Dict[str, float] = {}
        self._decision_status_log_interval: float = 60.0  # 同一symbol+原因每60秒输出一次
        self._spread_persistence_state: Dict[str, Dict[str, int]] = {}

    def _log_decision_status(
        self,
        symbol: str,
        reason_code: str,
        message: str,
        level: int = logging.INFO
    ) -> None:
        """限频记录决策状态，避免同样原因反复刷屏"""
        key = f"{symbol}:{reason_code}"
        now = time.time()
        last = self._decision_status_log_times.get(key, 0.0)
        if now - last < self._decision_status_log_interval:
            return
        self._decision_status_log_times[key] = now
        logger.log(level, message)

    def _reset_spread_persistence(
        self,
        symbol: str,
        reason_code: Optional[str] = None,
        message: Optional[str] = None,
        level: int = logging.INFO,
        required_seconds: Optional[int] = None
    ) -> None:
        state = self._spread_persistence_state.pop(symbol, None)
        if (
            reason_code
            and message
            and state
            and required_seconds
            and required_seconds > 1
            and state.get('count', 0) > 0
        ):
            count = state.get('count', 0)
            self._log_decision_status(
                symbol,
                reason_code,
                f"{message}（已累计{count}/{required_seconds}秒）",
                level=level
            )

    def _update_spread_persistence(self, symbol: str, required_seconds: int) -> int:
        """
        更新价差持续性状态并返回当前连续秒数
        """
        if required_seconds <= 1:
            self._spread_persistence_state.pop(symbol, None)
            return required_seconds

        bucket = int(time.time())
        state = self._spread_persistence_state.setdefault(
            symbol, {'last_bucket': None, 'count': 0}
        )
        last_bucket = state['last_bucket']

        if last_bucket is None:
            state['count'] = 1
        elif bucket == last_bucket:
            # 已经在当前秒计数，保持不变
            pass
        elif bucket == last_bucket + 1:
            state['count'] += 1
        else:
            # 出现间隔，重新计数
            state['count'] = 1

        state['last_bucket'] = bucket
        return state['count']
    
    async def should_open_position(
        self,
        symbol: str,
        spread_data: Optional[SpreadData],
        funding_rate_data: Optional[FundingRateData]
    ) -> tuple[bool, Optional[str], Optional[str]]:
        """
        判断是否应该开仓
        
        Args:
            symbol: 交易对
            spread_data: 价差数据
            funding_rate_data: 资金费率数据
        
        Returns:
            (是否开仓, 开仓模式, 开仓条件)
            开仓模式: "spread" 或 "funding_rate"
            开仓条件: "condition1" 或 "condition2"
        """
        if not self.config.enabled:
            self._log_decision_status(
                symbol,
                "disabled",
                "[套利决策] 系统配置未启用套利决策，跳过"
            )
            return False, None, None
        
        # 检查是否已有持仓
        if symbol in self.positions and self.positions[symbol].is_open:
            self._log_decision_status(
                symbol,
                "existing_position",
                "[套利决策] 该交易对已有开放持仓，等待平仓"
            )
            return False, None, None
        
        # 检查价差套利条件
        if self.config.enable_spread_arbitrage:
            should_open, mode, condition = await self._check_spread_arbitrage_conditions(
                symbol, spread_data, funding_rate_data
            )
            if should_open:
                return True, mode, condition
        
        # 检查资金费率套利条件
        if self.config.enable_funding_rate_arbitrage:
            should_open, mode, condition = await self._check_funding_rate_arbitrage_conditions(
                symbol, spread_data, funding_rate_data
            )
            if should_open:
                return True, mode, condition
        
        self._log_decision_status(
            symbol,
            "conditions_not_met",
            "[套利决策] 所有套利条件均未满足"
        )
        return False, None, None
    
    async def _check_spread_arbitrage_conditions(
        self,
        symbol: str,
        spread_data: Optional[SpreadData],
        funding_rate_data: Optional[FundingRateData]
    ) -> tuple[bool, Optional[str], Optional[str]]:
        """
        检查价差套利开仓条件
        
        首要必须条件：
        - 当前价差 ≥ X（价差套利触发阈值）
        - 扣除手续费后的价差仍为正
        
        其他条件（需同时满足）：
        1. 资金费率条件：可以赚取资金费率（无论大小）或需要支付但年化 < F
        2. 🔥 天然价差条件：真实套利空间 = 当前价差 - max(天然价差, 0) ≥ X
           - 天然价差为正数：扣除后计算真实套利空间
           - 天然价差为负数：当作0处理（负数天然价差毫无意义）
           - 数据不足时拒绝开仓（安全第一）
        3. 🔥 数据新鲜度条件：天然价差数据必须在200秒内更新
           - 超过200秒视为过期，拒绝开仓
        """
        persistence_required = max(1, int(getattr(self.thresholds, 'spread_persistence_seconds', 1) or 1))

        if not spread_data:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_no_data",
                message="[套利决策] 价差数据缺失，连续监控中断",
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:no_data",
                "[套利决策] 价差数据缺失，无法判断价差套利条件"
            )
            return False, None, None
        
        buy_fee_pct, sell_fee_pct = self._calculate_fee_breakdown(spread_data)
        total_fee_pct = buy_fee_pct + sell_fee_pct
        net_spread_pct = spread_data.spread_pct - total_fee_pct
        
        if net_spread_pct <= 0:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_net_negative",
                message="[套利决策] 净价差降至≤0，连续监控中断",
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:net_negative",
                f"[套利决策] 扣除手续费后无正价差 (净价差={net_spread_pct:.4f}%)"
            )
            return False, None, None
        
        # 首要必须条件：价差 ≥ spread_arbitrage_threshold
        if net_spread_pct < self.thresholds.spread_arbitrage_threshold:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_below_threshold",
                message="[套利决策] 净价差低于阈值，连续监控中断",
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:below_threshold",
                (f"[套利决策] 净价差 {net_spread_pct:.4f}% 低于阈值 "
                 f"{self.thresholds.spread_arbitrage_threshold:.4f}%")
            )
            return False, None, None
        
        # 🔥 获取天然价差（可正可负，或None表示数据不足）
        natural_spread, last_update = await self.history_calculator.get_natural_spread(
            symbol, spread_data.exchange_buy, spread_data.exchange_sell
        )
        
        # 🔥 数据不足处理：拒绝开仓
        if natural_spread is None or last_update is None:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_natural_insufficient",
                message="[套利决策] 天然价差数据不足，连续监控中断",
                level=logging.WARNING,
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:natural_insufficient",
                (f"[套利决策] {symbol} {spread_data.exchange_buy}→{spread_data.exchange_sell}: "
                 "天然价差数据不足（<10个数据点或运行时间<3分钟）"),
                level=logging.WARNING
            )
            return False, None, None
        
        # 🔥 数据新鲜度检查：必须是200秒内的新鲜数据
        data_age_seconds = (datetime.now() - last_update).total_seconds()
        if data_age_seconds > 200:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_natural_stale",
                message="[套利决策] 天然价差数据过期，连续监控中断",
                level=logging.WARNING,
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:natural_stale",
                (f"[套利决策] {symbol} {spread_data.exchange_buy}→{spread_data.exchange_sell}: "
                 f"天然价差数据过期（{data_age_seconds:.0f}秒前，要求200秒内）"),
                level=logging.WARNING
            )
            return False, None, None
        
        # 🔥 负数天然价差当作0处理（负数天然价差毫无意义）
        effective_natural_spread = max(0.0, natural_spread)
        
        # 🔥 计算真实套利空间和实际触发点
        real_arbitrage_space = net_spread_pct - effective_natural_spread
        actual_trigger_point = (
            self.thresholds.spread_arbitrage_threshold
            + effective_natural_spread
            + total_fee_pct
        )
        
        # 🔥 天然价差条件：真实套利空间 ≥ 阈值
        if real_arbitrage_space < self.thresholds.spread_arbitrage_threshold:
            self._reset_spread_persistence(
                symbol,
                reason_code="spread:persistence_reset_real_space",
                message="[套利决策] 真实套利空间不足，连续监控中断",
                required_seconds=persistence_required
            )
            self._log_decision_status(
                symbol,
                "spread:real_space_insufficient",
                (f"[套利决策] {symbol} {spread_data.exchange_buy}→{spread_data.exchange_sell}: "
                 f"真实套利空间不足 ({real_arbitrage_space:.4f}% < 阈值 "
                 f"{self.thresholds.spread_arbitrage_threshold:.4f}%)")
            )
            return False, None, None

        if persistence_required > 1:
            current_seconds = self._update_spread_persistence(symbol, persistence_required)
            if current_seconds < persistence_required:
                self._log_decision_status(
                    symbol,
                    "spread:persistence_pending",
                    (f"[套利决策] 价差需要连续{persistence_required}秒，目前仅满足"
                     f"{current_seconds}秒")
                )
                return False, None, None
            # 🔥 满足持续性要求，但保持状态（与分段模式逻辑一致）
            # 虽然基础模式有 round_pause_seconds 保护，但保持状态更合理
        else:
            self._reset_spread_persistence(symbol)
        
        # 🔥 满足条件，打印详细日志（不清除状态，保持连续性）
        logger.info(
            f"✅ [套利机会] {symbol} {spread_data.exchange_buy}→{spread_data.exchange_sell}\n"
            f"   当前价差: {spread_data.spread_pct:.4f}%\n"
            f"   交易手续费: 总计 {total_fee_pct:.4f}% "
            f"(买入 {buy_fee_pct:.4f}%, 卖出 {sell_fee_pct:.4f}%)\n"
            f"   净价差: {net_spread_pct:.4f}%\n"
            f"   天然价差: {natural_spread:+.4f}% {'(当作0处理)' if natural_spread < 0 else ''}\n"
            f"   有效天然价差: {effective_natural_spread:.4f}%\n"
            f"   真实套利空间: {real_arbitrage_space:.4f}% ✅\n"
            f"   基础阈值: {self.thresholds.spread_arbitrage_threshold:.4f}%\n"
            f"   实际触发点: {actual_trigger_point:.4f}% "
            f"(基础阈值 + 天然价差 + 手续费)\n"
            f"   天然价差更新: {data_age_seconds:.0f}秒前 (历史统计数据，每60秒更新)"
        )
        
        # 资金费率条件
        if funding_rate_data:
            # 可以赚取资金费率（资金费率差 > 0）
            if funding_rate_data.funding_rate_diff > 0:
                return True, "spread", "condition1"
            
            # 需要支付资金费率，但年化 < funding_rate_annual_threshold
            if funding_rate_data.funding_rate_diff < 0:
                if abs(funding_rate_data.funding_rate_diff_annual) < self.thresholds.funding_rate_annual_threshold:
                    return True, "spread", "condition1"
        
        # 如果没有资金费率数据，默认允许开仓（但记录警告）
        self._log_decision_status(
            symbol,
            "spread:no_funding_data",
            "[套利决策] 缺少资金费率数据，但价差条件满足，放行",
            level=logging.WARNING
        )
        return True, "spread", "condition1"
    
    async def _check_funding_rate_arbitrage_conditions(
        self,
        symbol: str,
        spread_data: Optional[SpreadData],
        funding_rate_data: Optional[FundingRateData]
    ) -> tuple[bool, Optional[str], Optional[str]]:
        """
        检查资金费率套利开仓条件
        
        首要必须条件：
        - 价差无利可图（两个方向的价差均为负数）
        - 资金费率差 > 0（可以赚取资金费率）
        - 🔥 方向一致性：费率方向必须有利于持仓方向
        
        其他条件（需同时满足）：
        1. 价差亏损 < Q（价差亏损百分比阈值）
        2. 历史资金费率差稳定
        3. 历史价差稳定
        4. 资金费率差 ≥ Y（资金费率差套利触发阈值）
        """
        if not funding_rate_data or funding_rate_data.funding_rate_diff <= 0:
            self._log_decision_status(
                symbol,
                "funding:insufficient_diff",
                "[套利决策] 资金费率差不足，无法进行资金费率套利"
            )
            return False, None, None
        
        # 🔥 关键检查：方向必须有利
        # 如果 funding_rate_sell < funding_rate_buy，说明开仓方向不利（会亏损）
        if not funding_rate_data.is_favorable_for_position:
            self._log_decision_status(
                symbol,
                "funding:direction_unfavorable",
                (f"[套利决策] 资金费率方向不利 "
                 f"(buy={funding_rate_data.funding_rate_buy:.4f}%, "
                 f"sell={funding_rate_data.funding_rate_sell:.4f}%)")
            )
            return False, None, None
        
        # 首要必须条件：资金费率差 ≥ funding_rate_diff_threshold
        if funding_rate_data.funding_rate_diff < self.thresholds.funding_rate_diff_threshold:
            self._log_decision_status(
                symbol,
                "funding:below_threshold",
                (f"[套利决策] 资金费率差 {funding_rate_data.funding_rate_diff:.4f}% "
                 f"低于阈值 {self.thresholds.funding_rate_diff_threshold:.4f}%")
            )
            return False, None, None
        
        # 检查价差无利可图（两个方向都为负）
        if spread_data:
            buy_fee_pct, sell_fee_pct = self._calculate_fee_breakdown(spread_data)
            total_fee_pct = buy_fee_pct + sell_fee_pct
            net_spread_pct = spread_data.spread_pct - total_fee_pct
            
            # 如果价差（扣除手续费后）有利可图，不符合资金费率套利条件
            if net_spread_pct > 0:
                self._log_decision_status(
                    symbol,
                    "funding:spread_profitable",
                    "[套利决策] 价差仍有利可图，不进入资金费率套利路径"
                )
                return False, None, None
            
            # 价差亏损条件：价差亏损 < loss_stop_threshold
            spread_loss = abs(net_spread_pct)
            if spread_loss >= self.thresholds.loss_stop_threshold:
                self._log_decision_status(
                    symbol,
                    "funding:spread_loss_too_high",
                    (f"[套利决策] 价差亏损 {spread_loss:.4f}% ≥ 止损阈值 "
                     f"{self.thresholds.loss_stop_threshold:.4f}%")
                )
                return False, None, None
        else:
            # 如果没有价差数据，假设价差无利可图
            self._log_decision_status(
                symbol,
                "funding:no_spread_data",
                "[套利决策] 缺少价差数据，假设价差无利可图",
                level=logging.WARNING
            )
        
        # 获取历史稳定性
        exchange_buy = spread_data.exchange_buy if spread_data else "unknown"
        exchange_sell = spread_data.exchange_sell if spread_data else "unknown"
        
        funding_rate_stable = await self.history_calculator.is_funding_rate_stable(
            symbol, exchange_buy, exchange_sell
        )
        
        spread_stable = await self.history_calculator.is_spread_stable(
            symbol, exchange_buy, exchange_sell
        )
        
        # 历史稳定性条件
        if not funding_rate_stable or not spread_stable:
            self._log_decision_status(
                symbol,
                "funding:history_unstable",
                "[套利决策] 历史资金费率或价差不稳定，拒绝资金费率套利"
            )
            return False, None, None
        
        return True, "funding_rate", "condition1"
    
    def should_close_position(
        self,
        symbol: str,
        spread_data: Optional[SpreadData],
        funding_rate_data: Optional[FundingRateData],
        close_context: Optional[ClosePositionContext] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        判断是否应该平仓
        
        平仓条件（三个条件触发其中一个就平仓）：
        1. 平仓有利可图（盈利 ≥ P）
        2. 价差亏损止损（亏损 ≥ Q）
        3. 资金费率不利止损（年化 ≥ F 且 持续时间 ≥ M）
        
        Args:
            symbol: 交易对
            spread_data: 当前价差数据
            funding_rate_data: 当前资金费率数据
        
        Returns:
            (是否平仓, 平仓原因)
        """
        if symbol not in self.positions or not self.positions[symbol].is_open:
            return False, None
        
        position = self.positions[symbol]
        
        if not spread_data:
            return False, None
        
        # 条件1：平仓有利可图（盈利 ≥ profit_target_threshold）
        if close_context:
            current_profit_pct = close_context.total_profit_pct
        else:
            current_profit_pct = self._calculate_profit_percentage(position, spread_data)
        if current_profit_pct >= self.thresholds.profit_target_threshold:
            return True, "profit_target_reached"
        
        # 条件2：价差亏损止损（亏损 ≥ loss_stop_threshold）
        current_loss_pct = abs(current_profit_pct) if current_profit_pct < 0 else 0
        if current_loss_pct >= self.thresholds.loss_stop_threshold:
            return True, "spread_loss_stop_loss"
        
        # 条件3：资金费率不利止损（年化 ≥ F 且 持续时间 ≥ M）
        if funding_rate_data:
            if self._check_funding_rate_unfavorable(position, funding_rate_data):
                return True, "funding_rate_unfavorable_stop_loss"
        
        return False, None
    
    def _calculate_profit_percentage(
        self,
        position: PositionInfo,
        current_spread_data: SpreadData
    ) -> float:
        """
        计算当前盈利百分比
        
        基于开仓时的价差和当前价差计算
        
        Args:
            position: 持仓信息
            current_spread_data: 当前价差数据
        
        Returns:
            盈利百分比（正数表示盈利，负数表示亏损）
        """
        # 计算当前价差百分比
        current_spread_pct = current_spread_data.spread_pct
        
        # 计算盈利百分比 = 当前价差 - 开仓价差
        profit_pct = current_spread_pct - position.open_spread_pct
        
        return profit_pct
    
    def _check_funding_rate_unfavorable(
        self,
        position: PositionInfo,
        current_funding_rate_data: FundingRateData
    ) -> bool:
        """
        检查资金费率是否不利
        
        条件（满足任一即为不利）：
        1. 🔥 方向反转：当前费率方向对持仓不利（is_favorable_for_position = False）
        2. 费率差过大：资金费率差年化 ≥ F（需要支付大量资金费率）
        
        触发平仓：不利情况持续时间 ≥ M（分钟）
        
        Args:
            position: 持仓信息
            current_funding_rate_data: 当前资金费率数据
        
        Returns:
            是否触发止损
        """
        # 🔥 关键检查1：方向是否反转
        is_unfavorable = False
        
        if not current_funding_rate_data.is_favorable_for_position:
            # 方向反转：原本 sell > buy，现在 buy > sell
            # 持仓方向变成需要支付费率
            is_unfavorable = True
            
            # 🔥 频率控制：每60秒最多打印一次，避免日志文件过大
            symbol = position.symbol
            current_time = time.time()
            last_log_time = self._funding_rate_warning_log_times.get(symbol, 0)
            
            if current_time - last_log_time >= self._funding_rate_warning_log_interval:
                logger.warning(
                    f"[套利决策] {symbol}: ⚠️ 资金费率方向反转！ "
                    f"(buy={current_funding_rate_data.funding_rate_buy:.4f}%, "
                    f"sell={current_funding_rate_data.funding_rate_sell:.4f}%)"
                )
                self._funding_rate_warning_log_times[symbol] = current_time
        
        # 检查2：费率差是否过大（年化 ≥ funding_rate_annual_threshold）
        elif abs(current_funding_rate_data.funding_rate_diff_annual) >= self.thresholds.funding_rate_annual_threshold:
            is_unfavorable = True
            
            # 🔥 频率控制：每60秒最多打印一次，避免日志文件过大
            symbol = position.symbol
            current_time = time.time()
            last_log_time = self._funding_rate_warning_log_times.get(symbol, 0)
            
            if current_time - last_log_time >= self._funding_rate_warning_log_interval:
                logger.warning(
                    f"[套利决策] {symbol}: ⚠️ 资金费率差过大 "
                    f"({current_funding_rate_data.funding_rate_diff_annual:.2f}%/年)"
                )
                self._funding_rate_warning_log_times[symbol] = current_time
        
        # 如果资金费率有利，清除不利持续时间跟踪
        if not is_unfavorable:
            symbol = position.symbol
            if symbol in self.funding_rate_unfavorable_duration:
                logger.info(f"[套利决策] {symbol}: ✅ 资金费率恢复有利状态")
                del self.funding_rate_unfavorable_duration[symbol]
                # 🔥 清除警告日志时间记录，允许下次状态变化时立即记录
                if symbol in self._funding_rate_warning_log_times:
                    del self._funding_rate_warning_log_times[symbol]
            return False
        
        # 检查是否开始不利
        if position.symbol not in self.funding_rate_unfavorable_duration:
            self.funding_rate_unfavorable_duration[position.symbol] = datetime.now()
            return False
        
        # 检查持续时间
        start_time = self.funding_rate_unfavorable_duration[position.symbol]
        duration_minutes = (datetime.now() - start_time).total_seconds() / 60
        
        if duration_minutes >= self.thresholds.unfavorable_funding_rate_duration_minutes:
            return True
        
        return False
    
    def _calculate_fee_breakdown(self, spread_data: SpreadData) -> Tuple[float, float]:
        """
        计算买入腿与卖出腿的手续费百分比（单位：%）
        """
        buy_mode = self._resolve_order_mode(spread_data.exchange_buy, is_buy=True)
        sell_mode = self._resolve_order_mode(spread_data.exchange_sell, is_buy=False)
        buy_fee_pct = self._get_fee_rate_percent(spread_data.exchange_buy, buy_mode)
        sell_fee_pct = self._get_fee_rate_percent(spread_data.exchange_sell, sell_mode)
        return buy_fee_pct, sell_fee_pct
    
    def _resolve_order_mode(self, exchange: str, is_buy: bool) -> str:
        """
        根据配置解析交易所当前腿使用的下单模式
        """
        config = (
            self.exchange_order_modes.get(exchange)
            or self.exchange_order_modes.get('default')
        )
        if config:
            order_mode = getattr(config, 'order_mode', None)
            if isinstance(order_mode, str) and order_mode.lower() in ('limit', 'market'):
                return order_mode.lower()
            
            legacy_field = getattr(config, 'buy_mode' if is_buy else 'sell_mode', None)
            if isinstance(legacy_field, str) and legacy_field.lower() in ('limit', 'market'):
                return legacy_field.lower()
        
        return 'market'
    
    def _get_fee_rate_percent(self, exchange: str, order_mode: str) -> float:
        fee_config = self._get_fee_config(exchange)
        if order_mode == 'limit':
            fee_rate = fee_config.limit_fee_rate
        else:
            fee_rate = fee_config.market_fee_rate
        return float(fee_rate) * 100
    
    def _get_fee_config(self, exchange: str) -> ExchangeFeeConfig:
        fee_config = self.exchange_fee_config.get(exchange)
        if not fee_config:
            fee_config = self.exchange_fee_config.get('default')
        if not fee_config:
            fee_config = ExchangeFeeConfig()
        return fee_config
    
    async def record_open_position(
        self,
        symbol: str,
        spread_data: SpreadData,
        funding_rate_data: Optional[FundingRateData],
        open_mode: str,
        open_condition: str,
        quantity: Decimal,
        leg: Optional[PositionLeg] = None
    ):
        """
        记录开仓信息
        
        Args:
            symbol: 交易对
            spread_data: 价差数据
            funding_rate_data: 资金费率数据
            open_mode: 开仓模式
            open_condition: 开仓条件
            quantity: 持仓数量
        """
        if symbol in self.positions and self.positions[symbol].is_open:
            position = self.positions[symbol]
            position.quantity += quantity
            position.quantity_buy += quantity
            position.quantity_sell += quantity
            if leg:
                if leg.executed_quantity is None or leg.executed_quantity <= Decimal('0'):
                    leg.executed_quantity = leg.quantity
                position.legs.append(leg)
        else:
            if leg:
                if leg.executed_quantity is None or leg.executed_quantity <= Decimal('0'):
                    leg.executed_quantity = leg.quantity
            position = PositionInfo(
                symbol=symbol,
                exchange_buy=spread_data.exchange_buy,
                exchange_sell=spread_data.exchange_sell,
                open_mode=open_mode,
                open_condition=open_condition,
                open_spread_pct=spread_data.spread_pct,
                open_funding_rate_diff=funding_rate_data.funding_rate_diff if funding_rate_data else 0.0,
                open_funding_rate_diff_annual=funding_rate_data.funding_rate_diff_annual if funding_rate_data else 0.0,
                open_price_buy=spread_data.price_buy,
                open_price_sell=spread_data.price_sell,
                open_funding_rate_buy=funding_rate_data.funding_rate_buy if funding_rate_data else 0.0,
                open_funding_rate_sell=funding_rate_data.funding_rate_sell if funding_rate_data else 0.0,
                open_time=datetime.now(),
                quantity=quantity,
                quantity_buy=quantity,
                quantity_sell=quantity,
                legs=[leg] if leg else [],
                is_open=True
            )
            self.positions[symbol] = position
        
        logger.info(
            f"✅ [套利决策] 开仓记录: {symbol} "
            f"模式={open_mode} 条件={open_condition} "
            f"价差={spread_data.spread_pct:.3f}% "
            f"数量={quantity}"
        )
    
    async def record_close_position(self, symbol: str, reason: str):
        """
        记录平仓信息
        
        Args:
            symbol: 交易对
            reason: 平仓原因
        """
        if symbol not in self.positions:
            return
        
        position = self.positions[symbol]
        position.is_open = False
        
        # 清除资金费率不利持续时间跟踪
        if symbol in self.funding_rate_unfavorable_duration:
            del self.funding_rate_unfavorable_duration[symbol]
        
        logger.info(
            f"🛑 [套利决策] 平仓记录: {symbol} 原因={reason} "
            f"持仓时间={(datetime.now() - position.open_time).total_seconds() / 60:.1f}分钟"
        )
    
    def get_position(self, symbol: str) -> Optional[PositionInfo]:
        """获取持仓信息"""
        return self.positions.get(symbol)
    
    def has_open_position(self, symbol: str) -> bool:
        """检查是否有持仓"""
        return (
            symbol in self.positions
            and self.positions[symbol].is_open
        )

