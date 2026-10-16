"""
套利对状态管理器 (Symbol State Manager)

功能说明：
    管理每个套利对的运行/暂停状态，用于“短暂等待/避让”等场景。
    
核心功能：
    1. defer() - 将套利对标记为"等待"状态（暂停执行）
    2. resume() - 恢复套利对的正常执行
    3. should_block() - 检查套利对是否应该被阻止执行
    
暂停触发场景（示例）：
    - 开仓失败后短暂等待，避免同一网格重复触发
    
恢复机制（仅针对本文件的“单 symbol 等待”）：
    - 自动恢复：当价差跨越网格级别时（如从T1变为T2）
    - 手动恢复：调用 resume() 方法

⚠️ 注意：
    - 本文件不负责“全局暂停（禁止开仓/平仓）”的恢复机制。
      全局暂停（连续失败暂停）由 GlobalRiskController 管理，到期自动恢复。
    
特点：
    - 线程安全（使用RLock保护状态）
    - 日志节流（避免频繁打印相同状态）
    - 不影响全局风控，仅针对单个套利对
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import RLock
from typing import Dict, Optional, Tuple, Any

from core.adapters.exchanges.utils.setup_logging import LoggingConfig


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file="unified_orchestrator.log",
    console_formatter=None,
    file_formatter="detailed",
    level=logging.INFO,
)
logger.propagate = False


@dataclass
class SymbolState:
    status: str  # running | waiting
    reason: str
    grid_level: Optional[int]
    exchange_buy: Optional[str]
    exchange_sell: Optional[str]
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["updated_at"] = self.updated_at.isoformat()
        return data


class SymbolStateManager:
    """按套利对维护运行/等待状态，不影响全局风险控制。"""

    def __init__(self) -> None:
        self._states: Dict[str, SymbolState] = {}
        self._lock = RLock()
        self._log_times: Dict[str, float] = {}

    def defer(
        self,
        symbol: str,
        reason: str,
        grid_level: Optional[int] = None,
        *,
        exchange_buy: Optional[str] = None,
        exchange_sell: Optional[str] = None,
    ) -> None:
        if not symbol:
            return
        key = symbol.upper()
        now = time.time()
        last = self._log_times.get(f"defer:{key}", 0.0)
        state = SymbolState(
            status="waiting",
            reason=reason,
            grid_level=grid_level,
            exchange_buy=exchange_buy,
            exchange_sell=exchange_sell,
            updated_at=datetime.now(),
        )
        with self._lock:
            self._states[key] = state
        # 节流：同一符号20秒内只打印一次等待日志
        if now - last >= 20:
            self._log_times[f"defer:{key}"] = now
            logger.warning(
                "⏱️ [符号等待] %s: %s (T%s)",
                key,
                reason,
                grid_level if grid_level is not None else "-",
            )

    def resume(self, symbol: str, *, cause: Optional[str] = None) -> None:
        if not symbol:
            return
        key = symbol.upper()
        with self._lock:
            if key not in self._states:
                return
            state = self._states.pop(key)
        now = time.time()
        last = self._log_times.get(f"resume:{key}", 0.0)
        # 节流：同一符号20秒内只打印一次恢复日志
        if now - last >= 20:
            self._log_times[f"resume:{key}"] = now
            logger.info(
                "▶️ [符号恢复] %s: %s",
                key,
                cause or state.reason,
            )

    def get_state(self, symbol: str) -> Optional[SymbolState]:
        key = symbol.upper()
        with self._lock:
            return self._states.get(key)

    def is_waiting(self, symbol: str) -> bool:
        state = self.get_state(symbol)
        return bool(state and state.status == "waiting")

    def should_block(self, symbol: str, current_grid: Optional[int]) -> Tuple[bool, Optional[SymbolState]]:
        state = self.get_state(symbol)
        if not state or state.status != "waiting":
            return False, None

        if (
            current_grid is not None
            and state.grid_level is not None
            and current_grid != state.grid_level
        ):
            self.resume(symbol, cause="网格级别发生变化，自动恢复执行")
            return False, None
        return True, state

    def list_states(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {symbol: state.to_dict() for symbol, state in self._states.items()}

