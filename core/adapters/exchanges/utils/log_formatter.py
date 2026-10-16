"""
优化的日志格式化器

提供简洁清晰的日志格式，同时保留完整信息
"""

import logging
from datetime import datetime


class CompactFormatter(logging.Formatter):
    """
    简洁格式化器

    优化点：
    1. 简化模块路径显示（只显示关键部分）
    2. 使用标签分类日志类型
    3. 关键信息前置
    4. 详细信息缩进显示
    """

    # 模块路径简化映射
    MODULE_SHORTCUTS = {
        'core.adapters.exchanges.adapters.lighter_websocket': 'WS',
        'core.adapters.exchanges.adapters.lighter_rest': 'REST',
        'core.services.grid.implementations.grid_engine_impl': 'Engine',
        'core.services.grid.coordinator.grid_coordinator': 'Coord',
        'core.services.grid.implementations.grid_strategy_impl': 'Strategy',
        'core.services.grid.implementations.order_health_checker': 'Health',
    }

    # 日志级别简化
    LEVEL_SHORTCUTS = {
        'DEBUG': 'D',
        'INFO': 'I',
        'WARNING': 'W',
        'ERROR': 'E',
        'CRITICAL': 'C',
    }

    def format(self, record):
        """格式化日志记录"""
        # 简化模块名
        module_name = record.name
        for full_path, shortcut in self.MODULE_SHORTCUTS.items():
            if module_name.startswith(full_path):
                module_name = shortcut
                break
        else:
            # 如果没有匹配，取最后两段
            parts = module_name.split('.')
            module_name = '.'.join(parts[-2:]) if len(parts) > 1 else parts[0]

        # 简化日志级别
        level = self.LEVEL_SHORTCUTS.get(record.levelname, record.levelname[0])

        # 格式化时间（只保留时:分:秒）
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')

        # 构建简洁的日志消息
        # 格式: HH:MM:SS [模块] 级别 - 消息
        formatted = f"{timestamp} [{module_name:8s}] {level} - {record.getMessage()}"

        # 如果有异常信息，添加到下一行并缩进
        if record.exc_info:
            import traceback
            exc_text = ''.join(traceback.format_exception(*record.exc_info))
            formatted += '\n' + \
                '\n'.join(
                    f"  │ {line}" for line in exc_text.split('\n') if line)

        return formatted


class DetailedFormatter(logging.Formatter):
    """
    详细格式化器（保留完整信息，用于文件日志）

    保留所有信息，但使用更清晰的布局
    """

    def format(self, record):
        """格式化日志记录"""
        timestamp = datetime.fromtimestamp(
            record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # 提取关键信息
        module = record.name
        level = record.levelname
        func = record.funcName
        lineno = record.lineno
        message = record.getMessage()

        # 检测消息类型并添加标签
        tag = self._detect_message_type(message)

        # 构建格式化消息
        # 文件日志不做固定列宽对齐：避免 logger name 被填充大量空格，影响阅读/复制
        formatted = f"{timestamp} [{level:8s}] [{module}] {func}:{lineno}"
        if tag:
            formatted += f" [{tag}]"
        formatted += f" - {message}"

        # 如果有异常信息，添加到下一行
        if record.exc_info:
            import traceback
            exc_text = ''.join(traceback.format_exception(*record.exc_info))
            formatted += '\n' + exc_text

        return formatted

    def _detect_message_type(self, message: str) -> str:
        """检测消息类型并返回标签"""
        if any(keyword in message for keyword in ['下单', '订单推送', '订单完全成交', 'place_order']):
            return 'ORDER'
        elif any(keyword in message for keyword in ['WebSocket', 'ws', '推送', 'subscription']):
            return 'WS'
        elif any(keyword in message for keyword in ['同步', 'sync', '映射']):
            return 'SYNC'
        elif any(keyword in message for keyword in ['健康检查', 'health']):
            return 'HEALTH'
        elif any(keyword in message for keyword in ['价格', 'price', 'market_stats']):
            return 'PRICE'
        return ''


class ColoredFormatter(logging.Formatter):
    """
    带颜色的格式化器（用于终端输出）

    使用ANSI颜色码来区分不同级别和类型的日志
    """

    # ANSI颜色码
    COLORS = {
        'DEBUG': '\033[36m',      # 青色
        'INFO': '\033[32m',       # 绿色
        'WARNING': '\033[33m',    # 黄色
        'ERROR': '\033[31m',      # 红色
        'CRITICAL': '\033[35m',   # 紫色
        'RESET': '\033[0m',       # 重置
        'BOLD': '\033[1m',        # 粗体
        'DIM': '\033[2m',         # 暗色
    }

    # 消息类型颜色
    TYPE_COLORS = {
        'ORDER': '\033[94m',      # 亮蓝色
        'WS': '\033[96m',         # 亮青色
        'SYNC': '\033[93m',       # 亮黄色
        'HEALTH': '\033[92m',     # 亮绿色
        'PRICE': '\033[90m',      # 灰色
    }

    def format(self, record):
        """格式化日志记录（带颜色）"""
        # 获取颜色
        level_color = self.COLORS.get(record.levelname, '')
        reset = self.COLORS['RESET']
        dim = self.COLORS['DIM']

        # 简化时间戳
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')

        # 简化模块名（只保留最后一段）
        module = record.name.split('.')[-1]

        # 检测消息类型
        message = record.getMessage()
        msg_type = self._detect_message_type(message)
        type_color = self.TYPE_COLORS.get(msg_type, '')

        # 构建彩色日志
        formatted = (
            f"{dim}{timestamp}{reset} "
            f"{level_color}[{record.levelname[0]}]{reset} "
            f"{dim}[{module:12s}]{reset} "
        )

        if msg_type:
            formatted += f"{type_color}[{msg_type}]{reset} "

        formatted += message

        return formatted

    def _detect_message_type(self, message: str) -> str:
        """检测消息类型"""
        if '下单' in message or '订单推送' in message or '订单完全成交' in message:
            return 'ORDER'
        elif 'WebSocket' in message or 'ws' in message.lower() or '推送' in message:
            return 'WS'
        elif '同步' in message or 'sync' in message.lower():
            return 'SYNC'
        elif '健康检查' in message or 'health' in message.lower():
            return 'HEALTH'
        elif '价格' in message or 'price' in message.lower():
            return 'PRICE'
        return ''


def simplify_order_id(order_id: str) -> str:
    """
    简化订单ID显示（但保留完整ID在详细信息中）

    Args:
        order_id: 完整订单ID

    Returns:
        简化的订单ID（显示前6位...后4位）
    """
    if not order_id or len(order_id) <= 10:
        return order_id

    return f"{order_id[:6]}...{order_id[-4:]}"


def format_order_log(order_type: str, side: str, amount: str, price: str,
                     order_id: str, grid_id: int = None, status: str = None) -> str:
    """
    格式化订单日志消息

    Args:
        order_type: 订单类型（下单/成交/取消）
        side: 方向（buy/sell）
        amount: 数量
        price: 价格
        order_id: 订单ID
        grid_id: 网格ID（可选）
        status: 状态（可选）

    Returns:
        格式化的日志消息
    """
    # 简化订单ID
    short_id = simplify_order_id(order_id)

    # 方向emoji
    side_emoji = "🟢" if side.lower() == "buy" else "🔴"

    # 构建主消息
    msg = f"{order_type} {side_emoji} {side.upper()} {amount}@{price}"

    # 添加网格信息
    if grid_id is not None:
        msg += f" [Grid{grid_id}]"

    # 添加订单ID
    msg += f" ID:{short_id}"

    # 添加状态
    if status:
        status_emoji = {
            'filled': '✅',
            'open': '📝',
            'cancelled': '❌',
            'pending': '⏳',
        }.get(status.lower(), '')
        if status_emoji:
            msg += f" {status_emoji}"

    return msg


def format_ws_log(event_type: str, details: str) -> str:
    """
    格式化WebSocket日志消息

    Args:
        event_type: 事件类型（连接/断开/推送/订阅）
        details: 详细信息

    Returns:
        格式化的日志消息
    """
    type_emoji = {
        '连接': '🔗',
        '断开': '❌',
        '推送': '📨',
        '订阅': '📡',
        '心跳': '💓',
    }

    emoji = type_emoji.get(event_type, '🔹')
    return f"{emoji} WS-{event_type}: {details}"


def format_sync_log(sync_type: str, stats: dict) -> str:
    """
    格式化同步日志消息

    Args:
        sync_type: 同步类型（ID映射/订单同步/健康检查）
        stats: 统计信息字典

    Returns:
        格式化的日志消息
    """
    msg = f"🔄 {sync_type}"

    # 添加统计信息
    if stats:
        stat_parts = []
        for key, value in stats.items():
            stat_parts.append(f"{key}={value}")
        msg += f" ({', '.join(stat_parts)})"

    return msg
