"""
简单打印器 - 完全放弃 Rich，使用纯粹的 print() 滚动输出

职责：
- 使用简单的 print() 输出（零延迟，最实时）
- 节流控制（500ms），避免阻塞 WebSocket
- 参考 test_sol_orderbook.py 的成功经验
- 从配置文件读取精度信息（动态精度）
- 显示资金费率和费率差
"""

import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional


class SimplePrinter:
    """
    简单打印器（纯 print() 模式，完全放弃 Rich）
    
    设计理念：
    1. 简单就是快速
    2. print() 是最实时的
    3. 终端自己处理滚动（不需要框架）
    4. 支持动态数量的交易所（可扩展）
    """
    
    def __init__(self, throttle_ms: float = 500, exchanges: Optional[list] = None):
        """
        初始化打印器
        
        Args:
            throttle_ms: 节流间隔（毫秒）
            exchanges: 交易所列表（用于动态生成表头），如果为None则使用默认的 ['edgex', 'lighter']
        """
        self.throttle_interval = throttle_ms / 1000  # 转换为秒
        self.last_print_time: float = 0  # 🔥 全局节流（用于对比显示）
        
        # 🔥 支持的交易所列表（动态配置）
        self.exchanges = exchanges or ['edgex', 'lighter']  # 默认支持2个交易所
        
        # 🔥 缓存所有交易所的最新数据（用于对比）
        # {symbol: {exchange: {bid_price, bid_size, ask_price, ask_size, funding_rate}}}
        self.latest_data: Dict[str, Dict[str, Dict]] = {}
        
        # 🔥 从配置文件读取精度信息（方法2：配置文件读取）
        self.market_precisions: Dict[str, Dict[str, int]] = {}  # {symbol: {price_decimals, size_decimals}}
        self._load_market_precisions()
        
        # 统计信息
        self.stats = {
            'orderbook_updates': 0,
            'opportunities_found': 0,
            'start_time': time.time()
        }
        
        # 🔥 代币去重跟踪（10秒内不显示重复代币）
        self._symbol_last_shown: Dict[str, float] = {}  # {symbol: last_shown_timestamp}
        self._symbol_dedup_seconds: float = 10.0  # 10秒去重窗口
        
        # 🔥 数据过期清理配置
        self.data_timeout_seconds: float = 60.0  # 数据超时时间（60秒无更新则清理）
        self.last_cleanup_time: float = time.time()  # 上次清理时间
        self.cleanup_interval: float = 30.0  # 清理间隔（30秒清理一次）
        
        # 🔥 终端清屏配置（可选，避免终端内容无限增长）
        self.clear_screen_enabled: bool = True  # 是否启用清屏
        self.clear_screen_interval: float = 300.0  # 清屏间隔（300秒=5分钟）
        self.last_clear_screen_time: float = time.time()  # 上次清屏时间
        
        # 🔥 启动时先清屏一次，再打印启动横幅
        self._clear_screen()
        self._print_banner()
    
    def _load_market_precisions(self):
        """从配置文件加载市场精度信息"""
        try:
            # 尝试多个可能的配置文件路径（参考 edgex.py 的实现方式）
            config_paths = [
                Path("config/exchanges/lighter_markets.json"),  # 从当前工作目录（项目根目录）
                Path(__file__).parent.parent.parent.parent.parent / "config" / "exchanges" / "lighter_markets.json",  # 从文件位置向上5级
            ]
            
            config_path = None
            for path in config_paths:
                if path.exists():
                    config_path = path
                    break
            
            if config_path and config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    
                    markets = config_data.get('markets', {})
                    for symbol, market_info in markets.items():
                        # 提取精度信息
                        price_decimals = market_info.get('price_decimals')
                        size_decimals = market_info.get('size_decimals')
                        
                        if price_decimals is not None and size_decimals is not None:
                            self.market_precisions[symbol] = {
                                'price_decimals': int(price_decimals),
                                'size_decimals': int(size_decimals)
                            }
                    
                    print(f"✅ 已加载 {len(self.market_precisions)} 个市场的精度信息（配置文件: {config_path}）")
            else:
                print(f"⚠️  配置文件不存在（尝试了 {len(config_paths)} 个路径），将使用默认精度（价格2位，数量1位）")
        except Exception as e:
            print(f"⚠️  加载精度配置失败: {e}，将使用默认精度（价格2位，数量1位）")
            import traceback
            traceback.print_exc()
    
    def _get_precision(self, symbol: str) -> Dict[str, int]:
        """
        获取交易对的精度信息
        
        Args:
            symbol: 交易对符号（如 "BTC-USDC-PERP"）
            
        Returns:
            {'price_decimals': int, 'size_decimals': int}
        """
        # 提取基础币种（如 "BTC-USDC-PERP" -> "BTC"）
        base_symbol = symbol.split('-')[0] if '-' in symbol else symbol.split('/')[0]
        
        if base_symbol in self.market_precisions:
            return self.market_precisions[base_symbol]
        else:
            # 默认精度（向后兼容）
            return {'price_decimals': 2, 'size_decimals': 1}
    
    def _format_size(self, size: float, size_decimals: int) -> str:
        """
        格式化数量（根据精度）
        
        Args:
            size: 数量
            size_decimals: 数量精度（小数位数）
            
        Returns:
            格式化后的字符串
        """
        if size_decimals == 0:
            # 整数格式
            return f"{size:>6,.0f}"
        else:
            # 小数格式
            return f"{size:>6,.{size_decimals}f}"
    
    def _print_banner(self):
        """打印启动横幅（动态生成表头）"""
        exchange_count = len(self.exchanges)
        exchanges_str = " vs ".join([ex.upper() for ex in self.exchanges])
        
        print("\n" + "=" * 200)
        if exchange_count == 2:
            print(f"{'🔥 套利监控系统 V2 - 双交易所对比模式（纯滚动输出）':^200}")
        else:
            print(f"{f'🔥 套利监控系统 V2 - {exchange_count}交易所对比模式（纯滚动输出）':^200}")
        print("=" * 200)
        print(f"{f'特点：{exchanges_str} 实时对比 | 零延迟 | 500ms 节流 | 价差+资金费率对比':^200}")
        print("=" * 200)
        print()
        print("📊 实时对比数据流（每 500ms 更新）：")
        print("-" * 200)
        
        # 🔥 动态生成表头
        header_parts = [f"{'时间':<15}", f"{'交易对':<15}"]
        
        # 为每个交易所添加列
        for exchange in self.exchanges:
            exchange_name = exchange.upper()
            header_parts.append(f"{exchange_name} 买1")
            header_parts.append(f"{exchange_name} 卖1")
            header_parts.append(f"{exchange_name}费率")
        
        # 添加价差和费率差列
        header_parts.append(f"{'价差':<15}")
        header_parts.append(f"{'费率差':<20}")
        
        header = " ".join(header_parts)
        print(header)
        print("-" * 200)
    
    def print_orderbook_update(
        self,
        exchange: str,
        symbol: str,
        bid_price: float,
        bid_size: float,
        ask_price: float,
        ask_size: float,
        funding_rate: Optional[float] = None
    ):
        """
        缓存订单簿数据并对比显示两个交易所
        
        Args:
            exchange: 交易所名称
            symbol: 交易对
            bid_price: 买一价
            bid_size: 买一量
            ask_price: 卖一价
            ask_size: 卖一量
            funding_rate: 资金费率（8小时，可选）
        """
        # 🔥 步骤1：更新缓存数据
        current_time = time.time()
        
        if symbol not in self.latest_data:
            self.latest_data[symbol] = {}
        
        self.latest_data[symbol][exchange] = {
            'bid_price': bid_price,
            'bid_size': bid_size,
            'ask_price': ask_price,
            'ask_size': ask_size,
            'funding_rate': funding_rate,  # 🔥 添加资金费率
            'update_time': current_time
        }
        
        self.stats['orderbook_updates'] += 1
        
        # 🔥 步骤1.5：定期清理过期数据（避免内存积累）
        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            self._cleanup_stale_data(current_time)
            self.last_cleanup_time = current_time
        
        # 🔥 步骤2：节流检查（全局节流，避免刷屏）
        if current_time - self.last_print_time < self.throttle_interval:
            return
        
        # 🔥 步骤3：打印数据（支持动态数量的交易所）
        if symbol in self.latest_data:
            data = self.latest_data[symbol]
            
            # 🔥 获取精度信息（动态精度）
            precision = self._get_precision(symbol)
            price_decimals = precision['price_decimals']
            size_decimals = precision['size_decimals']
            
            # 格式化时间
            time_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            # 🔥 动态检查所有交易所的数据
            available_exchanges = [ex for ex in self.exchanges if ex in data]
            
            # ============================================================
            # 🔥 多交易所支持扩展点
            # ============================================================
            # 当接入3个或更多交易所时，可以在这里实现通用的多交易所对比逻辑
            # 示例实现思路：
            # 1. 计算所有交易所之间的两两套利机会（N*(N-1)/2 种策略）
            # 2. 选择最优的套利策略（价差最大的）
            # 3. 动态生成显示行（遍历所有交易所，显示每个交易所的数据）
            # 4. 计算资金费率差（可以选择显示最大/最小费率差，或所有费率差）
            # ============================================================
            
            # 🔥 如果只有2个交易所，使用原有的对比逻辑（保持兼容性）
            if len(self.exchanges) == 2 and len(available_exchanges) == 2:
                # 使用原有的2交易所对比逻辑
                has_edgex = 'edgex' in data
                has_lighter = 'lighter' in data
                
                if has_edgex and has_lighter:
                    # 🔥 两个交易所都有数据：打印对比
                    edgex = data['edgex']
                    lighter = data['lighter']
                    
                    # 🔥 计算套利机会（EdgeX买 -> Lighter卖 或 Lighter买 -> EdgeX卖）
                    # 策略1: EdgeX买 -> Lighter卖
                    profit1 = lighter['bid_price'] - edgex['ask_price']
                    profit1_pct = (profit1 / edgex['ask_price']) * 100 if edgex['ask_price'] > 0 else 0
                    
                    # 策略2: Lighter买 -> EdgeX卖
                    profit2 = edgex['bid_price'] - lighter['ask_price']
                    profit2_pct = (profit2 / lighter['ask_price']) * 100 if lighter['ask_price'] > 0 else 0
                    
                    # 选择最优策略
                    if profit1_pct > profit2_pct:
                        best_profit_pct = profit1_pct
                    else:
                        best_profit_pct = profit2_pct
                    
                    # 🔥 计算资金费率差（参考 v1 实现）
                    edgex_fr = edgex.get('funding_rate')
                    lighter_fr = lighter.get('funding_rate')
                    
                    funding_rate_diff_str = "-"
                    if edgex_fr is not None and lighter_fr is not None:
                        # 🔥 资金费率差应该永远为正数（绝对值差值）
                        rate_diff = abs(edgex_fr - lighter_fr)
                        # 8小时差值（百分比）
                        diff_8h = float(rate_diff * 100)
                        # 年化差值：8小时差值 × 1095
                        diff_annual = diff_8h * 1095
                        # 费率差永远是正数，不需要符号
                        funding_rate_diff_str = f"{diff_8h:.4f}%/{diff_annual:.1f}%"
                    
                    # 🔥 格式化资金费率显示
                    edgex_fr_str = "-"
                    if edgex_fr is not None:
                        fr_8h = float(edgex_fr * 100)
                        fr_annual = fr_8h * 1095
                        edgex_fr_str = f"{fr_8h:.4f}%/{fr_annual:.1f}%"
                    
                    lighter_fr_str = "-"
                    if lighter_fr is not None:
                        fr_8h = float(lighter_fr * 100)
                        fr_annual = fr_8h * 1095
                        lighter_fr_str = f"{fr_8h:.4f}%/{fr_annual:.1f}%"
                    
                    # 🔥 高亮显示有利润的机会
                    if best_profit_pct > 0.1:  # 超过0.1%
                        emoji = "💰"
                        if best_profit_pct > 0.5:
                            emoji = "🔥"
                    else:
                        emoji = "  "
                    
                    # 🔥 动态格式化价格和数量（使用配置文件中的精度）
                    # 格式化价格
                    edgex_bid_price_str = f"${edgex['bid_price']:>8,.{price_decimals}f}"
                    edgex_ask_price_str = f"${edgex['ask_price']:>8,.{price_decimals}f}"
                    lighter_bid_price_str = f"${lighter['bid_price']:>8,.{price_decimals}f}"
                    lighter_ask_price_str = f"${lighter['ask_price']:>8,.{price_decimals}f}"
                    
                    # 🔥 格式化数量（使用专门的格式化方法）
                    edgex_bid_size_str = self._format_size(edgex['bid_size'], size_decimals)
                    edgex_ask_size_str = self._format_size(edgex['ask_size'], size_decimals)
                    lighter_bid_size_str = self._format_size(lighter['bid_size'], size_decimals)
                    lighter_ask_size_str = self._format_size(lighter['ask_size'], size_decimals)
                    
                    # 📊 打印对比行（包含资金费率）
                    line = (f"{time_str:<15} {symbol:<15} "
                           f"{edgex_bid_price_str}×{edgex_bid_size_str}  "
                           f"{edgex_ask_price_str}×{edgex_ask_size_str}  "
                           f"{edgex_fr_str:<15} "
                           f"{lighter_bid_price_str}×{lighter_bid_size_str}  "
                           f"{lighter_ask_price_str}×{lighter_ask_size_str}  "
                           f"{lighter_fr_str:<15} "
                           f"{emoji} {best_profit_pct:>+6.3f}%  "
                           f"{funding_rate_diff_str:<20}")
                    
                    print(line)
                    self.last_print_time = current_time
                    
                elif has_lighter:
                    # 🔥 只有 Lighter 数据：显示 Lighter，EdgeX 显示"等待中"
                    lighter = data['lighter']
                    
                    lighter_fr_str = "-"
                    if lighter.get('funding_rate') is not None:
                        fr_8h = float(lighter['funding_rate'] * 100)
                        fr_annual = fr_8h * 1095
                        lighter_fr_str = f"{fr_8h:.4f}%/{fr_annual:.1f}%"
                    
                    lighter_bid_price_str = f"${lighter['bid_price']:>8,.{price_decimals}f}"
                    lighter_bid_size_str = self._format_size(lighter['bid_size'], size_decimals)
                    lighter_ask_price_str = f"${lighter['ask_price']:>8,.{price_decimals}f}"
                    lighter_ask_size_str = self._format_size(lighter['ask_size'], size_decimals)
                    
                    line = (f"{time_str:<15} {symbol:<15} "
                           f"{'等待中':<20} {'等待中':<20} {'-':<15} "
                           f"{lighter_bid_price_str}×{lighter_bid_size_str}  "
                           f"{lighter_ask_price_str}×{lighter_ask_size_str}  "
                           f"{lighter_fr_str:<15} "
                           f"{'等待中':<15} {'-':<20}")
                    print(line)
                    self.last_print_time = current_time
                    
                elif has_edgex:
                    # 🔥 只有 EdgeX 数据：显示 EdgeX，Lighter 显示"等待中"
                    edgex = data['edgex']
                    
                    edgex_fr_str = "-"
                    if edgex.get('funding_rate') is not None:
                        fr_8h = float(edgex['funding_rate'] * 100)
                        fr_annual = fr_8h * 1095
                        edgex_fr_str = f"{fr_8h:.4f}%/{fr_annual:.1f}%"
                    
                    edgex_bid_price_str = f"${edgex['bid_price']:>8,.{price_decimals}f}"
                    edgex_bid_size_str = self._format_size(edgex['bid_size'], size_decimals)
                    edgex_ask_price_str = f"${edgex['ask_price']:>8,.{price_decimals}f}"
                    edgex_ask_size_str = self._format_size(edgex['ask_size'], size_decimals)
                    
                    line = (f"{time_str:<15} {symbol:<15} "
                           f"{edgex_bid_price_str}×{edgex_bid_size_str}  "
                           f"{edgex_ask_price_str}×{edgex_ask_size_str}  "
                           f"{edgex_fr_str:<15} "
                           f"{'等待中':<20} {'等待中':<20} {'-':<15} "
                           f"{'等待中':<15} {'-':<20}")
                    print(line)
                    self.last_print_time = current_time
            
            # ============================================================
            # 🔥 多交易所支持占位符（3个或更多交易所）
            # ============================================================
            # TODO: 当需要支持3个或更多交易所时，实现以下逻辑：
            #
            # elif len(self.exchanges) >= 3:
            #     # 多交易所对比逻辑
            #     # 1. 计算所有交易所之间的两两套利机会
            #     best_profit_pct = 0
            #     best_buy_exchange = None
            #     best_sell_exchange = None
            #     
            #     for i, ex1 in enumerate(available_exchanges):
            #         for j, ex2 in enumerate(available_exchanges):
            #             if i != j:
            #                 ex1_data = data[ex1]
            #                 ex2_data = data[ex2]
            #                 # 策略：ex1买 -> ex2卖
            #                 profit = ex2_data['bid_price'] - ex1_data['ask_price']
            #                 profit_pct = (profit / ex1_data['ask_price']) * 100 if ex1_data['ask_price'] > 0 else 0
            #                 if profit_pct > best_profit_pct:
            #                     best_profit_pct = profit_pct
            #                     best_buy_exchange = ex1
            #                     best_sell_exchange = ex2
            #     
            #     # 2. 动态生成显示行
            #     line_parts = [f"{time_str:<15}", f"{symbol:<15}"]
            #     
            #     for exchange in self.exchanges:
            #         if exchange in data:
            #             ex_data = data[exchange]
            #             bid_str = f"${ex_data['bid_price']:>8,.{price_decimals}f}×{self._format_size(ex_data['bid_size'], size_decimals)}"
            #             ask_str = f"${ex_data['ask_price']:>8,.{price_decimals}f}×{self._format_size(ex_data['ask_size'], size_decimals)}"
            #             fr_str = self._format_funding_rate(ex_data.get('funding_rate'))
            #             line_parts.extend([bid_str, ask_str, fr_str])
            #         else:
            #             line_parts.extend([f"{'等待中':<20}", f"{'等待中':<20}", f"{'-':<15}"])
            #     
            #     # 3. 添加价差和费率差
            #     if best_profit_pct > 0.1:
            #         emoji = "💰" if best_profit_pct <= 0.5 else "🔥"
            #     else:
            #         emoji = "  "
            #     line_parts.append(f"{emoji} {best_profit_pct:>+6.3f}%")
            #     line_parts.append(f"{best_buy_exchange}→{best_sell_exchange}" if best_buy_exchange else "-")
            #     
            #     line = " ".join(line_parts)
            #     print(line)
            #     self.last_print_time = current_time
            # ============================================================
    
    def _format_funding_rate(self, funding_rate: Optional[float]) -> str:
        """
        格式化资金费率显示（辅助方法，用于多交易所支持）
        
        Args:
            funding_rate: 资金费率（8小时）
            
        Returns:
            格式化后的字符串，格式：8h%/年化%
        """
        if funding_rate is None:
            return "-"
        fr_8h = float(funding_rate * 100)
        fr_annual = fr_8h * 1095
        return f"{fr_8h:.4f}%/{fr_annual:.1f}%"
    
    def print_opportunity(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str,
        price_buy: float,
        price_sell: float,
        spread_pct: float,
        funding_rate_diff: Optional[float] = None  # 🔥 资金费率差（8小时费率差，小数形式）
    ):
        """
        打印套利机会（高亮显示）
        
        Args:
            symbol: 交易对
            exchange_buy: 买入交易所
            exchange_sell: 卖出交易所
            price_buy: 买入价
            price_sell: 卖出价
            spread_pct: 价差百分比
            funding_rate_diff: 资金费率差（8小时费率差，小数形式，如0.0001表示0.01%）
        """
        # 🔥 10秒内不显示重复代币
        current_time = time.time()
        if symbol in self._symbol_last_shown:
            time_since_last_shown = current_time - self._symbol_last_shown[symbol]
            if time_since_last_shown < self._symbol_dedup_seconds:
                # 10秒内已显示过，跳过
                return
        
        # 更新最后显示时间
        self._symbol_last_shown[symbol] = current_time
        
        # 套利机会立即打印，不节流
        time_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        # 根据价差大小选择 emoji
        if spread_pct >= 0.5:
            emoji = "🔥🔥🔥"
            level = "极佳"
        elif spread_pct >= 0.2:
            emoji = "💰💰"
            level = "良好"
        else:
            emoji = "💰"
            level = "一般"
        
        # 🔥 格式化资金费率差（参考v1算法：8小时费率差转换为年化费率差）
        funding_rate_diff_str = ""
        if funding_rate_diff is not None:
            # funding_rate_diff 是8小时费率差（小数形式，如0.0001表示0.01%）
            # 🔥 资金费率差应该永远为正数（绝对值差值）
            rate_diff = abs(funding_rate_diff)  # 确保是正数
            # 8小时差值（百分比）
            diff_8h = float(rate_diff * 100)
            # 年化差值：8小时差值 × 1095
            diff_annual = diff_8h * 1095
            # 费率差永远是正数，不需要符号
            funding_rate_diff_str = f"   费率差(年化): {diff_annual:.1f}%\n"
        
        # 📢 打印套利机会（高亮）
        print()
        print("=" * 140)
        print(f"{emoji} [{time_str}] 套利机会！（{level}）")
        print(f"   代币: {symbol}")
        print(f"   策略: {exchange_buy} 买入 ${price_buy:,.2f} → {exchange_sell} 卖出 ${price_sell:,.2f}")
        print(f"   价差: {spread_pct:+.3f}%")
        if funding_rate_diff_str:
            print(funding_rate_diff_str, end="")
        print("=" * 140)
        print()
        
        self.stats['opportunities_found'] += 1
    
    def _cleanup_stale_data(self, current_time: float):
        """
        清理过期数据（避免内存积累）
        
        Args:
            current_time: 当前时间戳
        """
        # 清理 latest_data 中超过60秒未更新的数据
        symbols_to_remove = []
        for symbol, exchanges_data in self.latest_data.items():
            exchanges_to_remove = []
            for exchange, data in exchanges_data.items():
                update_time = data.get('update_time', 0)
                if current_time - update_time > self.data_timeout_seconds:
                    exchanges_to_remove.append(exchange)
            
            # 删除过期的交易所数据
            for exchange in exchanges_to_remove:
                del exchanges_data[exchange]
            
            # 如果该交易对的所有交易所数据都已过期，标记删除
            if not exchanges_data:
                symbols_to_remove.append(symbol)
        
        # 删除没有数据的交易对
        for symbol in symbols_to_remove:
            del self.latest_data[symbol]
        
        # 清理 _symbol_last_shown 中超过10分钟未显示的代币（避免无限增长）
        symbols_to_remove_from_shown = []
        for symbol, last_shown_time in self._symbol_last_shown.items():
            if current_time - last_shown_time > 600:  # 10分钟
                symbols_to_remove_from_shown.append(symbol)
        
        for symbol in symbols_to_remove_from_shown:
            del self._symbol_last_shown[symbol]
    
    def _clear_screen(self):
        """
        清屏（使用ANSI转义序列，跨平台兼容）
        """
        import os
        import sys
        
        # 使用ANSI转义序列清屏（跨平台）
        if sys.platform == 'win32':
            os.system('cls')
        else:
            # Unix/Linux/macOS: 使用ANSI转义序列
            print('\033[2J\033[H', end='')  # 清屏并移动光标到左上角
    
    def print_stats_summary(self):
        """打印统计摘要（定期调用，如每 60 秒）"""
        current_time = time.time()
        
        # 🔥 定期清屏（避免终端内容无限增长）
        if self.clear_screen_enabled:
            if current_time - self.last_clear_screen_time >= self.clear_screen_interval:
                self._clear_screen()
                # 重新打印横幅和表头
                self._print_banner()
                self.last_clear_screen_time = current_time
        
        elapsed = time.time() - self.stats['start_time']
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        # 🔥 显示内存使用情况
        latest_data_count = sum(len(exchanges) for exchanges in self.latest_data.values())
        symbol_shown_count = len(self._symbol_last_shown)
        
        print()
        print("─" * 140)
        print(f"📊 统计摘要（运行时长: {hours:02d}:{minutes:02d}:{seconds:02d}）")
        print(f"   订单簿更新: {self.stats['orderbook_updates']} 次")
        print(f"   套利机会: {self.stats['opportunities_found']} 个")
        print(f"   缓存数据: {len(self.latest_data)} 个交易对, {latest_data_count} 条记录")
        print(f"   去重跟踪: {symbol_shown_count} 个代币")
        print("─" * 140)
        print()
    
    def print_error(self, message: str):
        """打印错误信息"""
        time_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"❌ [{time_str}] 错误: {message}")
    
    def print_info(self, message: str):
        """打印信息"""
        time_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"ℹ️  [{time_str}] {message}")
    
    def print_success(self, message: str):
        """打印成功信息"""
        time_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"✅ [{time_str}] {message}")

