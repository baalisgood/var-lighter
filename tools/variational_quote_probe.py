"""
手动探测脚本：拉取 Variational 的 BBO（bid/ask/mark_price）

用途：
- 在本机快速验证 `POST /api/quotes/indicative` 是否可用
- 不属于单元测试（会访问真实网络），因此放在 tools/ 目录

示例：
  python tools/variational_quote_probe.py --symbol BTC --qty 0.001
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


def _ensure_repo_root_on_syspath() -> None:
    """
    允许直接 `python tools/xxx.py` 运行时也能 import 项目内模块（如 core.*）
    """
    repo_root = Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


_ensure_repo_root_on_syspath()

from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType
from core.adapters.exchanges.adapters.variational import VariationalAdapter


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://omni.variational.io", help="Variational base url")
    parser.add_argument("--symbol", default="BTC", help="Underlying symbol, e.g. BTC/ETH/SOL")
    parser.add_argument("--qty", default="0.001", help="Indicative quote qty (string). Different qty may change bid/ask.")
    parser.add_argument("--funding-interval-s", type=int, default=3600)
    parser.add_argument("--settlement-asset", default="USDC")
    parser.add_argument("--instrument-type", default="perpetual_future")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--token", default="", help="Optional: Bearer token for /api/quotes/indicative")
    parser.add_argument("--user-agent", default="", help="Optional: override User-Agent (to bypass WAF/403)")
    parser.add_argument("--origin", default="", help="Optional: override Origin header")
    parser.add_argument("--referer", default="", help="Optional: override Referer header")
    parser.add_argument(
        "--cookie",
        default="",
        help="Optional: Cookie header string, e.g. 'cf_clearance=...; session=...'. "
        "If empty, no cookie will be sent.",
    )
    return parser.parse_args()


async def _main() -> None:
    args = _build_args()

    config = ExchangeConfig(
        exchange_id="variational",
        name="Variational Omni",
        exchange_type=ExchangeType.PERPETUAL,
        # 约定：Variational 使用 api_key 字段承载 Bearer token（如果你不想用 extra_params.token）
        api_key=args.token or "",
        api_secret="",
        enable_websocket=False,
        enable_heartbeat=False,
        request_timeout=args.timeout,
        extra_params={
            "base_url": args.base_url,
            "indicative_quote_qty": args.qty,
            "funding_interval_s": args.funding_interval_s,
            "settlement_asset": args.settlement_asset,
            "instrument_type": args.instrument_type,
            **({"token": args.token} if args.token else {}),
            # 可选：用于绕过 403 的“浏览器头”
            **({"user_agent": args.user_agent} if args.user_agent else {}),
            **({"origin": args.origin} if args.origin else {}),
            **({"referer": args.referer} if args.referer else {}),
            **(
                {
                    "headers": {"Cookie": args.cookie},
                }
                if args.cookie
                else {}
            ),
        },
    )

    adapter = VariationalAdapter(config)
    await adapter.connect()
    try:
        ticker = await adapter.get_ticker(args.symbol)
        print(
            {
                "symbol": ticker.symbol,
                "bid": float(ticker.bid) if ticker.bid is not None else None,
                "ask": float(ticker.ask) if ticker.ask is not None else None,
                "mark_price": float(ticker.mark_price) if ticker.mark_price is not None else None,
                "last": float(ticker.last) if ticker.last is not None else None,
                "quote_qty": ticker.raw_data.get("quote_qty"),
            }
        )
    finally:
        await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(_main())


