"""
Lighter SignerClient 安全封装

解决官方 SDK 在初始化失败时未关闭 aiohttp ClientSession
导致的 `Unclosed client session` 警告。
"""

from typing import Any
import asyncio

from lighter.signer_client import SignerClient

from ..utils.logger_factory import get_exchange_logger

logger = get_exchange_logger("ExchangeAdapter.lighter")


def _run_async_fire_and_forget(coro):
    """
    在当前上下文安全地执行异步清理逻辑：
    - 如果没有运行中的事件循环：使用 asyncio.run
    - 如果有：在当前循环中创建任务
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # 当前线程还没有事件循环
        try:
            asyncio.run(coro)
        except RuntimeError:
            # asyncio.run 只能在顶层调用，兜底处理
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(coro)
            finally:
                loop.close()
    else:
        loop.create_task(coro)


class _SafeSignerClient(SignerClient):
    """
    对官方 SignerClient 的轻量封装，确保在 __init__ 失败时释放底层资源
    """

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except Exception:
            self._cleanup_on_failure()
            raise

    def _cleanup_on_failure(self):
        """
        当 SignerClient 初始化失败时尝试关闭 aiohttp ClientSession
        避免 `Unclosed client session` 警告
        """
        api_client = getattr(self, "api_client", None)
        if not api_client:
            return

        async def _close():
            try:
                await api_client.close()
            except Exception as close_exc:  # pragma: no cover - 清理失败不影响主流程
                logger.debug("SignerClient cleanup failed: %s", close_exc)

        _run_async_fire_and_forget(_close())


def create_signer_client(*args: Any, **kwargs: Any) -> SignerClient:
    """
    对外暴露的创建方法，签名与官方 `SignerClient` 保持一致

    Returns:
        SignerClient: 与官方实现兼容的实例
    """
    return _SafeSignerClient(*args, **kwargs)

