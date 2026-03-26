"""
并发数据获取器
使用线程池实现并发数据获取，提升性能
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Dict, Any, Optional, TypeVar
import threading


T = TypeVar('T')


class AsyncFetcher:
    """并发数据获取器"""

    # 默认最大并发数
    DEFAULT_MAX_WORKERS = 5

    # 默认超时时间（秒）
    DEFAULT_TIMEOUT = 30

    # 单例锁
    _lock = threading.Lock()
    _instance = None

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化并发获取器"""
        self.executor = ThreadPoolExecutor(max_workers=self.DEFAULT_MAX_WORKERS)

    def fetch_multiple(
        self,
        func: Callable,
        items: List[Any],
        timeout: int = None,
        show_progress: bool = False,
        progress_callback: Callable[[int, int], None] = None
    ) -> Dict[Any, Any]:
        """
        并发获取多个项目的数据

        Args:
            func: 获取函数，接受单个item作为参数
            items: 要获取的项目列表
            timeout: 超时时间（秒）
            show_progress: 是否显示进度
            progress_callback: 进度回调函数(current, total)

        Returns:
            {item: result} 字典
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        results = {}
        completed_count = 0
        total_count = len(items)

        # 提交所有任务
        future_to_item = {}
        for item in items:
            future = self.executor.submit(self._fetch_with_retry, func, item, timeout)
            future_to_item[future] = item

        # 收集结果
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                results[item] = result
            except Exception as e:
                print(f"获取 {item} 失败: {e}")
                results[item] = None

            completed_count += 1

            # 进度回调
            if progress_callback:
                progress_callback(completed_count, total_count)

            # 显示进度
            if show_progress and completed_count % 5 == 0:
                print(f"进度: {completed_count}/{total_count}")

        return results

    def _fetch_with_retry(
        self,
        func: Callable,
        item: Any,
        timeout: int,
        max_retries: int = 2
    ) -> Any:
        """
        带重试的获取

        Args:
            func: 获取函数
            item: 要获取的项目
            timeout: 超时时间
            max_retries: 最大重试次数

        Returns:
            获取结果
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                return func(item)
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    # 指数退避
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    raise last_error

    def fetch_with_progress(
        self,
        func: Callable,
        items: List[Any],
        desc: str = "处理中",
        timeout: int = None
    ) -> Dict[Any, Any]:
        """
        带进度显示的并发获取

        Args:
            func: 获取函数
            items: 要获取的项目列表
            desc: 描述文本
            timeout: 超时时间

        Returns:
            {item: result} 字典
        """
        def progress_callback(current: int, total: int):
            """进度回调"""
            percentage = (current / total) * 100
            print(f"\r{desc}: {current}/{total} ({percentage:.1f}%)", end='', flush=True)

        results = self.fetch_multiple(
            func=func,
            items=items,
            timeout=timeout,
            show_progress=False,
            progress_callback=progress_callback
        )

        print()  # 换行
        return results

    def fetch_navs_batch(
        self,
        fund_codes: List[str],
        get_nav_func: Callable,
        timeout: int = None
    ) -> Dict[str, Optional[float]]:
        """
        批量获取基金净值

        Args:
            fund_codes: 基金代码列表
            get_nav_func: 获取净值函数
            timeout: 超时时间

        Returns:
            {fund_code: nav} 字典
        """
        return self.fetch_with_progress(
            func=get_nav_func,
            items=fund_codes,
            desc="获取净值",
            timeout=timeout
        )

    def fetch_nav_details_batch(
        self,
        fund_codes: List[str],
        get_detail_func: Callable,
        timeout: int = None
    ) -> Dict[str, Optional[Dict]]:
        """
        批量获取基金详细信息

        Args:
            fund_codes: 基金代码列表
            get_detail_func: 获取详情函数
            timeout: 超时时间

        Returns:
            {fund_code: detail} 字典
        """
        return self.fetch_with_progress(
            func=get_detail_func,
            items=fund_codes,
            desc="获取详情",
            timeout=timeout
        )

    def shutdown(self, wait: bool = True):
        """
        关闭线程池

        Args:
            wait: 是否等待所有任务完成
        """
        self.executor.shutdown(wait=wait)


# 便捷函数
def get_async_fetcher() -> AsyncFetcher:
    """获取并发获取器单例"""
    return AsyncFetcher()


def fetch_multiple_navs(
    fund_codes: List[str],
    get_nav_func: Callable,
    timeout: int = None
) -> Dict[str, Optional[float]]:
    """
    批量获取基金净值（便捷函数）

    Args:
        fund_codes: 基金代码列表
        get_nav_func: 获取净值函数
        timeout: 超时时间

    Returns:
        {fund_code: nav} 字典
    """
    fetcher = get_async_fetcher()
    return fetcher.fetch_navs_batch(fund_codes, get_nav_func, timeout)


def fetch_concurrent(
    func: Callable,
    items: List[Any],
    max_workers: int = 5,
    timeout: int = None
) -> Dict[Any, Any]:
    """
    并发执行函数（便捷函数）

    Args:
        func: 要执行的函数
        items: 参数列表
        max_workers: 最大并发数
        timeout: 超时时间

    Returns:
        {item: result} 字典
    """
    fetcher = AsyncFetcher()
    # 临时修改并发数
    original_executor = fetcher.executor
    fetcher.executor = ThreadPoolExecutor(max_workers=max_workers)

    try:
        results = fetcher.fetch_multiple(func, items, timeout)
        return results
    finally:
        fetcher.executor.shutdown(wait=False)
        fetcher.executor = original_executor
