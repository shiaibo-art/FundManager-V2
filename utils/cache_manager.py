"""
缓存管理器
实现三层缓存架构：
- L1: 内存缓存（进程级，TTL 5分钟）
- L2: 文件缓存（持久化，TTL 1天）
- L3: 数据库缓存（历史数据）
"""
import os
import json
import time
import hashlib
import pickle
from datetime import datetime, timedelta
from typing import Any, Optional, Callable, TypeVar
from functools import wraps
import threading


T = TypeVar('T')


class CacheManager:
    """三层缓存管理器"""

    # 缓存目录
    CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'cache')
    MEMORY_CACHE_DIR = os.path.join(CACHE_DIR, 'memory')

    # 单例锁
    _lock = threading.Lock()
    _instance = None

    # L1: 内存缓存 {key: (value, expire_time)}
    _memory_cache: dict = {}

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化缓存管理器"""
        # 确保缓存目录存在
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        os.makedirs(self.MEMORY_CACHE_DIR, exist_ok=True)

    @staticmethod
    def _generate_key(func_name: str, args: tuple, kwargs: dict) -> str:
        """
        生成缓存键（兼容Windows文件系统）
        使用哈希确保文件名只包含安全字符

        Args:
            func_name: 函数名
            args: 位置参数
            kwargs: 关键字参数

        Returns:
            缓存键
        """
        # 将参数序列化为字符串（用于哈希）
        key_parts = [func_name]

        # 处理位置参数
        for arg in args:
            if arg is None:
                key_parts.append('None')
            elif isinstance(arg, (str, int, float, bool)):
                key_parts.append(str(arg))
            else:
                # 对于复杂对象，使用其字符串表示
                key_parts.append(str(hash(str(arg))))

        # 处理关键字参数（按字母顺序）
        for k in sorted(kwargs.keys()):
            v = kwargs[k]
            if v is None:
                key_parts.append(f'{k}=None')
            elif isinstance(v, (str, int, float, bool)):
                key_parts.append(f'{k}={v}')
            else:
                key_parts.append(f'{k}={hash(str(v))}')

        # 生成完整的键字符串
        key_string = ':'.join(key_parts)

        # 使用 MD5 哈希生成安全的文件名
        key_hash = hashlib.md5(key_string.encode('utf-8')).hexdigest()

        # 返回格式: 函数名_哈希值
        return f"{func_name}_{key_hash}"

    def get_memory(self, key: str) -> Optional[Any]:
        """
        从L1内存缓存获取

        Args:
            key: 缓存键

        Returns:
            缓存值或None
        """
        if key in self._memory_cache:
            value, expire_time = self._memory_cache[key]
            if expire_time is None or time.time() < expire_time:
                return value
            else:
                # 过期，删除
                del self._memory_cache[key]
        return None

    def set_memory(self, key: str, value: Any, ttl: int = None):
        """
        设置L1内存缓存

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒），None表示永不过期
        """
        expire_time = None
        if ttl is not None:
            expire_time = time.time() + ttl

        self._memory_cache[key] = (value, expire_time)

    def get_file(self, key: str) -> Optional[Any]:
        """
        从L2文件缓存获取

        Args:
            key: 缓存键

        Returns:
            缓存值或None
        """
        file_path = os.path.join(self.CACHE_DIR, f"{key}.cache")

        if not os.path.exists(file_path):
            return None

        try:
            # 读取缓存数据
            with open(file_path, 'rb') as f:
                cache_data = pickle.load(f)

            # 检查是否过期
            expire_time = cache_data.get('expire_time')
            if expire_time and time.time() > expire_time:
                os.remove(file_path)
                return None

            return cache_data.get('value')
        except (pickle.PickleError, IOError, EOFError):
            return None

    def set_file(self, key: str, value: Any, ttl: int = None):
        """
        设置L2文件缓存

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒），None表示永不过期
        """
        file_path = os.path.join(self.CACHE_DIR, f"{key}.cache")

        cache_data = {
            'value': value,
            'created_time': time.time(),
            'expire_time': time.time() + ttl if ttl else None
        }

        try:
            with open(file_path, 'wb') as f:
                pickle.dump(cache_data, f)
        except (pickle.PickleError, IOError) as e:
            print(f"保存文件缓存失败: {e}")

    def clear_memory(self):
        """清空L1内存缓存"""
        self._memory_cache.clear()

    def clear_file(self, pattern: str = None):
        """
        清空L2文件缓存

        Args:
            pattern: 文件名模式，None表示清空所有
        """
        if not os.path.exists(self.CACHE_DIR):
            return

        for filename in os.listdir(self.CACHE_DIR):
            if pattern is None or filename.startswith(pattern):
                file_path = os.path.join(self.CACHE_DIR, filename)
                try:
                    os.remove(file_path)
                except OSError:
                    pass

    def cleanup_expired(self):
        """清理所有过期的缓存"""
        current_time = time.time()

        # 清理内存缓存
        expired_keys = []
        for key, (value, expire_time) in self._memory_cache.items():
            if expire_time and current_time >= expire_time:
                expired_keys.append(key)

        for key in expired_keys:
            del self._memory_cache[key]

        # 清理文件缓存
        if os.path.exists(self.CACHE_DIR):
            for filename in os.listdir(self.CACHE_DIR):
                if not filename.endswith('.cache'):
                    continue

                file_path = os.path.join(self.CACHE_DIR, filename)
                try:
                    with open(file_path, 'rb') as f:
                        cache_data = pickle.load(f)

                    expire_time = cache_data.get('expire_time')
                    if expire_time and current_time >= expire_time:
                        os.remove(file_path)
                except (pickle.PickleError, IOError):
                    pass


# 缓存装饰器
def cache_memory(ttl: int = 300, key_func: Callable = None):
    """
    内存缓存装饰器

    Args:
        ttl: 缓存过期时间（秒），默认5分钟
        key_func: 自定义键生成函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 生成缓存键
            if key_func:
                key = key_func(func.__name__, args, kwargs)
            else:
                manager = CacheManager()
                key = manager._generate_key(func.__name__, args, kwargs)

            # 尝试从内存获取
            manager = CacheManager()
            value = manager.get_memory(key)
            if value is not None:
                return value

            # 执行函数
            result = func(*args, **kwargs)

            # 存入内存
            manager.set_memory(key, result, ttl)

            return result
        return wrapper
    return decorator


def cache_file(ttl: int = 86400, key_func: Callable = None):
    """
    文件缓存装饰器

    Args:
        ttl: 缓存过期时间（秒），默认1天
        key_func: 自定义键生成函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 生成缓存键
            if key_func:
                key = key_func(func.__name__, args, kwargs)
            else:
                manager = CacheManager()
                key = manager._generate_key(func.__name__, args, kwargs)

            # 尝试从文件获取
            manager = CacheManager()
            value = manager.get_file(key)
            if value is not None:
                return value

            # 执行函数
            result = func(*args, **kwargs)

            # 存入文件
            manager.set_file(key, result, ttl)

            return result
        return wrapper
    return decorator


def cache_multi(memory_ttl: int = 300, file_ttl: int = 86400, key_func: Callable = None):
    """
    多层缓存装饰器（先内存后文件）

    Args:
        memory_ttl: 内存缓存过期时间（秒）
        file_ttl: 文件缓存过期时间（秒）
        key_func: 自定义键生成函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 生成缓存键
            if key_func:
                key = key_func(func.__name__, args, kwargs)
            else:
                manager = CacheManager()
                key = manager._generate_key(func.__name__, args, kwargs)

            manager = CacheManager()

            # 尝试从内存获取
            value = manager.get_memory(key)
            if value is not None:
                return value

            # 尝试从文件获取
            value = manager.get_file(key)
            if value is not None:
                # 同时存入内存
                manager.set_memory(key, value, memory_ttl)
                return value

            # 执行函数
            result = func(*args, **kwargs)

            # 同时存入内存和文件
            manager.set_memory(key, result, memory_ttl)
            manager.set_file(key, result, file_ttl)

            return result
        return wrapper
    return decorator


# 便捷函数
def get_cache_manager() -> CacheManager:
    """获取缓存管理器单例"""
    return CacheManager()


def clear_cache():
    """清空所有缓存"""
    manager = CacheManager()
    manager.clear_memory()
    manager.clear_file()


def cleanup_expired_cache():
    """清理过期缓存"""
    manager = CacheManager()
    manager.cleanup_expired()
