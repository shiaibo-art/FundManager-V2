"""
基金列表缓存模块
从天天基金获取完整基金列表并缓存到本地，实现快速搜索
"""
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import threading


class FundListCache:
    """基金列表缓存管理器"""

    # 缓存文件路径
    CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    CACHE_FILE = os.path.join(CACHE_DIR, 'fund_list_cache.json')

    # 缓存有效期（天）
    CACHE_VALID_DAYS = 7

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
        """初始化缓存"""
        self._fund_list = None
        self._fund_dict = None  # {code: fund_info}
        self._name_dict = None  # {name: [fund_info]}
        self._pinyin_dict = None  # {pinyin: [fund_info]}
        self._last_load = None

    @classmethod
    def get_fund_info(cls, fund_code: str) -> Optional[Dict]:
        """
        获取基金信息（从缓存）

        Args:
            fund_code: 基金代码

        Returns:
            基金信息字典或None
        """
        instance = cls()
        if instance._fund_dict is None:
            instance._load_cache()

        return instance._fund_dict.get(fund_code)

    @classmethod
    def get_fund_type(cls, fund_code: str) -> Optional[str]:
        """
        获取基金类型（从缓存）

        Args:
            fund_code: 基金代码

        Returns:
            基金类型或None
        """
        fund_info = cls.get_fund_info(fund_code)
        return fund_info.get('type') if fund_info else None

    @classmethod
    def search(cls, keyword: str, limit: int = 20) -> List[Dict]:
        """
        搜索基金（从本地缓存）

        Args:
            keyword: 搜索关键词（支持代码、名称、拼音）
            limit: 返回结果数量限制

        Returns:
            基金列表
        """
        instance = cls()
        if instance._fund_dict is None:
            instance._load_cache()

        if not instance._fund_dict:
            return []

        keyword = keyword.strip().lower()
        results = []

        # 优先精确匹配基金代码
        if keyword in instance._fund_dict:
            return [instance._fund_dict[keyword]]

        # 搜索基金代码（前缀匹配）
        for code, fund in instance._fund_dict.items():
            if code.startswith(keyword):
                results.append(fund)
                if len(results) >= limit:
                    break

        # 如果结果不足，搜索基金名称
        if len(results) < limit:
            for fund in instance._fund_dict.values():
                name = fund.get('name', '')
                if keyword in name.lower():
                    if fund not in results:
                        results.append(fund)
                    if len(results) >= limit:
                        break

        # 如果结果仍不足，搜索拼音
        if len(results) < limit and instance._pinyin_dict:
            for pinyin, funds in instance._pinyin_dict.items():
                if keyword in pinyin.lower():
                    for fund in funds:
                        if fund not in results:
                            results.append(fund)
                        if len(results) >= limit:
                            break
                if len(results) >= limit:
                    break

        return results[:limit]

    @classmethod
    def update_from_api(cls, force: bool = False) -> bool:
        """
        从API更新基金列表缓存

        Args:
            force: 是否强制更新（忽略缓存有效期）

        Returns:
            是否更新成功
        """
        # 检查缓存是否有效
        if not force and cls._is_cache_valid():
            print("基金列表缓存仍然有效，跳过更新")
            return False

        from utils.eastmoney_api import get_eastmoney_api

        print("开始从天天基金获取基金列表...")
        api = get_eastmoney_api()
        fund_list = api.get_all_funds_list()

        if not fund_list:
            print("获取基金列表失败")
            return False

        print(f"成功获取 {len(fund_list)} 只基金，正在保存缓存...")

        # 保存缓存
        cache_data = {
            'update_time': datetime.now().isoformat(),
            'count': len(fund_list),
            'funds': fund_list
        }

        # 确保目录存在
        os.makedirs(cls.CACHE_DIR, exist_ok=True)

        with open(cls.CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"基金列表缓存已保存到 {cls.CACHE_FILE}")

        # 清除内存缓存
        instance = cls()
        instance._fund_list = None
        instance._fund_dict = None
        instance._name_dict = None
        instance._pinyin_dict = None

        return True

    @classmethod
    def _is_cache_valid(cls) -> bool:
        """检查缓存是否有效"""
        if not os.path.exists(cls.CACHE_FILE):
            return False

        try:
            with open(cls.CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            update_time_str = cache_data.get('update_time')
            if not update_time_str:
                return False

            update_time = datetime.fromisoformat(update_time_str)
            expire_time = update_time + timedelta(days=cls.CACHE_VALID_DAYS)

            return datetime.now() < expire_time
        except (json.JSONDecodeError, ValueError):
            return False

    def _load_cache(self):
        """加载缓存到内存"""
        # 检查文件是否存在
        if not os.path.exists(self.CACHE_FILE):
            print("基金列表缓存文件不存在，尝试从API获取...")
            if self.update_from_api():
                self._load_cache()
            return

        try:
            with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            fund_list = cache_data.get('funds', [])
            print(f"从缓存加载了 {len(fund_list)} 只基金")

            # 构建索引
            self._fund_list = fund_list
            self._fund_dict = {}
            self._name_dict = {}
            self._pinyin_dict = {}

            for fund in fund_list:
                code = fund.get('code', '')
                name = fund.get('name', '')
                pinyin = fund.get('pinyin', '')

                # 代码索引
                if code:
                    self._fund_dict[code] = fund

                # 名称索引
                if name:
                    if name not in self._name_dict:
                        self._name_dict[name] = []
                    self._name_dict[name].append(fund)

                # 拼音索引
                if pinyin:
                    if pinyin not in self._pinyin_dict:
                        self._pinyin_dict[pinyin] = []
                    self._pinyin_dict[pinyin].append(fund)

            self._last_load = time.time()

        except (json.JSONDecodeError, IOError) as e:
            print(f"加载缓存失败: {e}")
            self._fund_list = []
            self._fund_dict = {}
            self._name_dict = {}
            self._pinyin_dict = {}

    @classmethod
    def get_cache_info(cls) -> Dict:
        """获取缓存信息"""
        instance = cls()
        if instance._fund_dict is None:
            instance._load_cache()

        info = {
            'cache_file': cls.CACHE_FILE,
            'cache_exists': os.path.exists(cls.CACHE_FILE),
            'total_funds': len(instance._fund_dict) if instance._fund_dict else 0,
            'cache_valid': cls._is_cache_valid(),
        }

        if os.path.exists(cls.CACHE_FILE):
            try:
                with open(cls.CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                info['update_time'] = cache_data.get('update_time')
                info['cached_count'] = cache_data.get('count', 0)
            except:
                pass

        return info


# 便捷函数
def get_fund_name(fund_code: str) -> Optional[str]:
    """获取基金名称"""
    fund_info = FundListCache.get_fund_info(fund_code)
    return fund_info.get('name') if fund_info else None


def search_fund(keyword: str, limit: int = 20) -> List[Dict]:
    """搜索基金"""
    return FundListCache.search(keyword, limit)


def update_fund_list(force: bool = False) -> bool:
    """更新基金列表缓存"""
    return FundListCache.update_from_api(force)
