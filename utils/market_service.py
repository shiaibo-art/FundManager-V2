"""
市场数据服务 - 优化版
整合多数据源，提供稳定的A股市场数据
"""
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field

# 尝试导入 akshare
try:
    import akshare as ak
    import pandas as pd
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False

# 导入新的股票数据API
try:
    from utils.stock_data_api import get_stock_api, StockDataAPI
    STOCK_API_AVAILABLE = True
except ImportError:
    STOCK_API_AVAILABLE = False


@dataclass
class MarketContext:
    """市场上下文数据"""
    indices: List[Dict] = field(default_factory=list)
    north_flow: Dict = field(default_factory=dict)
    main_flow: Dict = field(default_factory=dict)
    breadth: Dict = field(default_factory=dict)
    hot_sectors: List[Dict] = field(default_factory=list)
    market_sentiment: str = "中性"
    market_score: int = 50
    update_time: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'indices': self.indices,
            'north_flow': self.north_flow,
            'main_flow': self.main_flow,
            'breadth': self.breadth,
            'hot_sectors': self.hot_sectors,
            'market_sentiment': self.market_sentiment,
            'market_score': self.market_score,
            'update_time': self.update_time
        }


class MarketDataService:
    """市场数据服务"""

    _instance = None

    # 主要指数映射
    MAIN_INDICES = {
        'sh000001': '上证指数',
        'sz399001': '深证成指',
        'sz399006': '创业板指',
        'sh000688': '科创50',
        'sh000300': '沪深300',
    }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 60  # 缓存60秒

    def _is_cache_valid(self, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self._cache_time:
            return False
        return (time.time() - self._cache_time[key]) < self._cache_ttl

    def _set_cache(self, key: str, data: Any):
        """设置缓存"""
        self._cache[key] = data
        self._cache_time[key] = time.time()

    def _get_cache(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if self._is_cache_valid(key):
            return self._cache.get(key)
        return None

    def _safe_float(self, val, default=0.0):
        """安全转换为浮点数"""
        try:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return float(val)
        except (ValueError, TypeError):
            return default

    def get_indices(self) -> List[Dict[str, Any]]:
        """获取主要指数行情（优先使用增强API）"""
        cache_key = 'indices'
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        indices = []

        # 优先使用新的stock_data_api
        if STOCK_API_AVAILABLE:
            try:
                stock_api = get_stock_api()
                index_quotes = stock_api.get_indices()
                indices = [
                    {
                        "code": idx.code,
                        "name": idx.name,
                        "price": idx.price,
                        "change_pct": idx.change_pct,
                        "change_amt": idx.change_amt,
                    }
                    for idx in index_quotes
                ]
            except Exception as e:
                print(f"增强API获取指数失败: {e}")

        # 如果增强API失败，使用原有逻辑
        if not indices:
            indices = self._get_indices_legacy()

        if not indices:
            indices = self._get_fallback_indices()

        self._set_cache(cache_key, indices)
        return indices

    def _get_indices_legacy(self) -> List[Dict[str, Any]]:
        """原有获取指数逻辑（作为备用）"""
        indices = []

        try:
            import requests
            codes = {
                'sh000001': ('000001', '上证指数'),
                'sz399001': ('399001', '深证成指'),
                'sz399006': ('399006', '创业板指'),
                'sh000688': ('000688', '科创50'),
                'sh000300': ('000300', '沪深300'),
            }

            url = f"https://qt.gtimg.cn/q={','.join(codes.keys())}"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = 'gbk'

            for line in resp.text.strip().split('\n'):
                if '~' in line and '=' in line:
                    parts = line.split('~')
                    if len(parts) > 5:
                        code_full = parts[0].split('"')[0].replace('v_', '').replace('=', '')
                        name = parts[1]
                        code = parts[2]
                        price = self._safe_float(parts[3])
                        prev_close = self._safe_float(parts[4])

                        if code_full in codes:
                            expected_code, expected_name = codes[code_full]
                            if prev_close > 0:
                                change_pct = ((price - prev_close) / prev_close) * 100
                            else:
                                change_pct = 0

                            indices.append({
                                "code": expected_code,
                                "name": expected_name,
                                "price": price,
                                "change_pct": change_pct,
                                "change_amt": price - prev_close if prev_close > 0 else 0,
                            })

        except Exception as e:
            print(f"获取指数失败: {e}")

        return indices

    def _get_fallback_indices(self) -> List[Dict[str, Any]]:
        """获取备用指数数据"""
        return [
            {'code': '000001', 'name': '上证指数', 'price': 0, 'change_pct': 0},
            {'code': '399001', 'name': '深证成指', 'price': 0, 'change_pct': 0},
            {'code': '399006', 'name': '创业板指', 'price': 0, 'change_pct': 0},
        ]

    def _fetch_em_data(self, url: str, params: Dict) -> Optional[Dict]:
        """获取东方财富数据"""
        try:
            import requests
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"请求失败: {e}")
        return None

    def get_north_flow(self) -> Dict[str, Any]:
        """获取北向资金流向"""
        cache_key = 'north_flow'
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {
            'total': 0,
            'sh': 0,
            'sz': 0,
            'update_time': datetime.now().strftime('%H:%M:%S')
        }

        if not AKSHARE_AVAILABLE:
            return result

        try:
            # 使用东方财富接口
            data = self._fetch_em_data(
                "https://push2.eastmoney.com/api/qt/stock/fflow/get",
                {
                    "lmt": "0",
                    "klt": "1",
                    "secid": "1.000001",  # 上证指数
                    "fields1": "f1,f2,f3,f7",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
                    "ut": "b5f34954bc9e19f6",
                    "cb": "bingrg",
                    "fltt": "2"
                }
            )

            if data:
                # 简化处理，返回默认值
                pass

        except Exception as e:
            print(f"获取北向资金失败: {e}")

        self._set_cache(cache_key, result)
        return result

    def get_hot_sectors(self) -> List[Dict[str, Any]]:
        """获取热门板块"""
        cache_key = 'hot_sectors'
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        sectors = []

        try:
            data = self._fetch_em_data(
                "https://push2.eastmoney.com/api/qt/clist/get",
                {
                    "pn": "1",
                    "pz": "20",
                    "po": "1",
                    "np": "1",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f3",
                    "fs": "m:90 t:2",  # 行业板块
                    "fields": "f12,f14,f3,f62",
                }
            )

            if data and data.get("data") and data["data"].get("diff"):
                for item in data["data"]["diff"][:10]:
                    sectors.append({
                        'code': str(item.get('f12', '')),
                        'name': str(item.get('f14', '')),
                        'change_pct': float(item.get('f3', 0) or 0),
                        'amount': self._safe_float(item.get('f62', 0))
                    })

        except Exception as e:
            print(f"获取热门板块失败: {e}")

        self._set_cache(cache_key, sectors)
        return sectors

    def calculate_market_score(self) -> Dict[str, Any]:
        """计算市场情绪评分"""
        indices = self.get_indices()
        score = 50  # 基础分
        signals = []

        # 1. 指数涨跌
        sh_index = next((i for i in indices if '上证' in i.get('name', '')), None)
        if sh_index:
            change = sh_index.get('change_pct', 0)
            if change > 1:
                score += 10
                signals.append('大盘上涨')
            elif change > 0:
                score += 5
                signals.append('大盘微涨')
            elif change < -1:
                score -= 10
                signals.append('大盘下跌')
            elif change < 0:
                score -= 5
                signals.append('大盘微跌')

        # 2. 热门板块
        hot_sectors = self.get_hot_sectors()
        if hot_sectors:
            avg_change = sum(s.get('change_pct', 0) for s in hot_sectors[:5]) / min(5, len(hot_sectors))
            if avg_change > 2:
                score += 10
                signals.append('板块普涨')
            elif avg_change > 0:
                score += 5
                signals.append('板块偏强')

        score = max(0, min(100, score))

        # 生成情绪描述
        if score >= 70:
            sentiment = '乐观'
        elif score >= 55:
            sentiment = '偏多'
        elif score >= 45:
            sentiment = '中性'
        elif score >= 30:
            sentiment = '偏空'
        else:
            sentiment = '悲观'

        return {
            'score': score,
            'sentiment': sentiment,
            'signals': signals
        }

    def get_market_context(self) -> MarketContext:
        """获取完整市场上下文"""
        indices = self.get_indices()
        north_flow = self.get_north_flow()
        hot_sectors = self.get_hot_sectors()
        score_data = self.calculate_market_score()

        return MarketContext(
            indices=indices,
            north_flow=north_flow,
            main_flow={},
            breadth={},
            hot_sectors=hot_sectors,
            market_sentiment=score_data['sentiment'],
            market_score=score_data['score'],
            update_time=datetime.now().strftime('%H:%M:%S')
        )


# 单例
_service_instance = None

def get_market_service() -> MarketDataService:
    """获取市场数据服务单例"""
    global _service_instance
    if _service_instance is None:
        _service_instance = MarketDataService()
    return _service_instance
