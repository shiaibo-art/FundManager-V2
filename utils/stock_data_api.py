"""
A股数据增强接口
整合多个数据源，提供稳定的A股数据获取能力
支持：实时行情、历史数据、板块数据、市场概况等
"""
import time
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
import requests

# 数据源优先级
DATA_SOURCES = {
    'tencent': {
        'name': '腾讯财经',
        'priority': 1,
        'available': True
    },
    'sina': {
        'name': '新浪财经',
        'priority': 2,
        'available': True
    },
    'eastmoney': {
        'name': '东方财富',
        'priority': 3,
        'available': True
    },
    'akshare': {
        'name': 'AkShare',
        'priority': 4,
        'available': False  # 需要导入检查
    }
}


@dataclass
class StockQuote:
    """股票行情数据"""
    code: str
    name: str = ""
    price: float = 0.0
    change_pct: float = 0.0
    change_amt: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    prev_close: float = 0.0
    volume: float = 0.0
    amount: float = 0.0
    timestamp: str = ""


@dataclass
class IndexQuote:
    """指数行情数据"""
    code: str
    name: str
    price: float
    change_pct: float
    change_amt: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: float = 0.0
    timestamp: str = ""


class StockDataAPI:
    """A股数据API - 整合多数据源"""

    # 主要指数代码
    MAIN_INDICES = {
        'sh000001': '上证指数',
        'sz399001': '深证成指',
        'sz399006': '创业板指',
        'sh000688': '科创50',
        'sh000300': '沪深300',
        'sh000016': '上证50',
        'sz399005': '中小板指',
    }

    # 请求超时
    TIMEOUT = 10
    MAX_RETRIES = 3

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 60  # 缓存60秒

        # 检查AkShare是否可用
        self._check_akshare()

    def _check_akshare(self):
        """检查AkShare是否可用"""
        try:
            import akshare as ak
            DATA_SOURCES['akshare']['available'] = True
            self.akshare = ak
        except ImportError:
            DATA_SOURCES['akshare']['available'] = False
            self.akshare = None

    def _is_cache_valid(self, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self._cache_time:
            return False
        return (time.time() - self._cache_time[key]) < self._cache_ttl

    def _set_cache(self, key: str, data):
        """设置缓存"""
        self._cache[key] = data
        self._cache_time[key] = time.time()

    def _get_cache(self, key: str):
        """获取缓存"""
        if self._is_cache_valid(key):
            return self._cache.get(key)
        return None

    def get_indices(self) -> List[IndexQuote]:
        """
        获取主要指数行情

        Returns:
            List[IndexQuote]: 指数列表
        """
        cache_key = 'indices'
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        indices = []

        # 方案1: 腾讯API
        try:
            codes = list(self.MAIN_INDICES.keys())
            url = f"https://qt.gtimg.cn/q={','.join(codes)}"
            resp = self.session.get(url, timeout=self.TIMEOUT)
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

                        if code_full in self.MAIN_INDICES:
                            if prev_close > 0:
                                change_pct = ((price - prev_close) / prev_close) * 100
                            else:
                                change_pct = 0

                            indices.append(IndexQuote(
                                code=code,
                                name=self.MAIN_INDICES.get(code_full, name),
                                price=price,
                                change_pct=change_pct,
                                change_amt=price - prev_close,
                                timestamp=datetime.now().strftime('%H:%M:%S')
                            ))
        except Exception as e:
            print(f"腾讯API获取指数失败: {e}")

        # 如果腾讯失败，尝试新浪
        if not indices:
            indices = self._get_indices_sina()

        self._set_cache(cache_key, indices)
        return indices

    def _get_indices_sina(self) -> List[IndexQuote]:
        """使用新浪API获取指数"""
        indices = []
        try:
            # 新浪指数代码
            sina_codes = ['sh000001', 'sz399001', 'sz399006', 'sh000688', 'sh000300']
            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.encoding = 'gbk'

            for line in resp.text.strip().split('\n'):
                if 'hq_str_' in line and '=' in line:
                    parts = line.split('=')
                    code = parts[0].split('var hq_str_')[1].split(';')[0]
                    data_str = parts[1].strip('";')
                    data = data_str.split(',')

                    if len(data) > 2 and data[0]:
                        price = float(data[1]) if data[1] else 0
                        prev_close = float(data[2]) if data[2] else 0
                        change_pct = ((price - prev_close) / prev_close) * 100 if prev_close > 0 else 0

                        indices.append(IndexQuote(
                            code=code[2:],
                            name=data[0],
                            price=price,
                            change_pct=change_pct,
                            change_amt=price - prev_close,
                            timestamp=datetime.now().strftime('%H:%M:%S')
                        ))
        except Exception as e:
            print(f"新浪API获取指数失败: {e}")

        return indices

    def get_stock_quote(self, stock_codes: List[str]) -> Dict[str, StockQuote]:
        """
        获取股票实时行情

        Args:
            stock_codes: 股票代码列表，支持6位代码或带市场前缀（sh600000）

        Returns:
            Dict[str, StockQuote]: {股票代码: 行情数据}
        """
        result = {}

        # 清理代码格式
        clean_codes = []
        for code in stock_codes:
            clean_code = code.replace('SH', '').replace('SZ', '')[-6:]
            clean_codes.append(clean_code)

        # 构建腾讯格式代码
        tencent_codes = []
        for code in clean_codes:
            if code.startswith('6'):
                tencent_codes.append(f'sh{code}')
            else:
                tencent_codes.append(f'sz{code}')

        try:
            url = f"https://qt.gtimg.cn/q={','.join(tencent_codes)}"
            resp = self.session.get(url, timeout=self.TIMEOUT)
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
                        high = self._safe_float(parts[6]) if len(parts) > 6 else 0
                        low = self._safe_float(parts[5]) if len(parts) > 5 else 0
                        open_price = self._safe_float(parts[5]) if len(parts) > 5 else 0

                        if prev_close > 0:
                            change_pct = ((price - prev_close) / prev_close) * 100
                        else:
                            change_pct = 0

                        result[code] = StockQuote(
                            code=code,
                            name=name,
                            price=price,
                            change_pct=change_pct,
                            change_amt=price - prev_close,
                            open=open_price,
                            high=high,
                            low=low,
                            prev_close=prev_close,
                            timestamp=datetime.now().strftime('%H:%M:%S')
                        )
        except Exception as e:
            print(f"腾讯API获取股票行情失败: {e}")

        # 如果有股票没获取到，尝试新浪
        missing_codes = [c for c in clean_codes if c not in result]
        if missing_codes:
            sina_result = self._get_stock_quote_sina(missing_codes)
            result.update(sina_result)

        return result

    def _get_stock_quote_sina(self, stock_codes: List[str]) -> Dict[str, StockQuote]:
        """使用新浪API获取股票行情"""
        result = {}
        try:
            sina_codes = []
            code_map = {}
            for code in stock_codes:
                if code.startswith('6'):
                    sc = f"sh{code}"
                else:
                    sc = f"sz{code}"
                sina_codes.append(sc)
                code_map[sc] = code

            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.encoding = 'gbk'

            for line in resp.text.strip().split('\n'):
                if 'hq_str_' in line:
                    sc = line.split('var hq_str_')[1].split('=')[0]
                    data_str = line.split('=')[1].strip('";')
                    data = data_str.split(',')

                    if len(data) > 3 and data[0]:
                        name = data[0]
                        open_price = float(data[1]) if data[1] else 0
                        prev_close = float(data[2]) if data[2] else 0
                        price = float(data[3]) if data[3] else 0
                        high = float(data[4]) if len(data) > 4 and data[4] else 0
                        low = float(data[5]) if len(data) > 5 and data[5] else 0

                        if prev_close > 0:
                            change_pct = ((price - prev_close) / prev_close) * 100
                        else:
                            change_pct = 0

                        clean_code = code_map.get(sc, sc[2:])
                        result[clean_code] = StockQuote(
                            code=clean_code,
                            name=name,
                            price=price,
                            change_pct=change_pct,
                            change_amt=price - prev_close,
                            open=open_price,
                            high=high,
                            low=low,
                            prev_close=prev_close,
                            timestamp=datetime.now().strftime('%H:%M:%S')
                        )
        except Exception as e:
            print(f"新浪API获取股票行情失败: {e}")

        return result

    def get_hot_sectors(self, limit: int = 20) -> List[Dict]:
        """
        获取热门板块

        Args:
            limit: 返回数量

        Returns:
            List[Dict]: [{'code': str, 'name': str, 'change_pct': float, 'amount': float}]
        """
        cache_key = 'hot_sectors'
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        sectors = []

        try:
            # 东方财富板块API
            url = "https://push2.eastmoney.com/api/qt/clist/get"
            params = {
                "pn": "1",
                "pz": str(limit),
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": "m:90 t:2",  # 行业板块
                "fields": "f12,f14,f3,f62",
            }

            resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
            data = resp.json()

            if data.get("data") and data["data"].get("diff"):
                for item in data["data"]["diff"][:limit]:
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

    def get_market_summary(self) -> Dict:
        """
        获取市场概况

        Returns:
            Dict: {
                'indices': List[IndexQuote],
                'hot_sectors': List[Dict],
                'market_sentiment': str,
                'market_score': int,
                'update_time': str
            }
        """
        indices = self.get_indices()
        hot_sectors = self.get_hot_sectors(limit=10)

        # 计算市场情绪
        score = 50
        signals = []

        if indices:
            sh_index = next((i for i in indices if '上证' in i.name), None)
            if sh_index:
                change = sh_index.change_pct
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

        if hot_sectors:
            avg_change = sum(s['change_pct'] for s in hot_sectors[:5]) / min(5, len(hot_sectors))
            if avg_change > 2:
                score += 10
                signals.append('板块普涨')
            elif avg_change > 0:
                score += 5
                signals.append('板块偏强')

        score = max(0, min(100, score))

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
            'indices': [{'code': i.code, 'name': i.name, 'price': i.price, 'change_pct': i.change_pct} for i in indices],
            'hot_sectors': hot_sectors,
            'market_sentiment': sentiment,
            'market_score': score,
            'update_time': datetime.now().strftime('%H:%M:%S')
        }

    def get_stock_history(self, stock_code: str, period: str = "1月") -> List[Dict]:
        """
        获取股票历史数据

        Args:
            stock_code: 股票代码（6位）
            period: 1周, 1月, 3月, 6月, 1年

        Returns:
            List[Dict]: [{'date': str, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float}]
        """
        days_map = {
            "1周": 7,
            "1月": 30,
            "3月": 90,
            "6月": 180,
            "1年": 365,
        }
        days = days_map.get(period, 30)

        # 如果AkShare可用，使用AkShare
        if self.akshare:
            try:
                clean_code = stock_code[-6:]
                if clean_code.startswith('6'):
                    symbol = f"sh{clean_code}"
                else:
                    symbol = f"sz{clean_code}"

                df = self.akshare.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=(datetime.now() - __import__('datetime').timedelta(days=days)).strftime('%Y%m%d'),
                    adjust=""
                )

                if df is not None and len(df) > 0:
                    result = []
                    for _, row in df.iterrows():
                        result.append({
                            'date': str(row['日期']),
                            'open': float(row['开盘']),
                            'high': float(row['最高']),
                            'low': float(row['最低']),
                            'close': float(row['收盘']),
                            'volume': float(row['成交量'])
                        })
                    return result
            except Exception as e:
                print(f"AkShare获取历史数据失败: {e}")

        # 备用方案：使用东方财富HTTP接口
        return self._get_history_eastmoney(stock_code, days)

    def _get_history_eastmoney(self, stock_code: str, days: int) -> List[Dict]:
        """使用东方财富接口获取历史数据"""
        try:
            clean_code = stock_code[-6:]
            if clean_code.startswith('6'):
                secid = f"1.{clean_code}"
            else:
                secid = f"0.{clean_code}"

            url = "https://push2his.eastmoney.com/api/qt/stock/klt"
            params = {
                'secid': secid,
                'fields1': 'f1,f2,f3,f4,f5,f6',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
                'klt': 101,  # 日K
                'fqt': 0,
                'end': '20500101',
                'lmt': days,
            }

            resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
            data = resp.json()

            if data.get('rc') == 0 and 'data' in data:
                items = data['data'].get('items', [])
                result = []
                for item in items:
                    if len(item) >= 6:
                        result.append({
                            'date': item[0],
                            'open': float(item[1]) if item[1] else 0,
                            'close': float(item[2]) if item[2] else 0,
                            'high': float(item[3]) if item[3] else 0,
                            'low': float(item[4]) if item[4] else 0,
                            'volume': float(item[5]) if item[5] else 0,
                        })
                return result[::-1]  # 反转顺序，从旧到新
        except Exception as e:
            print(f"东方财富获取历史数据失败: {e}")

        return []

    def _safe_float(self, val, default=0.0):
        """安全转换为浮点数"""
        try:
            if val is None or (isinstance(val, float) and __import__('pandas').isna(val)):
                return default
            return float(val)
        except:
            return default


# 单例
_instance = None


def get_stock_api() -> StockDataAPI:
    """获取StockDataAPI单例"""
    global _instance
    if _instance is None:
        _instance = StockDataAPI()
    return _instance


if __name__ == "__main__":
    # 测试
    api = get_stock_api()

    print("="*50)
    print("测试1: 获取主要指数")
    print("="*50)
    indices = api.get_indices()
    for idx in indices:
        print(f"  {idx.name}: {idx.price:.2f}, {idx.change_pct:+.2f}%")

    print("\n" + "="*50)
    print("测试2: 获取市场概况")
    print("="*50)
    summary = api.get_market_summary()
    print(f"市场情绪: {summary['market_sentiment']} (评分: {summary['market_score']})")
    print(f"更新时间: {summary['update_time']}")

    print("\n热门板块:")
    for sec in summary['hot_sectors'][:5]:
        print(f"  {sec['name']}: {sec['change_pct']:+.2f}%")
