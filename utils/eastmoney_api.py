"""
天天基金API客户端
从天天基金网获取基金数据，支持快速接口
"""
import re
import json
import time
from datetime import datetime
from typing import Optional, Dict, List
import requests
from functools import lru_cache


class EastMoneyAPI:
    """天天基金API客户端"""

    # 基金数据API地址
    FUND_DATA_URL = "http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
    # 实时估值API地址
    REALTIME_ESTIMATE_URL = "http://fundgz.1234567.com.cn/js/{fund_code}.js"
    # 基金列表API地址
    FUND_LIST_URL = "http://fund.eastmoney.com/js/fundcode_search.js"

    # 股票实时行情API地址
    STOCK_REALTIME_URL = "http://push2.eastmoney.com/api/qt/stock/get"
    STOCK_SINA_URL = "http://hq.sinajs.cn/list={}"

    # 请求超时设置
    TIMEOUT = 10
    # 重试次数
    MAX_RETRIES = 3

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def _request(self, url: str, retries: int = None) -> Optional[str]:
        """
        发送HTTP请求（带重试）

        Args:
            url: 请求地址
            retries: 重试次数

        Returns:
            响应文本或None
        """
        if retries is None:
            retries = self.MAX_RETRIES

        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.TIMEOUT)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(0.5 * (attempt + 1))  # 递增延迟
                else:
                    print(f"请求失败 ({url}): {e}")
        return None

    def _parse_js_var(self, text: str, var_name: str) -> Optional[str]:
        """
        解析JS变量值

        Args:
            text: JS文本
            var_name: 变量名

        Returns:
            变量值字符串或None
        """
        pattern = rf'var\s+{re.escape(var_name)}\s*=\s*(.+?);'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
        return None

    def _parse_json_content(self, content: str) -> Optional[Dict]:
        """
        解析JSON内容（处理可能的引号问题）

        Args:
            content: JSON字符串

        Returns:
            解析后的字典或None
        """
        try:
            # 处理天天基金返回的特殊格式
            # 有些数据使用单引号，需要转换为双引号
            content = content.replace("'", '"')
            return json.loads(content)
        except json.JSONDecodeError:
            return None

    def get_fund_data(self, fund_code: str) -> Optional[Dict]:
        """
        获取完整基金数据

        Args:
            fund_code: 基金代码

        Returns:
            基金数据字典或None
        """
        url = self.FUND_DATA_URL.format(fund_code=fund_code)
        timestamp = int(time.time())
        url = f"{url}?v={timestamp}"

        text = self._request(url)
        if not text:
            return None

        result = {}

        # 解析基金名称
        fund_name = self._parse_js_var(text, 'fS_name')
        if fund_name:
            result['fund_name'] = fund_name.strip('"\'')

        # 解析基金代码
        result['fund_code'] = fund_code

        # 解析基金类型
        fund_type = self._parse_js_var(text, 'fS_type')
        if fund_type:
            result['fund_type'] = fund_type.strip('"\'')

        # 解析净值数据
        data_nav = self._parse_js_var(text, 'Data_netWorthTrend')
        if data_nav:
            try:
                nav_data = json.loads(data_nav)
                result['nav_history'] = nav_data
            except:
                pass

        return result if result else None

    def get_fund_name(self, fund_code: str) -> Optional[str]:
        """
        获取基金名称（快速接口）

        Args:
            fund_code: 基金代码

        Returns:
            基金名称或None
        """
        # 优先从缓存获取
        from utils.fund_list_cache import FundListCache
        fund_info = FundListCache.get_fund_info(fund_code)
        if fund_info:
            return fund_info.get('fund_name')

        # 从API获取
        url = self.FUND_DATA_URL.format(fund_code=fund_code)
        timestamp = int(time.time())
        url = f"{url}?v={timestamp}"

        text = self._request(url)
        if not text:
            return None

        fund_name = self._parse_js_var(text, 'fS_name')
        if fund_name:
            return fund_name.strip('"\'')

        return None

    def get_realtime_estimate(self, fund_code: str) -> Optional[Dict]:
        """
        获取基金实时估值

        Args:
            fund_code: 基金代码

        Returns:
            实时估值字典或None
        """
        url = self.REALTIME_ESTIMATE_URL.format(fund_code=fund_code)
        text = self._request(url)

        if not text:
            return None

        # 解析JS格式的响应
        # 格式类似: jsonpgz({"fundcode":"...","name":"...","gsz":"...","gszzl":"..."});
        pattern = r'jsonpgz\((.+?)\);?'
        match = re.search(pattern, text)
        if not match:
            return None

        try:
            data = json.loads(match.group(1))
            return {
                'fund_code': data.get('fundcode'),
                'fund_name': data.get('name'),
                'estimate_nav': data.get('gsz'),  # 估算净值
                'estimate_change_percent': data.get('gszzl'),  # 估算涨跌幅
                'estimate_time': data.get('gztime'),  # 估算时间
            }
        except json.JSONDecodeError:
            return None

    def get_nav_history(self, fund_code: str, period: str = "1年") -> Optional[List[Dict]]:
        """
        获取历史净值数据

        Args:
            fund_code: 基金代码
            period: 时间段（1月, 3月, 6月, 1年, 3年, 5年, 全部）

        Returns:
            净值历史列表或None
        """
        url = self.FUND_DATA_URL.format(fund_code=fund_code)
        timestamp = int(time.time())
        url = f"{url}?v={timestamp}"

        text = self._request(url)
        if not text:
            return None

        # 解析净值数据
        data_nav = self._parse_js_var(text, 'Data_netWorthTrend')
        if not data_nav:
            return None

        try:
            nav_data = json.loads(data_nav)
            if not nav_data or 'data' not in nav_data:
                return None

            # 提取净值数据
            result = []
            for item in nav_data['data']:
                # item格式: [日期, 净值, 累计净值, ...]
                if len(item) >= 2:
                    result.append({
                        'date': item[0],
                        'nav': item[1],
                    })

            # 根据周期筛选数据
            days_map = {
                "1月": 30,
                "3月": 90,
                "6月": 180,
                "1年": 365,
                "3年": 1090,
                "5年": 1825,
                "全部": 3650
            }
            days = days_map.get(period, 365)

            if days < 3650 and len(result) > days:
                result = result[-days:]

            return result
        except (json.JSONDecodeError, KeyError):
            return None

    def get_stock_realtime_quote_sina(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        获取股票实时行情（使用新浪HTTP接口）

        Args:
            stock_codes: 股票代码列表（6位代码）

        Returns:
            {stock_code: {'name': str, 'price': float, 'change_pct': float}}
        """
        result = {}

        # 转换代码格式为新浪格式
        # 上交所: sh600000, 深交所: sz000001
        sina_codes = []
        for code in stock_codes:
            clean_code = code[-6:]  # 取最后6位
            if clean_code.startswith('6'):
                sina_codes.append(f"sh{clean_code}")
            elif clean_code.startswith(('0', '3')):
                sina_codes.append(f"sz{clean_code}")
            else:
                sina_codes.append(f"sh{clean_code}")  # 默认

        if not sina_codes:
            return result

        # 构建请求URL
        url = self.STOCK_SINA_URL.format(','.join(sina_codes))

        text = self._request(url)
        if not text:
            return result

        # 解析响应
        # 格式: var hq_str_sh600000="股票名,今开,昨收,当前价,最高,最低,买一,卖一,...";
        for i, sina_code in enumerate(sina_codes):
            pattern = rf'var hq_str_{re.escape(sina_code)}="([^"]+)";'
            match = re.search(pattern, text)
            if match:
                data_str = match.group(1)
                parts = data_str.split(',')

                if len(parts) >= 4:
                    try:
                        name = parts[0]
                        current_price = float(parts[3]) if parts[3] else 0
                        prev_close = float(parts[2]) if parts[2] else 0

                        # 计算涨跌幅
                        change_pct = 0
                        if prev_close > 0:
                            change_pct = (current_price - prev_close) / prev_close * 100

                        # 获取原始6位代码
                        clean_code = sina_code[2:]
                        result[clean_code] = {
                            'name': name,
                            'price': current_price,
                            'change_pct': change_pct,
                            'prev_close': prev_close,
                            'open': float(parts[1]) if len(parts) > 1 and parts[1] else 0,
                            'high': float(parts[4]) if len(parts) > 4 and parts[4] else 0,
                            'low': float(parts[5]) if len(parts) > 5 and parts[5] else 0,
                        }
                    except (ValueError, IndexError):
                        continue

        return result

    def get_stock_realtime_quote(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        获取股票实时行情（使用东方财富HTTP接口）

        Args:
            stock_codes: 股票代码列表（6位代码）

        Returns:
            {stock_code: {'name': str, 'price': float, 'change_pct': float}}
        """
        result = {}

        # 构建证券代码列表（逗号分隔）
        secids = []
        code_map = {}  # 用于映射secid到原始代码

        for code in stock_codes:
            clean_code = code[-6:]  # 取最后6位
            if clean_code.startswith('6'):
                # 上交所: 1.600000
                secid = f"1.{clean_code}"
            elif clean_code.startswith(('0', '3')):
                # 深交所: 0.000001 或 0.300001
                secid = f"0.{clean_code}"
            else:
                secid = f"1.{clean_code}"

            secids.append(secid)
            code_map[secid] = clean_code

        if not secids:
            return result

        # 构建请求参数
        secid_str = ','.join(secids)
        params = {
            'secid': secid_str,
            'fields': 'f57,f58,f162,f163,f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f60',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'cb': 'jQuery',
            '_': int(time.time() * 1000)
        }

        try:
            response = self.session.get(
                self.STOCK_REALTIME_URL,
                params=params,
                timeout=self.TIMEOUT
            )
            response.raise_for_status()

            # 解析JSON响应
            data = response.json()

            if data.get('rc') == 0 and 'data' in data:
                for item in data['data']:
                    if 'diff' in item:
                        for stock in item['diff']:
                            # f57: 代码, f58: 名称, f43: 最新价, f60: 涨跌幅
                            # f162: 今开, f163: 昨收, f44: 最高, f45: 最低
                            code = stock.get('f57', '')
                            name = stock.get('f58', '')
                            price = stock.get('f43', 0)
                            change_pct = stock.get('f60', 0)

                            if code:
                                result[code] = {
                                    'name': name,
                                    'price': float(price) if price else 0,
                                    'change_pct': float(change_pct) if change_pct else 0,
                                    'open': float(stock.get('f162', 0)) if stock.get('f162') else 0,
                                    'prev_close': float(stock.get('f163', 0)) if stock.get('f163') else 0,
                                    'high': float(stock.get('f44', 0)) if stock.get('f44') else 0,
                                    'low': float(stock.get('f45', 0)) if stock.get('f45') else 0,
                                }
        except Exception as e:
            print(f"东方财富接口获取股票行情失败: {e}")
            # 失败时尝试新浪接口
            return self.get_stock_realtime_quote_sina(stock_codes)

        return result

    def get_stock_intraday_sina(self, stock_code: str) -> Optional[List[Dict]]:
        """
        获取股票分时数据（使用新浪HTTP接口）

        Args:
            stock_code: 股票代码（6位）

        Returns:
            分时数据列表 [{'time': '09:30', 'price': 10.5, 'change_pct': 1.2}, ...]
        """
        clean_code = stock_code[-6:]

        # 判断市场
        if clean_code.startswith('6'):
            sina_code = f"sh{clean_code}"
        else:
            sina_code = f"sz{clean_code}"

        # 新浪分时数据接口
        url = f"http://image.sinajs.cn/newchart/min/n/{sina_code}.gif"

        # 这个接口返回的是图片，不是数据
        # 需要使用其他接口获取分时数据
        # 暂时返回空，可以使用腾讯接口作为替代
        return None

    def get_all_funds_list(self) -> Optional[List[Dict]]:
        """
        获取所有基金列表

        Returns:
            基金列表 [{'code': '', 'name': '', 'type': ''}, ...]
        """
        text = self._request(self.FUND_LIST_URL)
        if not text:
            return None

        # 解析JS数组格式
        # 格式类似: var r = [["000001","HXCZDD","华夏成长混合","混合型"],...];
        pattern = r'var\s+r\s*=\s*(\[.+?\]);'
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            return None

        try:
            fund_list = json.loads(match.group(1))
            result = []
            for item in fund_list:
                if len(item) >= 4:
                    result.append({
                        'code': item[0],
                        'name': item[2],
                        'type': item[3],
                        'pinyin': item[1] if len(item) > 1 else ''
                    })
            return result
        except json.JSONDecodeError:
            return None


# 单例模式
_instance = None

def get_eastmoney_api() -> EastMoneyAPI:
    """获取EastMoneyAPI单例"""
    global _instance
    if _instance is None:
        _instance = EastMoneyAPI()
    return _instance
