"""
基金数据获取模块
主要使用天天基金API，AkShare作为备用
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

# 导入天天基金API
try:
    from utils.eastmoney_api import get_eastmoney_api
    from utils.fund_list_cache import FundListCache
    from utils.cache_manager import cache_multi
    EASTMONEY_AVAILABLE = True
except ImportError:
    EASTMONEY_AVAILABLE = False

# 是否使用AkShare作为备用（当天天基金API失败时）
USE_AKSHARE_FALLBACK = True


def get_etf_nav_history(etf_code: str, period: str = "1年") -> pd.DataFrame:
    """
    获取ETF历史净值
    使用 fund_open_fund_info_em 接口

    Args:
        etf_code: ETF代码
        period: 1月, 3月, 6月, 1年, 3年, 5年, 全部

    Returns:
        DataFrame with 'date' and 'nav' columns
    """
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

    try:
        df = ak.fund_open_fund_info_em(
            symbol=etf_code,
            indicator="单位净值走势"
        )

        if df is None or len(df) == 0:
            return pd.DataFrame()

        # 重命名列
        rename_map = {}
        for col in df.columns:
            if '净值日期' in col or 'date' in col.lower():
                rename_map[col] = 'date'
            elif '单位净值' in col or '净值' in col:
                rename_map[col] = 'nav'

        if rename_map:
            df = df.rename(columns=rename_map)

        # 确保有date列
        if 'date' not in df.columns:
            for col in df.columns:
                if '日期' in col or 'date' in col.lower():
                    df['date'] = pd.to_datetime(df[col])
                    break
            else:
                df = df.reset_index()
                df['date'] = pd.to_datetime(df.iloc[:, 0])
        else:
            df['date'] = pd.to_datetime(df['date'])

        # 确保有nav列
        if 'nav' not in df.columns:
            for col in df.columns:
                if '净值' in col or 'nav' in col.lower():
                    df['nav'] = pd.to_numeric(df[col], errors='coerce')
                    break
            else:
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                if len(numeric_cols) > 0:
                    df['nav'] = pd.to_numeric(df[numeric_cols[0]], errors='coerce')

        df = df.dropna(subset=['nav'])

        # 根据period筛选数据
        cutoff_date = datetime.now() - timedelta(days=days)
        if hasattr(df['date'].iloc[0], 'date'):
            df = df[df['date'].dt.date >= cutoff_date.date()].copy()
        else:
            df = df[df['date'] >= cutoff_date.date()].copy()

        return df[['date', 'nav']].sort_values('date').reset_index(drop=True)

    except Exception:
        return pd.DataFrame()


@cache_multi(memory_ttl=600, file_ttl=7200)  # 内存10分钟，文件2小时
def get_fund_nav_history(fund_code: str, period: str = "1年") -> pd.DataFrame:
    """
    获取基金历史净值（优先使用天天基金）
    fund_code: 基金代码
    period: 1月, 3月, 6月, 1年, 3年, 5年, 全部
    """
    # 1. 优先使用天天基金API
    if EASTMONEY_AVAILABLE:
        try:
            api = get_eastmoney_api()
            nav_list = api.get_nav_history(fund_code, period=period)
            if nav_list and len(nav_list) > 0:
                # 转换为DataFrame
                df = pd.DataFrame(nav_list)
                df['date'] = pd.to_datetime(df['date'])
                df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
                df = df.dropna(subset=['nav'])
                return df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            print(f"天天基金API获取历史净值失败: {e}")

    # 2. 备用方案：使用AkShare
    if USE_AKSHARE_FALLBACK:
        try:
            # 获取单位净值走势 - 使用正确的akshare API
            # akshare 使用 symbol 参数
            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")

            if df is None or len(df) == 0:
                return pd.DataFrame()

            # 处理列名（akshare返回的列名可能不同）
            # 常见列名: 净值日期, 单位净值, 累计净值, 日增长率
            column_mapping = {}
            for col in df.columns:
                if '净值日期' in col or 'date' in col.lower():
                    column_mapping[col] = 'date'
                elif '单位净值' in col or '净值' in col:
                    column_mapping[col] = 'nav'
                elif '累计净值' in col:
                    column_mapping[col] = 'accumulated_nav'
                elif '日增长率' in col or '增长率' in col:
                    column_mapping[col] = 'daily_return'

            if column_mapping:
                df = df.rename(columns=column_mapping)

            # 确保有所需的列
            if 'nav' not in df.columns:
                # 尝试查找数值列作为净值
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                if len(numeric_cols) > 0:
                    df['nav'] = df[numeric_cols[0]]
                else:
                    return pd.DataFrame()

            # 标准化列名
            if 'date' not in df.columns:
                # 使用索引作为日期
                df = df.reset_index()
                df['date'] = pd.to_datetime(df.iloc[:, 0])
            else:
                df['date'] = pd.to_datetime(df['date'])

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
            start_date = datetime.now() - timedelta(days=days)

            df = df[df['date'] >= start_date].copy()

            # 确保有所需的列
            required_cols = ['date', 'nav']
            optional_cols = ['accumulated_nav', 'daily_return']
            available_cols = [c for c in optional_cols if c in df.columns]
            cols_to_keep = required_cols + available_cols

            return df[cols_to_keep].sort_values('date').reset_index(drop=True)
        except Exception as e:
            print(f"AkShare获取历史净值失败: {e}")

    return pd.DataFrame()


@cache_multi(memory_ttl=300, file_ttl=3600)  # 内存5分钟，文件1小时
def get_fund_latest_nav(fund_code: str) -> Optional[float]:
    """
    获取基金最新净值（优先使用天天基金）
    """
    # 1. 优先使用天天基金API
    if EASTMONEY_AVAILABLE:
        try:
            api = get_eastmoney_api()
            nav_history = api.get_nav_history(fund_code, period="1月")
            if nav_history and len(nav_history) > 0:
                return float(nav_history[-1]['nav'])
        except Exception as e:
            print(f"天天基金API获取最新净值失败: {e}")

    # 2. 备用方案：使用AkShare
    if USE_AKSHARE_FALLBACK:
        try:
            df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
            if df is not None and len(df) > 0:
                # 获取最新净值
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                if len(numeric_cols) > 0:
                    return float(df[numeric_cols[0]].iloc[-1])
        except Exception as e:
            print(f"AkShare获取最新净值失败: {e}")

    return None


@cache_multi(memory_ttl=3600, file_ttl=86400)  # 内存1小时，文件1天
def get_fund_name(fund_code: str) -> str:
    """
    获取基金名称（优先使用缓存）
    """
    # 1. 优先从基金列表缓存获取
    if EASTMONEY_AVAILABLE:
        fund_info = FundListCache.get_fund_info(fund_code)
        if fund_info and fund_info.get('name'):
            return fund_info['name']

    # 2. 如果缓存未找到，使用天天基金API
    if EASTMONEY_AVAILABLE:
        try:
            api = get_eastmoney_api()
            name = api.get_fund_name(fund_code)
            if name:
                return name
        except Exception as e:
            print(f"天天基金API获取基金名称失败: {e}")

    # 3. 备用方案：使用AkShare
    if USE_AKSHARE_FALLBACK:
        try:
            import akshare as ak
            df = ak.fund_name_em()

            if df is not None and len(df) > 0:
                code_col = df.columns[0]
                name_col = df.columns[2]

                result = df[df[code_col].astype(str) == fund_code]
                if len(result) > 0:
                    return str(result.iloc[0][name_col])
        except Exception as e:
            print(f"AkShare获取基金名称失败: {e}")

    # 如果都找不到，返回默认值
    return f"基金{fund_code}"


def get_fund_info(fund_code: str) -> Optional[Dict]:
    """
    获取基金基本信息
    fund_code: 基金代码，6位数字
    """
    try:
        df = get_fund_nav_history(fund_code, period="1月")

        if df is None or len(df) == 0:
            return None

        # 获取最新一行
        latest = df.iloc[-1]

        info = {
            'fund_code': fund_code,
            'fund_name': get_fund_name(fund_code),
            'latest_nav': float(latest['nav']),
            'date': str(latest['date'].date())
        }

        if 'accumulated_nav' in df.columns:
            info['accumulated_nav'] = float(latest['accumulated_nav'])

        return info
    except Exception as e:
        print(f"获取基金信息失败: {e}")
        return None


def calculate_max_drawdown(df: pd.DataFrame) -> Tuple[float, str, str]:
    """
    计算最大回撤
    返回: (最大回撤比例, 回撤开始日期, 回撤结束日期)
    """
    if len(df) < 2:
        return 0.0, "", ""

    # 计算累计最高净值
    df = df.copy()
    df['cummax'] = df['nav'].cummax()

    # 计算回撤
    df['drawdown'] = (df['nav'] - df['cummax']) / df['cummax'] * 100

    # 找到最大回撤
    max_dd_idx = df['drawdown'].idxmin()
    max_dd = df.loc[max_dd_idx, 'drawdown']

    # 找到回撤开始和结束日期
    peak_idx = df.loc[:max_dd_idx, 'nav'].idxmax()
    peak_date = str(df.loc[peak_idx, 'date'].date())
    end_date = str(df.loc[max_dd_idx, 'date'].date())

    return round(max_dd, 2), peak_date, end_date


def calculate_volatility(df: pd.DataFrame, annualize: bool = True) -> float:
    """
    计算波动率（标准差）
    annualize: 是否年化
    """
    if len(df) < 2:
        return 0.0

    # 计算日收益率
    df = df.copy()
    df['daily_return'] = df['nav'].pct_change() * 100

    # 计算标准差
    volatility = df['daily_return'].std()

    # 年化波动率（假设一年252个交易日）
    if annualize:
        volatility = volatility * (252 ** 0.5)

    return round(volatility, 2)


def calculate_returns_by_period(df: pd.DataFrame) -> Dict[str, float]:
    """
    计算各时间段收益率
    """
    if len(df) < 2:
        return {}

    returns = {}
    latest_nav = df.iloc[-1]['nav']

    # 近1周
    if len(df) >= 5:
        week_ago_nav = df.iloc[-5]['nav']
        returns['近1周'] = round((latest_nav - week_ago_nav) / week_ago_nav * 100, 2)

    # 近1月
    if len(df) >= 20:
        month_ago_nav = df.iloc[-20]['nav']
        returns['近1月'] = round((latest_nav - month_ago_nav) / month_ago_nav * 100, 2)

    # 近3月
    if len(df) >= 60:
        quarter_ago_nav = df.iloc[-60]['nav']
        returns['近3月'] = round((latest_nav - quarter_ago_nav) / quarter_ago_nav * 100, 2)

    # 近6月
    if len(df) >= 120:
        half_year_ago_nav = df.iloc[-120]['nav']
        returns['近6月'] = round((latest_nav - half_year_ago_nav) / half_year_ago_nav * 100, 2)

    # 近1年
    if len(df) >= 252:
        year_ago_nav = df.iloc[-252]['nav']
        returns['近1年'] = round((latest_nav - year_ago_nav) / year_ago_nav * 100, 2)

    # 今年以来
    start_of_year = df[df['date'].dt.month == 1]
    if len(start_of_year) > 0:
        year_start_nav = start_of_year.iloc[0]['nav']
        returns['今年以来'] = round((latest_nav - year_start_nav) / year_start_nav * 100, 2)

    # 成立以来
    if len(df) > 0:
        inception_nav = df.iloc[0]['nav']
        returns['成立以来'] = round((latest_nav - inception_nav) / inception_nav * 100, 2)

    return returns


def get_fund_statistics(df: pd.DataFrame) -> Dict:
    """
    获取基金统计指标
    """
    if df is None or len(df) == 0:
        return {}

    stats = {
        '最新净值': round(df.iloc[-1]['nav'], 4),
        '最高净值': round(df['nav'].max(), 4),
        '最低净值': round(df['nav'].min(), 4),
        '平均净值': round(df['nav'].mean(), 4),
        '数据天数': len(df),
        '开始日期': str(df.iloc[0]['date'].date()),
        '结束日期': str(df.iloc[-1]['date'].date())
    }

    # 计算最大回撤
    max_dd, peak_date, end_date = calculate_max_drawdown(df)
    stats['最大回撤'] = max_dd
    stats['回撤峰值日'] = peak_date
    stats['回撤谷底日'] = end_date

    # 计算波动率
    stats['年化波动率'] = calculate_volatility(df, annualize=True)

    return stats


def get_fund_performance(fund_code: str, period: str = "1年") -> Dict:
    """
    获取基金业绩数据
    """
    try:
        df = get_fund_nav_history(fund_code, period=period)

        if df is None or len(df) < 2:
            return {}

        # 获取基本信息
        latest_nav = df.iloc[-1]['nav']
        earliest_nav = df.iloc[0]['nav']

        # 计算区间收益率
        total_return = (latest_nav - earliest_nav) / earliest_nav * 100 if earliest_nav > 0 else 0

        # 计算各阶段收益率
        period_returns = calculate_returns_by_period(df)

        # 获取统计指标
        stats = get_fund_statistics(df)

        result = {
            'fund_code': fund_code,
            'period': period,
            '区间收益率': round(total_return, 2),
            '最新净值': round(latest_nav, 4),
            '净值日期': str(df.iloc[-1]['date'].date()),
            **period_returns,
            **stats
        }

        return result
    except Exception as e:
        print(f"获取基金业绩失败: {e}")
        return {}


def search_fund(keyword: str, limit: int = 20) -> List[Dict]:
    """
    搜索基金（使用本地缓存，快速响应）
    keyword: 搜索关键词（支持代码、名称、拼音）
    limit: 返回结果数量限制
    """
    # 优先使用基金列表缓存
    if EASTMONEY_AVAILABLE:
        try:
            results = FundListCache.search(keyword, limit)
            if results:
                # 转换为统一格式
                return [
                    {
                        'fund_code': r['code'],
                        'fund_name': r['name'],
                        'fund_type': r.get('type', '')
                    }
                    for r in results
                ]
        except Exception as e:
            print(f"基金列表缓存搜索失败: {e}")

    # 备用方案：返回基于关键词的结果
    return [{'fund_code': keyword, 'fund_name': f'基金{keyword}', 'fund_type': ''}]


def classify_fund_type(fund_name: str, fund_code: str = "") -> Dict[str, str]:
    """
    根据基金名称和代码分类基金类型
    返回: {'category': '股票型|债券型|混合型|QDII|商品型|REITs|其他',
           'sub_category': '详细分类'}
    """
    fund_name_lower = fund_name.lower()

    # 股票型基金
    if '股票' in fund_name or '指数' in fund_name or 'etf' in fund_name_lower:
        if '白酒' in fund_name or '食品' in fund_name:
            return {'category': '股票型', 'sub_category': '白酒食品'}
        elif '半导体' in fund_name or '芯片' in fund_name or '电子' in fund_name:
            return {'category': '股票型', 'sub_category': '科技电子'}
        elif '医药' in fund_name or '生物' in fund_name or '健康' in fund_name:
            return {'category': '股票型', 'sub_category': '医药健康'}
        elif '新能源' in fund_name or '光伏' in fund_name or '风电' in fund_name:
            return {'category': '股票型', 'sub_category': '新能源'}
        elif '军工' in fund_name or '国防' in fund_name:
            return {'category': '股票型', 'sub_category': '军工'}
        elif '金融' in fund_name or '银行' in fund_name or '证券' in fund_name:
            return {'category': '股票型', 'sub_category': '金融地产'}
        elif '消费' in fund_name:
            return {'category': '股票型', 'sub_category': '消费'}
        else:
            return {'category': '股票型', 'sub_category': '宽基指数'}

    # 债券型基金
    elif '债券' in fund_name or '国债' in fund_name or '信用债' in fund_name:
        if '可转债' in fund_name:
            return {'category': '债券型', 'sub_category': '可转债'}
        else:
            return {'category': '债券型', 'sub_category': '纯债'}

    # 混合型基金
    elif '混合' in fund_name:
        return {'category': '混合型', 'sub_category': '混合型'}

    # QDII基金（投资海外）
    elif 'qdii' in fund_name_lower or '恒生' in fund_name or '纳斯达克' in fund_name or '标普' in fund_name:
        return {'category': 'QDII', 'sub_category': '海外市场'}

    # 商品基金
    elif '黄金' in fund_name or '商品' in fund_name or '期货' in fund_name:
        return {'category': '商品型', 'sub_category': '贵金属商品'}

    # REITs
    elif 'reit' in fund_name_lower or '房地产' in fund_name:
        return {'category': 'REITs', 'sub_category': '不动产'}

    # 货币基金
    elif '货币' in fund_name or '现金' in fund_name:
        return {'category': '货币型', 'sub_category': '货币基金'}

    # 默认分类
    else:
        return {'category': '其他', 'sub_category': '其他'}


def calculate_fund_correlation(nav_history_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    计算基金之间的相关性矩阵
    nav_history_dict: {fund_code: nav_dataframe}
    返回: 相关性矩阵DataFrame
    """
    if len(nav_history_dict) < 2:
        return pd.DataFrame()

    # 提取所有基金的净值数据并对齐日期
    nav_series = {}
    for fund_code, df in nav_history_dict.items():
        if 'nav' in df.columns and len(df) > 0:
            df_sorted = df.sort_values('date')
            df_sorted.set_index('date', inplace=True)
            nav_series[fund_code] = df_sorted['nav']

    if len(nav_series) < 2:
        return pd.DataFrame()

    # 合并所有基金的净值数据
    combined_df = pd.DataFrame(nav_series)

    # 计算相关性矩阵
    correlation_matrix = combined_df.corr()

    return correlation_matrix


def get_fund_portfolio_holdings(fund_code: str) -> Optional[Dict[str, any]]:
    """
    获取基金前十大重仓股
    使用 AkShare: fund_portfolio_hold_em

    Args:
        fund_code: 基金代码

    Returns:
        Dict: {'data': DataFrame, 'report_date': str} or None
    """
    try:
        import akshare as ak
        df = ak.fund_portfolio_hold_em(symbol=fund_code)

        if df is None or len(df) == 0:
            return None

        # 清理列名
        df.columns = df.columns.str.strip()

        # 提取报告期（通常是最后一列）
        report_date_col = df.columns[-1]
        report_date = df[report_date_col].iloc[0] if len(df) > 0 else '未知'

        return {
            'data': df,
            'report_date': str(report_date)
        }
    except Exception:
        return None


def find_etf_from_etf_feeder_fund(feeder_fund_code: str) -> Optional[str]:
    """
    从ETF联接基金查找对应的场内ETF代码
    通过基金名称匹配查找对应的ETF

    Args:
        feeder_fund_code: ETF联接基金代码

    Returns:
        ETF代码或None
    """
    # 常见ETF联接基金手动映射表（备用）
    ETF_FEEDER_MAP = {
        # 广发中证基建ETF联接 -> 中证基建ETF
        '016019': '516050',  # A份额
        # 招商中证电池主题ETF联接C -> 招商中证电池主题ETF
        '016020': '561910',
        # 可根据需要添加更多映射
    }

    # 先检查手动映射表
    if feeder_fund_code in ETF_FEEDER_MAP:
        etf_code = ETF_FEEDER_MAP[feeder_fund_code]
        print(f"  从映射表找到ETF: {etf_code}")
        return etf_code

    try:
        import akshare as ak

        # 获取联接基金信息
        fund_info = get_fund_info(feeder_fund_code)
        if not fund_info:
            return None

        fund_name = fund_info.get('fund_name', '')

        # 检查是否是ETF联接基金
        if 'ETF联接' not in fund_name and 'etf联接' not in fund_name.lower():
            return None

        # 从基金名称中提取关键词（去掉"ETF联接"、"A"、"C"等后缀）
        keyword = fund_name.replace('ETF联接', '').replace('etf联接', '')
        # 去掉后缀A/C等
        keyword = keyword.rstrip('AC').strip()

        print(f"  查找ETF关键词: {keyword}")

        # 获取所有ETF列表
        etf_df = ak.fund_etf_spot_em()

        # 查找匹配的ETF（通过名称关键词匹配）
        # 使用第0列（代码）和第1列（名称）
        code_col = etf_df.columns[0]
        name_col = etf_df.columns[1]

        # 精确匹配或包含匹配
        for idx, row in etf_df.iterrows():
            etf_name = str(row[name_col])
            if keyword in etf_name or etf_name in keyword:
                etf_code = str(row[code_col])
                print(f"  找到匹配ETF: {etf_name}({etf_code})")
                return etf_code

        return None
    except Exception as e:
        print(f"查找ETF失败 ({feeder_fund_code}): {e}")
        return None


def get_fund_industry_allocation(fund_code: str) -> Optional[Dict[str, float]]:
    """
    获取基金行业配置比例
    使用 AkShare: fund_portfolio_industry_allocation_em

    Args:
        fund_code: 基金代码

    Returns:
        Dict: {'行业名': 占比, ...}
    """
    try:
        import akshare as ak
        df = ak.fund_portfolio_industry_allocation_em(symbol=fund_code)

        if df is None or len(df) == 0:
            return None

        # 查找行业名和占比列
        industry_col = None
        ratio_col = None

        for col in df.columns:
            if '行业' in col:
                industry_col = col
            elif '占净值比例' in col or '占比' in col:
                ratio_col = col

        # 如果找不到列名，使用位置索引
        if industry_col is None:
            # 第二列通常是行业名（第一列是序号）
            industry_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

        if ratio_col is None:
            # 占净值比例列通常是第三列
            ratio_col = df.columns[2] if len(df.columns) > 2 else df.columns[-1]

        # 提取行业和占比数据
        industry_allocation = {}
        for _, row in df.iterrows():
            industry = row[industry_col]
            ratio = row[ratio_col]

            # 跳过空值
            if pd.isna(industry) or pd.isna(ratio):
                continue

            # 处理占比数据（可能是百分比字符串）
            if isinstance(ratio, str):
                ratio = ratio.replace('%', '').strip()
                ratio = float(ratio) / 100 if ratio else 0
            else:
                ratio = float(ratio)

            # 只保存占比大于0的行业
            if ratio > 0:
                industry_allocation[str(industry)] = ratio

        return industry_allocation
    except Exception as e:
        print(f"获取基金行业配置失败 ({fund_code}): {e}")
        return None


def analyze_portfolio_concentration(fund_codes: List[str]) -> Dict[str, Dict]:
    """
    分析持仓基金的重仓股集中度
    只分析每只基金的前十大重仓股
    如果是ETF联接基金，自动查找对应场内ETF并获取成分股

    Args:
        fund_codes: 基金代码列表

    Returns:
        Dict: {
            'stock_exposure': {
                '股票名': {
                    'ratio': 总占比,
                    'funds': ['基金1(代码1)', '基金2(代码2)']
                }
            }
        }
    """
    stock_holdings = {}  # {'股票名': {'ratio': 总占比, 'funds': []}}

    for fund_code in fund_codes:
        holdings_result = get_fund_portfolio_holdings(fund_code)

        # 如果没有直接持仓数据，尝试查找对应的ETF（针对ETF联接基金）
        if holdings_result is None:
            etf_code = find_etf_from_etf_feeder_fund(fund_code)
            if etf_code:
                print(f"ETF联接基金 {fund_code} 找到对应ETF: {etf_code}")
                holdings_result = get_fund_portfolio_holdings(etf_code)

        if holdings_result is not None:
            holdings_df = holdings_result['data']

            fund_info = get_fund_info(fund_code)
            fund_name = fund_info['fund_name'] if fund_info else fund_code
            fund_label = f"{fund_name}({fund_code})"

            # 只取前十大重仓股（按占净值比例排序）
            # 找到占净值比例列
            ratio_col = None
            for col in holdings_df.columns:
                if '占净值比例' in col or '占比' in col:
                    ratio_col = col
                    break

            if ratio_col:
                # 按占净值比例降序排序，取前10
                top_10_df = holdings_df.sort_values(by=ratio_col, ascending=False).head(10)

                # 遍历前十大重仓股
                for _, row in top_10_df.iterrows():
                    stock_name = None
                    ratio = None

                    # 查找股票名称和占比
                    for col in holdings_df.columns:
                        if '股票' in col and '名称' in col:
                            stock_name = row[col]
                        elif col == ratio_col:
                            ratio_val = row[col]
                            if isinstance(ratio_val, str):
                                ratio_val = ratio_val.replace('%', '').strip()
                                ratio = float(ratio_val) / 100 if ratio_val else 0
                            elif isinstance(ratio_val, (int, float)):
                                ratio = float(ratio_val) / 100 if ratio_val > 1 else ratio_val

                    if stock_name and ratio is not None:
                        if stock_name not in stock_holdings:
                            stock_holdings[stock_name] = {'ratio': 0.0, 'funds': []}

                        stock_holdings[stock_name]['ratio'] += ratio

                        # 添加基金到列表（避免重复）
                        if fund_label not in stock_holdings[stock_name]['funds']:
                            stock_holdings[stock_name]['funds'].append(fund_label)

    # 按占比排序
    sorted_stocks = sorted(stock_holdings.items(), key=lambda x: x[1]['ratio'], reverse=True)

    return {
        'stock_exposure': dict(sorted_stocks)
    }


def backtest_strategy(
    fund_code: str,
    strategy: str = "定投",
    start_date: str = None,
    end_date: str = None,
    monthly_amount: float = 1000,
    ma_short: int = 30,
    ma_long: int = 180
) -> Dict:
    """
    基金策略回测

    Args:
        fund_code: 基金代码
        strategy: 策略类型 ("定投", "智能定投", "均线策略", "价值平均")
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        monthly_amount: 每月定投金额
        ma_short: 短期均线天数
        ma_long: 长期均线天数

    Returns:
        Dict: 回测结果
    """
    from datetime import datetime, timedelta

    # 获取历史净值数据
    df = get_fund_nav_history(fund_code, period="全部")

    if df is None or len(df) < 100:
        return {
            'success': False,
            'error': '数据不足，无法回测'
        }

    # 设置日期范围
    if start_date:
        df = df[df['date'] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df['date'] <= pd.to_datetime(end_date)]

    if len(df) < 50:
        return {
            'success': False,
            'error': '选定时间段数据不足'
        }

    df = df.sort_values('date').reset_index(drop=True)

    # 初始化回测结果
    result = {
        'success': True,
        'strategy': strategy,
        'fund_code': fund_code,
        'start_date': str(df['date'].iloc[0].date()),
        'end_date': str(df['date'].iloc[-1].date()),
        'transactions': [],
        'daily_values': [],
        'total_invested': 0,
        'final_value': 0,
        'total_return': 0,
        'annual_return': 0,
        'max_drawdown': 0,
        'sharpe_ratio': 0
    }

    # 根据策略执行回测
    if strategy == "定投":
        result = backtest_fixed_investment(df, monthly_amount, result)
    elif strategy == "智能定投":
        result = backtest_smart_investment(df, monthly_amount, result)
    elif strategy == "均线策略":
        result = backtest_ma_strategy(df, monthly_amount, ma_short, ma_long, result)
    elif strategy == "价值平均":
        result = backtest_value_averaging(df, monthly_amount, result)
    else:
        return {'success': False, 'error': '未知策略'}

    # 计算性能指标
    result = calculate_performance_metrics(result)

    return result


def backtest_fixed_investment(df: pd.DataFrame, monthly_amount: float, result: Dict) -> Dict:
    """
    定投策略：每月固定金额投资
    """
    holdings = 0  # 持有份额
    total_cost = 0  # 总成本
    last_month = None

    for idx, row in df.iterrows():
        current_date = row['date']
        nav = row['nav']

        # 每月第一天买入
        if last_month is None or current_date.month != last_month:
            units = monthly_amount / nav
            holdings += units
            total_cost += monthly_amount

            result['transactions'].append({
                'date': str(current_date.date()),
                'type': '买入',
                'units': units,
                'nav': nav,
                'amount': monthly_amount
            })

            last_month = current_date.month

        # 计算当日持仓市值
        current_value = holdings * nav
        result['daily_values'].append({
            'date': str(current_date.date()),
            'value': current_value,
            'invested': total_cost  # 当前累计投入
        })

    result['total_invested'] = total_cost
    result['final_value'] = holdings * df.iloc[-1]['nav']
    result['holdings'] = holdings

    return result


def backtest_smart_investment(df: pd.DataFrame, monthly_amount: float, result: Dict) -> Dict:
    """
    智能定投：低点多买，高点少买
    基于近250日均价判断点位
    """
    holdings = 0
    total_cost = 0
    last_month = None

    for idx, row in df.iterrows():
        current_date = row['date']
        nav = row['nav']

        # 计算买入金额（基于历史均价）
        if idx >= 250:
            avg_nav = df.iloc[idx-250:idx]['nav'].mean()
            ratio = avg_nav / nav if nav > 0 else 1

            # 低点多买，高点少买
            # ratio > 1: 当前净值低于均价，多买
            # ratio < 1: 当前净值高于均价，少买
            invest_ratio = min(max(ratio, 0.5), 1.5)  # 限制在0.5-1.5倍
            invest_amount = monthly_amount * invest_ratio
        else:
            # 初期按正常定投
            invest_amount = monthly_amount

        # 每月第一天买入
        if last_month is None or current_date.month != last_month:
            units = invest_amount / nav
            holdings += units
            total_cost += invest_amount

            result['transactions'].append({
                'date': str(current_date.date()),
                'type': '买入',
                'units': units,
                'nav': nav,
                'amount': invest_amount,
                'ratio': round(invest_amount / monthly_amount, 2)
            })

            last_month = current_date.month

        # 计算当日持仓市值
        current_value = holdings * nav
        result['daily_values'].append({
            'date': str(current_date.date()),
            'value': current_value,
            'invested': total_cost  # 当前累计投入
        })

    result['total_invested'] = total_cost
    result['final_value'] = holdings * df.iloc[-1]['nav']
    result['holdings'] = holdings

    return result


def backtest_ma_strategy(df: pd.DataFrame, monthly_amount: float, ma_short: int, ma_long: int, result: Dict) -> Dict:
    """
    均线策略：金叉买入，死叉卖出
    """
    holdings = 0
    cash = 0  # 可用现金
    total_invested = 0  # 累计投入
    last_month = None
    position = False  # 是否持仓
    last_ma_short = None
    last_ma_long = None

    for idx, row in df.iterrows():
        current_date = row['date']
        nav = row['nav']

        # 计算均线
        if idx >= ma_long:
            ma_short_val = df.iloc[idx-ma_short:idx]['nav'].mean()
            ma_long_val = df.iloc[idx-ma_long:idx]['nav'].mean()

            # 金叉买入
            if last_ma_short and last_ma_long:
                if last_ma_short <= last_ma_long and ma_short_val > ma_long_val and not position:
                    # 金叉，买入
                    buy_amount = monthly_amount * 3  # 一次性买入3倍月定投
                    units = buy_amount / nav
                    holdings += units
                    position = True

                    result['transactions'].append({
                        'date': str(current_date.date()),
                        'type': '金叉买入',
                        'units': units,
                        'nav': nav,
                        'amount': buy_amount
                    })

                # 死叉卖出
                elif last_ma_short >= last_ma_long and ma_short_val < ma_long_val and position:
                    # 死叉，卖出
                    sell_amount = holdings * nav
                    cash += sell_amount
                    holdings = 0
                    position = False

                    result['transactions'].append({
                        'date': str(current_date.date()),
                        'type': '死叉卖出',
                        'units': 0,
                        'nav': nav,
                        'amount': -sell_amount
                    })

            last_ma_short = ma_short_val
            last_ma_long = ma_long_val

        # 每月定投（补充资金）
        if last_month is None or current_date.month != last_month:
            cash += monthly_amount
            total_invested += monthly_amount
            last_month = current_date.month

        # 计算当日总资产
        current_value = holdings * nav + cash
        result['daily_values'].append({
            'date': str(current_date.date()),
            'value': current_value,
            'invested': total_invested  # 当前累计投入
        })

    result['total_invested'] = total_invested  # 使用实际累计投入
    result['final_value'] = holdings * df.iloc[-1]['nav'] + cash
    result['holdings'] = holdings
    result['cash'] = cash

    return result


def backtest_value_averaging(df: pd.DataFrame, target_value: float, result: Dict) -> Dict:
    """
    价值平均策略：使持仓价值按目标增长
    """
    holdings = 0
    total_cost = 0
    target_portfolio_value = target_value  # 目标持仓价值
    last_month = None
    month_count = 0

    for idx, row in df.iterrows():
        current_date = row['date']
        nav = row['nav']

        # 每月调整
        if last_month is None or current_date.month != last_month:
            month_count += 1
            current_value = holdings * nav
            target_value_growth = target_portfolio_value * month_count

            # 计算需要投入的金额
            value_gap = target_value_growth - current_value

            if value_gap > 0:
                # 需要买入
                invest_amount = value_gap
                units = invest_amount / nav
                holdings += units
                total_cost += invest_amount
            elif value_gap < -100:
                # 价值超出目标太多，卖出部分
                sell_amount = abs(value_gap)
                units_to_sell = sell_amount / nav
                holdings -= units_to_sell
                total_cost -= sell_amount

            result['transactions'].append({
                'date': str(current_date.date()),
                'type': '调整',
                'units': holdings,
                'nav': nav,
                'amount': value_gap
            })

            last_month = current_date.month

        # 计算当日持仓市值
        current_value = holdings * nav
        result['daily_values'].append({
            'date': str(current_date.date()),
            'value': current_value,
            'invested': total_cost  # 当前累计投入
        })

    result['total_invested'] = total_cost
    result['final_value'] = holdings * df.iloc[-1]['nav']
    result['holdings'] = holdings

    return result


def calculate_performance_metrics(result: Dict) -> Dict:
    """
    计算回测性能指标
    """
    if not result['daily_values']:
        return result

    daily_values = result['daily_values']

    # 计算收益率
    if result['total_invested'] > 0:
        result['total_return'] = (result['final_value'] - result['total_invested']) / result['total_invested'] * 100
    else:
        result['total_return'] = 0

    # 计算年化收益率
    start_date = pd.to_datetime(result['start_date'])
    end_date = pd.to_datetime(result['end_date'])
    years = (end_date - start_date).days / 365.25

    if years > 0 and result['total_invested'] > 0:
        result['annual_return'] = ((result['final_value'] / result['total_invested']) ** (1 / years) - 1) * 100
    else:
        result['annual_return'] = 0

    # 计算最大回撤
    values = [v['value'] for v in daily_values]
    peak = values[0]
    max_dd = 0

    for value in values:
        if value > peak:
            peak = value
        dd = (peak - value) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    result['max_drawdown'] = max_dd

    # 计算夏普比率（简化版）
    if len(values) > 1:
        returns = []
        for i in range(1, len(values)):
            if values[i-1] > 0:
                ret = (values[i] - values[i-1]) / values[i-1]
                returns.append(ret)

        if len(returns) > 0:
            avg_return = sum(returns) / len(returns)
            # 假设无风险利率为3%
            rf = 0.03 / 252  # 日化无风险利率
            excess_return = avg_return - rf

            if returns:
                std_return = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
                if std_return > 0:
                    # 年化夏普比率
                    result['sharpe_ratio'] = (excess_return / std_return) * (252 ** 0.5)
                else:
                    result['sharpe_ratio'] = 0
            else:
                result['sharpe_ratio'] = 0

    return result


def get_stock_intraday_data(stock_codes: List[str]) -> Dict[str, pd.DataFrame]:
    """
    获取股票分时行情数据（HTTP模式）

    Args:
        stock_codes: 股票代码列表

    Returns:
        Dict: {stock_code: DataFrame with columns ['time', 'price', 'change_pct']}
    """
    result = {}

    # 使用东方财富HTTP接口获取分时数据
    if EASTMONEY_AVAILABLE:
        try:
            from utils.eastmoney_api import get_eastmoney_api
            api = get_eastmoney_api()

            for stock_code in stock_codes:
                try:
                    # 东方财富分时数据接口
                    # 格式: http://push2his.eastmoney.com/api/qt/stock/klt?...
                    clean_code = stock_code.replace('SZ', '').replace('SH', '')[-6:]

                    # 判断市场
                    if clean_code.startswith('6'):
                        secid = f"1.{clean_code}"  # 上交所
                    else:
                        secid = f"0.{clean_code}"  # 深交所

                    # 获取分时数据（1分钟K线）
                    import requests
                    url = "http://push2his.eastmoney.com/api/qt/stock/klt"
                    params = {
                        'secid': secid,
                        'fields1': 'f1,f2,f3,f4,f5,f6',
                        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
                        'klt': 1,  # 1分钟
                        'fqt': 0,  # 不复权
                        'end': '20500101',
                        'lmt': 240,  # 获取240条数据（4小时）
                    }

                    response = requests.get(url, params=params, timeout=10)
                    data = response.json()

                    if data.get('rc') == 0 and 'data' in data:
                        items = data['data'].get('items', [])

                        if items:
                            time_series = []
                            price_series = []

                            for item in items:
                                # item格式: [时间, 开盘, 收盘, 最高, 最低, 成交量, ...]
                                if len(item) >= 3:
                                    time_str = item[0]  # 格式: "HHMM" 或 "YYYY-MM-DD HH:MM:SS"
                                    price = item[2] if len(item) > 2 else 0  # 收盘价

                                    # 处理时间格式
                                    if ' ' in time_str:
                                        # "2024-01-01 09:30:00" -> "09:30"
                                        time_only = time_str.split(' ')[1][:5]
                                    elif len(time_str) == 4:
                                        # "0930" -> "09:30"
                                        time_only = f"{time_str[:2]}:{time_str[2:]}"
                                    else:
                                        time_only = time_str

                                    time_series.append(time_only)
                                    price_series.append(float(price) if price else 0)

                            if time_series and price_series:
                                df = pd.DataFrame({
                                    'time': pd.to_datetime(time_series, format='%H:%M').time,
                                    'price': price_series
                                })

                                # 计算涨跌幅
                                if len(df) > 0:
                                    base_price = df['price'].iloc[0]
                                    df['change_pct'] = ((df['price'] - base_price) / base_price * 100).round(2)

                                result[stock_code] = df

                except Exception as e:
                    print(f"获取股票{stock_code}分时数据失败: {e}")
                    continue

        except Exception as e:
            print(f"东方财富分时数据接口失败: {e}")

    # 如果HTTP方式失败，尝试使用AkShare作为备用
    if not result and USE_AKSHARE_FALLBACK:
        try:
            import akshare as ak

            for stock_code in stock_codes:
                if stock_code in result:
                    continue  # 已获取

                try:
                    # 转换股票代码格式（6位代码）
                    clean_code = stock_code.replace('SZ', '').replace('SH', '')[-6:]

                    # 判断市场
                    if clean_code.startswith('6'):
                        symbol = f"sh{clean_code}"
                    else:
                        symbol = f"sz{clean_code}"

                    # 获取分时数据
                    df = ak.stock_zh_a_hist_min_em(symbol=symbol, period="1", adjust="")

                    if df is not None and len(df) > 0:
                        # 清理列名
                        df.columns = df.columns.str.strip()

                        # 查找时间、价格、涨跌幅列
                        time_col = None
                        price_col = None
                        change_pct_col = None

                        for col in df.columns:
                            if '时间' in col or 'time' in col.lower():
                                time_col = col
                            elif '收盘' in col or 'close' in col.lower() or '价格' in col:
                                price_col = col
                            elif '涨跌幅' in col or 'change_pct' in col.lower():
                                change_pct_col = col

                        if time_col and price_col:
                            df_result = pd.DataFrame()
                            df_result['time'] = pd.to_datetime(df[time_col])
                            df_result['price'] = pd.to_numeric(df[price_col], errors='coerce')

                            if change_pct_col:
                                df_result['change_pct'] = pd.to_numeric(df[change_pct_col], errors='coerce')
                            else:
                                # 如果没有涨跌幅列，计算涨跌幅（使用第一笔交易作为基准）
                                if len(df) > 0:
                                    base_price = df[price_col].iloc[0]
                                    df_result['change_pct'] = ((df[price_col] - base_price) / base_price * 100).values

                            result[stock_code] = df_result.dropna()

                except Exception as e:
                    print(f"AkShare获取股票{stock_code}分时数据失败: {e}")
                    continue

        except Exception as e:
            print(f"AkShare分时数据接口失败: {e}")

    return result


def get_stock_realtime_quote(stock_codes: List[str]) -> Dict[str, Dict]:
    """
    获取股票实时行情（HTTP模式，优先使用东方财富API）

    Args:
        stock_codes: 股票代码列表（如 ['000001', '600000']）

    Returns:
        Dict: {stock_code: {'name': str, 'price': float, 'change_pct': float}}
    """
    # 优先使用天天基金/东方财富HTTP接口
    if EASTMONEY_AVAILABLE:
        try:
            from utils.eastmoney_api import get_eastmoney_api
            api = get_eastmoney_api()
            result = api.get_stock_realtime_quote(stock_codes)
            if result:
                return result
        except Exception as e:
            print(f"东方财富HTTP接口获取股票行情失败: {e}")

    # 备用方案：使用AkShare
    if USE_AKSHARE_FALLBACK:
        result = {}
        try:
            import akshare as ak

            # 获取沪深A股实时行情
            df = ak.stock_zh_a_spot_em()

            if df is not None and len(df) > 0:
                # 清理列名
                df.columns = df.columns.str.strip()

                # 查找需要的列
                code_col = None
                name_col = None
                price_col = None
                change_pct_col = None

                for col in df.columns:
                    if '代码' in col or 'code' in col.lower():
                        code_col = col
                    elif '名称' in col or 'name' in col.lower():
                        name_col = col
                    elif '最新价' in col or '现价' in col or 'price' in col.lower():
                        price_col = col
                    elif '涨跌幅' in col or 'change_pct' in col.lower():
                        change_pct_col = col

                if code_col and price_col:
                    for stock_code in stock_codes:
                        # 匹配股票代码（需要处理6位代码格式）
                        match_rows = df[df[code_col].astype(str).str.contains(stock_code[-6:], na=False)]

                        if len(match_rows) > 0:
                            row = match_rows.iloc[0]
                            result[stock_code] = {
                                'name': str(row[name_col]) if name_col else stock_code,
                                'price': float(row[price_col]) if price_col else 0,
                                'change_pct': float(row[change_pct_col]) if change_pct_col else 0
                            }
        except Exception as e:
            print(f"AkShare获取股票行情失败: {e}")

        return result

    return {}


def get_fund_realtime_estimate(fund_code: str) -> Optional[Dict]:
    """
    获取基金实时估值（天天基金快速接口）

    Args:
        fund_code: 基金代码

    Returns:
        实时估值字典或None
        {
            'fund_code': str,
            'fund_name': str,
            'estimate_nav': float,  # 估算净值
            'estimate_change_percent': str,  # 估算涨跌幅百分比
            'estimate_time': str  # 估算时间
        }
    """
    # 使用天天基金实时估值接口
    if EASTMONEY_AVAILABLE:
        try:
            api = get_eastmoney_api()
            return api.get_realtime_estimate(fund_code)
        except Exception as e:
            print(f"天天基金API获取实时估值失败: {e}")

    return None


def calculate_fund_realtime_value(fund_code: str, fund_nav: float = None, time_points: int = 10, debug: bool = False) -> Optional[Dict]:
    """
    计算基金实时估值（基于重仓股）

    Args:
        fund_code: 基金代码
        fund_nav: 最新净值
        time_points: 时间点数量
        debug: 是否输出调试信息

    Returns:
        估值结果字典或None
    """
    debug_info = {'step': 'start', 'errors': []}

    try:
        # 步骤1: 获取基金持仓
        debug_info['step'] = 'get_holdings'
        holdings = get_fund_portfolio_holdings(fund_code)
        if not holdings:
            return None

        if holdings['data'] is None or len(holdings['data']) == 0:
            return None

        df = holdings['data']
        report_date = holdings.get('report_date', '未知')

        # 清理列名
        df.columns = df.columns.str.strip()

        # 步骤2: 查找相关列
        debug_info['step'] = 'find_columns'
        stock_code_col = None
        stock_name_col = None
        ratio_col = None
        shares_col = None

        for col in df.columns:
            if '代码' in col or 'code' in col.lower():
                stock_code_col = col
            elif '名称' in col or 'name' in col.lower():
                stock_name_col = col
            elif '占净值比' in col or '比例' in col or 'ratio' in col.lower():
                ratio_col = col
            elif '持股数' in col or '股数' in col or 'shares' in col.lower():
                shares_col = col

        if not stock_code_col or not ratio_col:
            debug_info['errors'].append("找不到股票代码列或比例列")
            return None

        # 步骤3: 提取前十大重仓股信息
        debug_info['step'] = 'extract_stocks'
        stock_list = []
        stock_codes = []
        total_ratio = 0

        for idx, row in df.head(10).iterrows():  # 限制前10只
            try:
                code = str(row[stock_code_col]).strip()
                if not code or code == 'nan':
                    continue

                # 处理股票代码格式（可能包含市场前缀）
                clean_code = code.replace('SZ', '').replace('SH', '')[-6:]

                name = str(row[stock_name_col]) if stock_name_col else code
                ratio = float(row[ratio_col]) if ratio_col else 0

                stock_codes.append(clean_code)
                stock_list.append({
                    'code': code,
                    'clean_code': clean_code,
                    'name': name,
                    'ratio': ratio
                })
                total_ratio += ratio
            except Exception as e:
                debug_info['errors'].append(f"处理持仓行失败: {e}")
                continue

        if not stock_codes:
            debug_info['errors'].append("没有找到有效股票代码")
            return None

        # 步骤4: 获取股票实时行情
        debug_info['step'] = 'get_quotes'
        stock_quotes = get_stock_realtime_quote(stock_codes)

        if not stock_quotes:
            debug_info['errors'].append("股票实时行情获取失败")
            return None

        # 步骤5: 计算加权平均涨跌幅
        debug_info['step'] = 'calculate_weighted'
        weighted_change = 0.0
        valid_stocks = []

        for stock in stock_list:
            quote = stock_quotes.get(stock['clean_code'])
            if quote:
                stock['current_price'] = quote['price']
                stock['change_pct'] = quote['change_pct']
                # 使用持仓比例加权
                weighted_change += stock['ratio'] * quote['change_pct']
                valid_stocks.append(stock)
            else:
                debug_info['errors'].append(f"股票 {stock['clean_code']} 未获取到行情")
                stock['current_price'] = 0
                stock['change_pct'] = 0

        if not valid_stocks:
            debug_info['errors'].append("没有获取到任何有效股票行情")
            return None

        # 假设股票持仓占总资金的80-90%（其余为现金、债券等）
        # 前十大重仓股通常占60-80%，这里假设前十大占70%，现金占30%
        stock_position_ratio = min(total_ratio / 100 * 0.95, 0.85)  # 最高85%
        cash_ratio = 1 - stock_position_ratio

        # 计算估算涨跌幅
        estimated_change_pct = weighted_change * stock_position_ratio

        # 如果提供了最新净值，计算估算净值
        estimated_nav = None
        change_amount = None
        if fund_nav:
            estimated_nav = fund_nav * (1 + estimated_change_pct / 100)
            change_amount = estimated_nav - fund_nav

        result = {
            'estimated_nav': estimated_nav,
            'change_pct': estimated_change_pct,
            'change_amount': change_amount,
            'stock_details': valid_stocks,
            'report_date': report_date,
            'cash_ratio': cash_ratio,
            'stock_position_ratio': stock_position_ratio,
            'time_series': pd.DataFrame(),
            'debug_info': debug_info if debug else None
        }

        # 生成时间序列数据（用于绘制估值曲线）
        try:
            # 获取分时数据
            intraday_data = get_stock_intraday_data(stock_codes)

            if intraday_data and fund_nav:
                # 找到所有股票的共同时间点
                common_times = None
                for stock_code, df_intraday in intraday_data.items():
                    if common_times is None:
                        common_times = set(df_intraday['time'].dt.strftime('%H:%M'))
                    else:
                        common_times = common_times.intersection(set(df_intraday['time'].dt.strftime('%H:%M')))

                if common_times:
                    # 转换为排序后的列表
                    times_list = sorted(list(common_times))

                    # 计算每个时间点的估值
                    time_series_data = []
                    for time_str in times_list:
                        weighted_change_at_time = 0.0
                        valid_count = 0

                        for stock in valid_stocks:
                            stock_intraday = intraday_data.get(stock['clean_code'])
                            if stock_intraday is not None and len(stock_intraday) > 0:
                                # 找到最接近的时间点
                                time_diffs = abs(stock_intraday['time'].dt.strftime('%H:%M') - time_str)
                                closest_idx = time_diffs.idxmin()
                                closest_row = stock_intraday.iloc[closest_idx]

                                weighted_change_at_time += stock['ratio'] * closest_row['change_pct']
                                valid_count += 1

                        if valid_count > 0:
                            change_pct_at_time = weighted_change_at_time * stock_position_ratio
                            nav_at_time = fund_nav * (1 + change_pct_at_time / 100)
                            time_series_data.append({
                                'time': time_str,
                                'nav': nav_at_time,
                                'change_pct': change_pct_at_time
                            })

                    if time_series_data:
                        result['time_series'] = pd.DataFrame(time_series_data)
                        result['time_series']['time'] = pd.to_datetime(result['time_series']['time'], format='%H:%M')

        except Exception as e:
            print(f"生成时间序列数据失败: {e}")

        return result

    except Exception as e:
        print(f"计算基金实时估值失败 ({fund_code}): {e}")
        return None


if __name__ == "__main__":
    # 测试代码
    fund_code = "161725"  # 招商中证白酒指数
    print(f"获取基金 {fund_code} 信息:")
    info = get_fund_info(fund_code)
    print(info)

    print(f"\n获取最新净值:")
    nav = get_fund_latest_nav(fund_code)
    print(f"最新净值: {nav}")

    print(f"\n获取基金业绩:")
    performance = get_fund_performance(fund_code, period="1年")
    for key, value in performance.items():
        print(f"{key}: {value}")
