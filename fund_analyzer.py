"""
基金技术分析模块
提供买卖信号、止盈止损等分析功能
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from fund_api import get_fund_nav_history


def calculate_ma(df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
    """
    计算移动平均线

    Args:
        df: 包含'nav'列的DataFrame
        periods: 均线周期列表，如 [5, 10, 20, 60]

    Returns:
        添加了MA列的DataFrame
    """
    df = df.copy()
    for period in periods:
        df[f'MA{period}'] = df['nav'].rolling(window=period).mean()
    return df


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算RSI相对强弱指标

    Args:
        df: 包含'nav'列的DataFrame
        period: RSI周期，默认14

    Returns:
        RSI序列
    """
    df = df.copy()
    # 计算价格变化
    delta = df['nav'].diff()

    # 分离涨跌
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)

    # 计算平均涨跌幅
    avg_gains = gains.rolling(window=period).mean()
    avg_losses = losses.rolling(window=period).mean()

    # 计算RSI
    rs = avg_gains / avg_losses
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2) -> pd.DataFrame:
    """
    计算布林带

    Args:
        df: 包含'nav'列的DataFrame
        period: 均线周期，默认20
        std_dev: 标准差倍数，默认2

    Returns:
        添加了上轨、中轨、下轨的DataFrame
    """
    df = df.copy()
    df['BB_middle'] = df['nav'].rolling(window=period).mean()
    df['BB_std'] = df['nav'].rolling(window=period).std()
    df['BB_upper'] = df['BB_middle'] + std_dev * df['BB_std']
    df['BB_lower'] = df['BB_middle'] - std_dev * df['BB_std']
    return df


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    计算MACD指标

    Args:
        df: 包含'nav'列的DataFrame
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9

    Returns:
        添加了MACD、Signal、Histogram的DataFrame
    """
    df = df.copy()
    # 计算EMA
    exp1 = df['nav'].ewm(span=fast, adjust=False).mean()
    exp2 = df['nav'].ewm(span=slow, adjust=False).mean()

    # MACD线
    df['MACD'] = exp1 - exp2

    # 信号线
    df['MACD_signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()

    # 柱状图
    df['MACD_hist'] = df['MACD'] - df['MACD_signal']

    return df


def analyze_buy_sell_signals(fund_code: str, days: int = 180) -> Dict:
    """
    分析基金的买卖信号

    Args:
        fund_code: 基金代码
        days: 分析天数，默认180天（约半年）

    Returns:
        包含各种信号和分析结果的字典
    """
    # 获取历史净值数据
    df = get_fund_nav_history(fund_code, period="全部")

    if df is None or len(df) < 60:
        return {
            'success': False,
            'error': '数据不足，无法分析'
        }

    # 只使用最近的N天数据
    df = df.tail(days).reset_index(drop=True)

    # 计算技术指标
    df = calculate_ma(df, [5, 10, 20, 60])
    df['RSI'] = calculate_rsi(df)
    df = calculate_bollinger_bands(df)
    df = calculate_macd(df)

    # 获取最新数据
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest

    # 1. 趋势分析
    trend_analysis = analyze_trend(df)

    # 2. 买卖信号
    signals = {
        'buy': [],
        'sell': [],
        'hold': []
    }

    # 均线信号
    ma_signals = analyze_ma_signals(df)
    signals['buy'].extend(ma_signals['buy'])
    signals['sell'].extend(ma_signals['sell'])

    # RSI信号
    rsi_signals = analyze_rsi_signals(df)
    signals['buy'].extend(rsi_signals['buy'])
    signals['sell'].extend(rsi_signals['sell'])

    # 布林带信号
    bb_signals = analyze_bollinger_signals(df)
    signals['buy'].extend(bb_signals['buy'])
    signals['sell'].extend(bb_signals['sell'])

    # MACD信号
    macd_signals = analyze_macd_signals(df)
    signals['buy'].extend(macd_signals['buy'])
    signals['sell'].extend(macd_signals['sell'])

    # 3. 综合建议
    overall_signal = generate_overall_signal(signals, trend_analysis)

    # 4. 估值分析
    valuation = analyze_valuation(df)

    return {
        'success': True,
        'fund_code': fund_code,
        'latest_nav': float(latest['nav']),
        'latest_date': str(latest['date'].date()),
        'trend': trend_analysis,
        'signals': signals,
        'overall_signal': overall_signal,
        'valuation': valuation,
        'indicators': {
            'ma5': float(latest['MA5']) if not pd.isna(latest['MA5']) else None,
            'ma10': float(latest['MA10']) if not pd.isna(latest['MA10']) else None,
            'ma20': float(latest['MA20']) if not pd.isna(latest['MA20']) else None,
            'ma60': float(latest['MA60']) if not pd.isna(latest['MA60']) else None,
            'rsi': float(latest['RSI']) if not pd.isna(latest['RSI']) else None,
            'bb_upper': float(latest['BB_upper']) if not pd.isna(latest['BB_upper']) else None,
            'bb_lower': float(latest['BB_lower']) if not pd.isna(latest['BB_lower']) else None,
            'macd': float(latest['MACD']) if not pd.isna(latest['MACD']) else None,
            'macd_signal': float(latest['MACD_signal']) if not pd.isna(latest['MACD_signal']) else None,
        },
        'data': df
    }


def analyze_trend(df: pd.DataFrame) -> Dict:
    """
    分析趋势方向
    """
    latest = df.iloc[-1]
    ma_trend = "震荡"

    if not pd.isna(latest['MA5']) and not pd.isna(latest['MA20']):
        if latest['nav'] > latest['MA5'] > latest['MA10'] > latest['MA20']:
            ma_trend = "强势上涨"
        elif latest['nav'] < latest['MA5'] < latest['MA10'] < latest['MA20']:
            ma_trend = "强势下跌"
        elif latest['MA5'] > latest['MA20']:
            ma_trend = "上涨趋势"
        elif latest['MA5'] < latest['MA20']:
            ma_trend = "下跌趋势"

    return {
        'direction': ma_trend,
        'strength': 'strong' if '强势' in ma_trend else 'weak'
    }


def analyze_ma_signals(df: pd.DataFrame) -> Dict:
    """
    分析均线买卖信号
    """
    signals = {'buy': [], 'sell': []}

    if len(df) < 2:
        return signals

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # 金叉/死叉信号
    if not pd.isna(latest['MA5']) and not pd.isna(latest['MA10']):
        if (prev['MA5'] <= prev['MA10'] and latest['MA5'] > latest['MA10']):
            signals['buy'].append('MA5上穿MA10（金叉）')
        elif (prev['MA5'] >= prev['MA10'] and latest['MA5'] < latest['MA10']):
            signals['sell'].append('MA5下穿MA10（死叉）')

    if not pd.isna(latest['MA10']) and not pd.isna(latest['MA20']):
        if (prev['MA10'] <= prev['MA20'] and latest['MA10'] > latest['MA20']):
            signals['buy'].append('MA10上穿MA20（金叉）')
        elif (prev['MA10'] >= prev['MA20'] and latest['MA10'] < latest['MA20']):
            signals['sell'].append('MA10下穿MA20（死叉）')

    # 价格与均线关系
    nav = latest['nav']
    if not pd.isna(latest['MA20']):
        if nav < latest['MA20'] * 0.95:
            signals['buy'].append(f'价格低于MA20 5%（超卖）')
        elif nav > latest['MA20'] * 1.05:
            signals['sell'].append(f'价格高于MA20 5%（超买）')

    return signals


def analyze_rsi_signals(df: pd.DataFrame) -> Dict:
    """
    分析RSI买卖信号
    """
    signals = {'buy': [], 'sell': []}
    latest = df.iloc[-1]

    if pd.isna(latest['RSI']):
        return signals

    rsi = latest['RSI']

    if rsi < 30:
        signals['buy'].append(f'RSI超卖（{rsi:.1f} < 30）')
    elif rsi > 70:
        signals['sell'].append(f'RSI超买（{rsi:.1f} > 70）')
    elif rsi < 40:
        signals['buy'].append(f'RSI偏低（{rsi:.1f} < 40）')
    elif rsi > 60:
        signals['sell'].append(f'RSI偏高（{rsi:.1f} > 60）')

    return signals


def analyze_bollinger_signals(df: pd.DataFrame) -> Dict:
    """
    分析布林带买卖信号
    """
    signals = {'buy': [], 'sell': []}
    latest = df.iloc[-1]

    if pd.isna(latest['BB_upper']) or pd.isna(latest['BB_lower']):
        return signals

    nav = latest['nav']
    bb_width = latest['BB_upper'] - latest['BB_lower']

    # 价格触及布林带
    if nav <= latest['BB_lower']:
        signals['buy'].append('价格触及布林带下轨（超卖）')
    elif nav >= latest['BB_upper']:
        signals['sell'].append('价格触及布林带上轨（超买）')

    # 布林带收口/开口
    prev_bb_width = df.iloc[-2]['BB_upper'] - df.iloc[-2]['BB_lower'] if len(df) > 1 else bb_width
    if bb_width > prev_bb_width * 1.2:
        signals['buy'].append('布林带开口（波动增大）')

    return signals


def analyze_macd_signals(df: pd.DataFrame) -> Dict:
    """
    分析MACD买卖信号
    """
    signals = {'buy': [], 'sell': []}

    if len(df) < 2:
        return signals

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(latest['MACD']) or pd.isna(latest['MACD_signal']):
        return signals

    # MACD金叉/死叉
    if prev['MACD'] <= prev['MACD_signal'] and latest['MACD'] > latest['MACD_signal']:
        signals['buy'].append('MACD金叉')
    elif prev['MACD'] >= prev['MACD_signal'] and latest['MACD'] < latest['MACD_signal']:
        signals['sell'].append('MACD死叉')

    # 柱状图
    hist = latest['MACD_hist']
    if hist > 0:
        signals['buy'].append(f'MACD柱状图为正（{hist:.4f}）')
    elif hist < 0:
        signals['sell'].append(f'MACD柱状图为负（{hist:.4f}）')

    return signals


def generate_overall_signal(signals: Dict, trend: Dict) -> Dict:
    """
    生成综合买卖建议

    Args:
        signals: 各类信号集合
        trend: 趋势分析结果

    Returns:
        综合建议
    """
    buy_count = len(signals['buy'])
    sell_count = len(signals['sell'])

    # 综合判断
    if buy_count >= 3:
        return {
            'action': 'strong_buy',
            'text': '强烈买入',
            'color': '#ff4d4f',
            'reason': f'出现{buy_count}个买入信号'
        }
    elif buy_count >= 2:
        return {
            'action': 'buy',
            'text': '买入',
            'color': '#ff7875',
            'reason': f'出现{buy_count}个买入信号'
        }
    elif sell_count >= 3:
        return {
            'action': 'strong_sell',
            'text': '强烈卖出',
            'color': '#52c41a',
            'reason': f'出现{sell_count}个卖出信号'
        }
    elif sell_count >= 2:
        return {
            'action': 'sell',
            'text': '卖出',
            'color': '#73d13d',
            'reason': f'出现{sell_count}个卖出信号'
        }
    else:
        # 根据趋势判断
        if '上涨' in trend['direction']:
            return {
                'action': 'hold',
                'text': '持有',
                'color': '#1890ff',
                'reason': f'{trend["direction"]}，暂无明确信号'
            }
        elif '下跌' in trend['direction']:
            return {
                'action': 'watch',
                'text': '观望',
                'color': '#faad14',
                'reason': f'{trend["direction"]}，等待买入时机'
            }
        else:
            return {
                'action': 'neutral',
                'text': '中性',
                'color': '#8c8c8c',
                'reason': '震荡市，暂无明确方向'
            }


def analyze_valuation(df: pd.DataFrame) -> Dict:
    """
    分析估值水平

    Returns:
        估值分析结果
    """
    latest = df.iloc[-1]
    nav = latest['nav']

    # 计算在历史数据中的分位数
    all_navs = df['nav'].values
    percentile = (all_navs < nav).mean() * 100

    if percentile < 20:
        level = '低估'
        color = '#52c41a'
    elif percentile < 40:
        level = '偏低'
        color = '#73d13d'
    elif percentile < 60:
        level = '合理'
        color = '#1890ff'
    elif percentile < 80:
        level = '偏高'
        color = '#faad14'
    else:
        level = '高估'
        color = '#ff4d4f'

    return {
        'level': level,
        'percentile': percentile,
        'color': color,
        'min_nav': float(all_navs.min()),
        'max_nav': float(all_navs.max()),
        'avg_nav': float(all_navs.mean())
    }


def analyze_position_profit_loss(fund_code: str, inventory: List[Dict]) -> Dict:
    """
    分析持仓的止盈止损建议

    Args:
        fund_code: 基金代码
        inventory: 库存列表

    Returns:
        止盈止损建议
    """
    from fund_api import get_fund_nav_history

    if not inventory:
        return {
            'success': False,
            'message': '暂无持仓'
        }

    # 获取最新净值
    df = get_fund_nav_history(fund_code, period="1月")
    if df is None or len(df) == 0:
        return {
            'success': False,
            'message': '无法获取最新净值'
        }

    latest_nav = df.iloc[-1]['nav']

    recommendations = []
    total_units = sum([item['remaining_units'] for item in inventory])

    for item in inventory:
        buy_nav = item['nav']
        remaining_units = item['remaining_units']
        buy_cost = remaining_units * buy_nav
        current_value = remaining_units * latest_nav

        profit = current_value - buy_cost
        profit_ratio = (profit / buy_cost * 100) if buy_cost > 0 else 0
        hold_days = item['hold_days']

        rec = {
            'buy_date': item['buy_date'],
            'buy_nav': buy_nav,
            'units': remaining_units,
            'cost': buy_cost,
            'current_value': current_value,
            'profit': profit,
            'profit_ratio': profit_ratio,
            'hold_days': hold_days,
            'recommendation': None,
            'reason': ''
        }

        # 止盈建议
        if profit_ratio >= 20:
            rec['recommendation'] = 'sell'
            rec['reason'] = f'盈利{profit_ratio:.1f}%，建议止盈'
        elif profit_ratio >= 15:
            rec['recommendation'] = 'partial_sell'
            rec['reason'] = f'盈利{profit_ratio:.1f}%，可考虑部分止盈'
        elif profit_ratio >= 10:
            rec['recommendation'] = 'watch'
            rec['reason'] = f'盈利{profit_ratio:.1f}%，设置止盈位'

        # 止损建议
        elif profit_ratio <= -10:
            rec['recommendation'] = 'sell'
            rec['reason'] = f'亏损{profit_ratio:.1f}%，建议止损'
        elif profit_ratio <= -5:
            rec['recommendation'] = 'stop_loss'
            rec['reason'] = f'亏损{profit_ratio:.1f}%，关注止损位'

        # 补仓建议
        elif profit_ratio <= -15:
            rec['recommendation'] = 'add_position'
            rec['reason'] = f'亏损{profit_ratio:.1f}%，可考虑补仓'
        elif profit_ratio <= -10 and hold_days >= 30:
            rec['recommendation'] = 'add_position'
            rec['reason'] = f'亏损{profit_ratio:.1f}%且持有{hold_days}天，可补仓'

        # 持有建议
        else:
            rec['recommendation'] = 'hold'
            if profit_ratio > 0:
                rec['reason'] = f'盈利{profit_ratio:.1f}%，继续持有'
            else:
                rec['reason'] = f'亏损{profit_ratio:.1f}%，耐心持有'

        recommendations.append(rec)

    return {
        'success': True,
        'latest_nav': latest_nav,
        'total_units': total_units,
        'recommendations': recommendations
    }
