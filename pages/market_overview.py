"""市场全景分析页面 - 整合市场温度计和基金雷达功能"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from db import get_holdings
from fund_api import get_fund_nav_history, get_etf_nav_history, calculate_max_drawdown
import akshare as ak
import numpy as np


# ============== 行业关键词映射 ==============
INDUSTRY_KEYWORDS = {
    '储能': ['储能', '电池', '锂电', '钠电', '固态电池', '动力电池'],
    '黄金': ['黄金', '贵金属', '金', '白银', '银', '避险'],
    '半导体': ['半导体', '芯片', '集成电路', '存储', '模拟', '射频'],
    '人工智能': ['人工智能', 'AI', '智能', '算法', '算力', 'CPO', '光模块', '科技', 'TMT'],
    '新能源车': ['新能源车', '电动车', '汽车', '智能驾驶', '自动驾驶'],
    '新能源': ['新能源', '光伏', '风电', '太阳能', '清洁能源'],
    '白酒': ['白酒', '酒', '茅台', '五粮液', '泸州', '食品饮料'],
    '医药': ['医药', '生物', '疫苗', '创新药', '中药', '医疗', '健康', '药业'],
    '军工': ['军工', '国防', '航空', '航天', '兵器'],
    '金融': ['银行', '证券', '保险', '金融', '券商', '非银金融'],
    '房地产': ['房地产', '地产', '物业', '建筑', '建材'],
    '传媒': ['传媒', '影视', '游戏', '广告', '出版', '文体'],
    '消费': ['消费', '零售', '食品', '家电', '旅游', '商贸', '日用'],
    '煤炭': ['煤炭', '采掘', '矿产'],
    '有色': ['有色', '有色金属', '铜', '铝', '锌', '铅', '镍', '钴', '锂', '资源'],
    '石油': ['石油', '原油', '油气', '石化', '能源'],
    '电力': ['电力', '电网', '发电', '新能源电', '公用事业', '水电', '火电', '核电'],
    '通信': ['通信', '5G', '6G', '光通信', '卫星'],
    '计算机': ['计算机', '软件', '云计算', '大数据', '互联网', '信息技术'],
    '电子': ['电子', '消费电子', '面板', 'LED', 'OLED'],
    '化工': ['化工', '石化', '化纤', '塑料', '橡胶', '化学'],
    '机械': ['机械', '设备', '工程机械', '数控', '制造'],
    '农业': ['农业', '种业', '畜牧', '饲料', '农林牧渔'],
    '红利': ['红利', '高股息', '股息', '价值', '低波'],
    '高端制造': ['高端制造', '先进制造', '工业'],
    '环保': ['环保', '节能', '环境', '绿色', '碳中和'],
    '教育': ['教育', '培训', '学校'],
    '物流': ['物流', '快递', '供应链'],
    '交运': ['交通', '运输', '港口', '机场', '航运', '交运'],
}

# ============== 行业ETF列表 ==============
SECTOR_ETFS = [
    {'code': '161725', 'name': '白酒', 'industry': '白酒'},
    {'code': '516090', 'name': '新能源', 'industry': '新能源'},
    {'code': '512480', 'name': '半导体', 'industry': '半导体'},
    {'code': '512980', 'name': '传媒', 'industry': '传媒'},
    {'code': '515220', 'name': '煤炭', 'industry': '煤炭'},
    {'code': '515180', 'name': '红利', 'industry': '红利'},
    {'code': '512170', 'name': '医疗', 'industry': '医药'},
    {'code': '515030', 'name': '新能源车', 'industry': '新能源车'},
    {'code': '512880', 'name': '证券', 'industry': '金融'},
    {'code': '512800', 'name': '银行', 'industry': '金融'},
    {'code': '518880', 'name': '黄金', 'industry': '黄金'},
    {'code': '159915', 'name': '创业板ETF', 'industry': '宽基'},
]

# ============== 市场指数列表（使用对应的ETF代码） ==============
BENCHMARK_INDICES = [
    {'code': '510300', 'name': '沪深300ETF'},
    {'code': '510500', 'name': '中证500ETF'},
    {'code': '159915', 'name': '创业板ETF'},
    {'code': '510050', 'name': '上证50ETF'},
    {'code': '512100', 'name': '中证1000ETF'},
]

# ============== 行业名称映射 ==============
SECTOR_NAME_MAPPING = {
    '白酒': '酿酒行业',
    '新能源': '光伏设备',
    '半导体': '半导体',
    '传媒': '文化传媒',
    '煤炭': '煤炭行业',
    '红利': '银行',
    '医疗': '医疗器械',
    '新能源车': '汽车整车',
    '证券': '证券',
    '银行': '银行',
    '黄金': '贵金属',
}


# ============== 工具函数 ==============
def classify_fund_industry(fund_name: str) -> str:
    """根据基金名称分类所属行业"""
    if not fund_name or not isinstance(fund_name, str):
        return '其他'

    fund_name_lower = fund_name.lower()
    priority_order = [
        '储能', '黄金', '半导体', '人工智能',
        '新能源车', '新能源', '白酒', '医药',
        '军工', '金融', '房地产', '传媒', '消费',
        '煤炭', '有色', '石油', '电力', '通信',
        '计算机', '电子', '化工', '机械', '农业',
        '红利', '高端制造', '环保', '教育', '物流', '交运'
    ]

    for industry in priority_order:
        if industry not in INDUSTRY_KEYWORDS:
            continue
        keywords = INDUSTRY_KEYWORDS[industry]
        for keyword in keywords:
            if keyword in fund_name or keyword.lower() in fund_name_lower:
                return industry

    return '其他'


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> float:
    """计算RSI指标"""
    if len(df) < period + 1:
        return 50.0

    df = df.copy()
    df['change'] = df['nav'].diff()
    df['gain'] = df['change'].where(df['change'] > 0, 0)
    df['loss'] = -df['change'].where(df['change'] < 0, 0)

    avg_gain = df['gain'].rolling(window=period).mean()
    avg_loss = df['loss'].rolling(window=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50.0


def calculate_price_percentile(df: pd.DataFrame) -> float:
    """计算当前价格在历史区间的百分位"""
    if len(df) < 20:
        return 50.0

    current_nav = df.iloc[-1]['nav']
    min_nav = df['nav'].min()
    max_nav = df['nav'].max()

    if max_nav == min_nav:
        return 50.0

    return (current_nav - min_nav) / (max_nav - min_nav) * 100


def get_sector_fund_flow_data() -> tuple:
    """获取行业资金流向数据"""
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        if df is not None and len(df) > 0:
            result = {}
            for _, row in df.iterrows():
                try:
                    sector_name = str(row['名称']).strip()
                    net_inflow = float(row['今日主力净流入-净额'])
                    net_ratio = float(row['今日主力净流入-净占比'])
                    change_pct = float(row['今日涨跌幅'])
                    result[sector_name] = {
                        'net_inflow': net_inflow,
                        'net_ratio': net_ratio,
                        'change_pct': change_pct,
                    }
                except:
                    continue
            return result, []
    except Exception as e:
        pass

    return {}, []


def calculate_market_temperature(fund_code: str, fund_name: str, fund_flow_data: dict = None) -> dict:
    """计算市场温度"""
    try:
        # 先尝试使用ETF API获取数据（如果是ETF）
        df = get_etf_nav_history(fund_code, period="1年")

        # 如果ETF API失败，尝试使用场外基金API
        if df is None or len(df) < 60:
            df = get_fund_nav_history(fund_code, period="1年")

        if df is None or len(df) < 60:
            return None

        latest_nav = df.iloc[-1]['nav']
        week_ago_nav = df.iloc[-5]['nav'] if len(df) >= 5 else df.iloc[0]['nav']
        month_ago_nav = df.iloc[-20]['nav'] if len(df) >= 20 else df.iloc[0]['nav']

        week_return = (latest_nav - week_ago_nav) / week_ago_nav * 100
        month_return = (latest_nav - month_ago_nav) / month_ago_nav * 100

        min_nav = df['nav'].min()
        max_nav = df['nav'].max()
        price_position = (latest_nav - min_nav) / (max_nav - min_nav) * 100 if max_nav > min_nav else 50

        df_copy = df.copy()
        df_copy['cummax'] = df_copy['nav'].cummax()
        current_drawdown = (df_copy.iloc[-1]['nav'] - df_copy.iloc[-1]['cummax']) / df_copy.iloc[-1]['cummax'] * 100

        if len(df) >= 20:
            ma20 = df.iloc[-20:]['nav'].mean()
            ma60 = df.iloc[-60:]['nav'].mean() if len(df) >= 60 else df.iloc[0:]['nav'].mean()
            trend_strength = ((latest_nav - ma20) / ma20 * 100 + (ma20 - ma60) / ma60 * 100) / 2
        else:
            trend_strength = 0

        # 资金流向数据
        volume_ratio = 0
        net_inflow = 0
        if fund_flow_data:
            mapped_name = SECTOR_NAME_MAPPING.get(fund_name, fund_name)
            if mapped_name in fund_flow_data:
                volume_ratio = abs(fund_flow_data[mapped_name]['net_inflow']) / 100000  # 简化计算
                net_inflow = fund_flow_data[mapped_name]['net_inflow']

        return_score = (week_return * 0.3 + month_return * 0.7) / 10 + 50
        return_score = max(0, min(100, return_score))

        position_score = price_position
        trend_score = max(0, min(100, 50 + trend_strength * 5))
        drawdown_score = max(0, min(100, 50 + current_drawdown))

        volume_heat = min(100, volume_ratio * 2) if volume_ratio > 0 else 0

        temperature = (
            return_score * 0.35 +
            position_score * 0.20 +
            trend_score * 0.15 +
            drawdown_score * 0.15 +
            volume_heat * 0.15
        )

        if temperature < 15:
            heat_level, icon = "冰冷", "🥶"
        elif temperature < 30:
            heat_level, icon = "寒冷", "🧊"
        elif temperature < 45:
            heat_level, icon = "凉爽", "🌤️"
        elif temperature < 60:
            heat_level, icon = "温和", "😊"
        elif temperature < 75:
            heat_level, icon = "炎热", "🔥"
        else:
            heat_level, icon = "滚烫", "🌋"

        return {
            'fund_code': fund_code,
            'fund_name': fund_name,
            'temperature': round(temperature, 1),
            'heat_level': heat_level,
            'icon': icon,
            'week_return': round(week_return, 2),
            'month_return': round(month_return, 2),
            'price_position': round(price_position, 1),
            'volume_ratio': round(volume_ratio, 1),
            'net_inflow': round(net_inflow, 2),
        }
    except Exception as e:
        return None


def get_fund_rank_data(symbol: str = "全部") -> pd.DataFrame:
    """获取基金排行数据"""
    try:
        df = ak.fund_open_fund_rank_em(symbol=symbol)
        if df is None or len(df) == 0:
            return pd.DataFrame()

        result_df = pd.DataFrame()
        if len(df.columns) > 2:
            result_df['code'] = df.iloc[:, 1].astype(str)
            result_df['name'] = df.iloc[:, 2].astype(str)

        if len(df.columns) > 7:
            result_df['week_return'] = pd.to_numeric(df.iloc[:, 7], errors='coerce')
        if len(df.columns) > 8:
            result_df['month_return'] = pd.to_numeric(df.iloc[:, 8], errors='coerce')

        return result_df
    except Exception as e:
        return pd.DataFrame()


def analyze_hot_sectors(week_top10: pd.DataFrame, month_top10: pd.DataFrame) -> dict:
    """分析热点行业"""
    week_industries = {}
    month_industries = {}

    for idx in week_top10.index:
        fund_name = week_top10.loc[idx, 'name']
        industry = classify_fund_industry(fund_name)
        week_industries[industry] = week_industries.get(industry, 0) + 1

    for idx in month_top10.index:
        fund_name = month_top10.loc[idx, 'name']
        industry = classify_fund_industry(fund_name)
        month_industries[industry] = month_industries.get(industry, 0) + 1

    hot_sectors = []
    all_industries = set(week_industries.keys()) | set(month_industries.keys())

    for industry in all_industries:
        week_count = week_industries.get(industry, 0)
        month_count = month_industries.get(industry, 0)
        total_score = week_count + month_count

        if total_score >= 2:
            hot_sectors.append({
                'industry': industry,
                'week_count': week_count,
                'month_count': month_count,
                'total_score': total_score,
                'is_hot': total_score >= 4
            })

    hot_sectors.sort(key=lambda x: x['total_score'], reverse=True)

    return {
        'week_distribution': week_industries,
        'month_distribution': month_industries,
        'hot_sectors': hot_sectors
    }


def check_reversal_signal(fund_code: str, fund_name: str) -> dict:
    """检查反转信号"""
    try:
        # 先尝试使用ETF API获取数据
        df = get_etf_nav_history(fund_code, period="6月")

        # 如果ETF API失败，尝试使用场外基金API
        if df is None or len(df) < 60:
            df = get_fund_nav_history(fund_code, period="6月")

        if df is None or len(df) < 60:
            return None

        rsi = calculate_rsi(df)
        price_percentile = calculate_price_percentile(df)
        recent_5_return = (df.iloc[-1]['nav'] - df.iloc[-5]['nav']) / df.iloc[-5]['nav'] * 100
        recent_20_return = (df.iloc[-1]['nav'] - df.iloc[-20]['nav']) / df.iloc[-20]['nav'] * 100

        is_oversold = rsi < 40
        is_low_price = price_percentile < 30
        is_recent_strong = recent_5_return > 2

        return {
            'fund_code': fund_code,
            'fund_name': fund_name,
            'rsi': round(rsi, 2),
            'price_percentile': round(price_percentile, 2),
            'recent_5_return': round(recent_5_return, 2),
            'recent_20_return': round(recent_20_return, 2),
            'is_reversal': is_oversold and is_low_price and is_recent_strong,
        }
    except Exception as e:
        return None


def check_overheat_warning(fund_code: str, fund_name: str) -> dict:
    """检查过热警告"""
    try:
        # 先尝试使用ETF API获取数据
        df = get_etf_nav_history(fund_code, period="1年")

        # 如果ETF API失败，尝试使用场外基金API
        if df is None or len(df) < 120:
            df = get_fund_nav_history(fund_code, period="1年")

        if df is None or len(df) < 120:
            return None

        price_percentile = calculate_price_percentile(df)
        recent_20_return = (df.iloc[-1]['nav'] - df.iloc[-20]['nav']) / df.iloc[-20]['nav'] * 100
        recent_60_return = (df.iloc[-1]['nav'] - df.iloc[-60]['nav']) / df.iloc[-60]['nav'] * 100
        max_dd, _, _ = calculate_max_drawdown(df)

        is_high_price = price_percentile > 90
        is_recent_hot = recent_20_return > 15
        is_long_term_hot = recent_60_return > 30

        return {
            'fund_code': fund_code,
            'fund_name': fund_name,
            'price_percentile': round(price_percentile, 2),
            'recent_20_return': round(recent_20_return, 2),
            'recent_60_return': round(recent_60_return, 2),
            'max_drawdown': round(max_dd, 2),
            'is_overheat': is_high_price and (is_recent_hot or is_long_term_hot),
            'warning_level': 'critical' if price_percentile > 95 else 'high'
        }
    except Exception as e:
        return None


# ============== 主页面 ==============
def market_overview_page():
    """市场全景分析页面"""
    st.title("📊 市场全景分析")
    st.info("🎯 整合市场温度、热点发现、资金动向、风险机会的多维度分析")

    # 温度说明
    with st.expander("📖 指标说明"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**🌡️ 温度等级**")
            st.markdown("🥶 0-15°C 冰冷 | 🧊 15-30°C 寒冷")
            st.markdown("🌤️ 30-45°C 凉爽 | 😊 45-60°C 温和")
            st.markdown("🔥 60-75°C 炎热 | 🌋 75-100°C 滚烫")
        with col2:
            st.markdown("**💰 资金流向**")
            st.markdown("连续净增加 = 大资金涌入")
            st.markdown("成交额放大 = 关注度高")
        with col3:
            st.markdown("**⚠️ 风险信号**")
            st.markdown("低位反转: RSI<40 + 价格<30%")
            st.markdown("高位过热: 价格>90% + 涨幅>15%")

    # 标签页
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 市场概览",
        "🔥 行业热点",
        "⚠️ 风险机会",
        "📊 我的持仓"
    ])

    # ==================== 市场概览 ====================
    with tab1:
        st.subheader("📈 主要指数温度")

        with st.spinner("正在分析指数温度..."):
            fund_flow_data, _ = get_sector_fund_flow_data()
            temperatures = []

            for idx in BENCHMARK_INDICES:
                result = calculate_market_temperature(idx['code'], idx['name'], fund_flow_data)
                if result:
                    temperatures.append(result)

        if temperatures:
            temperatures.sort(key=lambda x: x['temperature'], reverse=True)

            # 温度概览卡片
            cols = st.columns(4)
            for i, temp in enumerate(temperatures[:4]):
                col = cols[i % 4]
                with col:
                    delta_str = f"{temp['heat_level']}"
                    st.metric(
                        f"{temp['icon']} {temp['fund_name']}",
                        f"{temp['temperature']}°C",
                        delta=delta_str
                    )
                    st.caption(f"近1月: {temp['month_return']:+.1f}% | 位置: {temp['price_position']:.0f}%")

            st.divider()

            # 详细表格
            data = []
            for temp in temperatures:
                data.append({
                    '指数': temp['fund_name'],
                    '代码': temp['fund_code'],
                    '温度': temp['temperature'],
                    '状态': f"{temp['icon']} {temp['heat_level']}",
                    '近1周': f"{temp['week_return']:+.2f}%",
                    '近1月': f"{temp['month_return']:+.2f}%",
                    '价格位置': f"{temp['price_position']:.1f}%"
                })
            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 暂无数据，请检查网络连接或稍后再试")

    # ==================== 行业热点 ====================
    with tab2:
        st.subheader("🔥 行业热点分析")

        with st.spinner("正在获取基金排行数据..."):
            rank_df = get_fund_rank_data("全部")

        if rank_df is None or len(rank_df) == 0:
            st.warning("⚠️ 暂无数据")
        else:
            rank_df = rank_df.dropna(subset=['week_return', 'month_return'])
            week_top10 = rank_df.nlargest(10, 'week_return').copy()
            month_top10 = rank_df.nlargest(10, 'month_return').copy()

            hot_analysis = analyze_hot_sectors(week_top10, month_top10)

            # 行业全景热力图（整合温度分析和热点发现）
            st.markdown("### 🌡️ 行业全景热力图")

            with st.spinner("正在分析行业数据..."):
                fund_flow_data, _ = get_sector_fund_flow_data()
                sector_temps = []

                for sector in SECTOR_ETFS:
                    result = calculate_market_temperature(
                        sector['code'],
                        sector['name'],
                        fund_flow_data
                    )
                    if result:
                        result['industry'] = sector['industry']
                        # 添加在涨幅榜单中出现次数
                        result['hot_count'] = 0
                        for s in hot_analysis['hot_sectors']:
                            if s['industry'] == sector['industry']:
                                result['hot_count'] = s['total_score']
                                break
                        sector_temps.append(result)

            if sector_temps:
                sector_temps.sort(key=lambda x: x['temperature'], reverse=True)

                # 详细热力图表格
                st.markdown("#### 行业多维度分析")

                # 创建行业分析数据
                industry_data = []
                for sector in sector_temps:
                    # 资金流向状态
                    if sector.get('net_inflow', 0) > 10000:
                        flow_status = "🟢 大幅净流入"
                    elif sector.get('net_inflow', 0) > 0:
                        flow_status = "🟡 小幅净流入"
                    elif sector.get('net_inflow', 0) > -10000:
                        flow_status = "⚪ 平衡"
                    else:
                        flow_status = "🔴 净流出"

                    # 榜单热度
                    hot_icon = ""
                    if sector['hot_count'] >= 4:
                        hot_icon = "🔥🔥🔥"
                    elif sector['hot_count'] >= 2:
                        hot_icon = "🔥🔥"
                    elif sector['hot_count'] >= 1:
                        hot_icon = "🔥"

                    industry_data.append({
                        '行业': f"{hot_icon} {sector['industry']}",
                        '温度': f"{sector['temperature']}°C",
                        '状态': f"{sector['icon']} {sector['heat_level']}",
                        '近1月': f"{sector['month_return']:+.1f}%",
                        '价格位置': f"{sector['price_position']:.0f}%",
                        '资金流向': flow_status,
                        '榜单热度': f"{sector['hot_count']}次"
                    })

                st.dataframe(pd.DataFrame(industry_data), use_container_width=True, hide_index=True)

                # 可视化热力卡片
                st.divider()
                st.markdown("#### 🎨 行业热度可视化")

                # 按温度分类展示
                boiling = [s for s in sector_temps if s['temperature'] > 75]
                hot = [s for s in sector_temps if 60 < s['temperature'] <= 75]
                warm = [s for s in sector_temps if 45 <= s['temperature'] <= 60]
                cool = [s for s in sector_temps if 30 <= s['temperature'] < 45]
                cold = [s for s in sector_temps if s['temperature'] < 30]

                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.markdown("#### 🌋 滚烫")
                    for s in boiling[:3]:
                        hot_badge = f" 🔥{s['hot_count']}" if s['hot_count'] > 0 else ""
                        st.metric(f"{s['fund_name']}", f"{s['temperature']}°C", delta=f"近1月{s['month_return']:+.1f}%")
                        st.caption(f"位置{s['price_position']:.0f}%{hot_badge}")
                    if not boiling:
                        st.caption("无")

                with col2:
                    st.markdown("#### 🔥 炎热")
                    for s in hot[:3]:
                        hot_badge = f" 🔥{s['hot_count']}" if s['hot_count'] > 0 else ""
                        st.metric(f"{s['fund_name']}", f"{s['temperature']}°C", delta=f"近1月{s['month_return']:+.1f}%")
                        st.caption(f"位置{s['price_position']:.0f}%{hot_badge}")
                    if not hot:
                        st.caption("无")

                with col3:
                    st.markdown("#### 😊 温和")
                    for s in warm[:3]:
                        hot_badge = f" 🔥{s['hot_count']}" if s['hot_count'] > 0 else ""
                        st.metric(f"{s['fund_name']}", f"{s['temperature']}°C", delta=f"近1月{s['month_return']:+.1f}%")
                        st.caption(f"位置{s['price_position']:.0f}%{hot_badge}")
                    if not warm:
                        st.caption("无")

                with col4:
                    st.markdown("#### 🌤️ 凉爽")
                    for s in cool[:3]:
                        hot_badge = f" 🔥{s['hot_count']}" if s['hot_count'] > 0 else ""
                        st.metric(f"{s['fund_name']}", f"{s['temperature']}°C", delta=f"近1月{s['month_return']:+.1f}%")
                        st.caption(f"位置{s['price_position']:.0f}%{hot_badge}")
                    if not cool:
                        st.caption("无")

                with col5:
                    st.markdown("#### 🥶 寒冷")
                    for s in cold[:3]:
                        hot_badge = f" 🔥{s['hot_count']}" if s['hot_count'] > 0 else ""
                        st.metric(f"{s['fund_name']}", f"{s['temperature']}°C", delta=f"近1月{s['month_return']:+.1f}%")
                        st.caption(f"位置{s['price_position']:.0f}%{hot_badge}")
                    if not cold:
                        st.caption("无")

                # 综合建议
                st.divider()
                st.markdown("#### 💡 行业分析建议")

                hot_sectors_count = len(boiling) + len(hot)
                cold_sectors_count = len(cold)

                if hot_sectors_count >= 4:
                    st.error(f"🔴 **市场过热** - {hot_sectors_count}个行业处于高温状态，建议谨慎追高，注意止盈")
                elif hot_sectors_count >= 2:
                    st.warning(f"🟠 **市场偏热** - {hot_sectors_count}个行业处于高温状态，可适当关注机会")
                elif cold_sectors_count >= 4:
                    st.success(f"🟢 **市场偏冷** - {cold_sectors_count}个行业处于低温状态，可能是较好的布局时机")
                elif cold_sectors_count >= 2:
                    st.info(f"🔵 **市场温和** - {cold_sectors_count}个行业处于低温状态，建议保持正常定投")
                else:
                    st.info("😊 **市场平衡** - 各行业温度分布较为均衡")

            st.divider()

            # ==================== ETF板块资金流向 ====================
            st.markdown("### 💰 ETF板块资金流向")
            st.caption("通过行业资金流向追踪板块资金动向")

            with st.spinner("正在获取行业资金流向数据..."):
                fund_flow_data, _ = get_sector_fund_flow_data()

            if fund_flow_data:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### 🟢 资金净流入TOP10")
                    flow_sorted = sorted(fund_flow_data.items(), key=lambda x: x[1]['net_inflow'], reverse=True)
                    inflow_top10 = flow_sorted[:10]

                    for sector_name, data in inflow_top10:
                        inflow_str = f"{data['net_inflow']/100000000:.2f}亿" if abs(data['net_inflow']) >= 100000000 else f"{data['net_inflow']/10000:.1f}万"
                        delta_str = f"🟢 {data['change_pct']:+.2f}%" if data['change_pct'] > 0 else f"🔴 {data['change_pct']:+.2f}%"
                        st.metric(
                            sector_name,
                            inflow_str,
                            delta=f"占比: {data['net_ratio']:+.1f}% | {delta_str}"
                        )

                with col2:
                    st.markdown("#### 🔴 资金净流出TOP10")
                    outflow_top10 = flow_sorted[-10:]

                    for sector_name, data in outflow_top10:
                        inflow_str = f"{data['net_inflow']/100000000:.2f}亿" if abs(data['net_inflow']) >= 100000000 else f"{data['net_inflow']/10000:.1f}万"
                        delta_str = f"🟢 {data['change_pct']:+.2f}%" if data['change_pct'] > 0 else f"🔴 {data['change_pct']:+.2f}%"
                        st.metric(
                            sector_name,
                            inflow_str,
                            delta=f"占比: {data['net_ratio']:+.1f}% | {delta_str}"
                        )
            else:
                st.warning("暂无资金流向数据")

    # ==================== 风险机会 ====================
    with tab3:
        st.subheader("⚠️ 风险与机会分析")

        # 获取基金数据用于后续分析
        with st.spinner("正在获取基金排行数据..."):
            rank_df = get_fund_rank_data("全部")
            if rank_df is not None and len(rank_df) > 0:
                rank_df = rank_df.dropna(subset=['week_return', 'month_return'])
                week_top10 = rank_df.nlargest(10, 'week_return').copy()
                month_top10 = rank_df.nlargest(10, 'month_return').copy()
            else:
                week_top10 = pd.DataFrame()
                month_top10 = pd.DataFrame()

        # ==================== 场外基金机会 ====================
        if len(week_top10) > 0 and len(month_top10) > 0:
            st.markdown("### 🎯 场外基金机会")
            st.caption("当前市场热门场外基金")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 📈 近1周涨幅TOP10")
                week_data = []
                for idx in week_top10.index:
                    fund_name = week_top10.loc[idx, 'name']
                    industry = classify_fund_industry(fund_name)
                    week_data.append({
                        '基金名称': fund_name,
                        '代码': week_top10.loc[idx, 'code'],
                        '近1周': f"{week_top10.loc[idx, 'week_return']:+.2f}%",
                        '近1月': f"{week_top10.loc[idx, 'month_return']:+.2f}%",
                        '行业': industry
                    })
                st.dataframe(pd.DataFrame(week_data), use_container_width=True, hide_index=True)

            with col2:
                st.markdown("#### 📈 近1月涨幅TOP10")
                month_data = []
                for idx in month_top10.index:
                    fund_name = month_top10.loc[idx, 'name']
                    industry = classify_fund_industry(fund_name)
                    month_data.append({
                        '基金名称': fund_name,
                        '代码': month_top10.loc[idx, 'code'],
                        '近1月': f"{month_top10.loc[idx, 'month_return']:+.2f}%",
                        '近1周': f"{month_top10.loc[idx, 'week_return']:+.2f}%",
                        '行业': industry
                    })
                st.dataframe(pd.DataFrame(month_data), use_container_width=True, hide_index=True)

            # 交叉分析
            st.divider()
            st.markdown("### 🔍 交叉分析：周榜与月榜交集")
            st.caption("同时出现在周榜和月榜的基金，可能是持续性热点")

            intersection = pd.merge(
                week_top10[['code', 'name', 'week_return', 'month_return']],
                month_top10[['code', 'name']],
                on='code',
                how='inner',
                suffixes=('', '_y')
            )

            if len(intersection) > 0:
                st.success(f"🎯 发现 {len(intersection)} 只基金同时出现在两个榜单中，说明这些方向是当前市场最强热点！")

                for idx in intersection.index:
                    fund_name = intersection.loc[idx, 'name']
                    industry = classify_fund_industry(fund_name)
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                    c1.markdown(f"**{fund_name}**")
                    c2.metric("近1周", f"{intersection.loc[idx, 'week_return']:+.2f}%")
                    c3.metric("近1月", f"{intersection.loc[idx, 'month_return']:+.2f}%")
                    c4.markdown(f"🏷️ {industry}")
            else:
                st.info("暂无交集，说明市场热点切换较快")

        st.divider()

        # ==================== 技术信号分析 ====================
        col1, col2 = st.columns(2)

        # 低位反转机会
        with col1:
            st.markdown("### 🟢 低位反转机会")
            st.caption("连续下跌后RSI低位金叉，可能是反转机会")
            st.info("💡 标准: RSI<40 + 价格<30% + 近5日>2%")

            if rank_df is not None and len(rank_df) > 0:
                week_scan = rank_df.nlargest(20, 'week_return')

                reversal_candidates = []
                with st.spinner("正在扫描反转信号..."):
                    for idx in week_scan.index:
                        fund_code = week_scan.loc[idx, 'code']
                        fund_name = week_scan.loc[idx, 'name']
                        signal = check_reversal_signal(fund_code, fund_name)
                        if signal and signal['is_reversal']:
                            reversal_candidates.append(signal)

                if reversal_candidates:
                    st.success(f"🎯 发现 {len(reversal_candidates)} 个潜在反转机会！")
                    for signal in reversal_candidates[:5]:
                        with st.container():
                            c_a, c_b, c_c = st.columns(3)
                            c_a.markdown(f"**{signal['fund_name']}**")
                            c_b.metric("RSI", f"{signal['rsi']:.1f}")
                            c_c.metric("价格位", f"{signal['price_percentile']:.1f}%")
                            st.caption(f"近5日: {signal['recent_5_return']:+.2f}%")
                            st.divider()
                else:
                    st.info("暂无反转信号")

        # 高位过热警告
        with col2:
            st.markdown("### 🔴 高位过热警告")
            st.caption("估值过高且涨幅大，切勿追高")
            st.warning("⚠️ 标准: 价格>90% + 近20日>15%")

            if rank_df is not None and len(rank_df) > 0:
                month_scan = rank_df.nlargest(20, 'month_return')

                overheat_candidates = []
                with st.spinner("正在扫描过热信号..."):
                    for idx in month_scan.index:
                        fund_code = month_scan.loc[idx, 'code']
                        fund_name = month_scan.loc[idx, 'name']
                        signal = check_overheat_warning(fund_code, fund_name)
                        if signal and signal['is_overheat']:
                            overheat_candidates.append(signal)

                if overheat_candidates:
                    st.error(f"🚨 发现 {len(overheat_candidates)} 个过热警告！")
                    for signal in overheat_candidates[:5]:
                        with st.container():
                            icon = "🚨" if signal['warning_level'] == 'critical' else "⚠️"
                            c_a, c_b, c_c = st.columns(3)
                            c_a.markdown(f"**{icon} {signal['fund_name']}**")
                            c_b.metric("价格位", f"{signal['price_percentile']:.1f}%")
                            c_c.metric("近20日", f"{signal['recent_20_return']:+.2f}%")
                            st.caption(f"近60日: {signal['recent_60_return']:+.2f}%")
                            st.divider()
                else:
                    st.success("✅ 暂无过热警告")

    # ==================== 我的持仓 ====================
    with tab4:
        st.subheader("📊 我的持仓温度")

        holdings = get_holdings()

        if not holdings:
            st.warning("⚠️ 暂无持仓，请先买入基金")
            return

        with st.spinner("正在分析持仓温度..."):
            fund_flow_data, _ = get_sector_fund_flow_data()
            holding_temps = []
            failed_funds = []

            for h in holdings:
                result = calculate_market_temperature(h['fund_code'], h['fund_name'], fund_flow_data)
                if result:
                    result['current_value'] = h['current_value']
                    result['profit_loss'] = h['profit_loss']
                    result['profit_loss_ratio'] = h['profit_loss_ratio']
                    holding_temps.append(result)
                else:
                    failed_funds.append(f"{h['fund_name']}({h['fund_code']})")

        # 显示获取失败的基金
        if failed_funds:
            st.warning(f"⚠️ 以下 {len(failed_funds)} 只基金数据获取失败：")
            for fund in failed_funds[:5]:  # 只显示前5个
                st.caption(f"  - {fund}")
            if len(failed_funds) > 5:
                st.caption(f"  - 还有 {len(failed_funds) - 5} 只...")
            st.info("💡 可能原因：网络连接问题或数据源暂时不可用，请稍后重试")

        if holding_temps:
            holding_temps.sort(key=lambda x: x['temperature'], reverse=True)

            avg_temp = sum(h['temperature'] for h in holding_temps) / len(holding_temps)
            hot_count = sum(1 for h in holding_temps if h['temperature'] > 60)
            cold_count = sum(1 for h in holding_temps if h['temperature'] < 40)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("平均温度", f"{avg_temp:.1f}°C")
            with col2:
                st.metric("高温基金", hot_count, ">60°C")
            with col3:
                st.metric("低温基金", cold_count, "<40°C")
            with col4:
                total_value = sum(h['current_value'] for h in holding_temps)
                hot_value = sum(h['current_value'] for h in holding_temps if h['temperature'] > 60)
                st.metric("高温占比", f"{hot_value/total_value*100:.1f}%")

            st.divider()

            data = []
            for temp in holding_temps:
                pl_emoji = "🔴" if temp['profit_loss'] > 0 else "🟢"
                data.append({
                    '基金': temp['fund_name'],
                    '温度': temp['temperature'],
                    '状态': f"{temp['icon']} {temp['heat_level']}",
                    '市值': f"¥{temp['current_value']:.0f}",
                    '盈亏': f"{pl_emoji} {temp['profit_loss']:+.0f} ({temp['profit_loss_ratio']:+.1f}%)",
                    '近1月': f"{temp['month_return']:.2f}%"
                })

            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)

            st.divider()
            st.subheader("💡 持仓分析建议")

            if avg_temp < 30:
                st.success("🟢 **持仓偏冷** - 可能是较好的加仓时机")
            elif avg_temp < 50:
                st.info("🔵 **持仓温和** - 建议保持正常定投节奏")
            elif avg_temp < 70:
                st.warning("🟠 **持仓偏热** - 建议谨慎追高，可适当止盈")
            else:
                st.error("🔴 **持仓过热** - 建议控制仓位，注意风险")
        else:
            st.error("❌ 所有持仓基金数据获取失败")
            st.info("💡 建议：检查网络连接或稍后重试")
