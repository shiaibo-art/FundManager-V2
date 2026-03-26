"""
基金管理工具 - Streamlit主应用（重构后）
"""
import streamlit as st
import os

from db import init_db
from pages import (
    overview_page,
    transaction_page,
    history_page,
    fund_detail_page,
    fund_comparison_page,
    asset_allocation_page,
    backtest_page,
    signals_page,
    market_overview_page,
)

# 页面配置
st.set_page_config(
    page_title="基金管理工具",
    page_icon="📈",
    layout="wide"
)

# 初始化数据库
if not os.path.exists('data/fund_manager.db'):
    init_db()

# 初始化session state
if 'current_nav_cache' not in st.session_state:
    st.session_state.current_nav_cache = {}

# 初始化基金列表缓存（后台任务）
if 'fund_list_cache_initialized' not in st.session_state:
    try:
        from utils.fund_list_cache import FundListCache
        # 检查缓存是否存在，如果不存在则初始化
        cache_info = FundListCache.get_cache_info()
        if not cache_info.get('cache_exists'):
            print("正在初始化基金列表缓存...")
            FundListCache.update_from_api()
        st.session_state.fund_list_cache_initialized = True
    except Exception as e:
        print(f"初始化基金列表缓存失败: {e}")
        st.session_state.fund_list_cache_initialized = False


def main():
    """主应用入口"""
    st.sidebar.title("📈 基金管理工具")

    page = st.sidebar.radio(
        "导航",
        ["💼 资产概览", "💰 交易录入", "📜 交易历史", "🔍 基金详情", "🆚 基金对比", "⚖️ 资产配置监控", "📈 策略回测", "🎯 买卖信号", "🌐 市场全景分析"]
    )

    if page == "💼 资产概览":
        overview_page()
    elif page == "💰 交易录入":
        transaction_page()
    elif page == "📜 交易历史":
        history_page()
    elif page == "🔍 基金详情":
        fund_detail_page()
    elif page == "🆚 基金对比":
        fund_comparison_page()
    elif page == "⚖️ 资产配置监控":
        asset_allocation_page()
    elif page == "📈 策略回测":
        backtest_page()
    elif page == "🎯 买卖信号":
        signals_page()
    elif page == "🌐 市场全景分析":
        market_overview_page()


if __name__ == "__main__":
    main()
