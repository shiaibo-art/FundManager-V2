"""资产概览页面"""
import streamlit as st
import pandas as pd
from db import get_holdings, get_summary
from utils.refresh_helper import refresh_nav_cache


def overview_page():
    """概览页面"""
    st.title("📊 资产概览")

    # 刷新净值按钮
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        if st.button("🔄 刷新净值", use_container_width=True):
            with st.spinner("正在获取最新净值..."):
                refresh_nav_cache()
            st.success("净值已更新！")
            st.rerun()

    # 总体统计
    summary = get_summary()

    # 统计卡片
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="总投入",
            value=f"¥{summary['total_cost']:,.2f}",
            delta=None
        )
    with col2:
        st.metric(
            label="当前市值",
            value=f"¥{summary['current_value']:,.2f}"
        )
    with col3:
        delta_color = "inverse" if summary['profit_loss'] >= 0 else "normal"
        st.metric(
            label="总盈亏",
            value=f"¥{summary['profit_loss']:,.2f}",
            delta=f"{summary['profit_loss_ratio']:.2f}%",
            delta_color=delta_color
        )
    with col4:
        st.metric(
            label="持仓数量",
            value=f"{summary['fund_count']} 只"
        )

    st.divider()

    # 持仓明细
    st.subheader("持仓明细")

    holdings = get_holdings()

    if holdings:
        holdings_df = pd.DataFrame(holdings)

        # 格式化显示
        display_df = holdings_df[[
            'fund_code', 'fund_name', 'total_units', 'total_cost',
            'current_value', 'profit_loss', 'profit_loss_ratio'
        ]].copy()

        display_df.columns = [
            '基金代码', '基金名称', '持有份额', '投入成本',
            '当前市值', '盈亏金额', '盈亏比例'
        ]

        # 数值格式化
        display_df['持有份额'] = display_df['持有份额'].apply(lambda x: f"{x:.2f}")
        display_df['投入成本'] = display_df['投入成本'].apply(lambda x: f"¥{x:,.2f}")
        display_df['当前市值'] = display_df['当前市值'].apply(
            lambda x: f"¥{x:,.2f}" if x > 0 else "¥0.00"
        )
        display_df['盈亏金额'] = display_df['盈亏金额'].apply(
            lambda x: f"{'📈' if x >= 0 else '📉'} ¥{x:,.2f}"
        )
        display_df['盈亏比例'] = display_df['盈亏比例'].apply(
            lambda x: f"{x:.2f}%"
        )

        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("暂无持仓，请先添加基金并交易")
