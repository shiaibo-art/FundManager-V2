"""基金对比页面"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from db import get_all_funds
from fund_api import get_fund_nav_history


def fund_comparison_page():
    """基金对比页面"""
    st.title("📊 基金净值对比")

    funds = get_all_funds()

    if not funds or len(funds) < 2:
        st.warning("请至少添加2只基金才能进行对比")
        return

    st.subheader("选择要对比的基金")

    # 多选基金
    fund_options = {f['fund_name']: f['fund_code'] for f in funds}
    selected_fund_names = st.multiselect(
        "选择基金（最多5只）",
        options=list(fund_options.keys()),
        default=list(fund_options.keys())[:min(5, len(fund_options))],
        max_selections=5
    )

    if len(selected_fund_names) < 2:
        st.info("请至少选择2只基金进行对比")
        return

    # 选择时间周期
    period = st.selectbox("对比周期", ["1月", "3月", "6月", "1年", "3年"], index=3)

    # 获取数据
    with st.spinner("正在获取净值数据..."):
        comparison_data = {}
        for fund_name in selected_fund_names:
            fund_code = fund_options[fund_name]
            nav_data = get_fund_nav_history(fund_code, period=period)
            if nav_data is not None and len(nav_data) > 0:
                comparison_data[fund_name] = nav_data

    if not comparison_data:
        st.error("无法获取基金数据，请检查网络连接或基金代码")
        return

    # 合并数据进行对比
    st.subheader("净值走势对比")

    # 创建对比图表
    fig = go.Figure()

    for fund_name, df in comparison_data.items():
        # 归一化处理（以第一个交易日为基准）
        df_normalized = df.copy()
        base_nav = df_normalized.iloc[0]['nav']
        df_normalized['normalized_nav'] = (df_normalized['nav'] / base_nav) * 100

        fig.add_trace(go.Scatter(
            x=df_normalized['date'],
            y=df_normalized['normalized_nav'],
            mode='lines',
            name=fund_name,
            line=dict(width=2)
        ))

    fig.update_layout(
        title=f"基金净值走势对比（归一化，基准=100）",
        xaxis_title="日期",
        yaxis_title="相对净值 (%)",
        hovermode='x unified',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

    # 收益率对比表格
    st.subheader("收益率对比")

    comparison_rows = []
    for fund_name, df in comparison_data.items():
        latest_nav = df.iloc[-1]['nav']
        earliest_nav = df.iloc[0]['nav']
        period_return = (latest_nav - earliest_nav) / earliest_nav * 100

        comparison_rows.append({
            '基金名称': fund_name,
            '基金代码': fund_options[fund_name],
            '期初净值': f"{earliest_nav:.4f}",
            '期末净值': f"{latest_nav:.4f}",
            f'{period}收益率': f"{period_return:.2f}%"
        })

    comparison_df = pd.DataFrame(comparison_rows)
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)
