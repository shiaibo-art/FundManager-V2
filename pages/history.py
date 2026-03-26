"""交易历史页面"""
import streamlit as st
import pandas as pd
from datetime import datetime
from db import get_all_funds, get_transactions


def history_page():
    """历史记录页面"""
    st.title("📜 交易历史")

    # 筛选选项
    col1, col2 = st.columns([2, 1])

    with col1:
        funds = get_all_funds()
        fund_options = {"全部": None}
        fund_options.update({f['fund_name']: f['fund_code'] for f in funds})

        selected_fund_name = st.selectbox(
            "筛选基金",
            options=list(fund_options.keys())
        )
        fund_code_filter = fund_options[selected_fund_name]

    with col2:
        limit = st.selectbox("显示条数", [50, 100, 200, 500], index=1)

    # 获取交易记录
    transactions = get_transactions(fund_code=fund_code_filter, limit=limit)

    if transactions:
        # 转换为DataFrame
        df = pd.DataFrame(transactions)

        # 格式化交易日期（只显示日期部分），使用 errors='coerce' 处理格式不一致
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # 选择显示列（使用专业术语，移除备注）
        display_df = df[[
            'transaction_date', 'fund_name', 'fund_code',
            'transaction_type', 'units', 'nav', 'amount',
            'cumulative_amount', 'cumulative_units', 'cost_nav', 'hold_days'
        ]].copy()

        # 专业术语列名
        display_df.columns = [
            '交易日期', '基金名称', '基金代码', '交易类型',
            '交易份额', '单位净值', '交易金额',
            '累计投入', '累计份额', '成本净值', '持有天数'
        ]

        # 交易类型添加颜色标识
        display_df['交易类型'] = display_df['交易类型'].apply(
            lambda x: f"🔴 卖出" if x == "卖出" else f"🟢 买入"
        )

        # 格式化数值
        display_df['交易份额'] = display_df['交易份额'].apply(lambda x: f"{x:.2f}")
        display_df['单位净值'] = display_df['单位净值'].apply(lambda x: f"{x:.4f}")
        display_df['交易金额'] = display_df['交易金额'].apply(lambda x: f"¥{x:,.2f}")
        display_df['累计投入'] = display_df['累计投入'].apply(lambda x: f"¥{x:,.2f}")
        display_df['累计份额'] = display_df['累计份额'].apply(lambda x: f"{x:.2f}")
        display_df['成本净值'] = display_df['成本净值'].apply(lambda x: f"{x:.4f}")
        display_df['持有天数'] = display_df['持有天数'].apply(
            lambda x: f"{x}天" if x and x > 0 else "-"
        )

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 导出功能
        csv_export = df[[
            'transaction_date', 'fund_name', 'transaction_type', 'units',
            'nav', 'amount', 'cumulative_amount', 'cumulative_units',
            'cost_nav', 'hold_days'
        ]].copy()
        csv_export.columns = [
            '交易日期', '基金名称', '交易类型', '交易份额',
            '单位净值', '交易金额', '累计投入', '累计份额',
            '成本净值', '持有天数'
        ]
        csv = csv_export.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 导出CSV",
            data=csv,
            file_name=f"交易记录_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    else:
        st.info("暂无交易记录")
