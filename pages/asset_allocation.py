"""资产配置监控页面"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from db import get_holdings, get_fund, get_asset_allocation, get_industry_exposure, save_fund_correlation
from fund_api import get_fund_nav_history, analyze_portfolio_concentration, calculate_fund_correlation


def asset_allocation_page():
    """资产配置监控页面"""
    st.title("⚖️ 资产配置监控")

    # 获取所有持仓基金
    holdings = get_holdings()

    if not holdings:
        st.info("暂无持仓数据，请先买入基金")
        return

    # 创建三个tab
    tab1, tab2, tab3 = st.tabs(["📊 股债比例看板", "🔍 穿透化持仓分析", "🔗 相关性矩阵"])

    with tab1:
        st.subheader("股债比例看板")

        # 获取资产配置统计
        allocation = get_asset_allocation()

        if allocation:
            # 显示配置概览
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("资产类别数", f"{len(allocation)}")

            total_value = sum(item['市值'] for item in allocation.values())
            with col2:
                st.metric("总市值", f"¥{total_value:,.2f}")

            with col3:
                # 计算股债比例
                stock_ratio = allocation.get('股票型', {}).get('占比', 0)
                bond_ratio = allocation.get('债券型', {}).get('占比', 0)
                st.metric("股债比", f"{stock_ratio:.1f}% : {bond_ratio:.1f}%")

            st.divider()

            # 显示饼图
            labels = list(allocation.keys())
            values = [item['市值'] for item in allocation.values()]

            # 颜色映射
            color_map = {
                '股票型': '#ff4d4f',
                '债券型': '#52c41a',
                '混合型': '#1890ff',
                'QDII': '#faad14',
                '货币型': '#722ed1',
                '其他': '#8c8c8c'
            }
            colors = [color_map.get(label, '#8c8c8c') for label in labels]

            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                marker=dict(colors=colors),
                textinfo='label+percent',
                textposition='inside'
            )])

            fig.update_layout(
                title="资产配置分布",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # 详细配置表格
            st.subheader("详细配置")

            allocation_df = pd.DataFrame([
                {
                    '资产类别': cat,
                    '市值(元)': f"{data['市值']:,.2f}",
                    '占比': f"{data['占比']:.2f}%",
                    '基金数': data['基金数'],
                    '涉及基金': ', '.join(data.get('基金列表', []))
                }
                for cat, data in allocation.items()
            ])

            st.dataframe(allocation_df, use_container_width=True, hide_index=True)

            # 股债平衡建议
            st.divider()
            st.subheader("💡 配置建议")

            if stock_ratio > 80:
                st.warning("⚠️ 股票配置过高（>80%），建议增加债券配置降低风险")
            elif stock_ratio < 20:
                st.warning("⚠️ 股票配置过低（<20%），可能影响长期收益")
            elif 40 <= stock_ratio <= 60:
                st.success("✅ 股债比例均衡，配置合理")
            else:
                st.info("ℹ️ 股债比例适中，可根据个人风险偏好调整")

        else:
            st.info("暂无资产配置数据")

    with tab2:
        st.subheader("穿透化持仓分析")

        st.info("ℹ️ 此功能分析持仓基金的重仓股和行业分布，帮助识别集中度风险")

        # 获取所有持仓基金代码
        fund_codes = [h['fund_code'] for h in holdings]

        if len(fund_codes) > 0:
            # 重仓股分析
            st.divider()
            st.subheader("📊 重仓股分析")

            with st.spinner("正在获取重仓股数据..."):
                portfolio_analysis = analyze_portfolio_concentration(fund_codes)

            stock_exposure = portfolio_analysis.get('stock_exposure', {})

            if stock_exposure:
                # 显示汇总指标
                col1, col2 = st.columns(2)

                with col1:
                    st.metric("重仓股数量", f"{len(stock_exposure)}")

                with col2:
                    # 计算集中度（前三大重仓股占比）
                    sorted_stocks = sorted(stock_exposure.items(), key=lambda x: x[1]['ratio'], reverse=True)
                    top3_ratio = sum(item[1]['ratio'] for item in sorted_stocks[:3])
                    st.metric("前三大重仓股集中度", f"{top3_ratio:.1f}%")

                st.divider()

                # 重仓股详情表格
                st.subheader("重仓股详情")

                stock_df_data = []
                for stock_name, stock_info in sorted_stocks[:20]:  # 显示前20
                    stock_df_data.append({
                        '股票名称': stock_name,
                        '占比': f"{stock_info['ratio']:.2f}%",
                        '涉及基金': ', '.join(stock_info['funds'])
                    })

                stock_df = pd.DataFrame(stock_df_data)
                st.dataframe(stock_df, use_container_width=True, hide_index=True)
            else:
                st.info("💡 当前暂无重仓股数据，可能原因：")
                st.write("1. 基金尚未披露最新持仓数据")
                st.write("2. AkShare 数据源暂无该基金信息")
                st.write("3. 网络连接问题")

            # 行业配置分析
            st.divider()
            st.subheader("🏭 行业配置分析")

            # 获取行业暴露度
            industry_exposure = get_industry_exposure()

            if industry_exposure:
                # 显示行业集中度
                col1, col2 = st.columns(2)

                with col1:
                    st.metric("行业数", f"{len(industry_exposure)}")

                with col2:
                    # 计算集中度（前三大行业占比）
                    sorted_industries = sorted(industry_exposure.items(),
                                               key=lambda x: x[1]['占比'],
                                               reverse=True)
                    top3_ratio = sum(item[1]['占比'] for item in sorted_industries[:3])
                    st.metric("前三大行业集中度", f"{top3_ratio:.1f}%")

                st.divider()

                # 行业配置表格
                st.subheader("行业分布详情")

                industry_df = pd.DataFrame([
                    {
                        '行业': industry,
                        '暴露度(元)': f"{data['市值']:,.2f}",
                        '占比': f"{data['占比']:.2f}%",
                        '涉及基金数': data['基金数'],
                        '涉及基金': ', '.join(data.get('基金列表', []))
                    }
                    for industry, data in sorted_industries
                ])

                st.dataframe(industry_df, use_container_width=True, hide_index=True)

                # 风险提示
                st.divider()
                st.subheader("⚠️ 行业风险提示")

                for industry, data in sorted_industries:
                    if data['占比'] > 30:
                        st.error(f"🔴 {industry} 行业占比过高 ({data['占比']:.1f}%)，建议分散配置")
                    elif data['占比'] > 20:
                        st.warning(f"🟡 {industry} 行业占比较高 ({data['占比']:.1f}%)，注意风险")
                    elif data['占比'] > 10:
                        st.info(f"🔵 {industry} 行业配置适中 ({data['占比']:.1f}%)")

            else:
                st.info("💡 提示：行业配置数据需要手动输入，或者可以通过基金季报获取")
                st.write("\n当前暂无行业配置数据，您可以：")
                st.write("1. 在基金招募说明书中查看行业配置")
                st.write("2. 查看基金季报中的持仓明细")
                st.write("3. 使用第三方基金分析工具")
        else:
            st.info("💡 当前无持仓基金，无法进行分析")

    with tab3:
        st.subheader("基金相关性矩阵")

        st.info("ℹ️ 此功能分析持仓基金之间的相关性，帮助识别持仓雷同")

        # 获取所有持仓基金的代码
        fund_codes = [h['fund_code'] for h in holdings]

        if len(fund_codes) < 2:
            st.warning("⚠️ 持仓基金数量少于2只，无法计算相关性")
            return

        # 获取净值历史数据
        with st.spinner("正在计算相关性..."):
            nav_history_dict = {}
            for fund_code in fund_codes:
                nav_df = get_fund_nav_history(fund_code, period="1年")
                if nav_df is not None and len(nav_df) > 0:
                    nav_history_dict[fund_code] = nav_df

            # 计算相关性矩阵
            if len(nav_history_dict) >= 2:
                correlation_matrix = calculate_fund_correlation(nav_history_dict)

                if not correlation_matrix.empty:
                    # 创建基金代码到名称的映射
                    fund_names = {}
                    for fund_code in correlation_matrix.columns:
                        fund_info = get_fund(fund_code)
                        if fund_info:
                            fund_names[fund_code] = f"{fund_info['fund_name']}({fund_code})"
                        else:
                            fund_names[fund_code] = fund_code

                    # 使用基金名称作为标签
                    labels = [fund_names[code] for code in correlation_matrix.columns]

                    # 显示热力图
                    fig = px.imshow(
                        correlation_matrix,
                        labels=dict(x="基金", y="基金", color="相关系数"),
                        x=labels,
                        y=labels,
                        color_continuous_scale='RdYlGn',
                        color_continuous_midpoint=0.5,
                        text_auto=".3f",
                        aspect="auto"
                    )

                    fig.update_layout(
                        title="基金相关性热力图",
                        height=500,
                        xaxis={'tickangle': -45}
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    # 保存相关性数据到数据库
                    for i, fund1 in enumerate(correlation_matrix.columns):
                        for j, fund2 in enumerate(correlation_matrix.columns):
                            if i < j:  # 只保存上三角
                                corr_value = correlation_matrix.iloc[i, j]
                                save_fund_correlation(fund1, fund2, corr_value)

                    # 显示高相关性提示
                    st.divider()
                    st.subheader("⚠️ 持仓雷同提示")

                    high_corr_pairs = []
                    for i, fund1 in enumerate(correlation_matrix.columns):
                        for j, fund2 in enumerate(correlation_matrix.columns):
                            if i < j:
                                corr_value = correlation_matrix.iloc[i, j]
                                if corr_value >= 0.9:
                                    high_corr_pairs.append({
                                        'fund1_code': fund1,
                                        'fund2_code': fund2,
                                        '相关系数': corr_value
                                    })

                    if high_corr_pairs:
                        st.error(f"🔴 发现 {len(high_corr_pairs)} 对高度相关的基金（相关性 ≥ 0.9）")
                        st.write("\n这些基金持仓雷同，建议精简：")

                        for pair in high_corr_pairs:
                            fund1_label = fund_names[pair['fund1_code']]
                            fund2_label = fund_names[pair['fund2_code']]
                            st.write(f"**{fund1_label}** ↔ "
                                     f"**{fund2_label}**: "
                                     f"{pair['相关系数']:.3f}")
                    else:
                        st.success("✅ 未发现高度相关的基金，持仓分散性良好")

                else:
                    st.warning("⚠️ 无法计算相关性矩阵（数据不足）")
            else:
                st.warning("⚠️ 净值历史数据不足，无法计算相关性")
