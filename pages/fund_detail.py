"""基金详情页面（移除实时估值，添加AI分析）"""
import streamlit as st
import pandas as pd
from datetime import datetime
from db import get_all_funds, get_holdings
from fund_api import (
    get_fund_info,
    get_fund_nav_history,
    get_fund_performance,
    get_fund_portfolio_holdings,
)
from utils.fund_ai_analyzer import (
    FundAIAnalyzer,
    generate_ai_report,
    analyze_fund_industry_allocation,
    analyze_fund_portfolio,
    get_personalized_advice,
)
from utils.openai_client import get_model_name
from utils.enhanced_ai_analyzer import analyze_fund_with_market, MarketContext
from utils.market_service import get_market_service


def fund_detail_page():
    """基金详情页面"""
    st.title("🔍 基金详情 & AI分析")

    # 基金搜索/选择
    funds = get_all_funds()

    if not funds:
        st.warning("请先在交易页面添加基金")
        return

    fund_options = {f['fund_name']: f['fund_code'] for f in funds}
    selected_fund_name = st.selectbox("选择基金", options=list(fund_options.keys()), key="fund_select")
    fund_code = fund_options[selected_fund_name]

    st.divider()

    # AI分析选项
    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        period = st.selectbox("时间周期", ["1月", "3月", "6月", "1年", "3年", "5年", "全部"], index=3, key="period_select")
    with col_b:
        use_llm = st.checkbox("🤖 使用大模型", value=True, help="启用后将使用大模型进行AI分析，需要配置API Key")
    with col_c:
        use_market_data = st.checkbox("🌐 结合市场", value=True, help="结合实时市场数据进行分析")

    # 检查OpenAI状态
    try:
        from utils.openai_client import is_openai_available, get_model_name
        openai_available = is_openai_available()
        model_name = get_model_name()
    except:
        openai_available = False
        model_name = "未知模型"

    if use_llm and not openai_available:
        st.warning("⚠️ 大模型未配置，将使用规则算法分析。请配置API Key以启用AI分析。")
        st.info("💡 配置方法：将 `config/openai_config.example.json` 复制为 `config/openai_config.json` 并填入API Key")
        use_llm = False
        use_market_data = False

    with st.spinner("正在加载基金数据..."):
        nav_history = get_fund_nav_history(fund_code, period=period)
        performance = get_fund_performance(fund_code, period=period)
        fund_info = get_fund_info(fund_code)

    # 标签页布局
    tab1, tab2, tab3, tab4 = st.tabs(["📊 综合分析", "📈 净值走势", "🏭 持仓分析", "💼 我的持仓"])

    # ==================== 标签1: AI综合分析 ====================
    with tab1:
        st.subheader("🤖 基金智能分析")

        # 显示分析模式选择
        mode_col1, mode_col2, mode_col3 = st.columns([2, 1, 1])
        with mode_col1:
            if use_llm and openai_available:
                st.success(f"🤖 **分析模式: {model_name}**")
                st.caption("✨ 智能解读 · 深度分析 · 个性化建议")
            else:
                st.info("📊 **分析模式: 规则算法**")
                st.caption("⚡ 快速分析 · 量化指标 · 稳定可靠")
        with mode_col2:
            if use_llm and openai_available:
                st.metric("模型", model_name)
            else:
                st.metric("模型", "规则引擎")
        with mode_col3:
            if use_llm and not openai_available:
                st.warning("未配置API")
            elif use_llm:
                st.success("已连接")
            else:
                st.caption("默认模式")

        st.divider()

        if fund_info and nav_history is not None and len(nav_history) > 0:
            # 获取市场上下文（如果需要）
            market_context = None
            if use_llm and use_market_data:
                try:
                    market_service = get_market_service()
                    market_context = market_service.get_market_context()
                    # 显示市场状态
                    if market_context:
                        with st.expander(f"🌐 市场环境: {market_context.market_sentiment} (评分: {market_context.market_score})", expanded=False):
                            mcol1, mcol2, mcol3 = st.columns(3)
                            with mcol1:
                                if market_context.indices:
                                    sh = next((i for i in market_context.indices if '上证' in i.get('name', '')), None)
                                    if sh:
                                        st.metric(sh['name'], f"{sh['price']:.2f}", f"{sh['change_pct']:.2f}%")
                            with mcol2:
                                north = market_context.north_flow.get('total', 0)
                                st.metric("北向资金", f"{north:.2f}亿")
                            with mcol3:
                                hot = market_context.hot_sectors[:2] if market_context.hot_sectors else []
                                if hot:
                                    st.caption("热门板块")
                                    for sec in hot:
                                        st.caption(f"· {sec['name']}: {sec['change_pct']:.2f}%")
                except Exception as e:
                    st.warning(f"获取市场数据失败: {e}")
                    use_market_data = False

            # 运行AI分析
            try:
                with st.spinner(f"{'🤖 调用大模型分析中...' if use_llm and openai_available else '📊 分析中...'}"):
                    if use_llm and use_market_data and openai_available:
                        # 使用增强型分析器（结合市场数据）
                        nav_list = nav_history.to_dict('records') if hasattr(nav_history, 'to_dict') else []
                        portfolio_data = None
                        try:
                            portfolio_data = get_fund_portfolio_holdings(fund_code)
                        except:
                            pass

                        analysis_result = analyze_fund_with_market(
                            fund_info=fund_info,
                            performance=performance,
                            nav_history=nav_list,
                            portfolio=portfolio_data,
                            market_context=market_context
                        )

                        # 检查是否返回了备用结果（API调用失败）
                        if 'AI 服务不可用' in analysis_result.get('detailed_report', '') or analysis_result.get('sentiment_score') == 50:
                            st.error("⚠️ AI 服务调用失败")
                            st.info("💡 请检查：")
                            st.info("   1. API 配置是否正确 (config/openai_config.json)")
                            st.info("   2. API 服务是否正常运行")
                            st.info("   3. 网络连接是否正常")
                            st.info("   4. 查看 Streamlit 日志获取详细错误信息")
                            st.info("\n🔄 建议：暂时取消「结合市场」选项，使用标准分析模式")

                        # 转换为兼容格式
                        analysis_result = {
                            'overall_score': analysis_result.get('sentiment_score', 50),
                            'risk_level': analysis_result.get('dashboard', {}).get('risk_level', '中'),
                            'return_ability': analysis_result.get('dashboard', {}).get('performance_eval', '一般'),
                            'stability': analysis_result.get('dashboard', {}).get('manager_ability', '一般'),
                            'risk_metrics': {
                                'volatility': '中等',
                                'max_drawdown': 0
                            },
                            'suggestions': [analysis_result.get('operation_advice', '持有观望')],
                            'highlights': analysis_result.get('highlights', []),
                            'risk_warnings': analysis_result.get('risk_factors', []),
                            'analysis_method': model_name,
                            'detailed_report': analysis_result.get('detailed_report', ''),
                            'market_relevance': analysis_result.get('market_relevance', [])
                        }
                    else:
                        # 使用标准分析器
                        analyzer = FundAIAnalyzer(fund_code, use_llm=use_llm)
                        analysis_result = analyzer.analyze(nav_history, performance, fund_info)

            except ConnectionError as e:
                st.error("❌ 网络连接错误")
                st.warning(f"错误信息: {e}")
                st.info("💡 请检查：")
                st.info("   1. API 服务地址是否正确")
                st.info("   2. 网络连接是否正常")
                st.info("   3. 是否需要配置代理")
                st.stop()
            except Exception as e:
                st.error(f"❌ 分析过程出错: {type(e).__name__}")
                st.warning(f"错误信息: {e}")
                st.info("💡 建议：")
                st.info("   1. 取消「结合市场」选项重试")
                st.info("   2. 检查控制台日志获取详细错误")
                st.stop()

            # 检查分析结果是否有效
            if not isinstance(analysis_result, dict):
                st.error(f"❌ 分析结果格式错误: {type(analysis_result).__name__}")
                if isinstance(analysis_result, str):
                    st.info(f"📝 原始响应: {analysis_result[:500]}...")
                st.stop()

            # 显示分析方法徽章
            method = analysis_result.get('analysis_method', '规则算法')
            if method not in ['规则算法', '规则引擎']:
                # 使用大模型分析
                st.success(f"### ✨ 由 {method} 大模型生成分析")
            else:
                st.info("### 📊 由规则算法生成分析")

            st.divider()

            # 显示综合评分
            col1, col2, col3 = st.columns(3)

            with col1:
                score = analysis_result['overall_score']
                st.metric("综合评分", f"{score}/100")

                # 评分星级显示
                stars = "⭐" * (score // 20) + "☆" * (5 - score // 20)
                st.caption(stars)

                # 评级（兼容OpenAI返回的字符串格式）
                return_ability = analysis_result.get('return_ability', {})
                if isinstance(return_ability, dict):
                    rating = return_ability.get('rating', '中性')
                elif isinstance(return_ability, str):
                    rating = return_ability[:10]  # 截取前10个字符
                else:
                    rating = '中性'
                st.caption(f"收益评级: {rating}")

            with col2:
                risk_level = analysis_result['risk_level']
                st.metric("风险等级", risk_level)

                risk_metrics = analysis_result.get('risk_metrics', {})
                volatility = risk_metrics.get('volatility', '中等') if isinstance(risk_metrics, dict) else '中等'
                st.caption(f"波动水平: {volatility}")

                max_dd = analysis_result.get('risk_metrics', {}).get('max_drawdown', 0)
                st.caption(f"最大回撤: {max_dd:.2f}%")

            with col3:
                stability_data = analysis_result.get('stability', {})
                stability = stability_data.get('rating', '中等') if isinstance(stability_data, dict) else str(stability_data)[:10]
                st.metric("稳定性", stability)

                sharpe = analysis_result.get('risk_metrics', {}).get('sharpe_ratio', 'N/A')
                st.caption(f"夏普比率: {sharpe}")

                fund_name = analysis_result.get('fund_name', '')
                st.caption(f"基金: {fund_name}")

            st.divider()

            # 评分明细（仅规则算法有此数据）
            if 'score_breakdown' in analysis_result and isinstance(analysis_result.get('score_breakdown'), dict):
                col_a, col_b, col_c = st.columns(3)
                scores = analysis_result['score_breakdown']

                with col_a:
                    st.progress(min(scores['return_score'] / 100, 1.0))
                    st.caption(f"收益能力: {scores['return_score']}/100")
                    return_ability = analysis_result.get('return_ability', {})
                    details = return_ability.get('details', []) if isinstance(return_ability, dict) else []
                    for detail in details[:2]:
                        st.caption(f"• {detail}")

                with col_b:
                    st.progress(min(scores['risk_score'] / 100, 1.0))
                    st.caption(f"风险控制: {scores['risk_score']}/100")
                    risk_metrics = analysis_result.get('risk_metrics', {})
                    risk_desc = risk_metrics.get('risk_description', []) if isinstance(risk_metrics, dict) else []
                    for detail in risk_desc[:2]:
                        st.caption(f"• {detail}")

                with col_c:
                    st.progress(min(scores['stability_score'] / 100, 1.0))
                    st.caption(f"稳定性: {scores['stability_score']}/100")
                    stability_data = analysis_result.get('stability', {})
                    details = stability_data.get('details', []) if isinstance(stability_data, dict) else []
                    for detail in details[:2]:
                        st.caption(f"• {detail}")

                st.divider()

            st.divider()

            # 投资亮点
            if analysis_result.get('highlights'):
                st.subheader("✨ 投资亮点")
                highlight_col1, highlight_col2 = st.columns(2)
                for i, highlight in enumerate(analysis_result['highlights']):
                    if i % 2 == 0:
                        with highlight_col1:
                            st.success(highlight)
                    else:
                        with highlight_col2:
                            st.success(highlight)

            st.divider()

            # 投资建议
            if analysis_result.get('suggestions'):
                st.subheader("💡 投资建议")
                for suggestion in analysis_result['suggestions']:
                    if '推荐' in suggestion or '🟢' in suggestion:
                        st.success(suggestion)
                    elif '谨慎' in suggestion or '🟠' in suggestion:
                        st.warning(suggestion)
                    elif '观望' in suggestion or '🔴' in suggestion:
                        st.error(suggestion)
                    else:
                        st.info(suggestion)

            # 市场关联分析（仅增强型分析有此数据）
            if analysis_result.get('market_relevance'):
                st.divider()
                st.subheader("🌐 市场关联分析")
                for relevance in analysis_result['market_relevance']:
                    st.info(relevance)

            # 详细报告（如果有）
            if analysis_result.get('detailed_report'):
                st.divider()
                with st.expander("📋 查看详细分析报告", expanded=False):
                    st.markdown(analysis_result['detailed_report'])

            st.divider()

            # 生成分析报告
            if st.button("📄 生成完整分析报告", key="generate_report"):
                report = generate_ai_report(fund_code, nav_history, performance, fund_info)
                st.markdown(report)

                # 导出按钮
                st.download_button(
                    label="📥 下载分析报告",
                    data=report,
                    file_name=f"{fund_code}_AI分析报告_{datetime.now().strftime('%Y%m%d')}.md",
                    mime="text/markdown"
                )
        else:
            st.warning("暂无法获取足够的分析数据")

    # ==================== 标签2: 净值走势 ====================
    with tab2:
        st.subheader("📈 净值走势分析")

        if nav_history is not None and len(nav_history) > 0:
            # 基本信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("**基金基本信息**")
                if fund_info:
                    st.write(f"- 代码: {fund_code}")
                    st.write(f"- 名称: {fund_info['fund_name']}")
                    st.write(f"- 最新净值: {fund_info['latest_nav']:.4f}")

            with col2:
                st.write("**区间收益**")
                if performance and '区间收益率' in performance:
                    return_rate = performance['区间收益率']
                    delta_color = "🔴" if return_rate >= 0 else "🟢"
                    st.write(f"- {period}收益: {delta_color} {return_rate:.2f}%")

                # 各阶段收益
                st.write("**各阶段收益**")
                for period_name in ['近1月', '近3月', '近6月', '今年以来']:
                    if period_name in performance:
                        r = performance[period_name]
                        color = "🔴" if r >= 0 else "🟢"
                        st.write(f"- {period_name}: {color} {r:.2f}%")

            with col3:
                st.write("**风险指标**")
                if performance:
                    if '最大回撤' in performance:
                        st.write(f"- 最大回撤: {performance['最大回撤']:.2f}%")
                    if '年化波动率' in performance:
                        st.write(f"- 年化波动率: {performance['年化波动率']:.2f}%")
                    if '最高净值' in performance:
                        st.write(f"- 最高净值: {performance['最高净值']:.4f}")
                    if '最低净值' in performance:
                        st.write(f"- 最低净值: {performance['最低净值']:.4f}")

            st.divider()

            # 图表选项
            chart_type = st.radio("图表类型", ["折线图", "面积图"], horizontal=True, key="chart_type")

            chart_data = nav_history.set_index('date')[['nav']]

            if chart_type == "面积图":
                st.area_chart(chart_data)
            else:
                st.line_chart(chart_data)

            # 数据表格和导出
            with st.expander("查看和导出历史数据"):
                display_data = nav_history.copy()
                display_data['date_only'] = display_data['date'].dt.strftime('%Y-%m-%d')
                display_data['nav_formatted'] = display_data['nav'].apply(lambda x: f"{x:.4f}")

                if 'accumulated_nav' in display_data.columns:
                    display_data['accumulated_nav_formatted'] = display_data['accumulated_nav'].apply(lambda x: f"{x:.4f}")
                    display_cols = ['date_only', 'nav_formatted', 'accumulated_nav_formatted']
                    display_headers = ['日期', '单位净值', '累计净值']
                else:
                    display_cols = ['date_only', 'nav_formatted']
                    display_headers = ['日期', '单位净值']

                display_dataframe = display_data[display_cols].copy()
                display_dataframe.columns = display_headers
                st.dataframe(display_dataframe, hide_index=True)

                # 导出功能
                csv = display_data[['date', 'nav']].to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 导出历史净值CSV",
                    data=csv,
                    file_name=f"{fund_code}_净值历史_{period}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        else:
            st.warning("暂无法获取历史净值数据")

    # ==================== 标签3: 持仓分析 ====================
    with tab3:
        st.subheader("🏭 持仓与配置分析")

        # 行业配置分析
        with st.expander("📊 行业配置分析", expanded=True):
            if st.button("分析行业配置", key="analyze_industry"):
                with st.spinner("正在分析行业配置..."):
                    industry_analysis = analyze_fund_industry_allocation(fund_code)

                if 'error' in industry_analysis:
                    st.error(industry_analysis['error'])
                else:
                    st.write(f"**报告期**: {industry_analysis.get('report_date', '未知')}")
                    st.write(f"**集中度**: {industry_analysis.get('concentration', '未知')}")

                    if industry_analysis.get('top_industries'):
                        st.write("**前三大行业配置**:")
                        for industry in industry_analysis['top_industries']:
                            st.write(f"- {industry['name']}: {industry['ratio']:.2f}%")

                    if industry_analysis.get('diversification'):
                        for item in industry_analysis['diversification']:
                            st.caption(f"• {item}")

        # 股票持仓分析
        with st.expander("📈 股票持仓分析", expanded=True):
            if st.button("分析股票持仓", key="analyze_portfolio"):
                with st.spinner("正在分析持仓数据..."):
                    portfolio_analysis = analyze_fund_portfolio(fund_code)

                if 'error' in portfolio_analysis:
                    st.error(portfolio_analysis['error'])
                else:
                    st.write(f"**报告期**: {portfolio_analysis.get('report_date', '未知')}")
                    st.write(f"**股票集中度**: {portfolio_analysis.get('stock_concentration', '未知')}")

                    if portfolio_analysis.get('top_holdings'):
                        st.write("**前十大重仓股**:")

                        holdings_data = []
                        for holding in portfolio_analysis['top_holdings']:
                            holdings_data.append({
                                '股票代码': holding['code'],
                                '股票名称': holding['name'],
                                '持仓比例': f"{holding['ratio']:.2f}%"
                            })

                        st.dataframe(pd.DataFrame(holdings_data), hide_index=True)

    # ==================== 标签4: 我的持仓 ====================
    with tab4:
        st.subheader("💼 我的持仓")

        holdings = get_holdings()
        my_holding = next((h for h in holdings if h['fund_code'] == fund_code), None)

        if my_holding:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("持有份额", f"{my_holding['total_units']:.2f}")

            with col2:
                st.metric("投入成本", f"¥{my_holding['total_cost']:,.2f}")

            with col3:
                st.metric("当前市值", f"¥{my_holding['current_value']:,.2f}")

            st.divider()

            # 盈亏分析
            pl_col1, pl_col2, pl_col3 = st.columns(3)

            with pl_col1:
                profit_loss = my_holding['profit_loss']
                profit_loss_color = "🔴" if profit_loss >= 0 else "🟢"
                st.metric("总盈亏", f"{profit_loss_color} ¥{profit_loss:,.2f}")

            with pl_col2:
                profit_loss_ratio = my_holding['profit_loss_ratio']
                ratio_color = "🔴" if profit_loss_ratio >= 0 else "🟢"
                st.metric("盈亏比例", f"{ratio_color} {profit_loss_ratio:.2f}%")

            with pl_col3:
                if my_holding['total_cost'] > 0:
                    current_nav = my_holding['current_value'] / my_holding['total_units']
                    st.metric("当前成本价", f"¥{my_holding['total_cost'] / my_holding['total_units']:.4f}")

            # 持有天数分析
            from db import get_inventory
            inventory = get_inventory(fund_code)

            if inventory:
                st.subheader("持仓批次明细")
                inventory_data = []
                for inv in inventory:
                    inventory_data.append({
                        '批次ID': inv['batch_id'][:16] + '...',
                        '买入日期': inv['buy_date'],
                        '持有份额': f"{inv['remaining_units']:.2f}",
                        '买入净值': f"{inv['nav']:.4f}",
                        '持有天数': inv['hold_days'],
                        '备注': inv.get('note', '')
                    })

                st.dataframe(pd.DataFrame(inventory_data), hide_index=True)

                # 持有天数统计
                if len(inventory) > 0:
                    avg_days = sum(inv['hold_days'] for inv in inventory) / len(inventory)
                    min_days = min(inv['hold_days'] for inv in inventory)
                    max_days = max(inv['hold_days'] for inv in inventory)

                    stat_col1, stat_col2, stat_col3 = st.columns(3)
                    with stat_col1:
                        st.metric("平均持有天数", f"{avg_days:.0f}天")
                    with stat_col2:
                        st.metric("最短持有", f"{min_days}天")
                    with stat_col3:
                        st.metric("最长持有", f"{max_days}天")
        else:
            st.info("暂无此基金持仓")
            st.caption("请前往「交易录入」页面添加买入记录")
