"""买卖信号页面"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from db import get_all_funds, get_holdings, get_inventory
from fund_analyzer import analyze_buy_sell_signals, analyze_position_profit_loss


def signals_page():
    """买卖信号页面"""
    st.title("🎯 买卖信号看板")
    st.info("ℹ️ 基于技术指标分析，提供买卖建议、止盈止损等信号")

    # 获取所有持仓基金
    holdings = get_holdings()

    if not holdings:
        st.warning("⚠️ 暂无持仓，请先买入基金")
        return

    # ==================== 持仓基金信号汇总表 ====================
    st.subheader("📋 持仓基金信号汇总")

    with st.spinner("正在分析所有持仓基金..."):
        summary_data = []
        for h in holdings:
            fund_code = h['fund_code']
            fund_name = h['fund_name']

            # 分析买卖信号
            analysis = analyze_buy_sell_signals(fund_code)

            if analysis.get('success'):
                # 综合建议
                signal = analysis['overall_signal']
                # 趋势方向
                trend = analysis['trend']['direction']
                # 估值水平
                valuation = analysis['valuation']['level']
                # 最新净值
                latest_nav = analysis['latest_nav']
                # 分析日期
                latest_date = analysis['latest_date']

                summary_data.append({
                    '基金名称': fund_name,
                    '基金代码': fund_code,
                    '综合建议': signal['text'],
                    '趋势方向': trend,
                    '估值水平': valuation,
                    '最新净值': latest_nav,
                    '分析日期': latest_date
                })
            else:
                summary_data.append({
                    '基金名称': fund_name,
                    '基金代码': fund_code,
                    '综合建议': '分析失败',
                    '趋势方向': '-',
                    '估值水平': '-',
                    '最新净值': '-',
                    '分析日期': '-'
                })

    if summary_data:
        # 统计各类信号数量（根据实际的信号文本）
        buy_count = sum(1 for d in summary_data if d['综合建议'] in ['强烈买入', '买入'])
        sell_count = sum(1 for d in summary_data if d['综合建议'] in ['强烈卖出', '卖出'])
        hold_count = sum(1 for d in summary_data if d['综合建议'] == '持有')
        watch_count = sum(1 for d in summary_data if d['综合建议'] in ['观望', '中性'])

        # 显示统计卡片
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.markdown(
                f"<div style='text-align: center; padding: 10px; background-color: #f0f0f0; border-radius: 10px;'>"
                f"<div style='font-size: 0.9rem; color: #666;'>持仓基金</div>"
                f"<div style='font-size: 1.8rem; font-weight: bold; color: #333;'>{len(summary_data)}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

        with col2:
            st.markdown(
                f"<div style='text-align: center; padding: 10px; background-color: #d9f7be; border-radius: 10px;'>"
                f"<div style='font-size: 0.9rem; color: #389e0d;'>买入信号</div>"
                f"<div style='font-size: 1.8rem; font-weight: bold; color: #389e0d;'>{buy_count}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

        with col3:
            st.markdown(
                f"<div style='text-align: center; padding: 10px; background-color: #ffccc7; border-radius: 10px;'>"
                f"<div style='font-size: 0.9rem; color: #cf1322;'>卖出信号</div>"
                f"<div style='font-size: 1.8rem; font-weight: bold; color: #cf1322;'>{sell_count}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

        with col4:
            st.markdown(
                f"<div style='text-align: center; padding: 10px; background-color: #e6f7ff; border-radius: 10px;'>"
                f"<div style='font-size: 0.9rem; color: #0958d9;'>持有</div>"
                f"<div style='font-size: 1.8rem; font-weight: bold; color: #0958d9;'>{hold_count}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

        with col5:
            st.markdown(
                f"<div style='text-align: center; padding: 10px; background-color: #fff7e6; border-radius: 10px;'>"
                f"<div style='font-size: 0.9rem; color: #d46b08;'>观望/中性</div>"
                f"<div style='font-size: 1.8rem; font-weight: bold; color: #d46b08;'>{watch_count}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

        # 转换为DataFrame并显示
        df_summary = pd.DataFrame(summary_data)

        # 为综合建议列添加颜色标记
        def color_signal(val):
            if val in ['强烈买入', '买入']:
                return 'background-color: #d9f7be; color: #389e0d; font-weight: bold'
            elif val in ['强烈卖出', '卖出']:
                return 'background-color: #ffccc7; color: #cf1322; font-weight: bold'
            elif val == '持有':
                return 'background-color: #e6f7ff; color: #0958d9; font-weight: bold'
            elif val in ['观望', '中性']:
                return 'background-color: #fff7e6; color: #d46b08; font-weight: bold'
            else:
                return 'background-color: #f0f0f0; color: #666'

        # 使用 map 替代 applymap（pandas 2.0+）
        try:
            styled_df = df_summary.style.map(color_signal, subset=['综合建议'])
        except AttributeError:
            # 旧版本 pandas 使用 applymap
            styled_df = df_summary.style.applymap(color_signal, subset=['综合建议'])

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    st.divider()

    # ==================== 单个基金详细分析 ====================
    st.subheader("🔍 单个基金详细分析")

    # 基金选择
    fund_options = {f"{h['fund_name']} ({h['fund_code']})": h['fund_code'] for h in holdings}
    selected = st.selectbox("选择基金", list(fund_options.keys()))

    fund_code = fund_options[selected]
    fund_name = selected.split('(')[0].strip()

    st.divider()

    # 执行技术分析
    with st.spinner(f"正在分析 {fund_name} 的技术指标..."):
        analysis = analyze_buy_sell_signals(fund_code)

    if not analysis.get('success'):
        st.error(f"❌ 分析失败: {analysis.get('error', '未知错误')}")
        return

    # 获取库存信息
    inventory = get_inventory(fund_code)

    # 显示基本信息
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("最新净值", f"{analysis['latest_nav']:.4f}")
    with col2:
        st.metric("分析日期", analysis['latest_date'])
    with col3:
        trend = analysis['trend']
        trend_color = "normal" if '上涨' in trend['direction'] else "inverse"
        st.metric("趋势方向", trend['direction'], delta_color=trend_color)
    with col4:
        valuation = analysis['valuation']
        st.metric("估值水平", valuation['level'])

    st.divider()

    # 综合建议卡片
    signal = analysis['overall_signal']
    st.markdown(
        f"""
        <div style='background-color: {signal['color']}; padding: 20px; border-radius: 10px; text-align: center;'>
            <h2 style='color: white; margin: 0;'>{signal['text']}</h2>
            <p style='color: white; margin: 10px 0 0 0; font-size: 1.1rem;'>{signal['reason']}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()

    # 详细信号分析
    tab1, tab2, tab3, tab4 = st.tabs(["📈 技术指标", "💰 止盈止损", "📊 买卖信号", "📉 净值走势"])

    with tab1:
        st.subheader("技术指标概览")

        indicators = analysis['indicators']

        # 均线指标
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 移动平均线")
            ma_data = []
            for period in [5, 10, 20, 60]:
                ma_key = f'ma{period}'
                if ma_key in indicators and indicators[ma_key] is not None:
                    ma_data.append({
                        '周期': f'MA{period}',
                        '值': f"{indicators[ma_key]:.4f}"
                    })
            if ma_data:
                st.dataframe(pd.DataFrame(ma_data), use_container_width=True, hide_index=True)

        with col2:
            st.markdown("### 其他指标")
            other_data = []
            if indicators['rsi'] is not None:
                other_data.append({'指标': 'RSI', '值': f"{indicators['rsi']:.2f}"})
            if indicators['macd'] is not None:
                other_data.append({'指标': 'MACD', '值': f"{indicators['macd']:.6f}"})
            if indicators['bb_upper'] is not None:
                other_data.append({'指标': '布林带上轨', '值': f"{indicators['bb_upper']:.4f}"})
                other_data.append({'指标': '布林带下轨', '值': f"{indicators['bb_lower']:.4f}"})

            if other_data:
                st.dataframe(pd.DataFrame(other_data), use_container_width=True, hide_index=True)

        # 估值详情
        st.divider()
        st.subheader("估值分析")
        v = valuation

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("分位数", f"{v['percentile']:.1f}%")
        with col2:
            st.metric("最低净值", f"{v['min_nav']:.4f}")
        with col3:
            st.metric("平均净值", f"{v['avg_nav']:.4f}")
        with col4:
            st.metric("最高净值", f"{v['max_nav']:.4f}")

        # 估值进度条
        st.progress(v['percentile'] / 100)
        st.caption(f"当前净值处于历史数据的 {v['percentile']:.1f}% 分位数位置")

    with tab2:
        st.subheader("止盈止损建议")

        if inventory:
            # 获取持仓分析
            pl_analysis = analyze_position_profit_loss(fund_code, inventory)

            if pl_analysis.get('success'):
                st.metric("最新净值", f"{pl_analysis['latest_nav']:.4f}")
                st.metric("可卖份额", f"{pl_analysis['total_units']:.2f}份")

                st.divider()

                # 显示每个批次的建议
                for rec in pl_analysis['recommendations']:
                    # 根据建议设置样式
                    rec_type = rec['recommendation']

                    if rec_type == 'sell':
                        color = '#ff4d4f'
                        emoji = '🔴'
                        status = '建议卖出'
                    elif rec_type == 'partial_sell':
                        color = '#ff7875'
                        emoji = '🟠'
                        status = '可部分止盈'
                    elif rec_type == 'buy' or rec_type == 'add_position':
                        color = '#52c41a'
                        emoji = '🟢'
                        status = '建议补仓'
                    elif rec_type == 'stop_loss':
                        color = '#faad14'
                        emoji = '🟡'
                        status = '关注止损'
                    elif rec_type == 'watch':
                        color = '#1890ff'
                        emoji = '🔵'
                        status = '设置止盈'
                    else:  # hold
                        color = '#8c8c8c'
                        emoji = '⚪'
                        status = '继续持有'

                    st.markdown(
                        f"""
                        <div style='border-left: 4px solid {color}; padding: 10px; margin: 10px 0; background-color: {color}15; border-radius: 5px;'>
                            <div style='display: flex; justify-content: space-between; align-items: center;'>
                                <span style='font-size: 1.1rem;'>{emoji} <b>{rec['buy_date']}</b> — <b>{rec['units']:.2f}份</b></span>
                                <span style='color: {color}; font-weight: 600;'>{status}</span>
                            </div>
                            <div style='margin-top: 8px; font-size: 0.9rem; color: #666;'>
                                买入净值: {rec['buy_nav']:.4f} | 投入成本: ¥{rec['cost']:.2f} | 当前市值: ¥{rec['current_value']:.2f}<br/>
                                <span style='color: {"red" if rec['profit'] > 0 else "green"}'>盈亏: {rec['profit']:+.2f} ({rec['profit_ratio']:+.1f}%)</span> | 持有{rec['hold_days']}天
                            </div>
                            <div style='margin-top: 8px; padding: 5px 10px; background-color: rgba(0,0,0,0.03); border-radius: 3px;'>
                                💡 {rec['reason']}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
        else:
            st.info("暂无库存")

    with tab3:
        st.subheader("买卖信号详情")

        signals = analysis['signals']

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### ✅ 买入信号")
            if signals['buy']:
                for i, signal in enumerate(signals['buy'], 1):
                    st.markdown(f"{i}. {signal}")
            else:
                st.info("暂无买入信号")

        with col2:
            st.markdown("### ⚠️ 卖出信号")
            if signals['sell']:
                for i, signal in enumerate(signals['sell'], 1):
                    st.markdown(f"{i}. {signal}")
            else:
                st.info("暂无卖出信号")

    with tab4:
        st.subheader("净值走势与技术指标")

        df = analysis['data']

        # 创建图表
        fig = go.Figure()

        # 净值曲线
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['nav'],
            mode='lines',
            name='净值',
            line=dict(color='#1f77b4', width=2)
        ))

        # 添加均线
        ma_colors = ['#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        for i, period in enumerate([5, 10, 20, 60]):
            ma_key = f'MA{period}'
            if ma_key in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df[ma_key],
                    mode='lines',
                    name=f'MA{period}',
                    line=dict(color=ma_colors[i], width=1, dash='dot')
                ))

        # 布林带
        if 'BB_upper' in df.columns and 'BB_lower' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['BB_upper'],
                mode='lines',
                name='布林带上轨',
                line=dict(color='gray', width=1, dash='dash')
            ))
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['BB_lower'],
                mode='lines',
                name='布林带下轨',
                line=dict(color='gray', width=1, dash='dash'),
                fill='tonexty',
                fillcolor='rgba(128,128,128,0.1)'
            ))

        fig.update_layout(
            title="净值走势与技术指标",
            xaxis_title="日期",
            yaxis_title="净值",
            hovermode='x unified',
            height=500,
            legend=dict(x=0, y=1)
        )

        st.plotly_chart(fig, use_container_width=True)

        # RSI指标
        st.subheader("RSI相对强弱指标")
        fig_rsi = go.Figure()

        fig_rsi.add_trace(go.Scatter(
            x=df['date'],
            y=df['RSI'],
            mode='lines',
            name='RSI',
            line=dict(color='#9467bd', width=2)
        ))

        # 添加超买超卖区域
        fig_rsi.add_hrect(y0=70, y1=100, line_width=0, fillcolor="rgba(255,0,0,0.1)", annotation_text="超买区")
        fig_rsi.add_hrect(y0=30, y1=70, line_width=0, fillcolor="rgba(128,128,128,0.1)", annotation_text="中性区")
        fig_rsi.add_hrect(y0=0, y1=30, line_width=0, fillcolor="rgba(0,255,0,0.1)", annotation_text="超卖区")

        fig_rsi.update_layout(
            title="RSI指标",
            xaxis_title="日期",
            yaxis_title="RSI",
            height=300,
            yaxis=dict(range=[0, 100])
        )

        st.plotly_chart(fig_rsi, use_container_width=True)
