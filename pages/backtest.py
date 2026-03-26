"""策略回测页面"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import re
import os
import tempfile
from db import get_all_funds
from fund_api import get_fund_nav_history, backtest_strategy

# 尝试导入quantstats，如果未安装则提示
try:
    import quantstats
    QUANTSTATS_AVAILABLE = True
except ImportError:
    QUANTSTATS_AVAILABLE = False


def translate_quantstats_report(html_content):
    """
    将QuantStats生成的英文HTML报告翻译成中文

    Args:
        html_content: 原始HTML内容

    Returns:
        翻译后的HTML内容
    """
    if not html_content:
        return html_content

    # 英文到中文的翻译映射
    translations = {
        # 标题和指标名称
        r'\bCumulative Return\b': '累积收益',
        r'\bCAGR\b': '年化收益率',
        r'\bSharpe\b': '夏普比率',
        r'\bSortino\b': '索提诺比率',
        r'\bCalmar\b': '卡尔玛比率',
        r'\bMax Drawdown\b': '最大回撤',
        r'\bVolatility\(ann\)\b': '年化波动率',
        r'\bVolatility\(daily\)\b': '日波动率',
        r'\bBest Day\b': '最佳单日',
        r'\bWorst Day\b': '最差单日',
        r'\bAvg Gain\b': '平均涨幅',
        r'\bAvg Loss\b': '平均跌幅',
        r'\bWin Days\b': '盈利天数',
        r'\bLoss Days\b': '亏损天数',
        r'\bWin Days%\b': '盈利天数占比',
        r'\bLoss Days%\b': '亏损天数占比',
        r'\bExpectancy\b': '期望收益',
        r'\bSQN\b': '系统质量数',
        r'\bKelly Criterion\b': '凯利准则',
        r'\bRisk per Trade\b': '单笔风险',
        r'\bPortfolio Value\b': '投资组合价值',
        r'\bEquity Curve\b': '权益曲线',
        r'\bDrawdown\b': '回撤',
        r'\bMonthly Returns\b': '月度收益',
        r'\bYearly Returns\b': '年度收益',
        r'\bDistribution\b': '分布',
        r'\bSkew\b': '偏度',
        r'\bKurtosis\b': '峰度',
        r'\bTail Ratio\b': '尾部比率',
        r'\bCommon Sense Ratio\b': '常识比率',
        r'\bValue at Risk\b': '风险价值',
        r'\bConditional VaR\b': '条件风险价值',
        r'\bPayoff Ratio\b': '收益风险比',
        r'\bProfit Factor\b': '盈利因子',
        r'\b Recovery Factor\b': '恢复因子',
        r'\bRisk of Ruin\b': '破产风险',
        r'\bAvg Drawdown\b': '平均回撤',
        r'\bAvg Drawdown Days\b': '平均回撤天数',
        r'\bAvg Drawdown Duration\b': '平均回撤持续期',
        r'\bWin/Loss Ratio\b': '盈亏比',
        r'\bWin/Loss Spread\b': '盈亏差',

        # 统计相关
        r'\bMean\b': '均值',
        r'\bMedian\b': '中位数',
        r'\bStd Dev\b': '标准差',
        r'\bVariance\b': '方差',
        r'\bMin\b': '最小值',
        r'\bMax\b': '最大值',
        r'\bRange\b': '范围',
        r'\bCount\b': '数量',
        r'\bSum\b': '总和',

        # 时间相关
        r'\bStart\b': '开始',
        r'\bEnd\b': '结束',
        r'\bDuration\b': '持续期',
        r'\bDays\b': '天',
        r'\bWeeks\b': '周',
        r'\bMonths\b': '月',
        r'\bYears\b': '年',

        # 其他常用词
        r'\bStrategy\b': '策略',
        r'\bBenchmark\b': '基准',
        r'\bAlpha\b': '阿尔法',
        r'\bBeta\b': '贝塔',
        r'\bR-squared\b': 'R方值',
        r'\bInformation Ratio\b': '信息比率',
        r'\bTracking Error\b': '跟踪误差',
        r'\bCorrelation\b': '相关性',
        r'\bRolling Sharpe\b': '滚动夏普',
        r'\bRolling Sortino\b': '滚动索提诺',
        r'\bUlcer Index\b': '溃疡指数',
        r'\bPain Index\b': '痛苦指数',
        r'\bSterling Ratio\b': '斯特林比率',
        r'\bBurke Ratio\b': '伯克比率',

        # 月份
        r'\bJan\b': '一月',
        r'\bFeb\b': '二月',
        r'\bMar\b': '三月',
        r'\bApr\b': '四月',
        r'\bMay\b': '五月',
        r'\bJun\b': '六月',
        r'\bJul\b': '七月',
        r'\bAug\b': '八月',
        r'\bSep\b': '九月',
        r'\bOct\b': '十月',
        r'\bNov\b': '十一月',
        r'\bDec\b': '十二月',
    }

    # 执行替换
    translated_html = html_content
    for eng, chi in translations.items():
        translated_html = re.sub(eng, chi, translated_html, flags=re.IGNORECASE)

    # 添加中文字体支持
    # 在HTML head中添加中文字体
    font_style = """
    <style>
    body { font-family: "Microsoft YaHei", "SimHei", Arial, sans-serif; }
    table { font-family: "Microsoft YaHei", "SimHei", Arial, sans-serif; }
    </style>
    """

    # 将样式插入到</head>之前
    if '</head>' in translated_html:
        translated_html = translated_html.replace('</head>', font_style + '</head>')
    else:
        # 如果没有head标签，添加到html标签后
        translated_html = translated_html.replace('<html', '<html><head>' + font_style + '</head>', 1)

    return translated_html


def generate_quantstats_report(result, baseline_result, fund_nav_history, strategy_name):
    """
    使用QuantStats生成专业的HTML报告

    Args:
        result: 策略回测结果
        baseline_result: 定投基准回测结果
        fund_nav_history: 基金净值历史
        strategy_name: 策略名称

    Returns:
        HTML报告字符串（已翻译为中文）
    """
    if not QUANTSTATS_AVAILABLE:
        return None

    try:
        # 提取策略每日资产价值
        strategy_daily = pd.DataFrame(result['daily_values'])
        strategy_daily['date'] = pd.to_datetime(strategy_daily['date'])
        strategy_daily = strategy_daily.set_index('date')

        # 计算策略每日收益率（基于累计投入）
        if 'invested' in strategy_daily.columns:
            # 每日收益率 = (当日资产 - 前日资产) / 前日资产
            strategy_returns = strategy_daily['value'].pct_change().fillna(0)
        else:
            # 如果没有invested字段，使用简单的价值变化计算收益率
            strategy_returns = strategy_daily['value'].pct_change().fillna(0)

        # 获取定投基准作为benchmark
        if baseline_result and baseline_result.get('success'):
            baseline_daily = pd.DataFrame(baseline_result['daily_values'])
            baseline_daily['date'] = pd.to_datetime(baseline_daily['date'])
            baseline_daily = baseline_daily.set_index('date')
            baseline_returns = baseline_daily['value'].pct_change().fillna(0)
        else:
            # 如果没有基准，使用基金净值作为基准
            if fund_nav_history is not None and len(fund_nav_history) > 0:
                fund_nav = fund_nav_history.set_index('date')
                baseline_returns = fund_nav['nav'].pct_change().fillna(0)
            else:
                baseline_returns = None

        # 创建临时文件来保存HTML报告
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as tmp_file:
            tmp_path = tmp_file.name

        try:
            # 生成HTML报告
            if baseline_returns is not None:
                # 对齐日期索引
                common_dates = strategy_returns.index.intersection(baseline_returns.index)
                strategy_returns_aligned = strategy_returns.loc[common_dates]
                baseline_returns_aligned = baseline_returns.loc[common_dates]

                # 生成报告（包含基准对比）
                quantstats.reports.html(
                    strategy_returns_aligned,
                    baseline=baseline_returns_aligned,
                    title=f"{strategy_name}策略回测报告",
                    output=tmp_path
                )
            else:
                # 无基准的报告
                quantstats.reports.html(
                    strategy_returns,
                    title=f"{strategy_name}策略回测报告",
                    output=tmp_path
                )

            # 读取生成的HTML文件
            with open(tmp_path, 'r', encoding='utf-8') as f:
                html_report = f.read()

            # 翻译为中文
            html_report = translate_quantstats_report(html_report)

            return html_report

        finally:
            # 删除临时文件
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except Exception as e:
        st.error(f"生成QuantStats报告失败: {str(e)}")
        return None


def backtest_page():
    """策略回测页面"""
    st.title("📈 策略回测系统")
    st.info("ℹ️ 回测不同投资策略的历史表现，帮助选择最优投资方式")

    # 检查QuantStats是否可用
    if not QUANTSTATS_AVAILABLE:
        st.warning("⚠️ 未安装quantstats库，无法生成专业HTML报告。请运行: pip install quantstats")

    # 获取所有基金列表
    all_funds = get_all_funds()

    # 输入区域布局
    col1, col2, col3 = st.columns(3)

    with col1:
        # 选择基金
        fund_options = {f['fund_name']: f['fund_code'] for f in all_funds}
        selected_fund_name = st.selectbox("选择基金", list(fund_options.keys()))
        fund_code = fund_options[selected_fund_name]

    with col2:
        # 选择策略
        strategy = st.selectbox(
            "选择策略",
            ["定投", "智能定投", "均线策略", "价值平均"]
        )

    with col3:
        # 每月定投金额
        monthly_amount = st.number_input("每月定投金额（元）", min_value=100, max_value=100000, value=1000, step=100)

    st.divider()

    # 日期范围选择
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input("开始日期", value=pd.to_datetime('2020-01-01'))

    with col2:
        end_date = st.date_input("结束日期", value=datetime.now())

    # 高级设置（针对特定策略）
    if strategy == "均线策略":
        col1, col2 = st.columns(2)
        with col1:
            ma_short = st.number_input("短期均线（天）", min_value=5, max_value=60, value=30)
        with col2:
            ma_long = st.number_input("长期均线（天）", min_value=60, max_value=300, value=180)

    # 执行回测按钮
    if st.button("🚀 开始回测", type="primary"):
        with st.spinner("正在回测，请稍候..."):
            # 如果不是定投策略，先执行简单定投作为基准
            baseline_result = None
            if strategy != "定投":
                baseline_result = backtest_strategy(
                    fund_code=fund_code,
                    strategy="定投",
                    start_date=str(start_date),
                    end_date=str(end_date),
                    monthly_amount=monthly_amount
                )

            # 执行选定的策略回测
            if strategy == "均线策略":
                result = backtest_strategy(
                    fund_code=fund_code,
                    strategy=strategy,
                    start_date=str(start_date),
                    end_date=str(end_date),
                    monthly_amount=monthly_amount,
                    ma_short=ma_short,
                    ma_long=ma_long
                )
            else:
                result = backtest_strategy(
                    fund_code=fund_code,
                    strategy=strategy,
                    start_date=str(start_date),
                    end_date=str(end_date),
                    monthly_amount=monthly_amount
                )

        # 显示回测结果
        if not result.get('success'):
            st.error(f"❌ 回测失败: {result.get('error', '未知错误')}")
            return

        # 存储回测结果到 session_state，用于生成报告
        st.session_state.backtest_result = result
        st.session_state.backtest_baseline = baseline_result
        st.session_state.backtest_fund_code = fund_code
        st.session_state.backtest_strategy = strategy
        st.session_state.backtest_start_date = start_date
        st.session_state.backtest_end_date = end_date

        # 获取基金历史净值数据，用于计算一次性投入基准
        fund_nav_history = get_fund_nav_history(fund_code, period="全部")
        if fund_nav_history is not None and len(fund_nav_history) > 0:
            fund_nav_history = fund_nav_history[
                (fund_nav_history['date'] >= str(start_date)) &
                (fund_nav_history['date'] <= str(end_date))
            ].reset_index(drop=True)
        else:
            fund_nav_history = None

        st.session_state.backtest_fund_nav_history = fund_nav_history

        st.success("✅ 回测完成！")

        st.divider()

        # 显示策略概览
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("总收益率", f"{result['total_return']:.2f}%")

        with col2:
            st.metric("年化收益率", f"{result['annual_return']:.2f}%")

        with col3:
            st.metric("最大回撤", f"{result['max_drawdown']:.2f}%")

        with col4:
            st.metric("夏普比率", f"{result['sharpe_ratio']:.3f}")

        st.divider()

        # 投资概览
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("总投入", f"¥{result['total_invested']:,.2f}")

        with col2:
            st.metric("期末价值", f"¥{result['final_value']:,.2f}")

        with col3:
            profit = result['final_value'] - result['total_invested']
            profit_color = "normal" if profit >= 0 else "inverse"
            st.metric("总收益", f"¥{profit:,.2f}", delta_color=profit_color)

        st.divider()

        # 净值曲线图
        st.subheader("📈 资产净值曲线")

        daily_values = result.get('daily_values', [])
        if daily_values:
            # 转换为DataFrame
            df_plot = pd.DataFrame(daily_values)
            df_plot['date'] = pd.to_datetime(df_plot['date'])

            fig = go.Figure()

            # 计算收益率：基于当前累计投入
            # 收益率 = (当前资产 - 当前累计投入) / 当前累计投入 × 100%
            if 'invested' in df_plot.columns:
                df_plot['return_pct'] = ((df_plot['value'] - df_plot['invested']) / df_plot['invested'] * 100).round(2)
                df_plot['invested'] = df_plot['invested'].round(2)
            else:
                # 如果没有invested字段，使用初始值计算（兼容旧数据）
                initial_value = df_plot['value'].iloc[0]
                df_plot['return_pct'] = ((df_plot['value'] - initial_value) / initial_value * 100).round(2)
                df_plot['invested'] = initial_value  # 用于显示

            fig.add_trace(go.Scatter(
                x=df_plot['date'],
                y=df_plot['value'],
                mode='lines',
                name='资产净值',
                line=dict(color='#1f77b4', width=2),
                customdata=df_plot[['return_pct', 'invested']],
                hovertemplate='<b>%{x}</b><br>净值: ¥%{y:,.2f}<br>累计投入: ¥%{customdata[1]:,.2f}<br>收益率: %{customdata[0]:.2f}%<extra></extra>'
            ))

            # 添加定投基准线（如果不是定投策略）
            if strategy != "定投" and baseline_result and baseline_result.get('success'):
                # 使用简单定投的实际资产净值作为对比
                baseline_daily_values = baseline_result.get('daily_values', [])
                if baseline_daily_values:
                    df_baseline = pd.DataFrame(baseline_daily_values)
                    df_baseline['date'] = pd.to_datetime(df_baseline['date'])
                    # 确保日期对齐
                    df_baseline = df_baseline[df_baseline['date'].isin(df_plot['date'])].reset_index(drop=True)
                    # 计算收益率：基于当前累计投入
                    if 'invested' in df_baseline.columns:
                        df_baseline['return_pct'] = ((df_baseline['value'] - df_baseline['invested']) / df_baseline['invested'] * 100).round(2)
                        df_baseline['invested'] = df_baseline['invested'].round(2)
                    else:
                        baseline_initial = df_baseline['value'].iloc[0]
                        df_baseline['return_pct'] = ((df_baseline['value'] - baseline_initial) / baseline_initial * 100).round(2)
                        df_baseline['invested'] = baseline_initial
                    fig.add_trace(go.Scatter(
                        x=df_baseline['date'],
                        y=df_baseline['value'],
                        mode='lines',
                        name='定投基准',
                        line=dict(color='gray', width=1, dash='dash'),
                        customdata=df_baseline[['return_pct', 'invested']],
                        hovertemplate='<b>%{x}</b><br>净值: ¥%{y:,.2f}<br>累计投入: ¥%{customdata[1]:,.2f}<br>收益率: %{customdata[0]:.2f}%<extra></extra>'
                    ))

            # 添加一次性投入基准线（期初投入所有资金）
            if fund_nav_history is not None and len(fund_nav_history) > 0:
                # 使用简单定投的总投入作为一次性投入金额
                if baseline_result and baseline_result.get('success'):
                    lumpsum_amount = baseline_result.get('total_invested', monthly_amount * 12)
                else:
                    lumpsum_amount = result.get('total_invested', monthly_amount * 12)

                # 计算一次性投入的每日资产价值和收益率
                initial_nav = fund_nav_history.iloc[0]['nav']
                lumpsum_values = [lumpsum_amount * (nav / initial_nav) for nav in fund_nav_history['nav']]
                # 收益率 = (当前资产 - 投入金额) / 投入金额 × 100%
                lumpsum_returns = [((val - lumpsum_amount) / lumpsum_amount * 100) for val in lumpsum_values]
                # 一次性投入的累计投入始终等于初始投入
                lumpsum_invested = [lumpsum_amount] * len(lumpsum_values)

                fig.add_trace(go.Scatter(
                    x=fund_nav_history['date'],
                    y=lumpsum_values,
                    mode='lines',
                    name='一次性投入',
                    line=dict(color='#ff7f0e', width=1, dash='dot'),
                    customdata=list(zip(lumpsum_returns, lumpsum_invested)),
                    hovertemplate='<b>%{x}</b><br>净值: ¥%{y:,.2f}<br>累计投入: ¥%{customdata[1]:,.2f}<br>收益率: %{customdata[0]:.2f}%<extra></extra>'
                ))

            fig.update_layout(
                title="资产净值变化",
                xaxis_title="日期",
                yaxis_title="净值（元）",
                hovermode='x unified',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

        st.divider()

    # ========== 专业报告生成区域（独立显示） ==========
    if 'backtest_result' in st.session_state:
        st.divider()
        st.subheader("📊 专业回测报告")

        if QUANTSTATS_AVAILABLE:
            col1, col2 = st.columns([2, 1])
            with col1:
                st.info("ℹ️ 点击按钮生成中文专业报告，包含：累积收益对比、滚动夏普比率、最差持有天数、月度热力图等高级指标")
            with col2:
                generate_report = st.button("📄 生成中文专业报告", type="secondary", use_container_width=True, key="generate_report_btn")

            # 处理报告生成
            if generate_report:
                with st.spinner("正在生成中文专业报告，请稍候..."):
                    html_report = generate_quantstats_report(
                        result=st.session_state.backtest_result,
                        baseline_result=st.session_state.backtest_baseline,
                        fund_nav_history=st.session_state.backtest_fund_nav_history,
                        strategy_name=st.session_state.backtest_strategy
                    )

                if html_report:
                    st.success("✅ 报告生成成功！")
                    st.download_button(
                        label="📥 下载中文报告",
                        data=html_report,
                        file_name=f"回测报告_{st.session_state.backtest_strategy}_{st.session_state.backtest_fund_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                else:
                    st.error("❌ 报告生成失败，请检查数据是否完整")
        else:
            st.warning("⚠️ 未安装quantstats库，无法生成专业HTML报告。请运行: pip install quantstats")

        st.divider()

        # 交易记录
        st.subheader("📋 交易记录")

        transactions = st.session_state.backtest_result.get('transactions', [])
        if transactions:
            # 只显示前50条交易
            display_transactions = transactions[:50]

            trans_df = pd.DataFrame(display_transactions)

            # 格式化显示
            if 'ratio' in trans_df.columns:
                # 智能定投策略显示比例
                trans_df = trans_df[['date', 'type', 'nav', 'amount', 'ratio']]
                trans_df.columns = ['日期', '类型', '净值', '金额(元)', '投资比例']
            else:
                trans_df = trans_df[['date', 'type', 'nav', 'amount']]
                trans_df.columns = ['日期', '类型', '净值', '金额(元)']

            # 格式化数值
            trans_df['净值'] = trans_df['净值'].apply(lambda x: f"{x:.4f}")
            trans_df['金额(元)'] = trans_df['金额(元)'].apply(lambda x: f"{x:,.2f}")

            if 'ratio' in trans_df.columns:
                trans_df['投资比例'] = trans_df['投资比例'].apply(lambda x: f"{x}x")

            st.dataframe(trans_df, use_container_width=True, hide_index=True)

            if len(transactions) > 50:
                st.info(f"ℹ️ 共 {len(transactions)} 条交易记录，仅显示前50条")

        else:
            st.info("暂无交易记录")
