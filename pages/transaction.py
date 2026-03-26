"""交易录入页面"""
import streamlit as st
import pandas as pd
from datetime import datetime
import time
from db import get_all_funds, add_transaction, get_inventory, get_fund, add_fund
from fund_api import get_fund_nav_history, get_fund_info


# 使用缓存加速数据加载
@st.cache_data(ttl=3600)  # 缓存1小时
def get_funds_cached():
    """缓存基金列表"""
    return get_all_funds()


@st.cache_data(ttl=300)  # 缓存5分钟
def get_nav_history_cached(fund_code, period):
    """缓存净值历史数据"""
    return get_fund_nav_history(fund_code, period)


def get_inventory_silent(fund_code):
    """
    静默获取库存数据（不显示Running提示）
    使用session_state缓存，避免st.cache_data的Running提示
    """
    if 'inventory_cache' not in st.session_state:
        st.session_state.inventory_cache = {}

    cache_key = fund_code

    # 检查缓存
    if cache_key in st.session_state.inventory_cache:
        return st.session_state.inventory_cache[cache_key]

    # 从数据库获取
    inventory = get_inventory(fund_code)

    # 存入缓存
    st.session_state.inventory_cache[cache_key] = inventory

    return inventory


def get_fund_info_silent(fund_code):
    """
    静默获取基金信息（不显示Running提示）
    优化：避免重复API调用，从净值数据中提取基金名
    使用session_state缓存
    """
    if 'fund_info_cache' not in st.session_state:
        st.session_state.fund_info_cache = {}

    cache_key = fund_code

    # 检查缓存（5分钟过期）
    if cache_key in st.session_state.fund_info_cache:
        cached_data = st.session_state.fund_info_cache[cache_key]
        if time.time() - cached_data['timestamp'] < 300:  # 5分钟
            return cached_data['data']

    # 优化：只调用一次API获取净值数据（包含基金名）
    try:
        from fund_api import get_fund_nav_history

        # 获取净值数据（一次API调用）
        df = get_fund_nav_history(fund_code, period="1月")

        if df is None or len(df) == 0:
            return None

        # 从净值数据中提取信息，避免第二次API调用
        latest = df.iloc[-1]

        info = {
            'fund_code': fund_code,
            'fund_name': f"基金{fund_code}",  # 占位符，避免API调用
            'latest_nav': float(latest['nav']),
            'date': str(latest['date'].date())
        }

        if 'accumulated_nav' in df.columns:
            info['accumulated_nav'] = float(latest['accumulated_nav'])

        # 存入缓存
        st.session_state.fund_info_cache[cache_key] = {
            'data': info,
            'timestamp': time.time()
        }
        return info

    except Exception as e:
        return None


def transaction_page():
    """交易页面"""
    st.title("💰 交易录入")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("添加交易记录")

        # 基金选择
        funds = get_funds_cached()
        fund_options = {f['fund_name']: f['fund_code'] for f in funds}
        fund_options["+ 新增基金"] = "_new_"

        selected_fund = st.selectbox("选择基金", options=list(fund_options.keys()))

        fund_code = None
        fund_name = ""

        if fund_options[selected_fund] == "_new_":
            # 新增基金
            with st.expander("新增基金", expanded=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    new_fund_code = st.text_input("基金代码", max_chars=6, placeholder="例如: 161725", key="new_fund_code")
                with col_b:
                    new_fund_name = st.text_input("基金名称", placeholder="例如: 招商中证白酒指数", key="new_fund_name")

                if st.button("添加基金", use_container_width=True, key="add_fund_btn"):
                    if new_fund_code and new_fund_name:
                        if add_fund(new_fund_code, new_fund_name):
                            # 清除基金列表缓存
                            st.cache_data.clear()
                            st.success(f"基金 {new_fund_name} 添加成功！")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("添加失败，请检查基金代码")
            st.stop()
        else:
            fund_code = fund_options[selected_fund]
            fund_name = selected_fund

            # 选择日期和查询净值
            st.divider()

            # 交易日期、星期几、净值查询（三列统一布局）
            col_date, col_weekday, col_nav = st.columns(3)

            # 左列：交易日期
            with col_date:
                st.caption("交易日期")
                transaction_date = st.date_input(
                    "日期",
                    value=datetime.now().date(),
                    key="transaction_date_input",
                    label_visibility="collapsed"
                )

            # 中列：星期几
            with col_weekday:
                st.caption("星期")
                weekday_dict = {0: '一', 1: '二', 2: '三', 3: '四', 4: '五', 5: '六', 6: '日'}
                weekday = weekday_dict.get(transaction_date.weekday(), '')
                st.markdown(f"<div style='font-size: 1.1rem; font-weight: 500;'>周{weekday}</div>", unsafe_allow_html=True)

            # 右列：净值
            with col_nav:
                st.caption("净值")
                date_nav = None

                # 初始化缓存
                if 'nav_cache' not in st.session_state:
                    st.session_state.nav_cache = {}

                # 检查缓存中是否有当前基金和日期的净值数据
                cache_key = f"{fund_code}_{transaction_date}"
                if cache_key in st.session_state.nav_cache:
                    # 从缓存获取
                    cached_data = st.session_state.nav_cache[cache_key]
                    date_nav = cached_data['nav']
                    if cached_data.get('display'):
                        st.markdown(cached_data['display'], unsafe_allow_html=True)
                else:
                    # 自动查询净值（使用缓存）
                    try:
                        nav_history = get_nav_history_cached(fund_code, period="1年")
                        display_html = None

                        if nav_history is not None and len(nav_history) > 0:
                            nav_history['date_only'] = nav_history['date'].dt.strftime('%Y-%m-%d')
                            target_date_str = transaction_date.strftime('%Y-%m-%d')

                            # 查找精确匹配的净值
                            exact_match = nav_history[nav_history['date_only'] == target_date_str]
                            if len(exact_match) > 0:
                                date_nav = exact_match.iloc[0]['nav']

                                # 查找前一日的净值
                                current_index = exact_match.index[0]
                                if current_index > 0:
                                    prev_nav = nav_history.iloc[current_index - 1]['nav']
                                    change = date_nav - prev_nav
                                    change_pct = (change / prev_nav) * 100 if prev_nav > 0 else 0

                                    # 红涨绿跌
                                    if change > 0:
                                        color = "#ff4d4f"  # 红色
                                        arrow = "↑"
                                        sign = "+"
                                    elif change < 0:
                                        color = "#00c853"  # 绿色
                                        arrow = "↓"
                                        sign = ""
                                    else:
                                        color = "#888888"  # 灰色
                                        arrow = "-"
                                        sign = ""

                                    display_html = f"""
                                    <div style='font-size: 1.5rem; font-weight: 600; color: {color};'>{date_nav:.4f}</div>
                                    <div style='font-size: 0.85rem; color: {color};'>{arrow} {sign}{change:.4f} ({sign}{change_pct:.2f}%)</div>
                                    """
                                else:
                                    display_html = f"<div style='font-size: 1.5rem; font-weight: 600;'>{date_nav:.4f}</div>"
                            else:
                                display_html = "<div style='font-size: 1.1rem; color: #888;'>当日无净值</div>"
                        else:
                            display_html = "<div style='font-size: 1.1rem; color: #888;'>当日无净值</div>"

                        # 保存到缓存
                        st.session_state.nav_cache[cache_key] = {
                            'nav': date_nav,
                            'display': display_html
                        }

                        # 显示结果
                        if display_html:
                            st.markdown(display_html, unsafe_allow_html=True)

                    except Exception as e:
                        st.markdown(
                            "<div style='font-size: 1.1rem; color: #888;'>当日无净值</div>",
                            unsafe_allow_html=True
                        )

            # 交易表单
            st.divider()
            transaction_type = st.selectbox("交易类型", ["买入", "卖出"], key="transaction_type_select")

            if transaction_type == "买入":
                # 买入表单外部：输入金额
                col_amount, col_units, col_fee = st.columns([2, 2, 1])

                with col_amount:
                    buy_amount = st.number_input(
                        "交易金额 (元)",
                        min_value=0.0,
                        step=100.0,
                        format="%.2f",
                        key="buy_amount_input"
                    )

                with col_units:
                    # 计算确认份额
                    buy_units_calc = 0.0
                    if date_nav is not None and buy_amount > 0:
                        buy_units_calc = buy_amount / date_nav

                    # 使用动态 key 强制刷新显示
                    st.number_input(
                        "确认份额 (份)",
                        value=buy_units_calc,
                        min_value=0.0,
                        step=0.01,
                        format="%.2f",
                        disabled=True,
                        key=f"buy_units_display_{buy_amount}_{date_nav}"
                    )

                with col_fee:
                    buy_fee = st.number_input(
                        "手续费",
                        min_value=0.0,
                        step=1.0,
                        format="%.2f",
                        value=0.0,
                        key="buy_fee_input"
                    )

                with st.form("buy_form", clear_on_submit=False):
                    submitted = st.form_submit_button("提交买入记录", use_container_width=True, disabled=date_nav is None)

                    if submitted:
                        if buy_amount > 0 and date_nav is not None:
                            buy_units_calc = buy_amount / date_nav
                            if add_transaction(
                                fund_code=fund_code,
                                transaction_type="买入",
                                amount=buy_amount,
                                units=buy_units_calc,
                                nav=date_nav,
                                fee=buy_fee,
                                transaction_date=transaction_date.strftime('%Y-%m-%d')
                            ):
                                # 清除库存缓存
                                if 'inventory_cache' in st.session_state and fund_code in st.session_state.inventory_cache:
                                    del st.session_state.inventory_cache[fund_code]

                                st.success(f"买入记录添加成功！{buy_amount:,.2f}元, {buy_units_calc:.2f}份")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("添加失败，请重试")
                        else:
                            st.warning("请输入有效的金额")

            else:
                # 卖出表单外部：输入份额
                col_units, col_amount, col_fee = st.columns([2, 2, 1])

                with col_units:
                    sell_units = st.number_input(
                        "卖出份额 (份)",
                        min_value=0.0,
                        step=0.01,
                        format="%.2f",
                        key="sell_units_input"
                    )

                with col_amount:
                    # 计算交易金额
                    sell_amount_calc = 0.0
                    if date_nav is not None and sell_units > 0:
                        sell_amount_calc = sell_units * date_nav

                    # 使用动态 key 强制刷新显示
                    st.number_input(
                        "交易金额 (元)",
                        value=sell_amount_calc,
                        min_value=0.0,
                        step=0.01,
                        format="%.2f",
                        disabled=True,
                        key=f"sell_amount_display_{sell_units}_{date_nav}"
                    )

                with col_fee:
                    sell_fee = st.number_input(
                        "手续费",
                        min_value=0.0,
                        step=1.0,
                        format="%.2f",
                        value=0.0,
                        key="sell_fee_input"
                    )

                with st.form("sell_form", clear_on_submit=False):
                    submitted = st.form_submit_button("提交卖出记录", use_container_width=True, disabled=date_nav is None)

                    if submitted:
                        if sell_units > 0 and date_nav is not None:
                            sell_amount_calc = sell_units * date_nav
                            if add_transaction(
                                fund_code=fund_code,
                                transaction_type="卖出",
                                amount=sell_amount_calc,
                                units=sell_units,
                                nav=date_nav,
                                fee=sell_fee,
                                transaction_date=transaction_date.strftime('%Y-%m-%d')
                            ):
                                # 清除库存缓存
                                if 'inventory_cache' in st.session_state and fund_code in st.session_state.inventory_cache:
                                    del st.session_state.inventory_cache[fund_code]

                                st.success(f"卖出记录添加成功！{sell_units:.2f}份, {sell_amount_calc:,.2f}元")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("添加失败，请重试")
                        else:
                            st.warning("请输入有效的份额")

    with col2:
        # 显示库存信息
        if fund_code:
            st.subheader("当前库存")

            # 使用容器提前占位，避免"running..."闪烁
            inventory_container = st.container()

            with inventory_container:
                # 使用静默缓存获取库存
                inventory = get_inventory_silent(fund_code)

                # 性能优化：不获取最新净值，直接用成本作为市值
                current_nav = None  # 设为None，后续会用成本代替市值

                if inventory:
                    total_units = sum([item['remaining_units'] for item in inventory])

                    # 计算总成本（不计算当前市值，避免API调用）
                    total_cost = sum([item['remaining_units'] * item['nav'] for item in inventory])

                    st.metric("可卖份额", f"{total_units:.2f}份")
                    st.markdown(
                        f"<div style='font-size: 0.85rem;'>投入成本: <span style='font-weight: 600;'>¥{total_cost:,.2f}</span></div>",
                        unsafe_allow_html=True
                    )

                    st.divider()

                    # 库存明细（不显示实时盈亏，显示持有天数颜色）
                    for item in inventory:
                        buy_nav = item['nav']
                        remaining_units = item['remaining_units']
                        buy_cost = remaining_units * buy_nav

                        # 根据持有天数设置颜色和emoji
                        hold_days = item['hold_days']
                        if hold_days < 7:
                            day_emoji = "🔴"  # 小于7天
                        elif hold_days < 30:
                            day_emoji = "🟡"  # 7-29天
                        else:
                            day_emoji = "🟢"  # 大于等于30天

                        with st.expander(
                            f"{item['buy_date']}        **{item['remaining_units']:.2f}份**        {day_emoji} {item['hold_days']}天",
                            expanded=False
                        ):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown(
                                    f"<div style='font-size: 0.9rem;'>买入净值: <b>{buy_nav:.4f}</b></div>",
                                    unsafe_allow_html=True
                                )
                            with col2:
                                st.markdown(
                                    f"<div style='font-size: 0.9rem;'>投入成本: <b>¥{buy_cost:.2f}</b></div>",
                                    unsafe_allow_html=True
                                )
                else:
                    st.info("暂无库存")
                    st.caption("买入后会显示库存信息")
