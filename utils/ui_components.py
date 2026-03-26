"""UI组件和格式化函数"""
import streamlit as st


def format_amount(value, precision=2):
    """格式化金额"""
    if value is None:
        return "¥0.00"
    return f"¥{value:,.{precision}f}"


def format_units(value, precision=2):
    """格式化份额"""
    if value is None:
        return "0.00"
    return f"{value:,.{precision}f}"


def format_percentage(value, precision=2):
    """格式化百分比"""
    if value is None:
        return "0.00%"
    return f"{value:.{precision}f}%"


def format_profit_loss(amount, ratio=None, show_emoji=True):
    """格式化盈亏显示"""
    if amount is None:
        return {"text": "¥0.00", "color": "#888888", "emoji": ""}

    if amount > 0:
        color = "#ff4d4f"  # 红色
        emoji = "📈" if show_emoji else ""
        arrow = "↑"
        sign = "+"
    elif amount < 0:
        color = "#00c853"  # 绿色
        emoji = "📉" if show_emoji else ""
        arrow = "↓"
        sign = ""
    else:
        color = "#888888"  # 灰色
        emoji = "" if show_emoji else ""
        arrow = "-"
        sign = ""

    text = f"{sign}{format_amount(amount)}"

    if ratio is not None:
        text += f" ({sign}{ratio:.2f}%)"

    return {
        "text": text,
        "color": color,
        "emoji": emoji,
        "arrow": arrow
    }


def display_metric(label, value, delta=None, delta_color="normal"):
    """显示指标卡片"""
    if delta:
        st.metric(label=label, value=value, delta=delta, delta_color=delta_color)
    else:
        st.metric(label=label, value=value)


def display_profit_metric(label, amount, ratio=None):
    """显示盈亏指标"""
    pl = format_profit_loss(amount, ratio)
    text = f"{pl['emoji']} {pl['text']}" if pl['emoji'] else pl['text']
    delta_color = "inverse" if amount >= 0 else "normal"
    st.metric(label=label, value=text, delta=f"{ratio:.2f}%" if ratio is not None else None, delta_color=delta_color)
