"""净值刷新辅助函数（支持并发刷新）"""
import streamlit as st
from db import get_holdings, update_holding_nav
from fund_api import get_fund_latest_nav


def refresh_nav_cache():
    """刷新所有持仓的净值（串行方式，保持向后兼容）"""
    holdings = get_holdings()
    for holding in holdings:
        fund_code = holding['fund_code']
        nav = get_fund_latest_nav(fund_code)
        if nav:
            update_holding_nav(fund_code, nav)
            st.session_state.current_nav_cache[fund_code] = nav


def refresh_nav_cache_concurrent(show_progress: bool = True, max_workers: int = 5):
    """
    并发刷新所有持仓的净值（性能优化）

    Args:
        show_progress: 是否显示进度条
        max_workers: 最大并发数

    Returns:
        刷新结果统计 {'success': 成功数, 'failed': 失败数, 'total': 总数}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from utils.async_fetcher import get_async_fetcher

    holdings = get_holdings()

    if not holdings:
        return {'success': 0, 'failed': 0, 'total': 0}

    fund_codes = [h['fund_code'] for h in holdings]
    total_count = len(fund_codes)

    # 初始化结果
    result = {'success': 0, 'failed': 0, 'total': total_count}

    # 显示进度条
    if show_progress:
        progress_bar = st.progress(0)
        status_text = st.empty()

    # 使用并发获取器
    fetcher = get_async_fetcher()

    def get_and_update_nav(fund_code: str):
        """获取并更新净值"""
        try:
            nav = get_fund_latest_nav(fund_code)
            if nav:
                update_holding_nav(fund_code, nav)
                st.session_state.current_nav_cache[fund_code] = nav
                return fund_code, True, None
            else:
                return fund_code, False, "获取净值失败"
        except Exception as e:
            return fund_code, False, str(e)

    # 并发执行
    nav_results = fetcher.fetch_multiple(
        func=lambda code: get_and_update_nav(code),
        items=fund_codes,
        timeout=30,
        show_progress=False
    )

    # 统计结果
    for fund_code, nav_result in nav_results.items():
        if nav_result:
            fund_code, success, error = nav_result
            if success:
                result['success'] += 1
            else:
                result['failed'] += 1

        # 更新进度
        if show_progress:
            completed = result['success'] + result['failed']
            progress = completed / total_count
            progress_bar.progress(progress)
            status_text.text(f"已刷新 {completed}/{total_count} 只基金")

    # 完成进度
    if show_progress:
        progress_bar.progress(1.0)
        status_text.text(f"刷新完成！成功 {result['success']} 只，失败 {result['failed']} 只")

    return result


def get_and_update_nav(fund_code: str) -> bool:
    """
    获取并更新单只基金的净值

    Args:
        fund_code: 基金代码

    Returns:
        是否成功
    """
    try:
        nav = get_fund_latest_nav(fund_code)
        if nav:
            update_holding_nav(fund_code, nav)
            if 'current_nav_cache' in st.session_state:
                st.session_state.current_nav_cache[fund_code] = nav
            return True
    except Exception as e:
        print(f"更新 {fund_code} 净值失败: {e}")
    return False
