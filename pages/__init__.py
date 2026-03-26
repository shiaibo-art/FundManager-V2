"""页面模块"""
from .overview import overview_page
from .transaction import transaction_page
from .history import history_page
from .fund_detail import fund_detail_page
from .fund_comparison import fund_comparison_page
from .asset_allocation import asset_allocation_page
from .backtest import backtest_page
from .signals import signals_page
from .market_overview import market_overview_page

__all__ = [
    'overview_page',
    'transaction_page',
    'history_page',
    'fund_detail_page',
    'fund_comparison_page',
    'asset_allocation_page',
    'backtest_page',
    'signals_page',
    'market_overview_page',
]
