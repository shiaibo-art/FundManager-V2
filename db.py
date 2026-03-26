"""
数据库操作模块
使用SQLite进行数据持久化
"""
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
import os


# 数据库文件路径
DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'fund_manager.db')


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表结构"""
    conn = get_connection()
    cursor = conn.cursor()

    # 基金信息表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS funds (
            fund_code TEXT PRIMARY KEY,
            fund_name TEXT NOT NULL,
            fund_type TEXT,
            fund_type_detail TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 交易记录表（带库存追踪）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            amount REAL NOT NULL,
            units REAL,
            nav REAL,
            fee REAL DEFAULT 0,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note TEXT,
            batch_id TEXT,
            remaining_units REAL DEFAULT 0,
            is_closed INTEGER DEFAULT 0,
            FOREIGN KEY (fund_code) REFERENCES funds (fund_code)
        )
    ''')

    # 持仓表（通过交易记录计算得出，这里作为缓存）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            fund_code TEXT PRIMARY KEY,
            total_units REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            current_value REAL DEFAULT 0,
            profit_loss REAL DEFAULT 0,
            profit_loss_ratio REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fund_code) REFERENCES funds (fund_code)
        )
    ''')

    # 基金行业配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_industry_allocation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            industry_name TEXT NOT NULL,
            allocation_ratio REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, industry_name),
            FOREIGN KEY (fund_code) REFERENCES funds (fund_code)
        )
    ''')

    # 基金相关性表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_correlation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code_1 TEXT NOT NULL,
            fund_code_2 TEXT NOT NULL,
            correlation REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code_1, fund_code_2),
            FOREIGN KEY (fund_code_1) REFERENCES funds (fund_code),
            FOREIGN KEY (fund_code_2) REFERENCES funds (fund_code)
        )
    ''')

    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fund_code ON transactions(fund_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(transaction_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_batch ON transactions(batch_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_holdings_updated ON holdings(updated_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_industry_allocation_fund ON fund_industry_allocation(fund_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_correlation_1 ON fund_correlation(fund_code_1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_correlation_2 ON fund_correlation(fund_code_2)')

    conn.commit()
    conn.close()
    print("数据库初始化完成")


def add_fund(fund_code: str, fund_name: str, fund_type: str = "") -> bool:
    """添加基金信息"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO funds (fund_code, fund_name, fund_type, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (fund_code, fund_name, fund_type, datetime.now()))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"添加基金失败: {e}")
        return False


def get_fund(fund_code: str) -> Optional[Dict]:
    """获取基金信息"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM funds WHERE fund_code = ?', (fund_code,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_funds() -> List[Dict]:
    """获取所有基金"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM funds ORDER BY created_at DESC')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_transaction(fund_code: str, transaction_type: str, amount: float,
                    units: float = None, nav: float = None, fee: float = 0,
                    transaction_date: str = None, note: str = "",
                    batch_id: str = None, sell_from_batches: List[int] = None) -> bool:
    """
    添加交易记录（支持库存追踪）

    参数:
        fund_code: 基金代码
        transaction_type: 交易类型（买入/卖出）
        amount: 交易金额
        units: 交易份额
        nav: 单位净值
        fee: 手续费
        transaction_date: 交易日期
        note: 备注
        batch_id: 批次ID（买入时自动生成，卖出时可选）
        sell_from_batches: 卖出时指定的批次ID列表
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if transaction_date is None:
            transaction_date = datetime.now()

        if transaction_type == "买入":
            # 生成批次ID
            if batch_id is None:
                batch_id = f"{fund_code}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

            # 买入时，剩余份额 = 交易份额
            cursor.execute('''
                INSERT INTO transactions
                (fund_code, transaction_type, amount, units, nav, fee, transaction_date, note, batch_id, remaining_units, is_closed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', (fund_code, transaction_type, amount, units, nav, fee, transaction_date, note, batch_id, units))

        elif transaction_type == "卖出":
            # 卖出时，使用先进先出（FIFO）原则，从最早的库存开始扣减
            remaining_units_to_sell = units

            # 获取所有未关闭的买入记录（按交易日期升序排列）
            cursor.execute('''
                SELECT id, remaining_units FROM transactions
                WHERE fund_code = ? AND transaction_type = '买入' AND is_closed = 0
                ORDER BY transaction_date ASC
            ''', (fund_code,))

            batch_records = cursor.fetchall()

            for record in batch_records:
                if remaining_units_to_sell <= 0:
                    break

                record_id = record['id']
                available_units = record['remaining_units']

                if available_units <= 0:
                    continue

                # 计算本次卖出的份额
                units_to_sell = min(available_units, remaining_units_to_sell)
                new_remaining = available_units - units_to_sell

                # 更新该批次的剩余份额
                is_closed = 1 if new_remaining < 0.01 else 0
                cursor.execute('''
                    UPDATE transactions
                    SET remaining_units = ?, is_closed = ?
                    WHERE id = ?
                ''', (new_remaining, is_closed, record_id))

                remaining_units_to_sell -= units_to_sell

            # 插入卖出记录（不需要batch_id）
            cursor.execute('''
                INSERT INTO transactions
                (fund_code, transaction_type, amount, units, nav, fee, transaction_date, note, batch_id, remaining_units, is_closed)
                VALUES (?, ?, ?, ?, ?, ?, ?, '', NULL, 0, 1)
            ''', (fund_code, transaction_type, amount, units, nav, fee, transaction_date))

        # 更新持仓
        _update_holdings(cursor, fund_code)

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"添加交易记录失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_inventory(fund_code: str) -> List[Dict]:
    """
    获取基金当前库存（未卖出的买入记录）

    返回:
        库存列表，包含批次ID、买入日期、剩余份额、持有天数、买入净值等
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                id,
                batch_id,
                transaction_date,
                units,
                remaining_units,
                nav,
                note
            FROM transactions
            WHERE fund_code = ?
              AND transaction_type = '买入'
              AND is_closed = 0
              AND remaining_units > 0
            ORDER BY transaction_date ASC
        ''', (fund_code,))

        rows = cursor.fetchall()

        # 计算持有天数（买入当天算第1天）
        today = datetime.now().date()
        inventory = []

        for row in rows:
            # 统一使用纯日期格式 YYYY-MM-DD
            date_str = str(row['transaction_date'])
            buy_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            hold_days = (today - buy_date).days + 1  # 买入当天算第1天

            inventory.append({
                'id': row['id'],
                'batch_id': row['batch_id'],
                'buy_date': buy_date.strftime('%Y-%m-%d'),
                'units': row['units'],
                'remaining_units': row['remaining_units'],
                'nav': row['nav'],
                'hold_days': hold_days,
                'note': row['note']
            })

        conn.close()
        return inventory
    except Exception as e:
        print(f"获取库存失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def _update_holdings(cursor, fund_code: str):
    """更新持仓计算"""
    # 获取该基金的所有交易
    cursor.execute('''
        SELECT transaction_type, amount, units
        FROM transactions
        WHERE fund_code = ?
    ''', (fund_code,))
    transactions = cursor.fetchall()

    total_units = 0
    total_cost = 0

    for t in transactions:
        if t['transaction_type'] == '买入':
            total_units += t['units']
            total_cost += t['amount']  # 累加买入成本
        elif t['transaction_type'] == '卖出':
            total_units -= t['units']
            total_cost -= t['amount']  # 扣除卖出金额

    # 更新或插入持仓
    cursor.execute('''
        INSERT OR REPLACE INTO holdings
        (fund_code, total_units, total_cost, updated_at)
        VALUES (?, ?, ?, ?)
    ''', (fund_code, total_units, total_cost, datetime.now()))


def get_transactions(fund_code: str = None, limit: int = 100) -> List[Dict]:
    """
    获取交易记录（带动态计算字段）

    返回字段：
    - transaction_date: 交易日期
    - fund_name: 基金名称
    - fund_code: 基金代码
    - transaction_type: 交易类型（买入/卖出）
    - units: 交易份额
    - nav: 单位净值
    - amount: 交易金额
    - cumulative_amount: 累计投入金额（到该笔为止）
    - cumulative_units: 累计持有份额（到该笔为止）
    - cost_nav: 成本净值（累计投入/累计份额）
    - hold_days: 持有天数（仅买入记录）
    - note: 备注
    """
    conn = get_connection()
    cursor = conn.cursor()

    if fund_code:
        cursor.execute('''
            SELECT t.*, f.fund_name
            FROM transactions t
            JOIN funds f ON t.fund_code = f.fund_code
            WHERE t.fund_code = ?
            ORDER BY t.transaction_date ASC
        ''', (fund_code,))
    else:
        cursor.execute('''
            SELECT t.*, f.fund_name
            FROM transactions t
            JOIN funds f ON t.fund_code = f.fund_code
            ORDER BY t.transaction_date ASC
        ''', ())

    rows = cursor.fetchall()
    conn.close()

    # 动态计算累计值
    today = datetime.now().date()
    result = []

    # 按基金分组计算
    funds_data = {}
    for row in rows:
        row_dict = dict(row)
        fc = row_dict['fund_code']

        if fc not in funds_data:
            funds_data[fc] = {
                'cumulative_amount': 0,
                'cumulative_units': 0
            }

        # 计算累计值
        if row_dict['transaction_type'] == '买入':
            funds_data[fc]['cumulative_amount'] += row_dict['amount']
            funds_data[fc]['cumulative_units'] += row_dict['units']
        elif row_dict['transaction_type'] == '卖出':
            funds_data[fc]['cumulative_units'] -= row_dict['units']

        # 计算成本净值
        if funds_data[fc]['cumulative_units'] > 0:
            cost_nav = funds_data[fc]['cumulative_amount'] / funds_data[fc]['cumulative_units']
        else:
            cost_nav = row_dict['nav']

        # 计算持有天数（仅买入记录且有剩余份额）
        hold_days = None
        if row_dict['transaction_type'] == '买入' and row_dict.get('remaining_units', 0) > 0:
            try:
                # 统一使用纯日期格式 YYYY-MM-DD
                date_str = str(row_dict['transaction_date'])
                buy_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                hold_days = (today - buy_date).days + 1  # 买入当天算第1天
            except:
                pass

        row_dict['cumulative_amount'] = funds_data[fc]['cumulative_amount']
        row_dict['cumulative_units'] = funds_data[fc]['cumulative_units']
        row_dict['cost_nav'] = round(cost_nav, 4)
        row_dict['hold_days'] = hold_days

        result.append(row_dict)

    # 反转顺序（最新的在前）
    result.reverse()

    return result[:limit]


def get_holdings() -> List[Dict]:
    """获取当前持仓"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT h.*, f.fund_name
        FROM holdings h
        JOIN funds f ON h.fund_code = f.fund_code
        WHERE h.total_units > 0
        ORDER BY h.total_cost DESC
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_holding_nav(fund_code: str, current_nav: float) -> bool:
    """更新持仓的当前净值"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE holdings
            SET current_value = total_units * ?,
                profit_loss = total_units * ? - total_cost,
                profit_loss_ratio = CASE WHEN total_cost > 0
                    THEN (total_units * ? - total_cost) / total_cost * 100
                    ELSE 0 END,
                updated_at = ?
            WHERE fund_code = ?
        ''', (current_nav, current_nav, current_nav, datetime.now(), fund_code))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"更新净值失败: {e}")
        return False


def delete_transaction(transaction_id: int) -> bool:
    """删除交易记录"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT fund_code FROM transactions WHERE id = ?', (transaction_id,))
        fund_code = cursor.fetchone()[0]

        cursor.execute('DELETE FROM transactions WHERE id = ?', (transaction_id,))
        _update_holdings(cursor, fund_code)

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"删除交易记录失败: {e}")
        return False


def get_summary() -> Dict:
    """获取总体统计"""
    conn = get_connection()
    cursor = conn.cursor()

    # 总投入
    cursor.execute('SELECT SUM(total_cost) FROM holdings WHERE total_units > 0')
    total_cost = cursor.fetchone()[0] or 0

    # 当前市值
    cursor.execute('SELECT SUM(current_value) FROM holdings WHERE total_units > 0')
    current_value = cursor.fetchone()[0] or 0

    # 总盈亏
    profit_loss = current_value - total_cost
    profit_loss_ratio = (profit_loss / total_cost * 100) if total_cost > 0 else 0

    conn.close()

    return {
        'total_cost': total_cost,
        'current_value': current_value,
        'profit_loss': profit_loss,
        'profit_loss_ratio': profit_loss_ratio,
        'fund_count': len(get_holdings())
    }


def update_fund_type_detail(fund_code: str, fund_type_detail: str) -> bool:
    """更新基金的详细类型"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds SET fund_type_detail = ? WHERE fund_code = ?
        ''', (fund_type_detail, fund_code))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"更新基金类型失败: {e}")
        return False


def get_asset_allocation() -> Dict[str, Dict]:
    """
    获取资产配置统计
    返回: {'category': {'市值': float, '占比': float, '基金数': int, '基金列表': list}}
    """
    holdings = get_holdings()
    allocation = {}

    for holding in holdings:
        fund_code = holding['fund_code']
        fund = get_fund(fund_code)

        if not fund:
            continue

        # 从基金名称推断类型
        fund_name = fund.get('fund_name', '')
        if '股票' in fund_name or '指数' in fund_name:
            category = '股票型'
        elif '债券' in fund_name:
            category = '债券型'
        elif '混合' in fund_name:
            category = '混合型'
        elif 'QDII' in fund_name or '海外' in fund_name or '恒生' in fund_name:
            category = 'QDII'
        elif '货币' in fund_name:
            category = '货币型'
        else:
            category = '其他'

        if category not in allocation:
            allocation[category] = {'市值': 0.0, '基金数': 0, '基金列表': []}

        allocation[category]['市值'] += holding['current_value']
        allocation[category]['基金数'] += 1

        # 添加基金到列表
        fund_label = f"{fund_name}({fund_code})"
        if fund_label not in allocation[category]['基金列表']:
            allocation[category]['基金列表'].append(fund_label)

    # 计算总市值和占比
    total_value = sum(item['市值'] for item in allocation.values())

    for category in allocation:
        allocation[category]['占比'] = (allocation[category]['市值'] / total_value * 100) if total_value > 0 else 0
        allocation[category]['市值'] = allocation[category]['市值']

    return allocation


def save_industry_allocation(fund_code: str, industry_data: Dict[str, float]) -> bool:
    """
    保存基金行业配置数据
    industry_data: {'行业名': 占比, ...}
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 删除旧数据
        cursor.execute('DELETE FROM fund_industry_allocation WHERE fund_code = ?', (fund_code,))

        # 插入新数据
        for industry, ratio in industry_data.items():
            cursor.execute('''
                INSERT INTO fund_industry_allocation (fund_code, industry_name, allocation_ratio)
                VALUES (?, ?, ?)
            ''', (fund_code, industry, ratio))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"保存行业配置失败: {e}")
        return False


def get_industry_exposure() -> Dict[str, Dict]:
    """
    获取行业暴露度分析（优化版 - 单次查询）
    优先使用数据库缓存，如果没有则自动从 API 获取
    返回: {'行业名': {'市值': float, '占比': float, '基金数': int, '基金列表': list}}
    """
    from fund_api import get_fund_industry_allocation

    conn = get_connection()
    cursor = conn.cursor()

    # 获取所有持仓和基金名称
    cursor.execute('''
        SELECT h.fund_code, h.current_value, f.fund_name
        FROM holdings h
        JOIN funds f ON h.fund_code = f.fund_code
        WHERE h.total_units > 0
    ''')
    holdings_data = cursor.fetchall()

    # 一次性获取所有基金的行业配置（优化：单次查询）
    if holdings_data:
        fund_codes = [h['fund_code'] for h in holdings_data]
        placeholders = ','.join(['?' for _ in fund_codes])

        cursor.execute(f'''
            SELECT fund_code, industry_name, allocation_ratio
            FROM fund_industry_allocation
            WHERE fund_code IN ({placeholders})
        ''', fund_codes)

        # 构建基金行业配置字典
        fund_industry_map = {}
        for row in cursor.fetchall():
            fc = row['fund_code']
            if fc not in fund_industry_map:
                fund_industry_map[fc] = []
            fund_industry_map[fc].append({
                'industry': row['industry_name'],
                'ratio': row['allocation_ratio']
            })

    conn.close()

    industry_exposure = {}
    fund_codes_with_data = set()

    # 从数据库已有的行业配置计算暴露度
    for holding in holdings_data:
        fund_code = holding['fund_code']
        fund_value = holding['current_value']
        fund_name = holding['fund_name']

        if fund_code in fund_industry_map:
            for item in fund_industry_map[fund_code]:
                industry = item['industry']
                ratio = item['ratio']
                exposure = fund_value * ratio / 100

                if industry not in industry_exposure:
                    industry_exposure[industry] = {'市值': 0.0, '基金数': 0, '基金列表': []}

                industry_exposure[industry]['市值'] += exposure
                industry_exposure[industry]['基金数'] += 1

                # 添加基金到列表（避免重复）
                fund_info = f"{fund_name}({fund_code})"
                if fund_info not in industry_exposure[industry]['基金列表']:
                    industry_exposure[industry]['基金列表'].append(fund_info)

            fund_codes_with_data.add(fund_code)

    # 如果有基金没有行业配置数据，尝试从 API 获取
    holdings_list = [h for h in holdings_data if h['fund_code'] not in fund_codes_with_data]

    if holdings_list:
        for holding in holdings_list:
            fund_code = holding['fund_code']
            fund_value = holding['current_value']
            fund_name = holding['fund_name']

            try:
                # 从 API 获取行业配置
                industry_alloc = get_fund_industry_allocation(fund_code)

                if industry_alloc:
                    # 保存到数据库
                    save_industry_allocation(fund_code, industry_alloc)

                    # 计算暴露度
                    for industry, ratio in industry_alloc.items():
                        exposure = fund_value * ratio

                        if industry not in industry_exposure:
                            industry_exposure[industry] = {'市值': 0.0, '基金数': 0, '基金列表': []}

                        industry_exposure[industry]['市值'] += exposure
                        industry_exposure[industry]['基金数'] += 1

                        # 添加基金到列表
                        fund_info = f"{fund_name}({fund_code})"
                        if fund_info not in industry_exposure[industry]['基金列表']:
                            industry_exposure[industry]['基金列表'].append(fund_info)
            except Exception as e:
                print(f"获取 {fund_code} 行业配置失败: {e}")

    # 计算占比
    total_exposure = sum(item['市值'] for item in industry_exposure.values())

    for industry in industry_exposure:
        industry_exposure[industry]['占比'] = (industry_exposure[industry]['市值'] / total_exposure * 100) if total_exposure > 0 else 0

    return industry_exposure


def save_fund_correlation(fund_code_1: str, fund_code_2: str, correlation: float) -> bool:
    """保存基金相关性数据"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 确保 fund_code_1 < fund_code_2
        if fund_code_1 > fund_code_2:
            fund_code_1, fund_code_2 = fund_code_2, fund_code_1

        cursor.execute('''
            INSERT OR REPLACE INTO fund_correlation (fund_code_1, fund_code_2, correlation, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (fund_code_1, fund_code_2, correlation, datetime.now()))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"保存相关性失败: {e}")
        return False


def get_high_correlation_funds(threshold: float = 0.9) -> List[Dict]:
    """
    获取高相关性基金对
    threshold: 相关性阈值，默认0.9
    返回: [{'fund_code_1': str, 'fund_code_2': str, 'correlation': float}]
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT fund_code_1, fund_code_2, correlation
        FROM fund_correlation
        WHERE correlation >= ?
        ORDER BY correlation DESC
    ''', (threshold,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


if __name__ == "__main__":
    # 测试代码
    init_db()
