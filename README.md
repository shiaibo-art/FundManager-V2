# 基金管理工具

一个基于 Python + Streamlit 的轻量级个人基金管理工具，支持基金交易记录、持仓管理和净值追踪。

## 功能特性

- **交易录入**: 记录基金的买入和卖出操作
- **资产概览**: 查看总投入、当前市值、盈亏统计
- **持仓明细**: 实时查看各基金的持仓情况和盈亏
- **净值追踪**: 自动获取基金最新净值和历史走势
- **交易历史**: 查看、筛选和导出交易记录
- **基金详情**: 查看单只基金的详细信息和净值走势图

## 技术栈

- **前端框架**: Streamlit (纯Python，无需HTML/CSS/JS)
- **数据库**: SQLite (文件级存储，无需安装数据库服务)
- **数据源**: AkShare (免费开源的财经数据接口)

## 项目结构

```
FundManager/
├── app.py              # Streamlit主应用
├── db.py               # 数据库操作模块
├── fund_api.py         # 基金数据获取模块
├── data/
│   └── fund_manager.db # SQLite数据库文件 (自动生成)
├── .streamlit/
│   └── config.toml     # Streamlit配置
├── requirements.txt    # 依赖包列表
└── README.md          # 使用说明
```

## 安装步骤

### 1. 克隆或下载项目

```bash
cd FundManager
```

### 2. 创建虚拟环境 (推荐)

```bash
python -m venv venv

# Windows激活
venv\Scripts\activate

# Linux/Mac激活
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 启动应用

```bash
streamlit run app.py
```

启动后会自动打开浏览器，访问 `http://localhost:8501`

### 页面导航

1. **资产概览** - 查看总体资产统计和持仓明细
2. **交易录入** - 添加买入/卖出记录
3. **交易历史** - 查看和导出历史交易记录
4. **基金详情** - 查看单只基金的详细信息和净值走势

### 基本使用流程

1. 在 **交易录入** 页面添加新基金（输入基金代码和名称）
2. 录入买入记录（金额、净值、份额会自动计算）
3. 在 **资产概览** 页面点击 "刷新净值" 获取最新数据
4. 在 **基金详情** 页面查看净值走势图

## 数据备份

数据库文件位于 `data/fund_manager.db`，定期备份此文件即可保护数据。

```bash
# 备份数据库
copy data\fund_manager.db backup\fund_manager_backup_%date%.db
```

## 常见问题

**Q: 获取不到基金净值？**
A: AkShare数据源可能暂时不可用，或基金代码不正确。请确保输入的是6位基金代码。

**Q: 数据丢失怎么办？**
A: 定期备份 `data/fund_manager.db` 文件。如需恢复，直接替换该文件即可。

**Q: 如何修改端口？**
A: 运行时指定端口：`streamlit run app.py --server.port 8080`

## 开发说明

### 数据库表结构

- **funds** - 基金信息表
- **transactions** - 交易记录表
- **holdings** - 持仓表（自动计算）

### 添加新功能

1. 在 `db.py` 添加数据库操作函数
2. 在 `fund_api.py` 添加数据获取函数
3. 在 `app.py` 添加新的页面或组件

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
