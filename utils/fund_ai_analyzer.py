"""
基金AI分析模块（集成OpenAI GPT）
提供基金综合分析、风险评估、投资建议等功能
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd


class FundAIAnalyzer:
    """基金AI分析器（支持OpenAI GPT和规则分析）"""

    def __init__(self, fund_code: str, use_llm: bool = True):
        self.fund_code = fund_code
        self.use_llm = use_llm
        self.analysis_result = {}

    def analyze(self, nav_history_df: pd.DataFrame, performance: Dict, fund_info: Dict) -> Dict:
        """
        综合分析基金

        Args:
            nav_history_df: 历史净值数据
            performance: 业绩数据
            fund_info: 基金基本信息

        Returns:
            分析结果字典
        """
        # 如果启用LLM且可用，使用AI分析
        if self.use_llm:
            ai_result = self._analyze_with_llm(nav_history_df, performance, fund_info)
            if ai_result:
                return ai_result

        # 否则使用规则分析
        return self._analyze_with_rules(nav_history_df, performance, fund_info)

    def _analyze_with_llm(self, nav_history_df: pd.DataFrame, performance: Dict, fund_info: Dict) -> Optional[Dict]:
        """使用大模型进行分析"""
        try:
            from utils.openai_client import get_openai_client, get_model_name

            client = get_openai_client()
            if not client.is_available():
                return None

            model_name = get_model_name()

            # 构建净值数据摘要
            nav_summary = []
            if nav_history_df is not None and len(nav_history_df) > 0:
                recent_nav = nav_history_df.tail(30)  # 最近30条
                for _, row in recent_nav.iterrows():
                    nav_summary.append({
                        'date': str(row['date']),
                        'nav': float(row['nav'])
                    })

            # 调用OpenAI API
            ai_result = client.analyze_fund(fund_info, performance, nav_summary)

            if ai_result and 'error' not in ai_result:
                # 添加基本信息
                ai_result['fund_code'] = self.fund_code
                ai_result['fund_name'] = fund_info.get('fund_name', '')
                ai_result['analysis_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ai_result['analysis_method'] = model_name  # 使用实际模型名称
                return ai_result

        except Exception as e:
            print(f"大模型分析失败: {e}")

        return None

    def _analyze_with_rules(self, nav_history_df: pd.DataFrame, performance: Dict, fund_info: Dict) -> Dict:
        """使用规则算法进行分析（备用方案）"""
        result = {
            'fund_code': self.fund_code,
            'fund_name': fund_info.get('fund_name', ''),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'analysis_method': '规则算法',
            'overall_score': 0,
            'score_breakdown': {},
            'risk_level': '',
            'return_ability': {},
            'risk_metrics': {},
            'stability': {},
            'suggestions': [],
            'highlights': []
        }

        # 1. 收益能力分析
        return_analysis = self._analyze_return_ability(performance)
        result['return_ability'] = return_analysis

        # 2. 风险评估
        risk_analysis = self._analyze_risk(performance, nav_history_df)
        result['risk_metrics'] = risk_analysis

        # 3. 稳定性分析
        stability_analysis = self._analyze_stability(nav_history_df, performance)
        result['stability'] = stability_analysis

        # 4. 计算综合评分
        scores = {
            'return_score': return_analysis.get('score', 50),
            'risk_score': risk_analysis.get('score', 50),
            'stability_score': stability_analysis.get('score', 50)
        }
        result['score_breakdown'] = scores

        # 综合评分（加权平均）
        result['overall_score'] = int(
            scores['return_score'] * 0.4 +
            scores['risk_score'] * 0.3 +
            scores['stability_score'] * 0.3
        )

        # 5. 确定风险等级
        result['risk_level'] = self._determine_risk_level(risk_analysis)

        # 6. 生成投资建议
        result['suggestions'] = self._generate_suggestions(result)

        # 7. 生成亮点
        result['highlights'] = self._generate_highlights(result)

        return result

    def _analyze_return_ability(self, performance: Dict) -> Dict:
        """分析收益能力"""
        score = 50
        analysis = {
            'score': score,
            'rating': '中性',
            'details': []
        }

        # 近1年收益率
        one_year_return = performance.get('近1年', 0)
        if one_year_return > 20:
            analysis['score'] += 20
            analysis['details'].append(f"近1年收益率{one_year_return:.2f}%，表现优秀")
        elif one_year_return > 10:
            analysis['score'] += 10
            analysis['details'].append(f"近1年收益率{one_year_return:.2f}%，表现良好")
        elif one_year_return > 0:
            analysis['score'] += 5
            analysis['details'].append(f"近1年收益率{one_year_return:.2f}%，表现一般")
        else:
            analysis['score'] -= 10
            analysis['details'].append(f"近1年收益率{one_year_return:.2f}%，表现较弱")

        # 今年以来收益
        ytd_return = performance.get('今年以来', 0)
        if ytd_return > 0:
            analysis['score'] += 5
            analysis['details'].append(f"今年以来收益{ytd_return:.2f}%，跑赢通胀")

        # 与同类比较（简化：以沪深300为基准）
        benchmark_return = 10  # 假设基准收益率
        if one_year_return > benchmark_return:
            analysis['score'] += 10
            analysis['details'].append(f"近1年跑赢基准约{one_year_return - benchmark_return:.2f}%")

        # 评级
        if analysis['score'] >= 80:
            analysis['rating'] = '优秀'
        elif analysis['score'] >= 70:
            analysis['rating'] = '良好'
        elif analysis['score'] >= 60:
            analysis['rating'] = '中等'
        elif analysis['score'] >= 50:
            analysis['rating'] = '一般'
        else:
            analysis['rating'] = '较差'

        return analysis

    def _analyze_risk(self, performance: Dict, nav_history_df: pd.DataFrame) -> Dict:
        """分析风险水平"""
        score = 50
        analysis = {
            'score': score,
            'volatility': '中等',
            'max_drawdown': 0,
            'risk_description': []
        }

        # 最大回撤
        max_dd = performance.get('最大回撤', 0)
        analysis['max_drawdown'] = max_dd

        if max_dd < -5:
            analysis['score'] += 20
            analysis['risk_description'].append(f"最大回撤仅{max_dd:.2f}%，风险控制优秀")
        elif max_dd < -10:
            analysis['score'] += 10
            analysis['risk_description'].append(f"最大回撤{max_dd:.2f}%，风险控制良好")
        elif max_dd < -20:
            analysis['score'] -= 5
            analysis['risk_description'].append(f"最大回撤{max_dd:.2f}%，波动较大")
        else:
            analysis['score'] -= 15
            analysis['risk_description'].append(f"最大回撤{max_dd:.2f}%，风险较高")

        # 年化波动率
        volatility = performance.get('年化波动率', 0)
        if volatility < 15:
            analysis['score'] += 15
            analysis['volatility'] = '低'
            analysis['risk_description'].append(f"年化波动率{volatility:.2f}%，波动较低")
        elif volatility < 25:
            analysis['score'] += 5
            analysis['volatility'] = '中等'
            analysis['risk_description'].append(f"年化波动率{volatility:.2f}%，波动适中")
        else:
            analysis['score'] -= 10
            analysis['volatility'] = '高'
            analysis['risk_description'].append(f"年化波动率{volatility:.2f}%，波动较大")

        # 夏普比率（简化计算）
        if volatility > 0:
            one_year_return = performance.get('近1年', 0)
            sharpe = (one_year_return - 3) / volatility  # 假设无风险利率3%
            analysis['sharpe_ratio'] = round(sharpe, 2)

            if sharpe > 1.5:
                analysis['score'] += 15
                analysis['risk_description'].append(f"夏普比率{sharpe:.2f}，风险调整后收益优秀")
            elif sharpe > 1:
                analysis['score'] += 10
                analysis['risk_description'].append(f"夏普比率{sharpe:.2f}，风险调整后收益良好")
            elif sharpe > 0.5:
                analysis['risk_description'].append(f"夏普比率{sharpe:.2f}，风险调整后收益一般")
            else:
                analysis['score'] -= 5
                analysis['risk_description'].append(f"夏普比率{sharpe:.2f}，风险调整后收益偏低")

        return analysis

    def _analyze_stability(self, nav_history_df: pd.DataFrame, performance: Dict) -> Dict:
        """分析稳定性"""
        score = 50
        analysis = {
            'score': score,
            'rating': '中等',
            'details': []
        }

        if nav_history_df is None or len(nav_history_df) < 10:
            analysis['details'].append("数据不足，无法评估稳定性")
            return analysis

        # 计算净值连续上涨/下跌天数
        nav_values = nav_history_df['nav'].values

        # 计算日收益率标准差
        if len(nav_values) > 1:
            daily_returns = pd.Series(nav_values).pct_change().dropna()
            return_std = daily_returns.std()

            if return_std < 0.01:
                analysis['score'] += 20
                analysis['details'].append("日收益率波动较小，净值走势平稳")
            elif return_std < 0.02:
                analysis['score'] += 10
                analysis['details'].append("日收益率波动适中")
            else:
                analysis['score'] -= 10
                analysis['details'].append("日收益率波动较大")

        # 分析各阶段收益一致性
        returns = []
        for period in ['近1月', '近3月', '近6月', '近1年']:
            if period in performance:
                returns.append(performance[period])

        if len(returns) >= 3:
            # 检查收益是否为正（一致性）
            positive_count = sum(1 for r in returns if r > 0)
            if positive_count == len(returns):
                analysis['score'] += 20
                analysis['details'].append("各阶段收益均为正，表现稳定")
            elif positive_count >= len(returns) * 0.7:
                analysis['score'] += 10
                analysis['details'].append("多数阶段收益为正，稳定性较好")

        # 检查最大回撤恢复时间
        max_dd = performance.get('最大回撤', 0)
        if max_dd > -20:  # 回撤较小
            analysis['score'] += 10
            analysis['details'].append("历史回撤控制较好，恢复能力较强")

        # 评级
        if analysis['score'] >= 80:
            analysis['rating'] = '优秀'
        elif analysis['score'] >= 70:
            analysis['rating'] = '良好'
        elif analysis['score'] >= 60:
            analysis['rating'] = '中等'
        else:
            analysis['rating'] = '一般'

        return analysis

    def _determine_risk_level(self, risk_analysis: Dict) -> str:
        """确定风险等级"""
        score = risk_analysis.get('score', 50)
        volatility = risk_analysis.get('volatility', '中等')
        max_dd = risk_analysis.get('max_drawdown', 0)

        if score >= 75 and volatility == '低':
            return '低风险'
        elif score >= 60:
            return '中低风险'
        elif score >= 45:
            return '中等风险'
        elif score >= 30:
            return '中高风险'
        else:
            return '高风险'

    def _generate_suggestions(self, result: Dict) -> List[str]:
        """生成投资建议"""
        suggestions = []

        overall_score = result.get('overall_score', 50)
        return_ability = result.get('return_ability', {})
        risk_metrics = result.get('risk_metrics', {})

        # 根据综合评分给出建议
        if overall_score >= 80:
            suggestions.append("🟢 **推荐持有**：基金综合表现优秀，建议继续持有或适当加仓")
        elif overall_score >= 70:
            suggestions.append("🟡 **可继续持有**：基金表现良好，适合稳健型投资者持有")
        elif overall_score >= 60:
            suggestions.append("🟠 **谨慎持有**：基金表现一般，建议关注后续表现再决定")
        else:
            suggestions.append("🔴 **建议观望**：基金表现较弱，建议考虑减仓或转换")

        # 根据收益能力给出建议
        return_score = return_ability.get('score', 50)
        if return_score < 50:
            suggestions.append("⚠️ **收益预警**：近期收益表现不佳，建议关注基金经理操作和市场环境")

        # 根据风险给出建议
        max_dd = risk_metrics.get('max_drawdown', 0)
        if max_dd < -30:
            suggestions.append("⚠️ **风险提示**：历史最大回撤较大，注意仓位控制")

        # 根据波动率给出建议
        volatility = risk_metrics.get('volatility', '中等')
        if volatility == '高':
            suggestions.append("⚠️ **波动提醒**：基金波动较大，适合风险承受能力较强的投资者")

        return suggestions

    def _generate_highlights(self, result: Dict) -> List[str]:
        """生成亮点"""
        highlights = []

        return_ability = result.get('return_ability', {})
        risk_metrics = result.get('risk_metrics', {})
        stability = result.get('stability', {})

        # 收益亮点
        for detail in return_ability.get('details', []):
            if '优秀' in detail or '跑赢' in detail:
                highlights.append(f"📈 {detail}")

        # 风险控制亮点
        for desc in risk_metrics.get('risk_description', []):
            if '优秀' in desc or '良好' in desc:
                highlights.append(f"🛡️ {desc}")

        # 稳定性亮点
        for detail in stability.get('details', []):
            if '稳定' in detail or '平稳' in detail:
                highlights.append(f"💎 {detail}")

        return highlights


def analyze_fund_industry_allocation(fund_code: str) -> Dict:
    """
    分析基金行业配置

    Args:
        fund_code: 基金代码

    Returns:
        行业配置分析结果
    """
    from fund_api import get_fund_industry_allocation
    from db import save_industry_allocation

    try:
        # 获取行业配置
        industry_alloc = get_fund_industry_allocation(fund_code)

        if not industry_alloc:
            return {'error': '无法获取行业配置数据'}

        # 保存到数据库
        save_industry_allocation(fund_code, industry_alloc)

        # 分析行业配置
        analysis = {
            'fund_code': fund_code,
            'top_industries': [],
            'concentration': '',
            'diversification': []
        }

        # 按占比排序
        sorted_industries = sorted(industry_alloc.items(), key=lambda x: x[1], reverse=True)

        # 前三大行业
        for industry, ratio in sorted_industries[:3]:
            analysis['top_industries'].append({
                'name': industry,
                'ratio': ratio
            })

        # 集中度分析
        top3_ratio = sum(ratio for _, ratio in sorted_industries[:3])
        if top3_ratio > 60:
            analysis['concentration'] = '高集中度'
            analysis['diversification'].append(f"前三大行业占比{top3_ratio:.1f}%，行业配置较集中")
        elif top3_ratio > 40:
            analysis['concentration'] = '中等集中度'
            analysis['diversification'].append(f"前三大行业占比{top3_ratio:.1f}%，行业配置适中")
        else:
            analysis['concentration'] = '低集中度'
            analysis['diversification'].append(f"前三大行业占比{top3_ratio:.1f}%，行业配置分散")

        return analysis

    except Exception as e:
        return {'error': f'行业配置分析失败: {str(e)}'}


def analyze_fund_portfolio(fund_code: str) -> Dict:
    """
    分析基金持仓情况

    Args:
        fund_code: 基金代码

    Returns:
        持仓分析结果
    """
    from fund_api import get_fund_portfolio_holdings

    try:
        holdings = get_fund_portfolio_holdings(fund_code)

        if not holdings or holdings['data'] is None:
            return {'error': '无法获取持仓数据'}

        df = holdings['data']
        analysis = {
            'fund_code': fund_code,
            'report_date': holdings.get('report_date', '未知'),
            'top_holdings': [],
            'holding_style': '',
            'stock_concentration': ''
        }

        # 清理列名
        df.columns = df.columns.str.strip()

        # 查找相关列
        code_col = None
        name_col = None
        ratio_col = None

        for col in df.columns:
            if '代码' in col or 'code' in col.lower():
                code_col = col
            elif '名称' in col or 'name' in col.lower():
                name_col = col
            elif '占净值比' in col or '比例' in col or 'ratio' in col.lower():
                ratio_col = col

        if ratio_col and code_col:
            # 提取前十大持仓
            top_10 = df.head(10)

            total_ratio = 0
            for _, row in top_10.iterrows():
                try:
                    code = str(row[code_col]).strip()
                    name = str(row[name_col]) if name_col else code
                    ratio = float(row[ratio_col]) if ratio_col else 0

                    if code and code != 'nan':
                        analysis['top_holdings'].append({
                            'code': code,
                            'name': name,
                            'ratio': ratio
                        })
                        total_ratio += ratio
                except:
                    continue

            # 股票集中度
            if total_ratio > 70:
                analysis['stock_concentration'] = '高集中度'
            elif total_ratio > 50:
                analysis['stock_concentration'] = '中等集中度'
            else:
                analysis['stock_concentration'] = '低集中度'

        return analysis

    except Exception as e:
        return {'error': f'持仓分析失败: {str(e)}'}


def generate_ai_report(fund_code: str, nav_history_df: pd.DataFrame,
                       performance: Dict, fund_info: Dict) -> str:
    """
    生成AI分析报告

    Args:
        fund_code: 基金代码
        nav_history_df: 历史净值数据
        performance: 业绩数据
        fund_info: 基金基本信息

    Returns:
        分析报告文本
    """
    analyzer = FundAIAnalyzer(fund_code, use_llm=True)
    result = analyzer.analyze(nav_history_df, performance, fund_info)

    # 生成报告
    report_lines = []

    method = result.get('analysis_method', '规则算法')
    report_lines.append(f"# {result['fund_name']}({fund_code}) AI分析报告")
    report_lines.append(f"\n**分析时间**: {result['analysis_date']}")
    report_lines.append(f"**分析方法**: {method}\n")

    # 综合评分
    score = result['overall_score']
    report_lines.append(f"## 📊 综合评分: {score}/100")

    if score >= 80:
        report_lines.append("🌟🌟🌟🌟🌟 **表现优秀**")
    elif score >= 70:
        report_lines.append("🌟🌟🌟🌟 **表现良好**")
    elif score >= 60:
        report_lines.append("🌟🌟🌟 **表现中等**")
    elif score >= 50:
        report_lines.append("🌟🌟 **表现一般**")
    else:
        report_lines.append("🌟 **表现较弱**")

    # 评分明细
    report_lines.append("\n### 评分明细")
    scores = result.get('score_breakdown', {})
    if scores:
        report_lines.append(f"- 收益能力: {scores.get('return_score', 50)}/100")
        report_lines.append(f"- 风险控制: {scores.get('risk_score', 50)}/100")
        report_lines.append(f"- 稳定性: {scores.get('stability_score', 50)}/100")

    # 投资亮点
    if result.get('highlights'):
        report_lines.append("\n### ✨ 投资亮点")
        for highlight in result['highlights']:
            report_lines.append(f"- {highlight}")

    # 投资建议
    if result.get('suggestions'):
        report_lines.append("\n### 💡 投资建议")
        for suggestion in result['suggestions']:
            report_lines.append(f"- {suggestion}")

    # 风险提示
    report_lines.append(f"\n### ⚠️ 风险提示")
    report_lines.append(f"- 风险等级: **{result['risk_level']}**")

    risk_metrics = result.get('risk_metrics', {})
    for detail in risk_metrics.get('risk_description', [])[:3]:
        report_lines.append(f"- {detail}")

    return "\n".join(report_lines)


def get_personalized_advice(fund_code: str, fund_name: str, user_holdings: Dict, performance: Dict) -> Optional[str]:
    """
    获取个性化投资建议（使用OpenAI）

    Args:
        fund_code: 基金代码
        fund_name: 基金名称
        user_holdings: 用户持仓信息
        performance: 基金业绩

    Returns:
        投资建议文本或None
    """
    try:
        from utils.openai_client import get_openai_client

        client = get_openai_client()
        if not client.is_available():
            return None

        return client.generate_investment_advice(fund_code, fund_name, user_holdings, performance)
    except Exception as e:
        print(f"获取个性化建议失败: {e}")
        return None
