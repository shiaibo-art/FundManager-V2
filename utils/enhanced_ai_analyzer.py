"""
增强型基金 AI 分析模块
参考 GoFundBot 项目实现，结合市场数据、基金业绩、持仓结构进行综合评估
"""
import os
import re
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class MarketContext:
    """市场上下文数据"""
    indices: List[Dict] = field(default_factory=list)
    north_flow: Dict = field(default_factory=dict)
    main_flow: Dict = field(default_factory=dict)
    breadth: Dict = field(default_factory=dict)
    hot_sectors: List[Dict] = field(default_factory=list)
    market_sentiment: str = "中性"
    market_score: int = 50


class EnhancedFundAIAnalyzer:
    """增强型基金 AI 分析器"""

    def __init__(self):
        self.api_key = None
        self.base_url = None
        self.model = None
        self._load_config()

    def _load_config(self):
        """加载 OpenAI 配置"""
        config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'openai_config.json')
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.api_key = config.get('api_key')
                    self.base_url = config.get('base_url', 'https://api.openai.com/v1')
                    self.model = config.get('model', 'gpt-4o-mini')
        except Exception as e:
            print(f"加载配置失败: {e}")

    def is_available(self) -> bool:
        """检查是否可用"""
        return bool(self.api_key)

    def _call_llm(self, prompt: str, system_prompt: str = "") -> Optional[str]:
        """调用 LLM"""
        try:
            from openai import OpenAI
            import httpx

            # 创建自定义的 httpx 客户端，设置超时
            timeout = httpx.Timeout(
                connect=30.0,    # 连接超时 30 秒
                read=120.0,      # 读取超时 120 秒
                write=30.0,      # 写入超时 30 秒
                pool=10.0        # 连接池超时 10 秒
            )

            client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                http_client=httpx.Client(timeout=timeout)
            )

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            print(f"[AI分析] 正在调用模型: {self.model}")
            print(f"[AI分析] API地址: {self.base_url}")

            response = client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.3,
                max_tokens=4096
            )

            print(f"[AI分析] 调用成功")
            return response.choices[0].message.content

        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)

            # 详细的错误信息
            if 'Connection' in error_type or 'connection' in error_msg.lower():
                print(f"[AI分析] 连接失败: {e}")
                print(f"[AI分析] 请检查: 1) API地址是否正确 ({self.base_url}) 2) 网络是否正常 3) 服务是否运行中")
            elif 'Timeout' in error_type or 'timeout' in error_msg.lower():
                print(f"[AI分析] 请求超时: {e}")
                print(f"[AI分析] 模型响应时间过长，请稍后重试")
            elif 'Authentication' in error_type or '401' in error_msg:
                print(f"[AI分析] 认证失败: {e}")
                print(f"[AI分析] API Key 可能不正确")
            else:
                print(f"[AI分析] 调用失败 [{error_type}]: {e}")

            return None

    def analyze_fund_comprehensive(
        self,
        fund_info: Dict,
        performance: Dict,
        nav_history: List,
        portfolio: Dict = None,
        market_context: MarketContext = None
    ) -> Dict[str, Any]:
        """
        基金综合分析（结合市场数据）

        Args:
            fund_info: 基金基本信息
            performance: 基金业绩
            nav_history: 净值历史
            portfolio: 持仓信息
            market_context: 市场上下文

        Returns:
            分析结果
        """
        if not self.is_available():
            return self._get_fallback_result()

        try:
            prompt = self._build_comprehensive_prompt(
                fund_info, performance, nav_history, portfolio, market_context
            )

            system_prompt = """你是一位资深基金分析师，擅长基金投资分析和风险评估。

**重要原则**：
1. 基于实际数据进行分析，不要杜撰数值
2. sentiment_score（0-100）必须综合反映基金的投资价值
3. 操作建议必须基于分析结论

**输出格式**：
请严格按照以下 JSON 格式输出：
```json
{
    "sentiment_score": 75,
    "operation_advice": "建议买入/持有观望/建议减仓/强烈推荐/建议卖出",
    "summary": "综合分析总结（200-300字）",
    "dashboard": {
        "performance_eval": "优秀/良好/一般/较差",
        "risk_level": "低/中低/中/中高/高",
        "manager_ability": "优秀/良好/一般/较差",
        "position_style": "集中/均衡/分散",
        "market_adaptability": "强/中/弱"
    },
    "highlights": ["亮点1", "亮点2", "亮点3"],
    "risk_factors": ["风险1", "风险2", "风险3"],
    "market_relevance": ["市场关联分析1", "市场关联分析2"],
    "detailed_report": "Markdown格式的详细报告，包含：业绩归因、风险收益特征、投资风格分析、市场环境适应性、操作建议"
}
```
"""

            result = self._call_llm(prompt, system_prompt)
            if not result:
                return self._get_fallback_result()

            return self._parse_result(result)

        except Exception as e:
            print(f"综合分析失败: {e}")
            return self._get_fallback_result()

    def _build_comprehensive_prompt(
        self,
        fund_info: Dict,
        performance: Dict,
        nav_history: List,
        portfolio: Dict = None,
        market_context: MarketContext = None
    ) -> str:
        """构建综合分析提示"""

        prompt = f"""请对以下基金进行全面分析：

## 基金基本信息
- 基金名称：{fund_info.get('fund_name', '未知')}
- 基金代码：{fund_info.get('fund_code', '未知')}
- 基金类型：{fund_info.get('fund_type', '未知')}
- 成立日期：{fund_info.get('establish_date', '未知')}
- 基金规模：{fund_info.get('fund_size', '未知')}亿元

## 业绩表现
- 近1月收益：{performance.get('近1月', 'N/A')}%
- 近3月收益：{performance.get('近3月', 'N/A')}%
- 近6月收益：{performance.get('近6月', 'N/A')}%
- 近1年收益：{performance.get('近1年', 'N/A')}%
- 近3年收益：{performance.get('近3年', 'N/A')}%
- 今年以来：{performance.get('今年以来', 'N/A')}%
"""

        # 添加净值走势摘要
        if nav_history and len(nav_history) > 0:
            prompt += "\n## 净值走势（最近10个交易日）\n"
            recent = nav_history[-10:] if len(nav_history) >= 10 else nav_history
            for item in recent:
                prompt += f"- {item.get('date', '')}: {item.get('nav', 0):.4f}\n"

        # 添加市场环境分析
        if market_context:
            prompt += f"""
## 市场环境分析
- 市场情绪：{market_context.market_sentiment}
- 市场评分：{market_context.market_score}/100
"""

            if market_context.indices:
                sh_change = next((i.get('change_pct', 0) for i in market_context.indices if '上证' in i.get('name', '')), 0)
                prompt += f"- 上证指数涨跌：{sh_change:.2f}%\n"

            if market_context.north_flow:
                north = market_context.north_flow.get('total', 0)
                prompt += f"- 北向资金：{north:.2f}亿元\n"

            if market_context.hot_sectors:
                prompt += "- 热门板块：\n"
                for sec in market_context.hot_sectors[:3]:
                    prompt += f"  · {sec.get('name', '')}: {sec.get('change_pct', 0):.2f}%\n"

        # 添加持仓分析
        if portfolio:
            stocks = portfolio.get('stocks', [])
            if stocks:
                prompt += "\n## 重仓股票（前5）\n"
                for i, stock in enumerate(stocks[:5], 1):
                    if isinstance(stock, dict):
                        prompt += f"{i}. {stock.get('name', '')} ({stock.get('code', '')}): {stock.get('ratio', 0):.2f}%\n"
                    else:
                        prompt += f"{i}. {stock}\n"

            industries = portfolio.get('industries', [])
            if industries:
                prompt += "\n## 行业配置（前5）\n"
                for i, ind in enumerate(industries[:5], 1):
                    if isinstance(ind, dict):
                        prompt += f"{i}. {ind.get('name', '')}: {ind.get('ratio', 0):.2f}%\n"
                    else:
                        prompt += f"{i}. {ind}\n"

        prompt += """

## 分析要求
请结合以上数据，从以下维度进行分析：
1. **业绩评估**：与同类基金、市场基准相比的表现
2. **风险分析**：波动率、回撤、风险调整后收益
3. **经理能力**：选股能力、择时能力、风格稳定性
4. **持仓分析**：集中度、行业配置、个股选择
5. **市场适应性**：在不同市场环境下的表现
6. **投资建议**：基于当前市场环境给出具体建议
"""

        return prompt

    def _parse_result(self, result: str) -> Dict[str, Any]:
        """解析 LLM 返回结果"""
        try:
            # 提取 JSON
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', result)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = result.strip()
                start = json_str.find('{')
                end = json_str.rfind('}')
                if start != -1 and end != -1:
                    json_str = json_str[start:end+1]

            data = json.loads(json_str)

            # 验证必要字段
            required_fields = ['sentiment_score', 'operation_advice', 'summary',
                             'dashboard', 'highlights', 'risk_factors']
            for field in required_fields:
                if field not in data:
                    data[field] = self._get_default_value(field)

            data['model_used'] = self.model
            data['analysis_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            return data

        except json.JSONDecodeError as e:
            print(f"JSON 解析失败: {e}")
            return self._get_fallback_result()

    def _get_default_value(self, field: str) -> Any:
        """获取字段默认值"""
        defaults = {
            'sentiment_score': 50,
            'operation_advice': '持有观望',
            'summary': '暂无分析',
            'dashboard': {
                "performance_eval": "一般",
                "risk_level": "中",
                "manager_ability": "一般",
                "position_style": "均衡",
                "market_adaptability": "中"
            },
            'highlights': ["数据分析中"],
            'risk_factors': ["请谨慎投资"],
            'market_relevance': [],
            'detailed_report': '暂无详细分析'
        }
        return defaults.get(field, None)

    def _get_fallback_result(self) -> Dict[str, Any]:
        """获取备用结果"""
        return {
            "sentiment_score": 50,
            "operation_advice": "持有观望",
            "summary": "AI 服务不可用，请检查配置",
            "dashboard": {
                "performance_eval": "一般",
                "risk_level": "中",
                "manager_ability": "一般",
                "position_style": "均衡",
                "market_adaptability": "中"
            },
            "highlights": ["AI 服务未配置"],
            "risk_factors": ["请配置 API Key"],
            "market_relevance": [],
            "detailed_report": "请先配置 OpenAI API Key 以启用 AI 分析功能",
            "model_used": "N/A",
            "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def analyze_fund_with_market(
    fund_info: Dict,
    performance: Dict,
    nav_history: List,
    portfolio: Dict = None,
    market_context: MarketContext = None
) -> Dict[str, Any]:
    """
    便捷函数：结合市场数据分析基金

    Args:
        fund_info: 基金基本信息
        performance: 业绩数据
        nav_history: 净值历史
        portfolio: 持仓信息
        market_context: 市场上下文

    Returns:
        分析结果
    """
    analyzer = EnhancedFundAIAnalyzer()
    return analyzer.analyze_fund_comprehensive(
        fund_info, performance, nav_history, portfolio, market_context
    )
