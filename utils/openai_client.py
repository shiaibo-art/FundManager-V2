"""
OpenAI API客户端
用于基金AI分析
"""
import os
from typing import Optional, Dict, List
from dataclasses import dataclass


@dataclass
class OpenAIConfig:
    """OpenAI配置"""
    api_key: str
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-4o-mini"
    temperature: float = 0.7
    max_tokens: int = 2000


class OpenAIClient:
    """OpenAI API客户端"""

    _instance = None
    _config = None

    @classmethod
    def get_client(cls):
        """获取OpenAI客户端单例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """初始化客户端"""
        self.config = self._load_config()
        self.client = None

        if self.config and self.config.api_key:
            try:
                from openai import OpenAI
                self.client = OpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.base_url
                )
            except ImportError:
                print("未安装openai库，请运行: pip install openai")
                self.client = None
            except Exception as e:
                print(f"OpenAI初始化失败: {e}")
                self.client = None

    def _load_config(self) -> Optional[OpenAIConfig]:
        """加载配置"""
        # 1. 从环境变量读取
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        # 2. 从配置文件读取
        if not api_key:
            config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'openai_config.json')
            try:
                import json
                if os.path.exists(config_file):
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                        api_key = config_data.get('api_key')
                        base_url = config_data.get('base_url', base_url)
                        model = config_data.get('model', model)
            except:
                pass

        if api_key:
            return OpenAIConfig(
                api_key=api_key,
                base_url=base_url,
                model=model
            )
        return None

    def is_available(self) -> bool:
        """检查客户端是否可用"""
        return self.client is not None

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> Optional[str]:
        """
        发送聊天请求

        Args:
            messages: 消息列表 [{"role": "user", "content": "..."}]
            **kwargs: 其他参数（temperature, max_tokens等）

        Returns:
            AI回复内容或None
        """
        if not self.is_available():
            return None

        try:
            response = self.client.chat.completions.create(
                model=kwargs.get('model', self.config.model),
                messages=messages,
                temperature=kwargs.get('temperature', self.config.temperature),
                max_tokens=kwargs.get('max_tokens', self.config.max_tokens)
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"OpenAI API调用失败: {e}")
            return None

    def analyze_fund(self, fund_info: Dict, performance: Dict, nav_data: List[Dict]) -> Optional[Dict]:
        """
        使用AI分析基金

        Args:
            fund_info: 基金基本信息
            performance: 业绩数据
            nav_data: 净值历史数据

        Returns:
            AI分析结果或None
        """
        if not self.is_available():
            return None

        # 构建分析提示
        prompt = self._build_analysis_prompt(fund_info, performance, nav_data)

        messages = [
            {
                "role": "system",
                "content": "你是一位专业的基金分析师，擅长评估基金的投资价值、风险水平和未来前景。请用客观、专业的语言进行分析。"
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        response = self.chat(messages)

        if response:
            return self._parse_analysis_response(response)
        return None

    def _build_analysis_prompt(self, fund_info: Dict, performance: Dict, nav_data: List[Dict]) -> str:
        """构建分析提示"""
        fund_code = fund_info.get('fund_code', '')
        fund_name = fund_info.get('fund_name', '')
        latest_nav = fund_info.get('latest_nav', 0)

        # 构建业绩信息
        performance_text = ""
        for key, value in performance.items():
            if isinstance(value, (int, float)):
                performance_text += f"- {key}: {value:.2f}\n"
            else:
                performance_text += f"- {key}: {value}\n"

        prompt = f"""请分析以下基金：

**基金信息**
- 基金代码: {fund_code}
- 基金名称: {fund_name}
- 最新净值: {latest_nav}

**业绩表现**
{performance_text}

请从以下几个方面进行分析：
1. **综合评分**: 给出0-100分的综合评分，并说明理由
2. **收益能力**: 评估基金的盈利能力
3. **风险水平**: 评估基金的风险等级（低/中低/中/中高/高）
4. **投资亮点**: 列出3-5个亮点
5. **投资建议**: 给出持有/买入/卖出的建议
6. **风险提示**: 指出需要注意的风险

请用JSON格式返回分析结果：
{{
    "overall_score": 分数,
    "return_ability": "收益能力评价",
    "risk_level": "风险等级",
    "volatility": "波动水平",
    "stability": "稳定性评价",
    "highlights": ["亮点1", "亮点2", ...],
    "suggestions": ["建议1", "建议2", ...],
    "risk_warnings": ["风险1", "风险2", ...]
}}
"""
        return prompt

    def _parse_analysis_response(self, response: str) -> Dict:
        """解析AI响应"""
        try:
            import json
            # 尝试直接解析JSON
            response = response.strip()
            if response.startswith('```json'):
                response = response[7:]
            if response.startswith('```'):
                response = response[3:]
            if response.endswith('```'):
                response = response[:-3]
            response = response.strip()

            return json.loads(response)
        except:
            # 解析失败，返回文本格式
            return {
                'error': 'JSON解析失败',
                'raw_response': response
            }

    def generate_investment_advice(self, fund_code: str, fund_name: str,
                                   user_holdings: Dict, performance: Dict) -> Optional[str]:
        """
        生成个性化投资建议

        Args:
            fund_code: 基金代码
            fund_name: 基金名称
            user_holdings: 用户持仓信息
            performance: 基金业绩

        Returns:
            投资建议文本或None
        """
        if not self.is_available():
            return None

        holdings_text = f"""
        - 持有份额: {user_holdings.get('total_units', 0):.2f}
        - 投入成本: {user_holdings.get('total_cost', 0):.2f}
        - 当前市值: {user_holdings.get('current_value', 0):.2f}
        - 盈亏: {user_holdings.get('profit_loss', 0):.2f} ({user_holdings.get('profit_loss_ratio', 0):.2f}%)
        """

        prompt = f"""我持有以下基金：
**基金**: {fund_name} ({fund_code})

**我的持仓**:{holdings_text}

**基金近期表现**:
- 近1月: {performance.get('近1月', 'N/A')}
- 近3月: {performance.get('近3月', 'N/A')}
- 近6月: {performance.get('近6月', 'N/A')}
- 近1年: {performance.get('近1年', 'N/A')}

请根据我的持仓情况和基金表现，给出个性化的投资建议（继续持有/加仓/减仓/清仓），并说明理由。
"""

        messages = [
            {
                "role": "system",
                "content": "你是一位专业的投资顾问，擅长根据客户持仓情况给出个性化建议。请考虑客户的风险承受能力和投资目标。"
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        return self.chat(messages)

    def explain_fund_changes(self, fund_code: str, fund_name: str,
                             recent_performance: Dict) -> Optional[str]:
        """
        解释基金近期变化

        Args:
            fund_code: 基金代码
            fund_name: 基金名称
            recent_performance: 近期业绩数据

        Returns:
            解释文本或None
        """
        if not self.is_available():
            return None

        prompt = f"""请解释基金 {fund_name} ({fund_code}) 近期的表现变化：

**近期数据**:
{str(recent_performance)}

请分析：
1. 为什么会有这样的表现？
2. 可能受哪些因素影响（市场环境、行业轮动、持仓调整等）？
3. 后续走势如何判断？
"""

        messages = [
            {
                "role": "system",
                "content": "你是一位资深的基金经理，善于解读基金业绩变化背后的原因。"
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        return self.chat(messages)


# 便捷函数
def get_openai_client() -> OpenAIClient:
    """获取OpenAI客户端单例"""
    return OpenAIClient.get_client()


def is_openai_available() -> bool:
    """检查OpenAI是否可用"""
    client = get_openai_client()
    return client.is_available()


def get_model_name() -> str:
    """获取配置的模型名称"""
    client = get_openai_client()
    if client.config:
        return client.config.model
    return "未知模型"
