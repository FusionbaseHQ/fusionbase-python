"""Pre-configured tool sets for common agent use cases."""

from typing import Any, List

# Import all available tools
from fusionbase.ai.tools import (
    annual_financial_statements,  # Organization tools; Core relation tools; Convenience relation tools; Web tools
)
from fusionbase.ai.tools import balance_sheet_accounts
from fusionbase.ai.tools import financial_kpi
from fusionbase.ai.tools import google_search
from fusionbase.ai.tools import insolvency_publications
from fusionbase.ai.tools import network
from fusionbase.ai.tools import news
from fusionbase.ai.tools import organization_detail
from fusionbase.ai.tools import organization_search
from fusionbase.ai.tools import person_detail
from fusionbase.ai.tools import person_search
from fusionbase.ai.tools import profit_and_loss_account
from fusionbase.ai.tools import publications
from fusionbase.ai.tools import related_persons
from fusionbase.ai.tools import relation_detail
from fusionbase.ai.tools import relation_resolve
from fusionbase.ai.tools import relation_search
from fusionbase.ai.tools import web_content


# Import output tools from the agent module
# These need to be imported where they're defined to avoid circular imports
def _get_output_tools():
    """Get output tools from the agent module."""
    from fusionbase.ai.agents.company_research import Conclusion
    from fusionbase.ai.agents.company_research import ExtractCompanyName
    from fusionbase.ai.agents.company_research import FinalAnswer
    from fusionbase.ai.agents.company_research import GlobalFinding
    from fusionbase.ai.agents.company_research import Introduction
    from fusionbase.ai.agents.company_research import Plan
    from fusionbase.ai.agents.company_research import Queries
    from fusionbase.ai.agents.company_research import ResearchReflection
    return {
        'GlobalFinding': GlobalFinding,
        'ResearchReflection': ResearchReflection,
        'Queries': Queries,
        'Plan': Plan,
        'ExtractCompanyName': ExtractCompanyName,
        'Introduction': Introduction,
        'Conclusion': Conclusion,
        'FinalAnswer': FinalAnswer
    }


class ToolPresets:
    """Pre-configured tool sets for different research scenarios."""

    @staticmethod
    def minimal_researcher_tools() -> List[Any]:
        """Minimal tool set with only essential Fusionbase tools."""
        return [
            # Essential search
            organization_search,
            organization_detail,
            person_search,
            person_detail,

            # Basic relations
            network,
            related_persons,
        ]

    @staticmethod
    def financial_researcher_tools() -> List[Any]:
        """Tool set focused on financial analysis."""
        return [
            # Organization tools
            organization_search,
            organization_detail,

            # Financial relations
            financial_kpi,
            profit_and_loss_account,
            balance_sheet_accounts,
            annual_financial_statements,

            # Supporting relations
            network,
            publications,
        ]

    @staticmethod
    def news_researcher_tools() -> List[Any]:
        """Tool set focused on news and current events."""
        return [
            # Organization tools
            organization_search,
            organization_detail,

            # News and publications
            news,
            publications,
            insolvency_publications,

            # Web tools for current events
            google_search,
            web_content,
        ]

    @staticmethod
    def comprehensive_researcher_tools() -> List[Any]:
        """Complete tool set with all available tools."""
        return [
            # Organization tools
            organization_search,
            organization_detail,

            # Person tools
            person_search,
            person_detail,

            # All relation tools
            relation_search,
            relation_detail,
            relation_resolve,
            financial_kpi,
            network,
            related_persons,
            profit_and_loss_account,
            publications,
            balance_sheet_accounts,
            insolvency_publications,
            annual_financial_statements,
            news,

            # Web tools
            google_search,
            web_content,
        ]

    @staticmethod
    def web_only_researcher_tools() -> List[Any]:
        """Tool set using only web search, no Fusionbase data."""
        return [
            # Web tools only
            google_search,
            web_content,
        ]

    @staticmethod
    def default_planner_tools() -> List[Any]:
        """Default planner tools."""
        output_tools = _get_output_tools()
        return [
            organization_search,
            google_search,
            output_tools['Plan'],
            output_tools['ExtractCompanyName'],
        ]

    @staticmethod
    def default_synthesizer_tools() -> List[Any]:
        """Default synthesizer tools."""
        output_tools = _get_output_tools()
        return [
            output_tools['Introduction'],
            output_tools['Conclusion'],
            output_tools['FinalAnswer'],
        ]
