"""
Enum for FMP API endpoints.
"""

from enum import Enum
from importlib import import_module
from typing import Callable, Optional


class FMPEndpoint(Enum):
    """
    Enum for FMP API endpoints.
    """

    # Company Info (Aggregate)
    EMPLOYEE_COUNT = (
        "employee_count",
        "comapny_info.empCount_from_fmpAPI",
        "empCount_from_fmpAPI",
        False,
    )
    EXEC_COMP = (
        "executive_compensation",
        "comapny_info.execComp_from_fmpAPI",
        "execComp_from_fmpAPI",
        False,
    )
    GRADE = ("grade", "comapny_info.grade_from_fmpAPI", "grade_from_fmpAPI", False)
    ANALYST_EST = (
        "analyst-estimates",
        "comapny_info.analystEst_from_fmpAPI",
        "analystEst_from_fmpAPI",
        False,
    )
    ANALYST_REC = (
        "analyst-stock-recommendations",
        "comapny_info.analystRec_from_fmpAPI",
        "analystRec_from_fmpAPI",
        False,
    )

    # Company Info (Daily)
    MARKET_CAP = (
        "historical-market-capitalization",
        "comapny_info.marketCap_from_fmpAPI",
        "marketCap_from_fmpAPI",
        True,
    )

    # Financial Statements (Aggregate)
    INCOME_STMT = (
        "income-statement",
        "financial_statements.incomeStmt_from_fmpAPI",
        "incomeStmt_from_fmpAPI",
        False,
    )
    BALANCE_SHEET = (
        "balance-sheet-statement",
        "financial_statements.balanceSheet_from_fmpAPI",
        "balanceSheet_from_fmpAPI",
        False,
    )
    CASH_FLOW = (
        "cash-flow-statement",
        "financial_statements.cashFlow_from_fmpAPI",
        "cashFlow_from_fmpAPI",
        False,
    )

    # Statement Analysis (Aggregate)
    STMT_KM = (
        "key-metrics",
        "statement_analysis.stmtAnalKM_from_fmpAPI",
        "stmtAnalKM_from_fmpAPI",
        False,
    )
    STMT_RATIOS = (
        "ratios",
        "statement_analysis.stmtAnalRatios_from_fmpAPI",
        "stmtAnalRatios_from_fmpAPI",
        False,
    )
    STMT_CFG = (
        "cash-flow-statement-growth",
        "statement_analysis.stmtAnalCFG_from_fmpAPI",
        "stmtAnalCFG_from_fmpAPI",
        False,
    )
    STMT_INC_GR = (
        "income-statement-growth",
        "statement_analysis.stmtAnalIncGr_from_fmpAPI",
        "stmtAnalIncGr_from_fmpAPI",
        False,
    )
    STMT_BS_GR = (
        "balance-sheet-statement-growth",
        "statement_analysis.stmtAnalBSG_from_fmpAPI",
        "stmtAnalBSG_from_fmpAPI",
        False,
    )
    STMT_FIN_GR = (
        "financial-growth",
        "statement_analysis.stmtAnalFinGr_from_fmpAPI",
        "stmtAnalFinGr_from_fmpAPI",
        False,
    )
    STMT_ENT_VAL = (
        "enterprise-values",
        "statement_analysis.stmtAnalEntVal_from_fmpAPI",
        "stmtAnalEntVal_from_fmpAPI",
        False,
    )
    STMT_FIN_SCORE = (
        "score",
        "statement_analysis.stmtAnalFinScore_from_fmpAPI",
        "stmtAnalFinScore_from_fmpAPI",
        False,
    )
    STMT_OWN_EARN = (
        "owner_earnings",
        "statement_analysis.stmtAnalOwnEarn_from_fmpAPI",
        "stmtAnalOwnEarn_from_fmpAPI",
        False,
    )

    # Valuation (Aggregate)
    VAL_ADV_DCF = (
        "advanced_discounted_cash_flow",
        "valuation.valAdvDiscCF_from_fmpAPI",
        "valAdvDiscCF_from_fmpAPI",
        False,
    )
    VAL_LEV_DCF = (
        "advanced_levered_discounted_cash_flow",
        "valuation.valLevDiscCF_from_fmpAPI",
        "valLevDiscCF_from_fmpAPI",
        False,
    )
    VAL_HIST_RATE = (
        "historical-rating",
        "valuation.valHistRate_from_fmpAPI",
        "valHistRate_from_fmpAPI",
        False,
    )

    # Price Targets (Aggregate)
    PT_HIST = (
        "price-target",
        "price_targets.ptHistPrTargets_from_fmpAPI",
        "ptHistPrTargets_from_fmpAPI",
        False,
    )
    PT_CONSENSUS = (
        "price-target-consensus",
        "price_targets.ptConsensus_from_fmpAPI",
        "ptConsensus_from_fmpAPI",
        False,
    )

    # Upgrades and Downgrades (Aggregate)
    UD_HIST = (
        "upgrades-downgrades",
        "upgrades_downgrades.udHistory_from_fmpAPI",
        "udHistory_from_fmpAPI",
        False,
    )

    UD_CONSENSUS = (
        "upgrades-downgrades-consensus",
        "upgrades_downgrades.udConsensus_from_fmpAPI",
        "udConsensus_from_fmpAPI",
        False,
    )

    EARN_SURPRISE = (
        "earnings-surprises",
        "earnings.earnSurprise_from_fmpAPI",
        "earnSurprise_from_fmpAPI",
        False,
    )

    EARN_EST = (
        "historical/earning_calendar",
        "earnings.earnEst_from_fmpAPI",
        "earnEst_from_fmpAPI",
        False,
    )

    EARN_CALLS = (
        "earning_call_transcript",
        "earnings.earnCalls_from_fmpAPI",
        "earnCalls_from_fmpAPI",
        False,
    )

    def __init__(
        self, endpoint: str, module_path: str, function_name: str, is_daily: bool
    ):
        self.endpoint = endpoint
        self.module_path = module_path
        self.function_name = function_name
        self.is_daily = is_daily
        self._function: Optional[Callable] = None

    @property
    def function(self) -> Callable:
        """Lazily import and cache the function"""
        if self._function is None:
            module = import_module(f"hendricks.ingest_finData.{self.module_path}")
            self._function = getattr(module, self.function_name)
        return self._function

    @classmethod
    def get_by_endpoint(cls, endpoint: str) -> "FMPEndpoint":
        """Get enum member by endpoint string"""
        try:
            return next(e for e in cls if e.endpoint == endpoint)
        except StopIteration:
            raise ValueError(f"Unsupported endpoint: {endpoint}")
