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
        "historical/employee_count",
        "comapny_info.ciEmpCount_from_fmpAPI",
        "ciEmpCount_from_fmpAPI",
        False,
    )
    EXEC_COMP = (
        "governance/executive_compensation",
        "comapny_info.ciExecComp_from_fmpAPI",
        "ciExecComp_from_fmpAPI",
        False,
    )
    GRADE = ("grade", "comapny_info.ciGrade_from_fmpAPI", "ciGrade_from_fmpAPI", False)
    ANALYST_EST = (
        "analyst-estimates",
        "comapny_info.ciAnalystEst_from_fmpAPI",
        "ciAnalystEst_from_fmpAPI",
        False,
    )
    ANALYST_REC = (
        "analyst-stock-recommendations",
        "comapny_info.ciAnalystRec_from_fmpAPI",
        "ciAnalystRec_from_fmpAPI",
        False,
    )

    # Company Info (Daily)
    MARKET_CAP = (
        "historical-market-capitalization",
        "comapny_info.ciMarketCap_from_fmpAPI",
        "ciMarketCap_from_fmpAPI",
        True,
    )

    # Financial Statements (Aggregate)
    INCOME_STMT = (
        "income-statement",
        "financial_statements.fsIncomeStmt_from_fmpAPI",
        "fsIncomeStmt_from_fmpAPI",
        False,
    )
    BALANCE_SHEET = (
        "balance-sheet-statement",
        "financial_statements.fsBalanceSheet_from_fmpAPI",
        "fsBalanceSheet_from_fmpAPI",
        False,
    )
    CASH_FLOW = (
        "cash-flow-statement",
        "financial_statements.fsCashFlow_from_fmpAPI",
        "fsCashFlow_from_fmpAPI",
        False,
    )

    # Statement Analysis (Aggregate)
    STMT_KM = (
        "key-metrics",
        "statement_analysis.saKM_from_fmpAPI",
        "saKM_from_fmpAPI",
        False,
    )
    STMT_RATIOS = (
        "ratios",
        "statement_analysis.saRatios_from_fmpAPI",
        "saRatios_from_fmpAPI",
        False,
    )
    STMT_CFG = (
        "cash-flow-statement-growth",
        "statement_analysis.saCFG_from_fmpAPI",
        "saCFG_from_fmpAPI",
        False,
    )
    STMT_INC_GR = (
        "income-statement-growth",
        "statement_analysis.saIncGr_from_fmpAPI",
        "saIncGr_from_fmpAPI",
        False,
    )
    STMT_BS_GR = (
        "balance-sheet-statement-growth",
        "statement_analysis.saBSG_from_fmpAPI",
        "saBSG_from_fmpAPI",
        False,
    )
    STMT_FIN_GR = (
        "financial-growth",
        "statement_analysis.saFinGr_from_fmpAPI",
        "saFinGr_from_fmpAPI",
        False,
    )
    STMT_ENT_VAL = (
        "enterprise-values",
        "statement_analysis.saEntVal_from_fmpAPI",
        "saEntVal_from_fmpAPI",
        False,
    )
    STMT_FIN_SCORE = (
        "score",
        "statement_analysis.saFinScore_from_fmpAPI",
        "saFinScore_from_fmpAPI",
        False,
    )
    STMT_OWN_EARN = (
        "owner_earnings",
        "statement_analysis.saOwnEarn_from_fmpAPI",
        "saOwnEarn_from_fmpAPI",
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

    EARN_HIST = (
        "historical/earning_calendar",
        "earnings.earnHistorical_from_fmpAPI",
        "earnHistorical_from_fmpAPI",
        False,
    )

    EARN_CALLS = (
        "earning_call_transcript",
        "earnings.earnCalls_from_fmpAPI",
        "earnCalls_from_fmpAPI",
        False,
    )

    # Technical Indicators
    FMP_SMA = (
        "sma",
        "tech_indicator.tiSMA_from_fmpAPI",
        "tiSMA_from_fmpAPI",
        False,
    )

    FMP_EMA = (
        "ema",
        "tech_indicator.tiEMA_from_fmpAPI",
        "tiEMA_from_fmpAPI",
        False,
    )

    FMP_WMA = (
        "wma",
        "tech_indicator.tiWMA_from_fmpAPI",
        "tiWMA_from_fmpAPI",
        False,
    )

    FMP_DEMA = (
        "dema",
        "tech_indicator.tiDEMA_from_fmpAPI",
        "tiDEMA_from_fmpAPI",
        False,
    )

    FMP_TEMA = (
        "tema",
        "tech_indicator.tiTEMA_from_fmpAPI",
        "tiTEMA_from_fmpAPI",
        False,
    )

    FMP_WILL = (
        "williams",
        "tech_indicator.tiWILL_from_fmpAPI",
        "tiWILL_from_fmpAPI",
        False,
    )

    FMP_RSI = (
        "rsi",
        "tech_indicator.tiRSI_from_fmpAPI",
        "tiRSI_from_fmpAPI",
        False,
    )

    FMP_ADI = (
        "adx",
        "tech_indicator.tiADI_from_fmpAPI",
        "tiADI_from_fmpAPI",
        False,
    )

    FMP_STDEV = (
        "standardDeviation",
        "tech_indicator.tiSTDEV_from_fmpAPI",
        "tiSTDEV_from_fmpAPI",
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
