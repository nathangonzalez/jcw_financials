from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


CostType = Literal["material", "labor", "supervision"]


@dataclass
class JobBillingConfig:
    """
    Configuration for how to bill a single job in a given period.
    overhead_pct and profit_pct are decimal fractions (0.10 = 10%).
    """
    job: str
    overhead_pct: float
    profit_pct: float
    contract_type: Literal["simple", "aia"] = "simple"  # for future extension


@dataclass
class JobInvoice:
    """
    Represents the computed invoice for a single job over a period.
    """
    job: str
    period_label: str
    materials: float
    labor: float
    supervision: float
    overhead_pct: float
    profit_pct: float

    @property
    def base_cost(self) -> float:
        """
        Raw cost before markup: materials + labor + supervision.
        """
        return self.materials + self.labor + self.supervision

    @property
    def overhead_amount(self) -> float:
        """
        Overhead calculated as a percentage of base_cost.
        """
        return round(self.base_cost * self.overhead_pct, 2)

    @property
    def profit_amount(self) -> float:
        """
        Profit/fee calculated as a percentage of (base_cost + overhead).
        """
        return round((self.base_cost + self.overhead_amount) * self.profit_pct, 2)

    @property
    def total(self) -> float:
        """
        Final invoice total = base_cost + overhead + profit.
        """
        return round(self.base_cost + self.overhead_amount + self.profit_amount, 2)


def load_green_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a raw green-sheet dataframe into the internal schema.

    Expected *normalized* columns after this function:
        - job
        - date
        - vendor_or_employee
        - description
        - amount
        - cost_type  (one of: 'material', 'labor', 'supervision')
        - sov_line   (optional; for future AIA integration)
        - ref        (optional; check # or reference)

    This function should be tolerant of slightly different source column names.
    """
    gs = df.copy()

    # Normalize column names to snake_case
    gs.columns = [str(c).strip().lower().replace(" ", "_") for c in gs.columns]

    # Map likely source columns to our internal names
    rename_map: dict[str, str] = {}
    if "project" in gs.columns and "job" not in gs.columns:
        rename_map["project"] = "job"
    if "name" in gs.columns and "vendor_or_employee" not in gs.columns:
        rename_map["name"] = "vendor_or_employee"
    if "total" in gs.columns and "amount" not in gs.columns:
        rename_map["total"] = "amount"
    if "memo" in gs.columns and "description" not in gs.columns:
        rename_map["memo"] = "description"
    if "reference" in gs.columns and "ref" not in gs.columns:
        rename_map["reference"] = "ref"

    gs = gs.rename(columns=rename_map)

    # Ensure required columns exist
    required_cols = ["job", "date", "amount", "cost_type"]
    for col in required_cols:
        if col not in gs.columns:
            gs[col] = None

    # Optional columns
    for col in ["vendor_or_employee", "description", "sov_line", "ref"]:
        if col not in gs.columns:
            gs[col] = None

    # Types
    gs["date"] = pd.to_datetime(gs["date"], errors="coerce")
    gs["amount"] = pd.to_numeric(gs["amount"], errors="coerce").fillna(0.0)

    # Normalize cost_type strings
    gs["cost_type"] = gs["cost_type"].astype(str).str.lower().str.strip()

    return gs


def compute_job_invoice(
    gs: pd.DataFrame,
    job_cfg: JobBillingConfig,
    period_start,
    period_end,
) -> JobInvoice:
    """
    Compute the invoice for a single job over [period_start, period_end],
    based purely on green-sheet costs inside that window.
    """
    mask = (
        (gs["job"] == job_cfg.job)
        & (gs["date"] >= pd.to_datetime(period_start))
        & (gs["date"] <= pd.to_datetime(period_end))
    )
    subset = gs.loc[mask].copy()

    materials = subset.loc[subset["cost_type"] == "material", "amount"].sum()
    labor = subset.loc[subset["cost_type"] == "labor", "amount"].sum()
    supervision = subset.loc[subset["cost_type"] == "supervision", "amount"].sum()

    return JobInvoice(
        job=job_cfg.job,
        period_label=f"{period_start} → {period_end}",
        materials=round(materials, 2),
        labor=round(labor, 2),
        supervision=round(supervision, 2),
        overhead_pct=job_cfg.overhead_pct,
        profit_pct=job_cfg.profit_pct,
    )


def compute_period_billing(
    gs: pd.DataFrame,
    job_configs: list[JobBillingConfig],
    period_start,
    period_end,
) -> pd.DataFrame:
    """
    Compute cost-plus invoices for each configured job over [period_start, period_end].

    Returns a dataframe with one row per job:
        job, period, materials, labor, supervision,
        overhead_pct, profit_pct, overhead_amount, profit_amount, invoice_total
    """
    invoices: list[JobInvoice] = []

    for cfg in job_configs:
        inv = compute_job_invoice(gs, cfg, period_start, period_end)
        invoices.append(inv)

    rows: list[dict] = []
    for inv in invoices:
        rows.append(
            {
                "job": inv.job,
                "period": inv.period_label,
                "materials": inv.materials,
                "labor": inv.labor,
                "supervision": inv.supervision,
                "overhead_pct": inv.overhead_pct,
                "profit_pct": inv.profit_pct,
                "overhead_amount": inv.overhead_amount,
                "profit_amount": inv.profit_amount,
                "invoice_total": inv.total,
            }
        )

    return pd.DataFrame(rows)
