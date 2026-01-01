from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


@dataclass(frozen=True)
class AddbackRule:
    """Rule for flagging addback transactions.

    Supported fields (all optional unless noted):
    - name: str (required)
    - account_contains: str|list[str]
    - name_contains: str|list[str]
    - memo_contains: str|list[str]
    - amount: float (exact match, sign-agnostic)
    - amount_tolerance: float (absolute tolerance; defaults to 0.01)
    """

    name: str
    account_contains: list[str] | None = None
    name_contains: list[str] | None = None
    memo_contains: list[str] | None = None
    amount: float | None = None
    amount_tolerance: float | None = None


def _as_list(v: Any) -> list[str] | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else None
    if isinstance(v, list):
        out = [str(x).strip() for x in v if str(x).strip()]
        return out or None
    return None


def parse_rule(obj: dict[str, Any]) -> AddbackRule:
    name = str(obj.get("name") or "").strip()
    if not name:
        raise ValueError("Addback rule missing required 'name'")

    amount = obj.get("amount")
    amount_f: float | None
    if amount is None or amount == "":
        amount_f = None
    else:
        amount_f = float(amount)

    tol = obj.get("amount_tolerance")
    tol_f: float | None
    if tol is None or tol == "":
        tol_f = None
    else:
        tol_f = float(tol)

    return AddbackRule(
        name=name,
        account_contains=_as_list(obj.get("account_contains")),
        name_contains=_as_list(obj.get("name_contains")),
        memo_contains=_as_list(obj.get("memo_contains")),
        amount=amount_f,
        amount_tolerance=tol_f,
    )


def default_rules() -> list[AddbackRule]:
    # Built-in recurring payroll addback rule (transaction match)
    return [
        AddbackRule(
            name="weekly_payroll_addback_2880",
            memo_contains=["payroll"],
            amount=2880.0,
            amount_tolerance=25.0,
        )
    ]


def load_rules(path: Path) -> list[AddbackRule]:
    if not path.exists():
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("addback rules JSON must be a list")

    rules: list[AddbackRule] = []
    for obj in data:
        if not isinstance(obj, dict):
            continue
        rules.append(parse_rule(obj))
    return rules


def rules_to_jsonable(rules: Iterable[AddbackRule]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rules:
        out.append(
            {
                "name": r.name,
                "account_contains": r.account_contains,
                "name_contains": r.name_contains,
                "memo_contains": r.memo_contains,
                "amount": r.amount,
                "amount_tolerance": r.amount_tolerance,
            }
        )
    return out


def save_rules(path: Path, rules: Iterable[AddbackRule]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = rules_to_jsonable(rules)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _contains_all(haystack: pd.Series, needles: list[str] | None) -> pd.Series:
    if not needles:
        return pd.Series([True] * len(haystack), index=haystack.index)
    h = haystack.astype(str).str.lower().fillna("")
    mask = pd.Series([True] * len(h), index=h.index)
    for n in needles:
        n0 = str(n).strip().lower()
        if not n0:
            continue
        mask = mask & h.str.contains(n0, na=False)
    return mask


def apply_rules_to_df(
    df: pd.DataFrame,
    rules: Iterable[AddbackRule],
    *,
    payroll_start: dt.date = dt.date(2025, 11, 15),
) -> pd.DataFrame:
    """Apply rules to a classified ledger df.

    Expects columns: date, amount, account, name, memo.
    Updates/creates: sde_addback_flag, sde_addback_reason.

    The built-in payroll rule is treated as recurring starting payroll_start.
    """

    out = df.copy()
    if "sde_addback_flag" not in out.columns:
        out["sde_addback_flag"] = False
    if "sde_addback_reason" not in out.columns:
        out["sde_addback_reason"] = ""

    out["date"] = pd.to_datetime(out.get("date"), errors="coerce")

    account_s = out.get("account", pd.Series([""] * len(out), index=out.index))
    name_s = out.get("name", pd.Series([""] * len(out), index=out.index))
    memo_s = out.get("memo", pd.Series([""] * len(out), index=out.index))
    amt_s = pd.to_numeric(out.get("amount", 0.0), errors="coerce").fillna(0.0).abs()

    for rule in rules:
        tol = float(rule.amount_tolerance) if rule.amount_tolerance is not None else 0.01

        mask = pd.Series([True] * len(out), index=out.index)
        mask = mask & _contains_all(account_s, rule.account_contains)
        mask = mask & _contains_all(name_s, rule.name_contains)
        mask = mask & _contains_all(memo_s, rule.memo_contains)

        # Recurring payroll rule boundary
        if rule.name == "weekly_payroll_addback_2880":
            start_dt = pd.to_datetime(payroll_start)
            mask = mask & (out["date"] >= start_dt)

        if rule.amount is not None:
            target = abs(float(rule.amount))
            mask = mask & (amt_s.sub(target).abs() <= tol)

        if not mask.any():
            continue

        out.loc[mask, "sde_addback_flag"] = True

        # Append reason
        existing = out.loc[mask, "sde_addback_reason"].astype(str).fillna("")
        suffix = f"rule={rule.name}"
        sep = existing.apply(lambda x: ", " if x else "")
        out.loc[mask, "sde_addback_reason"] = existing + sep + suffix

    return out
