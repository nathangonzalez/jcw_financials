def currency(x) -> str:
    try:
        return f"${x:,.2f}"
    except Exception:
        return "-"
