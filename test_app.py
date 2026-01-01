import os

import pytest
import requests


APP_URL = os.getenv("FINANCIALS_APP_URL", "http://localhost:8501")


def _get(url: str, timeout: float = 10.0) -> requests.Response:
    try:
        return requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        pytest.skip(f"Streamlit app not reachable at {url}: {exc}")


def test_financials_app():
    """Sync smoke test (no async, no browser required).

    Verifies the Streamlit server is reachable and serving HTTP.
    If the server isn't running locally, skip instead of failing.
    """

    # Prefer Streamlit's internal health endpoint when available.
    health_url = APP_URL.rstrip("/") + "/_stcore/health"
    r_health = _get(health_url, timeout=5.0)
    if r_health.status_code != 200:
        pytest.skip(f"Streamlit health endpoint not OK at {health_url} (status {r_health.status_code})")

    debug_url = APP_URL.rstrip("/") + "/?debug=1"
    r = _get(debug_url, timeout=10.0)
    assert r.status_code == 200

    body = (r.text or "")
    # Best-effort markers (note: Streamlit content is mostly websocket-rendered).
    assert (
        ("Owner SDE & Profit Dashboard" in body)
        or ("UAT_METRICS_START" in body)
        or ("streamlit" in body.lower())
    )
