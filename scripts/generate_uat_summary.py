import argparse
import json
import re
from pathlib import Path
from typing import Any


_WINDOWS_ABS_PATH_RE = re.compile(r"[A-Za-z]:\\\\[^\s\"']+")
_UNC_PATH_RE = re.compile(r"\\\\\\\\[A-Za-z0-9_.-]+\\\\[^\s\"']+")
_URL_RE = re.compile(r"https?://[^\s\"']+")


def _scrub_string(value: str) -> str:
    value = _WINDOWS_ABS_PATH_RE.sub("<REDACTED_PATH>", value)
    value = _UNC_PATH_RE.sub("<REDACTED_PATH>", value)

    def _url_repl(match: re.Match[str]) -> str:
        url = match.group(0)
        lower = url.lower()
        if "streamlit" in lower or "upload" in lower:
            return "<REDACTED_URL>"
        return "<REDACTED_URL>" if "file_id=" in lower or "widget" in lower else url

    value = _URL_RE.sub(_url_repl, value)

    # Best-effort scrub for common ID-like markers
    value = re.sub(r"\b(file[_-]?id|widget[_-]?id)\b\s*[:=]\s*[^\s,;]+", "<REDACTED_ID>", value, flags=re.IGNORECASE)
    return value


def scrub(obj: Any) -> Any:
    if isinstance(obj, str):
        return _scrub_string(obj)
    if isinstance(obj, list):
        return [scrub(item) for item in obj]
    if isinstance(obj, dict):
        scrubbed: dict[str, Any] = {}
        for key, value in obj.items():
            key_lower = str(key).lower()
            if any(token in key_lower for token in ["absolute_path", "upload_url", "file_id", "widget_id", "username", "machine", "hostname"]):
                continue
            scrubbed[str(key)] = scrub(value)
        return scrubbed
    return obj


def build_sanitized_summary(run_summary: dict[str, Any]) -> dict[str, Any]:
    uat_payload = run_summary.get("uat_payload") or {}
    uat_metrics_only = {
        k: uat_payload.get(k)
        for k in [
            "dashboard_metrics",
            "qb_pnl_metrics_report_window",
            "reconciliation_bridge",
            "run_rates",
        ]
        if k in uat_payload
    }

    summary: dict[str, Any] = {
        "git_commit": run_summary.get("git_commit"),
        "pytest_status": run_summary.get("pytest_status"),
        "tabs_status": run_summary.get("tabs_status"),
        "uat_status": run_summary.get("uat_status"),
        "exit_codes": run_summary.get("exit_codes"),
        "tabs": run_summary.get("tabs"),
        "uat_payload": uat_metrics_only,
        "started_at": run_summary.get("started_at"),
        "finished_at": run_summary.get("finished_at"),
        "duration_seconds": run_summary.get("duration_seconds"),
    }

    # Ensure we don't accidentally leak URL/path fields even if present
    summary.pop("url", None)
    summary.pop("ledger", None)

    return scrub(summary)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sanitized UAT summary artifact.")
    parser.add_argument("--input", default="logs/run_summary.json")
    parser.add_argument("--output", default="artifacts/uat_summary.json")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run_summary = json.loads(input_path.read_text(encoding="utf-8-sig"))
    sanitized = build_sanitized_summary(run_summary)
    output_path.write_text(json.dumps(sanitized, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
