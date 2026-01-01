import warnings

import pandas as pd


def test_load_transaction_detail_no_infer_format_warning(tmp_path):
	# Minimal QB Transaction Detail-like CSV.
	# Date formats here should be handled by parse_date_series without
	# triggering pandas' "Could not infer format" warning.
	csv_path = tmp_path / "tx_detail.csv"
	csv_path.write_text(
		"Type,Date,Name,Memo,Account,Class,Amount,Account Type\n"
		"Invoice,12/05/2025,Customer A,Test memo,4000 Income,,100.00,Income\n",
		encoding="utf-8",
	)

	with warnings.catch_warnings(record=True) as caught:
		warnings.simplefilter("always")

		from scripts.compute_kpis import load_transaction_detail

		df = load_transaction_detail(str(csv_path))

	# Basic sanity checks
	assert len(df) == 1
	assert "date" in df.columns
	assert pd.api.types.is_datetime64_any_dtype(df["date"])

	infer_warnings = [
		w
		for w in caught
		if "Could not infer format" in str(w.message)
		or "falling back to dateutil" in str(w.message)
	]
	assert not infer_warnings, f"Unexpected date inference warnings: {[str(w.message) for w in infer_warnings]}"
