# MT4 Desktop Multi-Data + In-App Visualization Plan

> For Hermes: follow test-driven-development while implementing this plan.

Goal: extend the native MT4-style desktop app so users can configure db/excel/tushare data sources, run multi-symbol backtests, and view charts/results entirely inside the desktop client without relying on HTML reports.

Architecture: keep the existing engine contract and run artifacts, but expand the desktop helper layer and UI to handle multi-row data/symbol configuration. Add source aliases and Excel/db loaders in the data layer. Replace desktop HTML-centric actions with in-app chart/summary rendering while keeping CLI report writer backward-compatible.

Tech Stack: Python, PySide6/PySide6.QtCharts, pandas, existing my_bt_lab engine/loaders, unittest.

---

## Files expected to change
- Modify: `my_bt_lab/app/desktop_support.py`
- Modify: `tests/test_desktop_support.py`
- Create/Modify: `tests/test_data_loaders.py`
- Modify: `my_bt_lab/data/loaders_bt.py`
- Modify: `my_bt_lab/data/loaders_df.py`
- Modify: `my_bt_lab/app/mt4_desktop.py`
- Modify: `requirements.txt`
- Modify: `README.md`

## TDD targets
1. Helper tests for config <-> table row conversion and chart-point extraction.
2. Loader tests for Excel source and db/postgres alias handling.
3. Run tests to verify RED where applicable.
4. Implement minimal code to make tests pass.
5. Re-run full targeted suite and compile desktop modules.

## Feature tasks
1. Add normalized source handling: `db -> postgres`, `excel/xlsx/xls -> excel`.
2. Add Excel DataFrame loading and db alias support in both loader modules.
3. Add helper functions for multi-data rows, symbol-spec rows, and chart series.
4. Replace single-data/single-symbol desktop forms with editable multi-row tables and connection settings.
5. Add Tushare/global connection fields in the desktop form; use env var token instead of hardcoding secrets.
6. Disable desktop HTML report generation and remove HTML-first UI actions.
7. Add in-app chart tabs for equity/returns/drawdown-style visualization.
8. Update README with PowerShell env-var example and new desktop workflow.

## Verification
- `python3 -m unittest tests.test_desktop_support tests.test_data_loaders -v`
- `python3 -m py_compile my_bt_lab/app/desktop_support.py my_bt_lab/app/mt4_desktop.py my_bt_lab/data/loaders_bt.py my_bt_lab/data/loaders_df.py`

## Notes
- Do not store the user’s Tushare token in repo files; only support `TUSHARE_TOKEN` / configured env name.
- Keep CLI HTML reporting available for legacy/non-desktop flows if needed, but desktop runs should not depend on it.
