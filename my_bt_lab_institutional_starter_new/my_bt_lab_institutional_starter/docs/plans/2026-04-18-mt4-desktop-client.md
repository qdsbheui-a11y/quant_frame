# MT4 Desktop Client Implementation Plan

> For Hermes: follow test-driven-development while implementing this plan.

Goal: add a real Windows desktop backtest client with MT4-like layout so non-programmers can run/configure backtests without using a browser.

Architecture: keep the existing engine, YAML templates, run folder structure, and report writer unchanged. Add a new PySide6 desktop entrypoint that wraps the same backend and reuses pure helper functions from `my_bt_lab/app/desktop_support.py` for config discovery, history, metrics, and export inspection.

Tech Stack: Python, PySide6, existing my_bt_lab engine/reporting modules, unittest.

---

## Scope for this iteration

1. Add a native desktop app entrypoint.
2. Add helper functions needed by the desktop UI and cover them with tests first.
3. Add Windows start script for the desktop app.
4. Update `requirements.txt` and `README.md`.
5. Validate with unit tests and syntax compilation.

## Files to modify

- Modify: `my_bt_lab/app/desktop_support.py`
- Modify: `tests/test_desktop_support.py`
- Create: `my_bt_lab/app/mt4_desktop.py`
- Create: `start_mt4_desktop.bat`
- Modify: `requirements.txt`
- Modify: `README.md`

## UI shape

- Main window with menu/toolbar feel using `QMainWindow`
- Left panel: template, run mode, strategy params, broker, data, output, advanced JSON
- Right panel: summary, trades, orders, fills, snapshots, positions, logs, exports, history
- Bottom status bar for progress/messages
- Background worker thread for running backtests so the window stays responsive

## TDD steps

1. Add failing tests for new desktop helper behavior.
2. Run the targeted tests and confirm failure.
3. Implement helper functions.
4. Re-run tests until green.
5. Build desktop UI entrypoint using the tested helpers.
6. Run full test file plus syntax compilation.

## Verification

- `python3 -m unittest tests.test_desktop_support -v`
- `python3 -m py_compile my_bt_lab/app/desktop_support.py my_bt_lab/app/mt4_desktop.py my_bt_lab/app/run.py`

## Notes

- Current execution environment does not have PySide6 installed, so runtime GUI launch cannot be fully verified here.
- The desktop app should fail gracefully with a clear install message if PySide6 is missing on the user machine.
