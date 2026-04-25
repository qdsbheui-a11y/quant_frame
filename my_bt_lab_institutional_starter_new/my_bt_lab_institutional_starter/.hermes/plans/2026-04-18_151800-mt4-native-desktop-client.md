# MT4 Native Desktop Client Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add a native Windows desktop backtest client with MT4-style workflow for non-programmers, while keeping the existing backtest engine and output pipeline unchanged.

**Architecture:** Build a PySide6 QMainWindow application that edits YAML-backed config state in forms, runs backtests in a worker thread, and renders results/history/logs in docked panels and tabs. Reuse existing engine execution (`run_engine`) and output writer (`write_result`) so desktop and Streamlit produce the same artifacts.

**Tech Stack:** Python, PySide6/Qt Widgets, existing my_bt_lab backtest modules, unittest for pure helper tests.

---

### Task 1: Add pure desktop helper module

**Objective:** Create a reusable non-UI support layer for config discovery, metrics extraction, parameter grid parsing, history summaries, and temp-config writing.

**Files:**
- Create: `my_bt_lab/app/desktop_support.py`
- Test: `tests/test_desktop_support.py`

**Step 1: Write failing tests**
- Test config listing returns YAML files sorted by name.
- Test parameter grid parser converts booleans, ints, floats, strings.
- Test result metric extraction handles missing fields.
- Test history summarizer reads a run folder with `result.json` and `run_meta.json`.

**Step 2: Run test to verify failure**
Run: `python3 -m unittest tests.test_desktop_support -v`
Expected: FAIL because support module does not exist yet.

**Step 3: Write minimal implementation**
Implement helper functions only for behavior exercised by tests.

**Step 4: Run test to verify pass**
Run: `python3 -m unittest tests.test_desktop_support -v`
Expected: PASS.

### Task 2: Build native PySide6 MT4 workbench

**Objective:** Create a native desktop app with MT4-style form controls, run actions, worker thread execution, result tabs, history panel, and log viewer.

**Files:**
- Create: `my_bt_lab/app/mt4_desktop.py`
- Modify: `requirements.txt`
- Create: `start_mt4_desktop.bat`

**Step 1: Write failing smoke test or import guard**
- Add a support-level test for any pure helper introduced specifically for the desktop app.
- Avoid importing PySide6 in tests because the current environment may not have GUI libs.

**Step 2: Implement desktop app**
Required behaviors:
- Native `QMainWindow` with menu/status bar
- Left-side settings grouped as strategy tester controls
- Right/bottom-style tabs for results, trades, orders/fills, snapshots, logs, exports, history
- Background run thread so UI remains responsive
- Reuse current config/run/output pipeline
- Allow template selection and save config snapshot per run

**Step 3: Add launcher**
- Windows batch launcher should prefer `.venv\Scripts\python.exe`
- Start `python -m my_bt_lab.app.mt4_desktop`

**Step 4: Update dependency list**
- Add `PySide6`

### Task 3: Document the native desktop workflow

**Objective:** Update README so non-programmers know how to open and use the new native desktop terminal.

**Files:**
- Modify: `README.md`

**Step 1: Add startup section**
- Mention `start_mt4_desktop.bat`
- Mention dependency install

**Step 2: Add user workflow**
- Choose template
- Edit params
- Run backtest
- Inspect tabs/history

### Task 4: Verify implementation

**Objective:** Validate syntax and as much runtime behavior as current environment allows.

**Files:**
- No new files necessarily

**Step 1: Run unit tests**
Run: `python3 -m unittest tests.test_desktop_support -v`

**Step 2: Run syntax checks**
Run:
- `python3 -m py_compile my_bt_lab/app/desktop_support.py`
- `python3 -m py_compile my_bt_lab/app/mt4_desktop.py`

**Step 3: Attempt import smoke test**
Run a small import command. If PySide6 is unavailable, record that as an environment limitation in final notes.

### Files likely to change
- `my_bt_lab/app/desktop_support.py`
- `my_bt_lab/app/mt4_desktop.py`
- `tests/test_desktop_support.py`
- `requirements.txt`
- `README.md`
- `start_mt4_desktop.bat`

### Risks / tradeoffs
- Current environment lacks GUI dependencies, so runtime verification may be limited to syntax/import checks.
- To keep scope controlled, the first native desktop version will focus on single-run + optimization + result inspection, not advanced charting plugins.
- Qt WebEngine is intentionally avoided to reduce packaging complexity.
