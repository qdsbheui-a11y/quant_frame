# my_bt_lab (institutional starter)

This is a refactored starter project based on your current `my_bt_lab`, aiming for:
- Strategy pluginization (YAML selects strategy by name)
- Standardized output artifacts per run (config snapshot, trades, equity curve, meta)
- Keep using Backtrader as the research backend for now, while leaving a clean seam to replace the engine later.

## Quickstart

1) Install deps
```bash
pip install -r requirements.txt
```

2) Run with the bundled CSV
```bash
python -m my_bt_lab.app.run --config my_bt_lab/app/configs/cta.yaml --output runs --tag demo
```

3) Run with Tushare (requires env var)
```bash
export TUSHARE_TOKEN=***
python -m my_bt_lab.app.run --config my_bt_lab/app/configs/cta_tushare_demo.yaml --output runs --tag tushare
```

PowerShell example on Windows:
```powershell
$env:TUSHARE_TOKEN="<your-token>"
python -m my_bt_lab.app.mt4_desktop
```

## Output folder

Each run creates a folder like:
- runs/20260226_235959_demo/
  - run.log
  - config.yaml
  - run_meta.json
  - result.json
  - trades.csv
  - equity_curve.csv
  - time_return.csv

## How to add a new strategy

1) Create a new Backtrader strategy class in `my_bt_lab/strategies/`
2) Register it in `my_bt_lab/registry/strategy_registry.py`
3) Select it in YAML:
```yaml
strategy:
  name: your_strategy
  params:
    ...
```

## Note on execution

The starter uses Backtrader for execution. The included `simple_engine.py` is only a stub
to illustrate where the custom engine will live later.

## Native MT4-style desktop workbench (recommended for non-programmers)

This project now includes a real desktop client for Windows users who do not want a browser UI.

Main workflow:
- open the desktop app
- choose a YAML template
- adjust strategy / account / data settings in forms
- click one button to run backtest or optimization
- inspect results, logs, exports, and history in the same desktop window

### Start the desktop app (Windows)

From project root:

```bash
start_quant_lab_desktop.bat
```

This launcher will:
- create/use `.venv`
- install `requirements.txt`
- auto-load `my_bt_lab/app/configs/quant_lab_aliyun_ssh.yaml` into the desktop UI

General desktop launcher (manual template selection):

```bash
start_mt4_desktop.bat
```

or:

```bash
python -m my_bt_lab.app.mt4_desktop
```

If this is your first run, install dependencies first:

```bash
pip install -r requirements.txt
```

### Desktop UI features

If your PostgreSQL is only reachable through an SSH jump/server login, you can now fill SSH tunnel fields directly in the desktop app, or start from:
- `my_bt_lab/app/configs/stock_5m_pg_ssh_template.yaml`

- real `QMainWindow` desktop app via PySide6
- MT4 software-style dark layout
  - left side tabs = strategy tester / navigator / market watch
  - right side = terminal strip + summary area + bottom result tabs
- single backtest mode
- parameter optimization mode
  - simple grid text such as `fast=5,10,20`
  - exports optimization summary CSV into `runs/`
- multi-data-source configuration inside the desktop app
  - `csv`
  - `excel` / `.xlsx` / `.xls`
  - `db` (Postgres alias)
  - `tushare`
- multi-symbol / multi-data backtest editing in tables inside the desktop UI
- desktop connection settings for:
  - Postgres host / port / db / user / password env name
  - SSH tunnel host / port / user / password or key path / remote DB host:port
  - Tushare token env / asset / default api / default freq
- result tabs
  - in-app charts for equity and return series
  - overview metrics
  - equity/result table
  - trades
  - orders
  - fills
  - account snapshots
  - open positions
  - Journal logs
  - exported files
  - history tasks
- quick buttons
  - open current run folder
  - refresh in-app charts
  - refresh current log
- desktop runs disable HTML report dependency by default; results are meant to be viewed inside the software
- advanced JSON editor for power users
- history table supports loading prior run folders back into the result area

### Recommended user flow

1. Open `start_mt4_desktop.bat`
2. Select a template YAML
3. Adjust broker / strategy params visually
4. In the data tables, add or edit multiple symbols and choose `csv` / `excel` / `db` / `tushare`
5. If using Tushare on Windows PowerShell, set `$env:TUSHARE_TOKEN="<your-token>"` before launch
6. Click `开始回测`
7. Read the in-app chart tab, trade tabs, logs, and history on the right
8. If needed, switch to `参数优化` and test multiple parameter combinations

## Browser workbench (legacy / optional)

The previous Streamlit workbench is still available if you want a browser-based view:

```bash
start_mt4_workbench.bat
```

or:

```bash
python -m streamlit run my_bt_lab/app/mt4_workbench.py
```
## Simple engine (coarse execution)

You can switch to the built-in coarse engine (useful for debugging and for the future “replace backtrader” path).

1) In YAML:
```yaml
engine:
  name: simple
```

2) Run:
```bash
python -m my_bt_lab.app.run --config my_bt_lab/app/configs/cta_simple.yaml --output runs --tag simple
```

Simple engine assumptions:
- Market orders execute at **next bar open**
- Long-only
- No margin/slippage/partial fills (deliberately coarse)
