from __future__ import annotations

import subprocess
import sys
import webbrowser
from pathlib import Path
from typing import Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG = "my_bt_lab/app/configs/cta_mtf_tushare.yaml"
OUTPUT = "runs"
TAG = "demo"
OPEN_ALL_HTML = True


def _list_run_dirs(runs_dir: Path) -> List[Path]:
    if not runs_dir.exists():
        return []
    return [p for p in runs_dir.iterdir() if p.is_dir()]


def _pick_latest_run(before: Iterable[Path], after: Iterable[Path]) -> Path | None:
    before_set = {p.resolve() for p in before}
    candidates = [p for p in after if p.resolve() not in before_set]
    if not candidates:
        candidates = list(after)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _open_report_tabs(report_dir: Path) -> None:
    files = [report_dir / "index.html"]
    if OPEN_ALL_HTML:
        files.extend(
            [
                report_dir / "分析报告.html",
                report_dir / "资金曲线.html",
                report_dir / "阶段总结.html",
                report_dir / "交易详细.html",
            ]
        )

    opened = False
    for f in files:
        if f.exists():
            webbrowser.open_new_tab(f.resolve().as_uri())
            print(f"已打开报告: {f}")
            opened = True

    if not opened:
        print("回测已完成，但没有找到可打开的 HTML 报告。")
        print(f"请检查目录: {report_dir}")


def main() -> int:
    runs_dir = PROJECT_ROOT / OUTPUT
    before = _list_run_dirs(runs_dir)

    cmd = [
        sys.executable,
        "-m",
        "my_bt_lab.app.run",
        "--config",
        CONFIG,
        "--output",
        OUTPUT,
        "--tag",
        TAG,
    ]

    print("开始运行回测...\n")
    completed = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if completed.returncode != 0:
        print("\n回测运行失败，请先查看终端报错信息。")
        return completed.returncode

    after = _list_run_dirs(runs_dir)
    latest_run = _pick_latest_run(before, after)
    if latest_run is None:
        print("\n回测已完成，但没有找到 runs 目录。")
        return 1

    print(f"\n最新回测目录: {latest_run}")
    report_dir = latest_run / "report_html"
    _open_report_tabs(report_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
