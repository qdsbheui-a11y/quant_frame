from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import streamlit as st


def _default_runs_root() -> Path:
    # 默认假设项目根目录下有 runs 文件夹
    return Path(__file__).resolve().parents[2] / "runs"


def _list_run_dirs(runs_root: Path) -> List[Path]:
    if not runs_root.exists():
        return []
    return sorted(
        [p for p in runs_root.iterdir() if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )


def _load_json(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def main():
    st.set_page_config(page_title="回测结果分析面板", layout="wide")

    st.title("📈 回测结果分析（中文界面）")
    st.markdown(
        "本页面读取 `runs` 目录下的回测输出文件，展示 **权益曲线** 和 **交易记录** 等信息。"
    )

    # -------- 侧边栏：基础配置 --------
    st.sidebar.header("基础设置")
    default_root = _default_runs_root()
    runs_root_str = st.sidebar.text_input(
        "回测结果根目录（runs）", value=str(default_root), help="通常为项目根目录下的 runs 文件夹。"
    )
    runs_root = Path(runs_root_str).expanduser().resolve()

    run_dirs = _list_run_dirs(runs_root)
    if not run_dirs:
        st.warning(f"在目录 `{runs_root}` 下没有找到任何回测结果子目录。请先运行回测。")
        return

    run_names = [p.name for p in run_dirs]
    selected_name = st.sidebar.selectbox("选择一次回测结果文件夹", options=run_names)
    run_dir = runs_root / selected_name

    st.sidebar.markdown(f"当前选择：`{run_dir}`")

    # -------- 加载数据 --------
    result_json = _load_json(run_dir / "result.json") or {}
    equity_df = _load_csv(run_dir / "equity_curve.csv")
    trades_df = _load_csv(run_dir / "trades.csv")
    time_ret_df = _load_csv(run_dir / "time_return.csv")

    # -------- 概要信息 --------
    st.subheader("回测概要")

    start_value = float(result_json.get("start_value", float("nan")))
    end_value = float(result_json.get("end_value", float("nan")))
    total_ret_pct = (end_value / start_value - 1.0) * 100.0 if start_value else float(
        "nan"
    )

    drawdown = result_json.get("drawdown", {}) or {}
    max_dd_pct = float(drawdown.get("max_drawdown_pct", float("nan")))
    max_moneydown = float(drawdown.get("max_moneydown", float("nan")))

    trade_stats = result_json.get("trade_stats", {}) or {}
    closed_trades = int(trade_stats.get("closed_trades", 0))
    net_pnl = float(trade_stats.get("net_pnl", 0.0))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("初始资金", f"{start_value:,.2f}")
    col2.metric("结束资金", f"{end_value:,.2f}", f"{total_ret_pct:,.2f}%")
    col3.metric("最大回撤(%)", f"{max_dd_pct:,.2f}")
    col4.metric("净收益", f"{net_pnl:,.2f}")

    st.markdown(
        f"- **最大资金回撤**: {max_moneydown:,.2f}\n"
        f"- **已平仓笔数**: {closed_trades}"
    )

    # -------- 权益曲线与收益曲线 --------
    st.subheader("权益曲线 / 收益曲线")

    if equity_df is not None and not equity_df.empty:
        eq = equity_df.copy()
        if "datetime" in eq.columns:
            eq["datetime"] = pd.to_datetime(eq["datetime"])
            eq = eq.set_index("datetime")

        tab1, tab2 = st.tabs(["权益曲线", "资金曲线明细表"])

        with tab1:
            st.line_chart(eq[["value"]], height=400)

            if time_ret_df is not None and not time_ret_df.empty:
                tr = time_ret_df.copy()
                if "datetime" in tr.columns:
                    tr["datetime"] = pd.to_datetime(tr["datetime"])
                    tr = tr.set_index("datetime")
                st.area_chart(tr[["return"]], height=200)
        with tab2:
            st.dataframe(eq.reset_index(), use_container_width=True)
    else:
        st.info("当前回测未找到 `equity_curve.csv`，无法展示权益曲线。")

    # -------- 交易记录 --------
    st.subheader("交易记录明细")

    if trades_df is not None and not trades_df.empty:
        df = trades_df.copy()

        # 简单的中文列名映射（若存在相应列）
        rename_map = {
            "symbol": "标的",
            "direction": "方向",
            "size": "手数",
            "entry_dt": "开仓时间",
            "entry_price": "开仓价格",
            "exit_dt": "平仓时间",
            "exit_price": "平仓价格",
            "pnl": "盈亏",
            "pnlcomm": "扣费后盈亏",
        }
        df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns},
            inplace=True,
        )

        # 简单筛选：按标的过滤
        symbol_col = "标的" if "标的" in df.columns else "symbol" if "symbol" in df.columns else None
        if symbol_col:
            symbols = sorted(df[symbol_col].dropna().unique().tolist())
            selected_symbol = st.selectbox(
                "按标的过滤（可选）", options=["全部"] + symbols, index=0
            )
            if selected_symbol != "全部":
                df = df[df[symbol_col] == selected_symbol]

        st.dataframe(df, use_container_width=True, height=400)
    else:
        st.info("当前回测未找到 `trades.csv`，无法展示交易记录。")

    st.markdown("---")
    st.caption(
        "说明：本界面仅做结果展示，不参与交易。若要更新结果，请重新运行回测脚本后刷新本页面。"
    )


if __name__ == "__main__":
    main()

