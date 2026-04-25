from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# HTML templates (static pages)
# -----------------------------

INDEX_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="X-UA-Compatible" content="IE=Edge" />
  <title>__TITLE__</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif; margin: 24px; }
    a { display:block; margin: 10px 0; font-size: 16px; }
    .meta { color:#666; font-size: 12px; margin-top: 16px; }
  </style>
</head>
<body>
  <h2>__TITLE__</h2>
  <a href="分析报告.html">1) 分析报告</a>
  <a href="资金曲线.html">2) 资金曲线</a>
  <a href="阶段总结.html">3) 阶段总结</a>
  <a href="交易详细.html">4) 交易详细</a>
  <div class="meta">生成时间：__GENERATED_AT__</div>
</body>
</html>"""

ANALYSIS_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta http-equiv="X-UA-Compatible" content="IE=9" />
  <meta charset="utf-8" />
  __JQUERY_TAG__
  <title>分析报告</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif; }
    table { width: 520px; font-size: 12px; border-collapse: collapse; table-layout: fixed; }
    td { border: 1px solid #d0d0d0; padding: 6px; word-break: break-all; }
    tr:nth-child(odd) { background: #f8fbff; }
    .hdr { background:#0062A8; color:#fff; text-align:center; font-weight:600; }
  </style>
</head>
<script type="text/javascript">
function getJson(datas){
  var tableHtml = "";
  for (var i in datas) {
    tableHtml += "<tr>";
    tableHtml += "<td>" + datas[i].name + "</td>";
    tableHtml += "<td>" + datas[i].value + "</td>";
    tableHtml += "</tr>";
  }
  document.getElementById('aj_data').innerHTML = tableHtml;
}
</script>
<body>
  <table cellpadding="3" cellspacing="0" border="0">
    <tr><td colspan="2" class="hdr">分析报告</td></tr>
    <tbody id="aj_data"></tbody>
  </table>
  <p style="font-size:12px; color:#666">提示：同目录下还有 <a href="index.html">index.html</a> 导航页。</p>
</body>
<script type="text/javascript" src="./ReportInfo.json"></script>
</html>"""

EQUITY_HTML = """<!DOCTYPE html>
<html style="height:100%" lang="zh">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=9">
  <meta charset="utf-8">
  __JQUERY_TAG__
  __ECHARTS_TAG__
  <title>资金曲线</title>
</head>
<body style="height: 90%;">
  <div id="main" style="width: 100%; height: 100%;"></div>
  <script type="text/javascript">
    var dom = document.getElementById('main');
    var myChart = echarts.init(dom);
    function getJson(data){
      var option = {
        title: { text: '资金收益曲线' },
        tooltip: {
          trigger: 'axis',
          formatter: function(params){
            var res = '时间:' + params[0].name;
            for (var i=0;i<params.length;i++){
              var val = params[i].value;
              if (params[i].seriesName === '收益率') {
                val = (val * 100).toFixed(2) + '%';
              }
              res += '<br/>' + params[i].seriesName + ' : ' + val;
            }
            return res;
          }
        },
        toolbox: { show: true, feature: { dataView: { show: true, readOnly: false }, restore: { show: true } } },
        dataZoom: { show: true, start: 0 },
        legend: {
          data: ['动态权益','静态权益','可用资金','手续费','收益率'],
          selected: { '动态权益': false, '静态权益': false, '可用资金': false, '手续费': false, '收益率': true }
        },
        grid: { y2: 80 },
        xAxis: [{ data: data.Time }],
        yAxis: [
          { type: 'value', scale: true, axisLabel: { formatter: function(v){ return (v * 100).toFixed(2) + '%'; } } }
        ],
        series: [
          { name:'手续费', type:'line', data: data.TCost },
          { name:'可用资金', type:'line', data: data.Avail },
          { name:'静态权益', type:'line', data: data.Statric },
          { name:'动态权益', type:'line', data: data.Dynamic },
          { name:'收益率', type:'line', data: data.Yield }
        ]
      };
      myChart.setOption(option);
      window.onresize = myChart.resize;
    }
  </script>
  <script type="text/javascript" src="./EquityCurve.json"></script>
</body>
</html>"""

STAGE_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh">
<head>
  <meta http-equiv="X-UA-Compatible" content="IE=9" />
  <meta charset="utf-8" />
  __JQUERY_TAG__
  <title>阶段总结</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif; }
    table { border: solid 1px #666; border-collapse: collapse; width: 900px; font-size: 12px; }
    th, td { border: 1px solid #d0d0d0; padding: 6px; }
    .hdr { text-align:center; background-color:#0062A8; color:#FFFFFF; font-weight:600; }
    .subhdr { text-align:left; background-color:#CAE9FB; }
  </style>
</head>
<body>

<table cellpadding="3" cellspacing="0" border="0">
  <tr><td colspan="7" class="hdr">年度分析</td></tr>
  <tr class="subhdr">
    <td width="115">年份</td><td width="115">权益</td><td width="115">净利润</td>
    <td width="115">盈利率</td><td width="115">胜率</td><td width="115">平均盈利/亏损</td><td width="115">权益增长速度</td>
  </tr>
  <tbody id="aj_yaerdata"></tbody>
</table>

<p></p>
<table cellpadding="3" cellspacing="0" border="0">
  <tr><td colspan="7" class="hdr">季度分析</td></tr>
  <tr class="subhdr">
    <td width="115">季度</td><td width="115">权益</td><td width="115">净利润</td>
    <td width="115">盈利率</td><td width="115">胜率</td><td width="115">平均盈利/亏损</td><td width="115">权益增长速度</td>
  </tr>
  <tbody id="aj_quarterdata"></tbody>
</table>

<p></p>
<table cellpadding="3" cellspacing="0" border="0">
  <tr><td colspan="7" class="hdr">月度分析</td></tr>
  <tr class="subhdr">
    <td width="115">月份</td><td width="115">权益</td><td width="115">净利润</td>
    <td width="115">盈利率</td><td width="115">胜率</td><td width="115">平均盈利/亏损</td><td width="115">权益增长速度</td>
  </tr>
  <tbody id="aj_monthdata"></tbody>
</table>

</body>

<script type="text/javascript">
function getJson(datas) {
  var YaerHtml = "";
  var QuarterHtml = "";
  var MonthHtml = "";
  for (var i in datas) {
    if (datas[i].type==0) {
      YaerHtml += "<tr>"+
        "<td>"+datas[i].Date+"</td>"+
        "<td>"+datas[i].Equity+"</td>"+
        "<td>"+datas[i].NetProfit+"</td>"+
        "<td>"+datas[i].YieldRate+"</td>"+
        "<td>"+datas[i].WinRate+"</td>"+
        "<td>"+datas[i].AvgWinLose+"</td>"+
        "<td>"+datas[i].IncSpeed+"</td>"+
      "</tr>";
    }
    if (datas[i].type==1) {
      QuarterHtml += "<tr>"+
        "<td>"+datas[i].Date+"</td>"+
        "<td>"+datas[i].Equity+"</td>"+
        "<td>"+datas[i].NetProfit+"</td>"+
        "<td>"+datas[i].YieldRate+"</td>"+
        "<td>"+datas[i].WinRate+"</td>"+
        "<td>"+datas[i].AvgWinLose+"</td>"+
        "<td>"+datas[i].IncSpeed+"</td>"+
      "</tr>";
    }
    if (datas[i].type==2) {
      MonthHtml += "<tr>"+
        "<td>"+datas[i].Date+"</td>"+
        "<td>"+datas[i].Equity+"</td>"+
        "<td>"+datas[i].NetProfit+"</td>"+
        "<td>"+datas[i].YieldRate+"</td>"+
        "<td>"+datas[i].WinRate+"</td>"+
        "<td>"+datas[i].AvgWinLose+"</td>"+
        "<td>"+datas[i].IncSpeed+"</td>"+
      "</tr>";
    }
  }
  document.getElementById('aj_yaerdata').innerHTML = YaerHtml;
  document.getElementById('aj_quarterdata').innerHTML = QuarterHtml;
  document.getElementById('aj_monthdata').innerHTML = MonthHtml;
}
</script>
<script type="text/javascript" src="./StageStatis.json"></script>
</html>"""

TRADES_HTML = """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh">
<head>
  <meta http-equiv="X-UA-Compatible" content="IE=9" />
  <meta charset="utf-8" />
  __JQUERY_TAG__
  <title>交易详细</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif; }
    table { border: solid 1px #666; border-collapse: collapse; width: 100%; font-size: 12px; }
    th, td { border: 1px solid #d0d0d0; padding: 6px; }
    thead tr { background-color:#CAE9FB; text-align:left; }
    tr.hover { background: #f8fbff; }
  </style>
</head>
<body>
  <table>
    <thead>
      <tr>
        <th width="115">时间</th>
        <th width="115">合约</th>
        <th width="115">交易类型</th>
        <th width="115">下单类型</th>
        <th width="115">成交数量</th>
        <th width="115">成交价</th>
        <th width="115">成交额</th>
        <th width="115">委托数量</th>
        <th width="115">平仓盈亏</th>
        <th width="115">手续费</th>
        <th width="115">滑点损耗</th>
      </tr>
    </thead>
    <tbody id="aj_data"></tbody>
  </table>
</body>

<script type="text/javascript">
function getJson(datas) {
  var tableHtml = "";
  for (var i in datas) {
    tableHtml += (i==0) ? "<tr class=\\"hover\\">" : "<tr>";
    tableHtml += "<td>" + datas[i].DateTime + "</td>";
    tableHtml += "<td>" + datas[i].Cont + "</td>";
    tableHtml += "<td>" + datas[i].TradeType + "</td>";
    tableHtml += "<td>" + datas[i].OrderType + "</td>";
    tableHtml += "<td>" + datas[i].MarketQty + "</td>";
    tableHtml += "<td>" + datas[i].OrderPrice + "</td>";
    tableHtml += "<td>" + datas[i].Turnover + "</td>";
    tableHtml += "<td>" + datas[i].OrderQty + "</td>";
    tableHtml += "<td>" + datas[i].LiquidateProfit + "</td>";
    tableHtml += "<td>" + datas[i].Cost + "</td>";
    tableHtml += "<td>" + datas[i].SlippageLoss + "</td>";
    tableHtml += "</tr>";
  }
  document.getElementById('aj_data').innerHTML = tableHtml;
}
</script>

<script type="text/javascript" src="./OrderInfo.json"></script>
</html>"""


# -----------------------------
# Data helpers
# -----------------------------

def _to_dict_result(result: Any) -> Dict[str, Any]:
    try:
        return asdict(result)
    except Exception:
        return result if isinstance(result, dict) else {}


def _dt_parse(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(s), fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return None


def _fmt_time_for_report(dt: datetime) -> str:
    # match sample: YYYYMMDD HH:MM
    return dt.strftime("%Y%m%d %H:%M")


def _json_js_call(obj: Any) -> str:
    return "getJson(" + json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + ")"


def _calc_daily_sharpe_from_equity(eq_df: pd.DataFrame) -> Tuple[float, float]:
    """Return (daily_std, sharpe). Risk-free assumed 0."""
    if eq_df.empty or "value" not in eq_df.columns:
        return float("nan"), float("nan")
    s = eq_df.set_index("datetime")["value"].astype(float).sort_index()
    # resample to daily close
    daily = s.resample("1D").last().dropna()
    if len(daily) < 3:
        return float("nan"), float("nan")
    dr = daily.pct_change().dropna()
    if dr.std(ddof=1) == 0 or math.isnan(float(dr.std(ddof=1))):
        return float("nan"), float("nan")
    sharpe = (dr.mean() / dr.std(ddof=1)) * math.sqrt(252)
    return float(dr.std(ddof=1)), float(sharpe)


def _extract_mult(cfg: Dict[str, Any], symbol: str) -> float:
    # best-effort: cfg.symbols.<name>.mult, else commission_default.mult
    # In this starter, trade symbol is usually data feed name (e.g., 'tushare').
    symbols = (cfg.get("symbols") or {})
    if isinstance(symbols, dict):
        # if a key matches, use it
        spec = symbols.get(symbol)
        if isinstance(spec, dict) and spec.get("mult") is not None:
            try:
                return float(spec.get("mult"))
            except Exception:
                pass
    try:
        return float(((cfg.get("commission_default") or {}).get("mult")) or 1.0)
    except Exception:
        return 1.0


def build_report_info(cfg: Dict[str, Any], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    start_value = float(payload.get("start_value", float("nan")))
    end_value = float(payload.get("end_value", float("nan")))
    dd = payload.get("drawdown") or {}
    max_dd_pct = float(dd.get("max_drawdown_pct", float("nan")))
    max_moneydown = float(dd.get("max_moneydown", float("nan")))

    realized_pnl = float(payload.get("realized_pnl", 0.0) or 0.0)
    floating_pnl = float(payload.get("floating_pnl", 0.0) or 0.0)
    open_positions = payload.get("open_positions") or []

    trades = payload.get("trades") or []
    fills = payload.get("fills") or []
    equity = payload.get("equity_curve") or []
    eq_df = pd.DataFrame(equity)
    if not eq_df.empty and "datetime" in eq_df.columns:
        eq_df["datetime"] = pd.to_datetime(eq_df["datetime"], errors="coerce")
        eq_df = eq_df.dropna(subset=["datetime"])

    start_dt = eq_df["datetime"].min().to_pydatetime() if not eq_df.empty else None
    end_dt = eq_df["datetime"].max().to_pydatetime() if not eq_df.empty else None
    days = (end_dt.date() - start_dt.date()).days + 1 if start_dt and end_dt else None

    pnlcomm = [float(t.get("pnlcomm", 0.0)) for t in trades if t is not None]
    wins = [x for x in pnlcomm if x > 0]
    loses = [x for x in pnlcomm if x < 0]
    win_times = len(wins)
    lose_times = len(loses)
    total_trades = len(pnlcomm)
    win_rate = (win_times / total_trades) if total_trades else float("nan")
    avg_win = (sum(wins) / win_times) if win_times else 0.0
    avg_loss = (sum(loses) / lose_times) if lose_times else 0.0
    pnl_ratio = (avg_win / abs(avg_loss)) if avg_loss else float("nan")

    total_comm = 0.0
    for f in fills:
        try:
            total_comm += abs(float(f.get("commission", 0.0) or 0.0))
        except Exception:
            pass
    if total_comm == 0.0:
        for t in trades:
            try:
                comm = float(t.get("pnl", 0.0)) - float(t.get("pnlcomm", 0.0))
                if comm > 0:
                    total_comm += comm
            except Exception:
                pass

    daily_std, sharpe = _calc_daily_sharpe_from_equity(eq_df) if not eq_df.empty else (float("nan"), float("nan"))

    syms = sorted({str(t.get("symbol")) for t in trades if t.get("symbol")})
    if not syms:
        syms = sorted({str(f.get("symbol")) for f in fills if f.get("symbol")})
    if not syms:
        syms = sorted({str(p.get("symbol")) for p in open_positions if p.get("symbol")})
    sym_str = ",".join(syms) if syms else "(未知)"

    tf_txt = "(未知)"
    try:
        data_cfgs = cfg.get("data") or []
        if data_cfgs:
            first = data_cfgs[0]
            timeframe = str(first.get("timeframe") or "").lower()
            compression = int(first.get("compression") or 1)
            if timeframe in {"minutes", "minute", "min", "m"}:
                tf_txt = f"{compression}min"
            elif timeframe in {"days", "day", "d"}:
                tf_txt = "日线" if compression == 1 else f"{compression}日"
            elif timeframe in {"weeks", "week", "w"}:
                tf_txt = "周线" if compression == 1 else f"{compression}周"
            elif timeframe in {"months", "month", "mo"}:
                tf_txt = "月线" if compression == 1 else f"{compression}月"
    except Exception:
        pass

    items: List[Dict[str, Any]] = []
    items.append({"name": "初始资金：", "value": f"{start_value:,.2f}" if math.isfinite(start_value) else "NaN"})
    items.append({"name": "合约信息", "value": sym_str})
    items.append({"name": "K线周期：", "value": tf_txt})
    if start_dt:
        items.append({"name": "计算开始时间", "value": int(start_dt.strftime("%Y%m%d"))})
    if end_dt:
        items.append({"name": "计算结束时间", "value": int(end_dt.strftime("%Y%m%d"))})
    if days is not None:
        items.append({"name": "测试天数", "value": int(days)})
    items.append({"name": "最终权益", "value": f"{end_value:,.2f}" if math.isfinite(end_value) else "NaN"})

    if math.isfinite(start_value) and start_value != 0:
        items.append({"name": "总收益率", "value": f"{(end_value / start_value - 1.0) * 100.0:.2f}%"})

    items.append({"name": "已实现净利润", "value": f"{realized_pnl:,.2f}"})
    items.append({"name": "未实现浮动盈亏", "value": f"{floating_pnl:,.2f}"})
    items.append({"name": "未平仓合约数", "value": int(len(open_positions))})

    if math.isfinite(max_moneydown):
        items.append({"name": "权益最大回撤", "value": f"{max_moneydown:,.2f}"})
    if math.isfinite(max_dd_pct):
        items.append({"name": "权益最大回撤比", "value": f"{max_dd_pct:.2f}%"})

    if math.isfinite(daily_std):
        items.append({"name": "标准离差(按日收益率)", "value": f"{daily_std:.6f}"})
    if math.isfinite(sharpe):
        items.append({"name": "夏普比率(按日)", "value": f"{sharpe:.2f}"})

    items.append({"name": "交易次数(已平仓)", "value": int(total_trades)})
    if math.isfinite(win_rate):
        items.append({"name": "胜率", "value": f"{win_rate * 100.0:.2f}%"})
    items.append({"name": "平均盈利", "value": f"{avg_win:,.2f}"})
    items.append({"name": "平均亏损", "value": f"{avg_loss:,.2f}"})
    if math.isfinite(pnl_ratio):
        items.append({"name": "盈亏比(均值)", "value": f"{pnl_ratio:.2f}"})
    items.append({"name": "手续费(估算)", "value": f"{total_comm:,.2f}"})
    return items

def build_equity_curve(cfg: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, List[Any]]:
    snapshots = payload.get("snapshots") or []
    if snapshots:
        start_value = float(payload.get("start_value", 0.0) or 0.0)
        if start_value == 0 and snapshots:
            start_value = float(snapshots[0].get("dynamic_equity", 0.0) or 0.0)

        time_list: List[str] = []
        tdate: List[int] = []
        avail: List[float] = []
        dynamic: List[float] = []
        lmargin: List[float] = []
        smargin: List[float] = []
        static: List[float] = []
        tcost: List[float] = []
        yld: List[float] = []

        for x in snapshots:
            dt = pd.to_datetime(x["dt"])
            time_list.append(dt.strftime("%Y%m%d %H:%M"))
            tdate.append(int(dt.strftime("%Y%m%d")))
            avail.append(round(float(x.get("available", 0.0) or 0.0), 2))
            dynamic.append(round(float(x.get("dynamic_equity", 0.0) or 0.0), 2))
            lmargin.append(round(float(x.get("l_margin", 0.0) or 0.0), 2))
            smargin.append(round(float(x.get("s_margin", 0.0) or 0.0), 2))
            static.append(round(float(x.get("static_equity", 0.0) or 0.0), 2))
            tcost.append(round(float(x.get("fee_cum", 0.0) or 0.0), 2))
            if start_value:
                yld.append(float(x.get("dynamic_equity", 0.0) or 0.0) / start_value - 1.0)
            else:
                yld.append(0.0)

        return {
            "Avail": avail,
            "Dynamic": dynamic,
            "LMargin": lmargin,
            "SMargin": smargin,
            "Statric": static,
            "TCost": tcost,
            "TDate": tdate,
            "Time": time_list,
            "Yield": yld,
        }

    equity = payload.get("equity_curve") or []
    if not equity:
        return {k: [] for k in ["Avail", "Dynamic", "LMargin", "SMargin", "Statric", "TCost", "TDate", "Time", "Yield"]}

    eq_df = pd.DataFrame(equity)
    eq_df["datetime"] = pd.to_datetime(eq_df["datetime"], errors="coerce")
    eq_df = eq_df.dropna(subset=["datetime"]).sort_values("datetime")
    eq_df["value"] = eq_df["value"].astype(float)
    eq_df["cash"] = eq_df.get("cash", 0.0).astype(float)

    start_value = float(payload.get("start_value", eq_df["value"].iloc[0]))
    if not math.isfinite(start_value) or start_value == 0:
        start_value = float(eq_df["value"].iloc[0])

    times: List[str] = []
    dynamic: List[float] = []
    static: List[float] = []
    avail: List[float] = []
    lmargin: List[float] = []
    smargin: List[float] = []
    tcost: List[float] = []
    tdate: List[int] = []
    yld: List[float] = []

    cum_comm = 0.0
    for _, r in eq_df.iterrows():
        dt: datetime = r["datetime"].to_pydatetime()
        t = _fmt_time_for_report(dt)
        times.append(t)
        # 优先使用 dynamic_equity（包含持仓盈亏），否则用 value
        dyn_eq = float(r.get("dynamic_equity", r.get("value", 0.0)))
        static_eq = float(r.get("static_equity", r.get("value", 0.0)))
        cash = float(r.get("cash", 0.0))
        val = float(r.get("value", dyn_eq))
        dynamic.append(round(dyn_eq, 2))
        static.append(round(static_eq, 2))
        avail.append(round(cash, 2))
        lmargin.append(round(float(r.get("l_margin", 0.0)), 2))
        smargin.append(round(float(r.get("s_margin", 0.0)), 2))
        tcost.append(round(cum_comm, 2))
        tdate.append(int(dt.strftime("%Y%m%d")))
        yld.append(dyn_eq / start_value - 1.0)

    return {
        "Avail": avail,
        "Dynamic": dynamic,
        "LMargin": lmargin,
        "SMargin": smargin,
        "Statric": static,
        "TCost": tcost,
        "TDate": tdate,
        "Time": times,
        "Yield": yld,
    }

def build_stage_stats(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    equity = payload.get("equity_curve") or []
    if not equity:
        return []
    eq_df = pd.DataFrame(equity)
    eq_df["datetime"] = pd.to_datetime(eq_df["datetime"], errors="coerce")
    eq_df = eq_df.dropna(subset=["datetime"]).sort_values("datetime")
    eq_df["value"] = eq_df["value"].astype(float)
    eq_df = eq_df.set_index("datetime")

    trades = payload.get("trades") or []
    tdf = pd.DataFrame(trades) if trades else pd.DataFrame(columns=["exit_dt", "pnlcomm"])
    if not tdf.empty:
        tdf["exit_dt"] = pd.to_datetime(tdf.get("exit_dt"), errors="coerce")
        tdf = tdf.dropna(subset=["exit_dt"]).sort_values("exit_dt")
        tdf["pnlcomm"] = tdf.get("pnlcomm", 0.0).astype(float)

    out: List[Dict[str, Any]] = []
    # year / quarter / month
    for typ, rule in [
        (0, "YE"),
        (1, "QE"),
        (2, "ME"),
    ]:
        groups = eq_df["value"].resample(rule)
        for period_end, s in groups:
            if s.dropna().empty:
                continue
            start_eq = float(s.dropna().iloc[0])
            end_eq = float(s.dropna().iloc[-1])
            net_profit = end_eq - start_eq
            yld = (end_eq / start_eq - 1.0) * 100.0 if start_eq else float("nan")

            # trades in this period
            if not tdf.empty:
                if rule == "YE":
                    mask = (tdf["exit_dt"].dt.year == period_end.year)
                elif rule == "QE":
                    q = ((tdf["exit_dt"].dt.month - 1) // 3) + 1
                    mask = (tdf["exit_dt"].dt.year == period_end.year) & (q == ((period_end.month - 1) // 3) + 1)
                else:
                    mask = (tdf["exit_dt"].dt.year == period_end.year) & (tdf["exit_dt"].dt.month == period_end.month)
                pnl = tdf.loc[mask, "pnlcomm"].tolist()
            else:
                pnl = []

            trade_times = len(pnl)
            wins = [x for x in pnl if x > 0]
            loses = [x for x in pnl if x < 0]
            win_times = len(wins)
            lose_times = len(loses)
            win_rate = (win_times / trade_times) * 100.0 if trade_times else 0.0
            avg_win = sum(wins) / win_times if wins else 0.0
            avg_loss = sum(loses) / lose_times if loses else 0.0
            avg_wl = (avg_win / abs(avg_loss)) * 100.0 if avg_loss else 0.0

            # format label without relying on Period freq parsing
            if rule == "YE":
                label = f"{period_end.year}年"
            elif rule == "QE":
                q = ((period_end.month - 1) // 3) + 1
                label = f"{period_end.year}年第{q}季度"
            else:
                label = f"{period_end.year}年{period_end.month:02d}月"

            out.append(
                {
                    "Date": label,
                    "Equity": f"{end_eq:.2f}",
                    "NetProfit": f"{net_profit:.2f}",
                    "YieldRate": f"{yld:.2f}%" if math.isfinite(yld) else "NaN",
                    "WinRate": f"{win_rate:.2f}%",
                    "AvgWinLose": f"{avg_wl:.2f}%",
                    "IncSpeed": f"{yld:.2f}%" if math.isfinite(yld) else "NaN",
                    "TradeTimes": trade_times,
                    "WinTimes": win_times,
                    "LoseTimes": lose_times,
                    "TotalWin": float(sum(wins)) if wins else 0.0,
                    "TotalLose": float(abs(sum(loses))) if loses else 0.0,
                    "type": typ,
                }
            )
    return out


def build_order_info(cfg: Dict[str, Any], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    fills = payload.get("fills") or []
    if fills:
        out: List[Dict[str, Any]] = []
        trade_type_map = {
            "BUY_OPEN": "买开",
            "SELL_CLOSE": "卖平",
            "SELL_OPEN": "卖开",
            "BUY_CLOSE": "买平",
        }
        order_type_map = {
            "market": "市价单",
            "limit": "限价单",
            "stop": "止损单",
            "stoplimit": "止损限价单",
        }

        for f in fills:
            fill_qty = f.get("fill_qty")
            order_qty = f.get("order_qty")
            fill_price = float(f.get("fill_price", 0.0) or 0.0)
            out.append(
                {
                    "Cont": str(f.get("symbol") or ""),
                    "TradeType": trade_type_map.get(str(f.get("trade_type") or ""), str(f.get("trade_type") or "")),
                    "OrderType": order_type_map.get(str(f.get("order_type") or "").lower(), str(f.get("order_type") or "")),
                    "DateTime": str(f.get("dt") or ""),
                    "MarketQty": int(fill_qty) if isinstance(fill_qty, (int, float)) and float(fill_qty).is_integer() else fill_qty,
                    "OrderQty": int(order_qty) if isinstance(order_qty, (int, float)) and float(order_qty).is_integer() else order_qty,
                    "OrderPrice": f"{fill_price:.6f}".rstrip("0").rstrip("."),
                    "Turnover": f"{float(f.get('turnover', 0.0) or 0.0):.2f}",
                    "LiquidateProfit": f"{float(f.get('realized_pnl', 0.0) or 0.0):.2f}",
                    "Cost": f"{float(f.get('commission', 0.0) or 0.0):.2f}",
                    "SlippageLoss": f"{float(f.get('slippage_loss', 0.0) or 0.0):.2f}",
                }
            )
        return out

    trades = payload.get("trades") or []
    out: List[Dict[str, Any]] = []
    for t in trades:
        symbol = str(t.get("symbol") or "DATA")
        direction = str(t.get("direction") or "unknown")
        size = float(t.get("size") or 0.0)
        entry_dt = _dt_parse(t.get("entry_dt"))
        exit_dt = _dt_parse(t.get("exit_dt"))
        entry_price = t.get("entry_price")
        exit_price = t.get("exit_price")

        try:
            comm = float(t.get("pnl", 0.0)) - float(t.get("pnlcomm", 0.0))
            comm = comm if comm > 0 else 0.0
        except Exception:
            comm = 0.0
        pnlcomm = float(t.get("pnlcomm", 0.0) or 0.0)
        mult = _extract_mult(cfg, symbol)

        def mk(ttype: str, dt: Optional[datetime], price: Any, profit: float, cost: float) -> Dict[str, Any]:
            if not dt:
                dt = datetime.now()
            try:
                p = float(price)
            except Exception:
                p = 0.0
            turnover = p * float(size) * float(mult)
            return {
                "Cont": symbol,
                "TradeType": ttype,
                "OrderType": "市价单",
                "DateTime": _fmt_time_for_report(dt),
                "MarketQty": int(size) if float(size).is_integer() else size,
                "OrderQty": int(size) if float(size).is_integer() else size,
                "OrderPrice": str(p),
                "Turnover": f"{turnover:.2f}",
                "LiquidateProfit": f"{profit:.2f}",
                "Cost": f"{cost:.2f}",
                "SlippageLoss": "0.00",
            }

        if direction == "long":
            open_t, close_t = "买开", "卖平"
        elif direction == "short":
            open_t, close_t = "卖开", "买平"
        else:
            open_t, close_t = "开仓", "平仓"

        half = comm / 2.0 if comm else 0.0
        out.append(mk(open_t, entry_dt, entry_price, 0.0, half))
        out.append(mk(close_t, exit_dt, exit_price, pnlcomm, comm - half))

    return out


def write_html_report(
    run_dir: Path,
    cfg: Dict[str, Any],
    result: Any,
    title: str = "回测报告",
    asset_dir: Optional[str | Path] = None,
    out_folder: str = "report_html",
) -> Path:
    """Generate an offline HTML report folder.

    Output structure (similar to你给的示例报告):
      report_html/
        index.html
        分析报告.html + ReportInfo.json
        资金曲线.html + EquityCurve.json
        阶段总结.html + StageStatis.json
        交易详细.html + OrderInfo.json
        (optional) jquery.min.js / echarts-en.common.js
    """
    run_dir = Path(run_dir)
    report_dir = run_dir / out_folder
    report_dir.mkdir(parents=True, exist_ok=True)

    payload = _to_dict_result(result)

    # choose assets
    jquery_tag = '<script src="jquery.min.js"></script>'
    echarts_tag = '<script src="echarts-en.common.js"></script>'
    copied_assets = False

    if asset_dir:
        ad = Path(asset_dir).expanduser().resolve()
        jq = ad / "jquery.min.js"
        ec = ad / "echarts-en.common.js"
        if jq.exists() and ec.exists():
            (report_dir / "jquery.min.js").write_bytes(jq.read_bytes())
            (report_dir / "echarts-en.common.js").write_bytes(ec.read_bytes())
            copied_assets = True

    if not copied_assets:
        # fallback to CDN (if用户网络可用)
        jquery_tag = '<script src="https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js"></script>'
        echarts_tag = '<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>'

    # pages
    now_txt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    (report_dir / "index.html").write_text(
        INDEX_HTML.replace("__TITLE__", title).replace("__GENERATED_AT__", now_txt),
        encoding="utf-8",
    )
    (report_dir / "分析报告.html").write_text(
        ANALYSIS_HTML.replace("__JQUERY_TAG__", jquery_tag),
        encoding="utf-8",
    )
    (report_dir / "资金曲线.html").write_text(
        EQUITY_HTML.replace("__JQUERY_TAG__", jquery_tag).replace("__ECHARTS_TAG__", echarts_tag),
        encoding="utf-8",
    )
    (report_dir / "阶段总结.html").write_text(
        STAGE_HTML.replace("__JQUERY_TAG__", jquery_tag),
        encoding="utf-8",
    )
    (report_dir / "交易详细.html").write_text(
        TRADES_HTML.replace("__JQUERY_TAG__", jquery_tag),
        encoding="utf-8",
    )

    # data (as JS calling getJson)
    report_info = build_report_info(cfg=cfg, payload=payload)
    (report_dir / "ReportInfo.json").write_text(_json_js_call(report_info), encoding="utf-8")

    equity_curve = build_equity_curve(cfg=cfg, payload=payload)
    (report_dir / "EquityCurve.json").write_text(_json_js_call(equity_curve), encoding="utf-8")

    stage_stats = build_stage_stats(payload=payload)
    (report_dir / "StageStatis.json").write_text(_json_js_call(stage_stats), encoding="utf-8")

    order_info = build_order_info(cfg=cfg, payload=payload)
    (report_dir / "OrderInfo.json").write_text(_json_js_call(order_info), encoding="utf-8")

    return report_dir

