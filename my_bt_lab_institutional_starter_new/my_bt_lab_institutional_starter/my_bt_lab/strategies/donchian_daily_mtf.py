import math
import backtrader as bt

from my_bt_lab.strategies.base_strategy import BaseStrategy
from my_bt_lab.utils.market_rules import round_price_to_tick, round_size_to_step, format_price


class DonchianDailyMtfStrategy(BaseStrategy):
    """
    日线信号 + 执行周期下单 的 Donchian / ATR 趋势策略。

    设计目标：尽量贴近你原来的“极致量化平台脚本”思路：
    - 执行周期（如 5m）负责触发与下单
    - 信号周期（如 1d）负责生成 20 日突破 / 10 日退出 / ATR
    - 支持多空、固定止损、两档移动止损
    - 支持 range / range_then_close 两种入场方式
    - 支持同日平仓后再入场过滤（需突破当日此前高/低）

    说明：这里的“同日”按执行周期 bar 的 date() 识别。
    若你后面想做更严格的“期货交易日（含夜盘）”划分，再单独加一个 trade_date 映射函数即可。
    """

    params = dict(
        entry_lookback_days=20,
        exit_lookback_days=10,
        breakout_add_ticks=1,
        entry_mode="range",  # range / range_then_close
        atr_period=20,
        atr_mult=2.0,
        trail_lv1_atr=2.0,
        trail_lv2_atr=5.0,
        trail_lock_atr=2.0,
        risk_cash=10000.0,
        max_positions=99,
        min_size=1,
        printlog=True,
        symbol_specs=None,
        data_roles=None,
    )

    def __init__(self):
        super().__init__()
        self.symbol_specs = self.p.symbol_specs or {}
        self.data_roles = self.p.data_roles or {}

        self.exec_data_by_symbol = {}
        self.signal_data_by_symbol = {}
        self.orders = {}
        self.order_meta = {}
        self.signal_inds = {}
        self.last_signal_dt = {}
        self.state = {}

        for d in self.datas:
            name = getattr(d, "_name", "")
            meta = self.data_roles.get(name, {})
            symbol = str(meta.get("symbol") or name)
            role = str(meta.get("role") or "both").lower().strip()

            if role in {"exec", "both"}:
                self.exec_data_by_symbol[symbol] = d
            if role in {"signal", "both"}:
                self.signal_data_by_symbol[symbol] = d

        all_symbols = sorted(set(self.exec_data_by_symbol) | set(self.signal_data_by_symbol))
        for symbol in all_symbols:
            exec_d = self.exec_data_by_symbol.get(symbol)
            signal_d = self.signal_data_by_symbol.get(symbol)

            if exec_d is None and signal_d is not None:
                exec_d = signal_d
                self.exec_data_by_symbol[symbol] = exec_d
            if signal_d is None and exec_d is not None:
                signal_d = exec_d
                self.signal_data_by_symbol[symbol] = signal_d

            if exec_d is None or signal_d is None:
                continue

            atr = bt.ind.ATR(signal_d, period=self.p.atr_period)

            self.orders[exec_d] = None
            self.signal_inds[symbol] = {"atr": atr, "exec_data": exec_d, "signal_data": signal_d}
            self.last_signal_dt[symbol] = None
            self.state[symbol] = self._new_state()

    def _new_state(self):
        return {
            "signal_ready": False,
            "h20": None,
            "l20": None,
            "h10": None,
            "l10": None,
            "atr": None,
            "signal_dt": None,

            "entry_atr_long": None,
            "entry_atr_short": None,
            "stop_long": None,
            "stop_short": None,

            "pending_long": False,
            "pending_short": False,
            "pending_long_price": None,
            "pending_short_price": None,
            "pending_day": None,

            "trade_day": None,
            "today_high": None,
            "today_low": None,
            "last_exec_dt": None,

            "last_exit_day": None,
            "last_exit_dt": None,
            "wait_reentry_long": False,

            "last_exit_day_short": None,
            "last_exit_dt_short": None,
            "wait_reentry_short": False,
        }

    def _symbol_of(self, data) -> str:
        name = getattr(data, "_name", "") or ""
        meta = self.data_roles.get(name, {})
        return str(meta.get("symbol") or name)

    def _spec(self, data_or_symbol):
        if isinstance(data_or_symbol, str):
            symbol = data_or_symbol
        else:
            symbol = self._symbol_of(data_or_symbol)
        return self.symbol_specs.get(symbol, {})

    def _tick_size(self, data_or_symbol) -> float:
        spec = self._spec(data_or_symbol)
        return float(spec.get("tick_size", 0) or 0)

    def _price_precision(self, data_or_symbol):
        spec = self._spec(data_or_symbol)
        if "price_precision" in spec:
            return int(spec["price_precision"])
        return None

    def _size_step(self, data_or_symbol) -> float:
        spec = self._spec(data_or_symbol)
        return float(spec.get("size_step", 1) or 1)

    def _min_size(self, data_or_symbol) -> float:
        spec = self._spec(data_or_symbol)
        return float(spec.get("min_size", self.p.min_size) or self.p.min_size)

    def _fmt_price(self, data_or_symbol, price) -> str:
        tick = self._tick_size(data_or_symbol)
        precision = self._price_precision(data_or_symbol)
        return format_price(price, tick_size=tick if tick > 0 else None, price_precision=precision)

    def _round_price(self, data_or_symbol, price: float, mode: str = "nearest") -> float:
        tick = self._tick_size(data_or_symbol)
        if tick > 0:
            return round_price_to_tick(price, tick_size=tick, mode=mode)
        return float(price)

    def _round_size(self, data_or_symbol, size: float) -> int:
        return round_size_to_step(
            size=size,
            size_step=self._size_step(data_or_symbol),
            min_size=self._min_size(data_or_symbol),
            mode="down",
        )

    def _open_positions_count(self):
        cnt = 0
        for d in self.orders.keys():
            if self.getposition(d).size != 0:
                cnt += 1
        return cnt

    def _calc_size_by_risk(self, symbol: str, exec_d, atr: float) -> int:
        if atr is None or not math.isfinite(atr) or atr <= 0:
            return 0

        stop_distance = float(self.p.atr_mult) * float(atr)
        if stop_distance <= 0:
            return 0

        comminfo = self.broker.getcommissioninfo(exec_d)
        mult = float(getattr(comminfo.p, "mult", 1.0) or 1.0)
        risk_per_contract = stop_distance * mult
        if risk_per_contract <= 0:
            return 0

        size_by_risk = float(self.p.risk_cash) / risk_per_contract

        cash = float(self.broker.getcash())
        try:
            margin_per_contract = float(comminfo.get_margin(exec_d.close[0]) or 0.0)
        except Exception:
            margin_per_contract = 0.0

        if margin_per_contract > 0:
            size_by_cash = cash / margin_per_contract
            raw_size = min(size_by_risk, size_by_cash)
        else:
            raw_size = size_by_risk

        size = self._round_size(exec_d, raw_size)
        return max(int(size), 0)

    def _update_signal_cache(self, symbol: str):
        pack = self.signal_inds[symbol]
        signal_d = pack["signal_data"]
        atr_ind = pack["atr"]
        s = self.state[symbol]

        min_ready = max(self.p.entry_lookback_days, self.p.exit_lookback_days, self.p.atr_period)
        if len(signal_d) < min_ready:
            return

        signal_dt = signal_d.datetime.datetime(0)
        if self.last_signal_dt[symbol] == signal_dt:
            return
        self.last_signal_dt[symbol] = signal_dt

        try:
            highs_entry = [float(signal_d.high[-i]) for i in range(0, self.p.entry_lookback_days)]
            lows_entry = [float(signal_d.low[-i]) for i in range(0, self.p.entry_lookback_days)]
            highs_exit = [float(signal_d.high[-i]) for i in range(0, self.p.exit_lookback_days)]
            lows_exit = [float(signal_d.low[-i]) for i in range(0, self.p.exit_lookback_days)]
            atr = float(atr_ind[0])
        except Exception:
            return

        if not math.isfinite(atr) or atr <= 0:
            return

        s["h20"] = max(highs_entry)
        s["l20"] = min(lows_entry)
        s["h10"] = max(highs_exit)
        s["l10"] = min(lows_exit)
        s["atr"] = atr
        s["signal_ready"] = True
        s["signal_dt"] = signal_dt

        self.log(
            f"[D-SIGNAL] signal_dt={signal_dt.strftime('%Y-%m-%d %H:%M:%S')} "
            f"h20={self._fmt_price(signal_d, s['h20'])} "
            f"l20={self._fmt_price(signal_d, s['l20'])} "
            f"h10={self._fmt_price(signal_d, s['h10'])} "
            f"l10={self._fmt_price(signal_d, s['l10'])} "
            f"atr={self._fmt_price(signal_d, s['atr'])}",
            data=signal_d,
        )

    def _sync_trade_day_state(self, symbol: str, exec_d):
        s = self.state[symbol]
        exec_dt = exec_d.datetime.datetime(0)
        day = exec_dt.date()
        prev_today_high = s["today_high"]
        prev_today_low = s["today_low"]

        if s["trade_day"] != day:
            s["trade_day"] = day
            s["today_high"] = float(exec_d.high[0])
            s["today_low"] = float(exec_d.low[0])
            s["pending_long"] = False
            s["pending_short"] = False
            s["pending_long_price"] = None
            s["pending_short_price"] = None
            s["pending_day"] = None
            s["wait_reentry_long"] = False
            s["wait_reentry_short"] = False
            prev_today_high = None
            prev_today_low = None
        else:
            cur_high = float(exec_d.high[0])
            cur_low = float(exec_d.low[0])
            if s["today_high"] is None or cur_high > s["today_high"]:
                s["today_high"] = cur_high
            if s["today_low"] is None or cur_low < s["today_low"]:
                s["today_low"] = cur_low

        return day, exec_dt, prev_today_high, prev_today_low

    def _place_entry_long(self, symbol: str, exec_d, size: int, atr: float, trigger_price: float):
        self.log(
            f"[ENTRY-LONG] trigger={self._fmt_price(exec_d, trigger_price)} "
            f"atr={self._fmt_price(exec_d, atr)} size={size}",
            data=exec_d,
        )
        order = self.buy(data=exec_d, size=size)
        self.orders[exec_d] = order
        self.order_meta[order.ref] = {
            "symbol": symbol,
            "kind": "entry_long",
            "atr": float(atr),
            "trigger_price": float(trigger_price),
        }

    def _place_entry_short(self, symbol: str, exec_d, size: int, atr: float, trigger_price: float):
        self.log(
            f"[ENTRY-SHORT] trigger={self._fmt_price(exec_d, trigger_price)} "
            f"atr={self._fmt_price(exec_d, atr)} size={size}",
            data=exec_d,
        )
        order = self.sell(data=exec_d, size=size)
        self.orders[exec_d] = order
        self.order_meta[order.ref] = {
            "symbol": symbol,
            "kind": "entry_short",
            "atr": float(atr),
            "trigger_price": float(trigger_price),
        }

    def _place_exit_long(self, symbol: str, exec_d, reason: str):
        self.log(f"[EXIT-LONG] reason={reason}", data=exec_d)
        order = self.close(data=exec_d)
        self.orders[exec_d] = order
        self.order_meta[order.ref] = {"symbol": symbol, "kind": "exit_long", "reason": reason}

    def _place_exit_short(self, symbol: str, exec_d, reason: str):
        self.log(f"[EXIT-SHORT] reason={reason}", data=exec_d)
        order = self.close(data=exec_d)
        self.orders[exec_d] = order
        self.order_meta[order.ref] = {"symbol": symbol, "kind": "exit_short", "reason": reason}

    def next(self):
        for symbol, pack in self.signal_inds.items():
            signal_d = pack["signal_data"]
            exec_d = pack["exec_data"]
            s = self.state[symbol]

            if len(exec_d) < 1:
                continue

            self._update_signal_cache(symbol)
            if not s["signal_ready"]:
                continue

            exec_dt = exec_d.datetime.datetime(0)
            if s["last_exec_dt"] == exec_dt:
                continue
            s["last_exec_dt"] = exec_dt

            if self.orders.get(exec_d):
                continue

            day, exec_dt, prev_today_high, prev_today_low = self._sync_trade_day_state(symbol, exec_d)

            pos = self.getposition(exec_d)
            high_ = float(exec_d.high[0])
            low_ = float(exec_d.low[0])
            close_ = float(exec_d.close[0])

            tick = self._tick_size(exec_d)
            breakout_buffer = tick * float(self.p.breakout_add_ticks)

            entry_long = self._round_price(exec_d, s["h20"] + breakout_buffer, mode="up")
            entry_short = self._round_price(exec_d, s["l20"] - breakout_buffer, mode="down")
            exit_long = self._round_price(exec_d, s["l10"] - breakout_buffer, mode="down")
            exit_short = self._round_price(exec_d, s["h10"] + breakout_buffer, mode="up")

            # ===== 持多：先移动止损，再10日低点出场 =====
            if pos.size > 0:
                entry_price = float(pos.price)
                entry_atr = s["entry_atr_long"]
                if entry_atr is None or not math.isfinite(entry_atr) or entry_atr <= 0:
                    entry_atr = float(s["atr"])
                    s["entry_atr_long"] = entry_atr
                    s["stop_long"] = self._round_price(
                        exec_d,
                        entry_price - float(self.p.atr_mult) * entry_atr,
                        mode="down",
                    )

                profit = high_ - entry_price
                if profit >= float(self.p.trail_lv1_atr) * entry_atr:
                    s["stop_long"] = max(float(s["stop_long"]), entry_price)
                if profit >= float(self.p.trail_lv2_atr) * entry_atr:
                    s["stop_long"] = max(
                        float(s["stop_long"]),
                        entry_price + float(self.p.trail_lock_atr) * entry_atr,
                    )
                s["stop_long"] = self._round_price(exec_d, s["stop_long"], mode="down")

                if low_ <= float(s["stop_long"]):
                    self._place_exit_long(symbol, exec_d, reason=f"trail_stop@{self._fmt_price(exec_d, s['stop_long'])}")
                    continue
                if low_ <= exit_long:
                    self._place_exit_long(symbol, exec_d, reason=f"donchian_exit@{self._fmt_price(exec_d, exit_long)}")
                    continue
                continue

            # ===== 持空：先10日高点，再移动止损 =====
            if pos.size < 0:
                entry_price = float(pos.price)
                entry_atr = s["entry_atr_short"]
                if entry_atr is None or not math.isfinite(entry_atr) or entry_atr <= 0:
                    entry_atr = float(s["atr"])
                    s["entry_atr_short"] = entry_atr
                    s["stop_short"] = self._round_price(
                        exec_d,
                        entry_price + float(self.p.atr_mult) * entry_atr,
                        mode="up",
                    )

                if high_ >= exit_short:
                    self._place_exit_short(symbol, exec_d, reason=f"donchian_exit@{self._fmt_price(exec_d, exit_short)}")
                    continue

                profit = entry_price - low_
                if profit >= float(self.p.trail_lv1_atr) * entry_atr:
                    s["stop_short"] = min(float(s["stop_short"]), entry_price)
                if profit >= float(self.p.trail_lv2_atr) * entry_atr:
                    s["stop_short"] = min(
                        float(s["stop_short"]),
                        entry_price - float(self.p.trail_lock_atr) * entry_atr,
                    )
                s["stop_short"] = self._round_price(exec_d, s["stop_short"], mode="up")

                if high_ >= float(s["stop_short"]):
                    self._place_exit_short(symbol, exec_d, reason=f"trail_stop@{self._fmt_price(exec_d, s['stop_short'])}")
                    continue
                continue

            # ===== 空仓：准备开仓 =====
            if self._open_positions_count() >= int(self.p.max_positions):
                continue

            atr = s["atr"]
            if atr is None or not math.isfinite(atr) or atr <= 0:
                continue

            size = self._calc_size_by_risk(symbol, exec_d, atr)
            if size < self._min_size(exec_d):
                self.log(
                    f"[SKIP] size={size} < min_size={self._min_size(exec_d)} "
                    f"atr={self._fmt_price(exec_d, atr)}",
                    data=exec_d,
                )
                continue

            allow_long_entry = True
            if s["wait_reentry_long"] and day == s["last_exit_day"]:
                if s["last_exit_dt"] is not None and exec_dt <= s["last_exit_dt"]:
                    allow_long_entry = False
                else:
                    allow_long_entry = prev_today_high is not None and high_ > float(prev_today_high)

            allow_short_entry = True
            if s["wait_reentry_short"] and day == s["last_exit_day_short"]:
                if s["last_exit_dt_short"] is not None and exec_dt <= s["last_exit_dt_short"]:
                    allow_short_entry = False
                else:
                    allow_short_entry = prev_today_low is not None and low_ < float(prev_today_low)

            touched_long = high_ >= entry_long
            touched_short = low_ <= entry_short

            if touched_long and touched_short:
                s["pending_long"] = False
                s["pending_short"] = False
                s["pending_long_price"] = None
                s["pending_short_price"] = None
                s["pending_day"] = None
                self.log(
                    f"[SKIP] both_sides_triggered long={self._fmt_price(exec_d, entry_long)} short={self._fmt_price(exec_d, entry_short)}",
                    data=exec_d,
                )
                continue

            if str(self.p.entry_mode).strip().lower() == "range":
                if allow_long_entry and touched_long:
                    self._place_entry_long(symbol, exec_d, size=size, atr=atr, trigger_price=entry_long)
                    continue
                if allow_short_entry and touched_short:
                    self._place_entry_short(symbol, exec_d, size=size, atr=atr, trigger_price=entry_short)
                    continue
            else:
                if touched_long and allow_long_entry and (not s["pending_long"]) and (not s["pending_short"]):
                    s["pending_long"] = True
                    s["pending_long_price"] = entry_long
                    s["pending_day"] = day
                    self.log(f"[PENDING-LONG] price={self._fmt_price(exec_d, entry_long)}", data=exec_d)

                if touched_short and allow_short_entry and (not s["pending_short"]) and (not s["pending_long"]):
                    s["pending_short"] = True
                    s["pending_short_price"] = entry_short
                    s["pending_day"] = day
                    self.log(f"[PENDING-SHORT] price={self._fmt_price(exec_d, entry_short)}", data=exec_d)

                if s["pending_long"] and s["pending_day"] == day and allow_long_entry and close_ >= float(s["pending_long_price"]):
                    self._place_entry_long(symbol, exec_d, size=size, atr=atr, trigger_price=float(s["pending_long_price"]))
                    s["pending_long"] = False
                    s["pending_short"] = False
                    s["pending_long_price"] = None
                    s["pending_short_price"] = None
                    s["pending_day"] = None
                    continue

                if s["pending_short"] and s["pending_day"] == day and allow_short_entry and close_ <= float(s["pending_short_price"]):
                    self._place_entry_short(symbol, exec_d, size=size, atr=atr, trigger_price=float(s["pending_short_price"]))
                    s["pending_long"] = False
                    s["pending_short"] = False
                    s["pending_long_price"] = None
                    s["pending_short_price"] = None
                    s["pending_day"] = None
                    continue

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        d = order.data
        meta = self.order_meta.get(order.ref, {})
        symbol = meta.get("symbol") or self._symbol_of(d)
        s = self.state.get(symbol)

        if order.status == order.Completed:
            if order.isbuy():
                self.log(
                    f"ORDER BUY EXECUTED, Price={self._fmt_price(d, order.executed.price)}, "
                    f"Size={order.executed.size}, Value={order.executed.value:.2f}, Comm={order.executed.comm:.2f}",
                    data=d,
                )
            else:
                self.log(
                    f"ORDER SELL EXECUTED, Price={self._fmt_price(d, order.executed.price)}, "
                    f"Size={order.executed.size}, Value={order.executed.value:.2f}, Comm={order.executed.comm:.2f}",
                    data=d,
                )

            kind = meta.get("kind")
            if s is not None:
                if kind == "entry_long":
                    atr = float(meta.get("atr") or 0.0)
                    s["entry_atr_long"] = atr
                    s["entry_atr_short"] = None
                    s["stop_short"] = None
                    s["stop_long"] = self._round_price(
                        d,
                        float(order.executed.price) - float(self.p.atr_mult) * atr,
                        mode="down",
                    )
                    s["wait_reentry_long"] = False
                    s["wait_reentry_short"] = False
                    s["pending_long"] = False
                    s["pending_short"] = False
                    s["pending_long_price"] = None
                    s["pending_short_price"] = None
                    s["pending_day"] = None

                elif kind == "entry_short":
                    atr = float(meta.get("atr") or 0.0)
                    s["entry_atr_short"] = atr
                    s["entry_atr_long"] = None
                    s["stop_long"] = None
                    s["stop_short"] = self._round_price(
                        d,
                        float(order.executed.price) + float(self.p.atr_mult) * atr,
                        mode="up",
                    )
                    s["wait_reentry_long"] = False
                    s["wait_reentry_short"] = False
                    s["pending_long"] = False
                    s["pending_short"] = False
                    s["pending_long_price"] = None
                    s["pending_short_price"] = None
                    s["pending_day"] = None

                elif kind == "exit_long":
                    exec_dt = d.datetime.datetime(0)
                    day = exec_dt.date()
                    s["entry_atr_long"] = None
                    s["stop_long"] = None
                    s["last_exit_day"] = day
                    s["last_exit_dt"] = exec_dt
                    s["wait_reentry_long"] = True

                elif kind == "exit_short":
                    exec_dt = d.datetime.datetime(0)
                    day = exec_dt.date()
                    s["entry_atr_short"] = None
                    s["stop_short"] = None
                    s["last_exit_day_short"] = day
                    s["last_exit_dt_short"] = exec_dt
                    s["wait_reentry_short"] = True

        elif order.status == order.Canceled:
            self.log("ORDER CANCELED", data=d)
        elif order.status == order.Margin:
            self.log("ORDER MARGIN REJECTED", data=d)
        elif order.status == order.Rejected:
            self.log("ORDER REJECTED", data=d)

        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.orders[d] = None
            self.order_meta.pop(order.ref, None)
