import math
import backtrader as bt

from my_bt_lab.strategies.base_strategy import BaseStrategy
from my_bt_lab.utils.market_rules import (
    round_price_to_tick,
    round_size_to_step,
    format_price,
)


class MultiDataAtrCtaStrategy(BaseStrategy):
    params = dict(
        fast=10,
        slow=30,
        atr_period=14,
        atr_stop_mult=2.0,
        risk_per_trade=0.01,
        max_positions=2,
        min_size=1,
        printlog=True,
        symbol_specs=None,
        data_roles=None,   # 引擎自动传入
    )

    def __init__(self):
        super().__init__()
        self.symbol_specs = self.p.symbol_specs or {}
        self.data_roles = self.p.data_roles or {}

        self.orders = {}
        self.inds = {}
        self.stop_price = {}
        self.last_signal_dt = {}

        self.exec_data_by_symbol = {}
        self.signal_data_by_symbol = {}

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

            fast_ma = bt.ind.SMA(signal_d.close, period=self.p.fast)
            slow_ma = bt.ind.SMA(signal_d.close, period=self.p.slow)
            crossover = bt.ind.CrossOver(fast_ma, slow_ma)
            atr = bt.ind.ATR(signal_d, period=self.p.atr_period)

            self.orders[exec_d] = None
            self.inds[symbol] = dict(
                signal_data=signal_d,
                exec_data=exec_d,
                fast_ma=fast_ma,
                slow_ma=slow_ma,
                crossover=crossover,
                atr=atr,
            )
            self.stop_price[symbol] = None
            self.last_signal_dt[symbol] = None

    def _symbol_of(self, data) -> str:
        name = getattr(data, "_name", "") or ""
        meta = self.data_roles.get(name, {})
        return str(meta.get("symbol") or name)

    def _spec(self, data):
        symbol = self._symbol_of(data)
        return self.symbol_specs.get(symbol, self.symbol_specs.get(getattr(data, "_name", ""), {}))

    def _tick_size(self, data) -> float:
        spec = self._spec(data)
        return float(spec.get("tick_size", 0) or 0)

    def _price_precision(self, data):
        spec = self._spec(data)
        if "price_precision" in spec:
            return int(spec["price_precision"])
        return None

    def _size_step(self, data) -> float:
        spec = self._spec(data)
        return float(spec.get("size_step", 1) or 1)

    def _min_size(self, data) -> float:
        spec = self._spec(data)
        return float(spec.get("min_size", self.p.min_size) or 1)

    def _round_price(self, data, price: float, mode: str = "nearest") -> float:
        tick = self._tick_size(data)
        if tick > 0:
            return round_price_to_tick(price, tick_size=tick, mode=mode)
        return float(price)

    def _fmt_price(self, data, price) -> str:
        tick = self._tick_size(data)
        precision = self._price_precision(data)
        return format_price(
            price,
            tick_size=tick if tick > 0 else None,
            price_precision=precision,
        )

    def _round_size(self, data, size: float) -> int:
        return round_size_to_step(
            size=size,
            size_step=self._size_step(data),
            min_size=self._min_size(data),
            mode="down",
        )

    def _min_bars_ready(self):
        return max(self.p.fast, self.p.slow, self.p.atr_period) + 1

    def _open_positions_count(self):
        cnt = 0
        for d in self.orders.keys():
            if self.getposition(d).size != 0:
                cnt += 1
        return cnt

    def _calc_size_by_risk(self, symbol: str, exec_d):
        atr = float(self.inds[symbol]["atr"][0])
        if not math.isfinite(atr) or atr <= 0:
            return 0

        risk_cash = float(self.broker.getvalue()) * float(self.p.risk_per_trade)
        stop_distance = atr * float(self.p.atr_stop_mult)
        if stop_distance <= 0:
            return 0

        comminfo = self.broker.getcommissioninfo(exec_d)
        mult = float(getattr(comminfo.p, "mult", 1.0) or 1.0)

        risk_per_contract = stop_distance * mult
        if risk_per_contract <= 0:
            return 0

        size_by_risk = risk_cash / risk_per_contract

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

    def next(self):
        min_ready = self._min_bars_ready()

        for symbol, pack in self.inds.items():
            signal_d = pack["signal_data"]
            exec_d = pack["exec_data"]

            if len(signal_d) < min_ready or len(exec_d) < 1:
                continue

            signal_dt = signal_d.datetime.datetime(0)
            if self.last_signal_dt[symbol] == signal_dt:
                continue
            self.last_signal_dt[symbol] = signal_dt

            if self.orders[exec_d]:
                continue

            pos = self.getposition(exec_d)
            signal_close = float(signal_d.close[0])
            exec_close = float(exec_d.close[0])
            atr = float(pack["atr"][0])
            cross = float(pack["crossover"][0])

            if not math.isfinite(atr) or atr <= 0:
                continue

            if pos.size == 0:
                if self._open_positions_count() >= self.p.max_positions:
                    continue

                if cross > 0:
                    size = self._calc_size_by_risk(symbol, exec_d)
                    if size < self._min_size(exec_d):
                        self.log(
                            f"[MTF] SKIP BUY size={size} "
                            f"signal_close={self._fmt_price(signal_d, signal_close)} "
                            f"exec_close={self._fmt_price(exec_d, exec_close)} "
                            f"atr={self._fmt_price(signal_d, atr)}",
                            data=exec_d,
                        )
                        continue

                    raw_init_stop = signal_close - self.p.atr_stop_mult * atr
                    init_stop = self._round_price(exec_d, raw_init_stop, mode="down")
                    self.stop_price[symbol] = init_stop

                    self.log(
                        f"[MTF] BUY SIGNAL "
                        f"signal_dt={signal_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"signal_close={self._fmt_price(signal_d, signal_close)}, "
                        f"exec_close={self._fmt_price(exec_d, exec_close)}, "
                        f"atr={self._fmt_price(signal_d, atr)}, "
                        f"size={size}, "
                        f"init_stop={self._fmt_price(exec_d, init_stop)}",
                        data=exec_d,
                    )
                    self.orders[exec_d] = self.buy(data=exec_d, size=size)

            else:
                raw_atr_stop = float(pos.price) - self.p.atr_stop_mult * atr
                atr_stop = self._round_price(exec_d, raw_atr_stop, mode="down")
                self.stop_price[symbol] = atr_stop

                if signal_close < atr_stop:
                    self.log(
                        f"[MTF] EXIT SIGNAL (ATR_STOP) "
                        f"signal_dt={signal_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"signal_close={self._fmt_price(signal_d, signal_close)}, "
                        f"stop={self._fmt_price(exec_d, atr_stop)}, "
                        f"pos_price={self._fmt_price(exec_d, pos.price)}",
                        data=exec_d,
                    )
                    self.orders[exec_d] = self.close(data=exec_d)

                elif cross < 0:
                    self.log(
                        f"[MTF] EXIT SIGNAL (MA_CROSS) "
                        f"signal_dt={signal_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"signal_close={self._fmt_price(signal_d, signal_close)}",
                        data=exec_d,
                    )
                    self.orders[exec_d] = self.close(data=exec_d)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        d = order.data

        if order.status == order.Completed:
            if order.isbuy():
                self.log(
                    f"ORDER BUY EXECUTED, "
                    f"Price={self._fmt_price(d, order.executed.price)}, "
                    f"Size={order.executed.size}, "
                    f"Value={order.executed.value:.2f}, "
                    f"Comm={order.executed.comm:.2f}",
                    data=d,
                )
            else:
                self.log(
                    f"ORDER SELL EXECUTED, "
                    f"Price={self._fmt_price(d, order.executed.price)}, "
                    f"Size={order.executed.size}, "
                    f"Value={order.executed.value:.2f}, "
                    f"Comm={order.executed.comm:.2f}",
                    data=d,
                )

        elif order.status == order.Canceled:
            self.log("ORDER CANCELED", data=d)

        elif order.status == order.Margin:
            self.log("ORDER MARGIN REJECTED", data=d)

        elif order.status == order.Rejected:
            self.log("ORDER REJECTED", data=d)

        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.orders[order.data] = None