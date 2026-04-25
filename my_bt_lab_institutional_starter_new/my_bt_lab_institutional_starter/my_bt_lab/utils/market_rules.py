# utils/market_rules.py
from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional


def _to_decimal(x) -> Decimal:
    return Decimal(str(x))


def infer_price_precision_from_tick(tick_size: float) -> int:
    """
    由 tick_size 推断显示精度：
    1 -> 0位
    0.5 -> 1位
    0.01 -> 2位
    """
    d = _to_decimal(tick_size).normalize()
    exp = d.as_tuple().exponent
    return max(0, -exp)


def round_price_to_tick(
    price: float,
    tick_size: float,
    mode: str = "nearest",   # nearest / up / down
) -> float:
    """
    价格按最小变动价位取整
    mode:
      - nearest: 四舍五入到最近tick
      - up:      向上取整到tick
      - down:    向下取整到tick
    """
    if tick_size is None or tick_size <= 0:
        return float(price)

    p = _to_decimal(price)
    t = _to_decimal(tick_size)
    q = p / t

    if mode == "up":
        q2 = q.to_integral_value(rounding="ROUND_CEILING")
    elif mode == "down":
        q2 = q.to_integral_value(rounding="ROUND_FLOOR")
    else:
        # nearest
        q2 = q.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    out = q2 * t
    return float(out)


def round_size_to_step(
    size: float,
    size_step: float = 1,
    min_size: float = 1,
    mode: str = "down",   # 仓位控制通常向下更保守
) -> int:
    """
    手数按步长取整，并应用最小手数约束。
    返回 int（当前你的框架更适合整数手）
    """
    if size is None or size <= 0:
        return 0

    step = float(size_step or 1)
    min_s = float(min_size or step)

    units = size / step
    if mode == "up":
        units_i = math.ceil(units)
    elif mode == "nearest":
        units_i = int(round(units))
    else:
        units_i = math.floor(units)

    out = units_i * step
    if out < min_s:
        return 0  # 不够最小下单单位则不下单（比强行抬到min_size更保守）

    # 你的策略目前默认整数手，保险起见转 int
    return int(out)


def format_price(price: Optional[float], tick_size: Optional[float] = None, price_precision: Optional[int] = None) -> str:
    if price is None:
        return "None"
    if price_precision is None:
        if tick_size and tick_size > 0:
            price_precision = infer_price_precision_from_tick(tick_size)
        else:
            price_precision = 2
    return f"{float(price):.{int(price_precision)}f}"