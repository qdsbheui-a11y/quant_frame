from __future__ import annotations

import backtrader as bt


class SafeBackBroker(bt.brokers.BackBroker):
    """
    多周期/resample场景下，慢周期数据在尚未形成第一根bar时，
    估值阶段可能访问 data.close[0] 报 IndexError。
    这里改为：只对“当前已经有bar且已有持仓记录的数据”参与估值。
    """

    def _get_value(self, datas=None, lever=False):
        # BackBroker 没有 self.datas，这里改从 positions 取 data 对象
        if datas is None:
            datas = list(getattr(self, "positions", {}).keys())

        safe_datas = []
        for d in datas:
            try:
                if len(d) > 0:
                    _ = d.close[0]
                    safe_datas.append(d)
            except Exception:
                # 当前还没形成第一根bar，先跳过
                continue

        return super()._get_value(datas=safe_datas, lever=lever)