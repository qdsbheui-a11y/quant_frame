BEGIN;

-- =========================================
-- 清理旧表（按依赖顺序逆序删除）
-- =========================================
DROP TABLE IF EXISTS account_snapshot CASCADE;
DROP TABLE IF EXISTS position_snapshot CASCADE;
DROP TABLE IF EXISTS fills CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS strategy_run CASCADE;
DROP TABLE IF EXISTS tick_data CASCADE;
DROP TABLE IF EXISTS bar_data CASCADE;
DROP TABLE IF EXISTS instrument_spec CASCADE;
DROP TABLE IF EXISTS instrument CASCADE;

-- 兼容旧版命名的表，一并清理
DROP TABLE IF EXISTS bar_1m CASCADE;
DROP TABLE IF EXISTS future_contract CASCADE;
DROP TABLE IF EXISTS contract_spec CASCADE;

-- =========================================
-- 1. 通用标的主表
-- =========================================
CREATE TABLE instrument (
    instrument_id        BIGSERIAL PRIMARY KEY,
    market               TEXT NOT NULL,
    code                 TEXT NOT NULL,
    symbol               TEXT,
    name                 TEXT,
    asset_type           TEXT NOT NULL,
    exchange             TEXT,
    product_code         TEXT,
    underlying_code      TEXT,
    currency             TEXT NOT NULL DEFAULT 'CNY',
    price_scale          INTEGER,
    timezone             TEXT NOT NULL DEFAULT 'Asia/Shanghai',
    status               TEXT NOT NULL DEFAULT 'active',
    list_date            DATE,
    delist_date          DATE,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_instrument_code UNIQUE (code)
);

COMMENT ON TABLE instrument IS '通用标的主表：股票、期货、ETF、指数、期权等统一放这里';
COMMENT ON COLUMN instrument.instrument_id IS '标的主键';
COMMENT ON COLUMN instrument.market IS '市场标识，如 SZ/SH/SHFE/DCE/CFFEX';
COMMENT ON COLUMN instrument.code IS '统一代码，如 002311.SZ、RB2605.SHF';
COMMENT ON COLUMN instrument.symbol IS '简化代码，如 002311、RB2605';
COMMENT ON COLUMN instrument.name IS '标的名称';
COMMENT ON COLUMN instrument.asset_type IS '资产类型：stock/future/etf/index/option 等';
COMMENT ON COLUMN instrument.exchange IS '交易所中文或补充标识';
COMMENT ON COLUMN instrument.product_code IS '品种代码，如 RB/IF/AU；股票可为空';
COMMENT ON COLUMN instrument.underlying_code IS '标的物代码或连续合约映射用';
COMMENT ON COLUMN instrument.currency IS '币种，默认 CNY';
COMMENT ON COLUMN instrument.price_scale IS '价格小数位';
COMMENT ON COLUMN instrument.timezone IS '该标的数据时区，默认 Asia/Shanghai';
COMMENT ON COLUMN instrument.status IS '状态：active/delisted/suspended';
COMMENT ON COLUMN instrument.list_date IS '上市日期';
COMMENT ON COLUMN instrument.delist_date IS '退市/摘牌/到期日期';
COMMENT ON COLUMN instrument.metadata IS '扩展字段，JSONB';
COMMENT ON COLUMN instrument.created_at IS '创建时间';
COMMENT ON COLUMN instrument.updated_at IS '更新时间';

CREATE INDEX idx_instrument_market_asset ON instrument(market, asset_type);
CREATE INDEX idx_instrument_symbol ON instrument(symbol);
CREATE INDEX idx_instrument_product_code ON instrument(product_code);

-- =========================================
-- 2. 标的规格表
-- =========================================
CREATE TABLE instrument_spec (
    spec_id               BIGSERIAL PRIMARY KEY,
    instrument_id         BIGINT NOT NULL REFERENCES instrument(instrument_id) ON DELETE CASCADE,
    effective_from        DATE NOT NULL,
    effective_to          DATE,
    multiplier            NUMERIC(18,6),
    tick_size             NUMERIC(18,6),
    lot_size              NUMERIC(18,6),
    margin_rate_long      NUMERIC(10,6),
    margin_rate_short     NUMERIC(10,6),
    fee_open              NUMERIC(18,6),
    fee_close             NUMERIC(18,6),
    fee_close_today       NUMERIC(18,6),
    fee_mode              TEXT,
    upper_limit_rate      NUMERIC(10,6),
    lower_limit_rate      NUMERIC(10,6),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_instrument_spec UNIQUE (instrument_id, effective_from)
);

COMMENT ON TABLE instrument_spec IS '标的规格表：乘数、最小跳动、保证金、手续费等按生效日期管理';
COMMENT ON COLUMN instrument_spec.spec_id IS '规格主键';
COMMENT ON COLUMN instrument_spec.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN instrument_spec.effective_from IS '规格生效起始日期';
COMMENT ON COLUMN instrument_spec.effective_to IS '规格失效日期';
COMMENT ON COLUMN instrument_spec.multiplier IS '合约乘数；股票通常为 1';
COMMENT ON COLUMN instrument_spec.tick_size IS '最小变动价位';
COMMENT ON COLUMN instrument_spec.lot_size IS '最小交易单位；A股常见为 100';
COMMENT ON COLUMN instrument_spec.margin_rate_long IS '多头保证金率';
COMMENT ON COLUMN instrument_spec.margin_rate_short IS '空头保证金率';
COMMENT ON COLUMN instrument_spec.fee_open IS '开仓手续费';
COMMENT ON COLUMN instrument_spec.fee_close IS '平仓手续费';
COMMENT ON COLUMN instrument_spec.fee_close_today IS '平今手续费';
COMMENT ON COLUMN instrument_spec.fee_mode IS '手续费模式：by_amount/by_volume';
COMMENT ON COLUMN instrument_spec.upper_limit_rate IS '涨停限制比率';
COMMENT ON COLUMN instrument_spec.lower_limit_rate IS '跌停限制比率';
COMMENT ON COLUMN instrument_spec.metadata IS '扩展字段，JSONB';
COMMENT ON COLUMN instrument_spec.created_at IS '创建时间';

CREATE INDEX idx_instrument_spec_instr_dates
    ON instrument_spec(instrument_id, effective_from, effective_to);

-- =========================================
-- 3. 通用 K 线表
-- =========================================
CREATE TABLE bar_data (
    bar_id                BIGSERIAL PRIMARY KEY,
    instrument_id         BIGINT NOT NULL REFERENCES instrument(instrument_id) ON DELETE CASCADE,
    bar_time              TIMESTAMPTZ NOT NULL,
    timeframe_unit        TEXT NOT NULL,
    compression           INTEGER NOT NULL,
    open                  NUMERIC(18,6) NOT NULL,
    high                  NUMERIC(18,6) NOT NULL,
    low                   NUMERIC(18,6) NOT NULL,
    close                 NUMERIC(18,6) NOT NULL,
    volume                NUMERIC(20,4),
    turnover              NUMERIC(24,6),
    open_interest         NUMERIC(20,4),
    trading_day           DATE,
    is_final              BOOLEAN NOT NULL DEFAULT TRUE,
    source                TEXT,
    source_file           TEXT,
    ingest_batch_id       TEXT,
    extra                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_bar UNIQUE (instrument_id, bar_time, timeframe_unit, compression)
);

COMMENT ON TABLE bar_data IS '通用 K 线表：统一存 1m/5m/15m/60m/日线/周线/月线';
COMMENT ON COLUMN bar_data.bar_id IS 'K 线主键';
COMMENT ON COLUMN bar_data.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN bar_data.bar_time IS 'K 线时间；建议统一存为带时区时间';
COMMENT ON COLUMN bar_data.timeframe_unit IS '周期单位：second/minute/hour/day/week/month';
COMMENT ON COLUMN bar_data.compression IS '周期压缩倍数：如 1/5/15/60';
COMMENT ON COLUMN bar_data.open IS '开盘价';
COMMENT ON COLUMN bar_data.high IS '最高价';
COMMENT ON COLUMN bar_data.low IS '最低价';
COMMENT ON COLUMN bar_data.close IS '收盘价';
COMMENT ON COLUMN bar_data.volume IS '成交量';
COMMENT ON COLUMN bar_data.turnover IS '成交额';
COMMENT ON COLUMN bar_data.open_interest IS '持仓量；股票通常为空';
COMMENT ON COLUMN bar_data.trading_day IS '交易日，夜盘场景下建议显式存储';
COMMENT ON COLUMN bar_data.is_final IS '是否为最终确认 K 线';
COMMENT ON COLUMN bar_data.source IS '来源：csv/tushare/rq/manual 等';
COMMENT ON COLUMN bar_data.source_file IS '来源文件名';
COMMENT ON COLUMN bar_data.ingest_batch_id IS '导入批次号';
COMMENT ON COLUMN bar_data.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN bar_data.created_at IS '创建时间';
COMMENT ON COLUMN bar_data.updated_at IS '更新时间';

CREATE INDEX idx_bar_lookup
    ON bar_data(instrument_id, timeframe_unit, compression, bar_time);
CREATE INDEX idx_bar_trading_day
    ON bar_data(trading_day);
CREATE INDEX idx_bar_source_batch
    ON bar_data(ingest_batch_id);

-- =========================================
-- 4. Tick 表
-- =========================================
CREATE TABLE tick_data (
    tick_id               BIGSERIAL PRIMARY KEY,
    instrument_id         BIGINT NOT NULL REFERENCES instrument(instrument_id) ON DELETE CASCADE,
    tick_time             TIMESTAMPTZ NOT NULL,
    last_price            NUMERIC(18,6) NOT NULL,
    volume                NUMERIC(20,4),
    turnover              NUMERIC(24,6),
    open_interest         NUMERIC(20,4),
    bid_price_1           NUMERIC(18,6),
    ask_price_1           NUMERIC(18,6),
    bid_volume_1          NUMERIC(20,4),
    ask_volume_1          NUMERIC(20,4),
    trading_day           DATE,
    source                TEXT,
    source_file           TEXT,
    ingest_batch_id       TEXT,
    extra                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tick_data IS '逐笔/逐跳行情表；若上游给 tick 数据先落这里';
COMMENT ON COLUMN tick_data.tick_id IS 'Tick 主键';
COMMENT ON COLUMN tick_data.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN tick_data.tick_time IS 'Tick 时间';
COMMENT ON COLUMN tick_data.last_price IS '最新价';
COMMENT ON COLUMN tick_data.volume IS '成交量；需结合上游口径判断是增量还是累计量';
COMMENT ON COLUMN tick_data.turnover IS '成交额；需结合上游口径判断是增量还是累计值';
COMMENT ON COLUMN tick_data.open_interest IS '持仓量';
COMMENT ON COLUMN tick_data.bid_price_1 IS '买一价';
COMMENT ON COLUMN tick_data.ask_price_1 IS '卖一价';
COMMENT ON COLUMN tick_data.bid_volume_1 IS '买一量';
COMMENT ON COLUMN tick_data.ask_volume_1 IS '卖一量';
COMMENT ON COLUMN tick_data.trading_day IS '交易日';
COMMENT ON COLUMN tick_data.source IS '来源：csv/tushare/rq/manual 等';
COMMENT ON COLUMN tick_data.source_file IS '来源文件名';
COMMENT ON COLUMN tick_data.ingest_batch_id IS '导入批次号';
COMMENT ON COLUMN tick_data.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN tick_data.created_at IS '创建时间';

CREATE INDEX idx_tick_lookup
    ON tick_data(instrument_id, tick_time);
CREATE INDEX idx_tick_trading_day
    ON tick_data(trading_day);

-- =========================================
-- 5. 回测运行主表
-- =========================================
CREATE TABLE strategy_run (
    run_id                BIGSERIAL PRIMARY KEY,
    run_tag               TEXT,
    strategy_name         TEXT NOT NULL,
    strategy_version      TEXT,
    engine_name           TEXT,
    environment           TEXT,
    market_scope          TEXT,
    symbols               TEXT,
    timeframe_desc        TEXT,
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ,
    status                TEXT NOT NULL DEFAULT 'running',
    initial_cash          NUMERIC(24,6),
    final_equity          NUMERIC(24,6),
    net_pnl               NUMERIC(24,6),
    return_pct            NUMERIC(18,6),
    max_drawdown_pct      NUMERIC(18,6),
    sharpe                NUMERIC(18,6),
    params                JSONB NOT NULL DEFAULT '{}'::jsonb,
    config_path           TEXT,
    output_dir            TEXT,
    notes                 TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE strategy_run IS '一次回测或策略运行的主记录';
COMMENT ON COLUMN strategy_run.run_id IS '运行主键';
COMMENT ON COLUMN strategy_run.run_tag IS '运行标签';
COMMENT ON COLUMN strategy_run.strategy_name IS '策略名称';
COMMENT ON COLUMN strategy_run.strategy_version IS '策略版本';
COMMENT ON COLUMN strategy_run.engine_name IS '回测引擎名称';
COMMENT ON COLUMN strategy_run.environment IS '运行环境：backtest/paper/live 等';
COMMENT ON COLUMN strategy_run.market_scope IS '市场范围描述';
COMMENT ON COLUMN strategy_run.symbols IS '本次运行涉及的标的列表文本';
COMMENT ON COLUMN strategy_run.timeframe_desc IS '周期说明文本';
COMMENT ON COLUMN strategy_run.start_time IS '回测区间开始时间';
COMMENT ON COLUMN strategy_run.end_time IS '回测区间结束时间';
COMMENT ON COLUMN strategy_run.started_at IS '运行开始时间';
COMMENT ON COLUMN strategy_run.finished_at IS '运行结束时间';
COMMENT ON COLUMN strategy_run.status IS '状态：running/success/failed';
COMMENT ON COLUMN strategy_run.initial_cash IS '初始资金';
COMMENT ON COLUMN strategy_run.final_equity IS '最终权益';
COMMENT ON COLUMN strategy_run.net_pnl IS '净利润';
COMMENT ON COLUMN strategy_run.return_pct IS '收益率';
COMMENT ON COLUMN strategy_run.max_drawdown_pct IS '最大回撤';
COMMENT ON COLUMN strategy_run.sharpe IS '夏普比率';
COMMENT ON COLUMN strategy_run.params IS '参数 JSON';
COMMENT ON COLUMN strategy_run.config_path IS '配置文件路径';
COMMENT ON COLUMN strategy_run.output_dir IS '输出目录';
COMMENT ON COLUMN strategy_run.notes IS '备注';
COMMENT ON COLUMN strategy_run.created_at IS '创建时间';

CREATE INDEX idx_strategy_run_status ON strategy_run(status);
CREATE INDEX idx_strategy_run_started_at ON strategy_run(started_at);

-- =========================================
-- 6. 委托表
-- =========================================
CREATE TABLE orders (
    order_id               BIGSERIAL PRIMARY KEY,
    run_id                 BIGINT NOT NULL REFERENCES strategy_run(run_id) ON DELETE CASCADE,
    instrument_id          BIGINT REFERENCES instrument(instrument_id) ON DELETE SET NULL,
    broker_order_id        TEXT,
    order_time             TIMESTAMPTZ NOT NULL,
    status_time            TIMESTAMPTZ,
    side                   TEXT NOT NULL,
    position_effect        TEXT,
    order_type             TEXT,
    status                 TEXT,
    quantity               NUMERIC(20,4) NOT NULL,
    filled_quantity        NUMERIC(20,4),
    price                  NUMERIC(18,6),
    avg_fill_price         NUMERIC(18,6),
    stop_price             NUMERIC(18,6),
    commission             NUMERIC(18,6),
    slippage               NUMERIC(18,6),
    signal_time            TIMESTAMPTZ,
    reason                 TEXT,
    extra                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE orders IS '委托记录表';
COMMENT ON COLUMN orders.order_id IS '委托主键';
COMMENT ON COLUMN orders.run_id IS '关联 strategy_run.run_id';
COMMENT ON COLUMN orders.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN orders.broker_order_id IS '外部系统委托号';
COMMENT ON COLUMN orders.order_time IS '下单时间';
COMMENT ON COLUMN orders.status_time IS '状态更新时间';
COMMENT ON COLUMN orders.side IS '方向：buy/sell';
COMMENT ON COLUMN orders.position_effect IS '开平标识：open/close/close_today 等';
COMMENT ON COLUMN orders.order_type IS '委托类型：market/limit/stop 等';
COMMENT ON COLUMN orders.status IS '委托状态';
COMMENT ON COLUMN orders.quantity IS '委托数量';
COMMENT ON COLUMN orders.filled_quantity IS '成交数量';
COMMENT ON COLUMN orders.price IS '委托价格';
COMMENT ON COLUMN orders.avg_fill_price IS '平均成交价';
COMMENT ON COLUMN orders.stop_price IS '止损/触发价格';
COMMENT ON COLUMN orders.commission IS '累计手续费';
COMMENT ON COLUMN orders.slippage IS '累计滑点';
COMMENT ON COLUMN orders.signal_time IS '信号生成时间';
COMMENT ON COLUMN orders.reason IS '下单原因';
COMMENT ON COLUMN orders.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN orders.created_at IS '创建时间';

CREATE INDEX idx_orders_run_time ON orders(run_id, order_time);
CREATE INDEX idx_orders_instrument_time ON orders(instrument_id, order_time);
CREATE INDEX idx_orders_status ON orders(status);

-- =========================================
-- 7. 成交表
-- =========================================
CREATE TABLE fills (
    fill_id                BIGSERIAL PRIMARY KEY,
    run_id                 BIGINT NOT NULL REFERENCES strategy_run(run_id) ON DELETE CASCADE,
    order_id               BIGINT REFERENCES orders(order_id) ON DELETE SET NULL,
    instrument_id          BIGINT REFERENCES instrument(instrument_id) ON DELETE SET NULL,
    fill_time              TIMESTAMPTZ NOT NULL,
    side                   TEXT NOT NULL,
    position_effect        TEXT,
    quantity               NUMERIC(20,4) NOT NULL,
    fill_price             NUMERIC(18,6) NOT NULL,
    notional               NUMERIC(24,6),
    realized_pnl           NUMERIC(24,6),
    commission             NUMERIC(18,6),
    slippage               NUMERIC(18,6),
    trade_id               TEXT,
    extra                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE fills IS '成交记录表';
COMMENT ON COLUMN fills.fill_id IS '成交主键';
COMMENT ON COLUMN fills.run_id IS '关联 strategy_run.run_id';
COMMENT ON COLUMN fills.order_id IS '关联 orders.order_id';
COMMENT ON COLUMN fills.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN fills.fill_time IS '成交时间';
COMMENT ON COLUMN fills.side IS '方向：buy/sell';
COMMENT ON COLUMN fills.position_effect IS '开平标识';
COMMENT ON COLUMN fills.quantity IS '成交数量';
COMMENT ON COLUMN fills.fill_price IS '成交价格';
COMMENT ON COLUMN fills.notional IS '成交额';
COMMENT ON COLUMN fills.realized_pnl IS '已实现盈亏';
COMMENT ON COLUMN fills.commission IS '手续费';
COMMENT ON COLUMN fills.slippage IS '滑点';
COMMENT ON COLUMN fills.trade_id IS '外部成交号';
COMMENT ON COLUMN fills.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN fills.created_at IS '创建时间';

CREATE INDEX idx_fills_run_time ON fills(run_id, fill_time);
CREATE INDEX idx_fills_instrument_time ON fills(instrument_id, fill_time);

-- =========================================
-- 8. 持仓快照表
-- =========================================
CREATE TABLE position_snapshot (
    snapshot_id            BIGSERIAL PRIMARY KEY,
    run_id                 BIGINT NOT NULL REFERENCES strategy_run(run_id) ON DELETE CASCADE,
    instrument_id          BIGINT REFERENCES instrument(instrument_id) ON DELETE SET NULL,
    snapshot_time          TIMESTAMPTZ NOT NULL,
    direction              TEXT,
    quantity               NUMERIC(20,4),
    available_quantity     NUMERIC(20,4),
    avg_price              NUMERIC(18,6),
    last_price             NUMERIC(18,6),
    market_value           NUMERIC(24,6),
    unrealized_pnl         NUMERIC(24,6),
    margin                 NUMERIC(24,6),
    extra                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE position_snapshot IS '持仓快照表';
COMMENT ON COLUMN position_snapshot.snapshot_id IS '持仓快照主键';
COMMENT ON COLUMN position_snapshot.run_id IS '关联 strategy_run.run_id';
COMMENT ON COLUMN position_snapshot.instrument_id IS '关联 instrument.instrument_id';
COMMENT ON COLUMN position_snapshot.snapshot_time IS '快照时间';
COMMENT ON COLUMN position_snapshot.direction IS '持仓方向：long/short/net';
COMMENT ON COLUMN position_snapshot.quantity IS '持仓数量';
COMMENT ON COLUMN position_snapshot.available_quantity IS '可用数量';
COMMENT ON COLUMN position_snapshot.avg_price IS '持仓均价';
COMMENT ON COLUMN position_snapshot.last_price IS '最新价';
COMMENT ON COLUMN position_snapshot.market_value IS '持仓市值';
COMMENT ON COLUMN position_snapshot.unrealized_pnl IS '浮动盈亏';
COMMENT ON COLUMN position_snapshot.margin IS '占用保证金';
COMMENT ON COLUMN position_snapshot.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN position_snapshot.created_at IS '创建时间';

CREATE INDEX idx_position_snapshot_run_time
    ON position_snapshot(run_id, snapshot_time);
CREATE INDEX idx_position_snapshot_instrument_time
    ON position_snapshot(instrument_id, snapshot_time);
 
-- =========================================
-- 9. 账户快照表
-- =========================================
CREATE TABLE account_snapshot (
    snapshot_id            BIGSERIAL PRIMARY KEY,
    run_id                 BIGINT NOT NULL REFERENCES strategy_run(run_id) ON DELETE CASCADE,
    snapshot_time          TIMESTAMPTZ NOT NULL,
    cash                   NUMERIC(24,6),
    available_cash         NUMERIC(24,6),
    frozen_cash            NUMERIC(24,6),
    margin                 NUMERIC(24,6),
    market_value           NUMERIC(24,6),
    realized_pnl           NUMERIC(24,6),
    unrealized_pnl         NUMERIC(24,6),
    equity                 NUMERIC(24,6),
    nav                    NUMERIC(18,8),
    drawdown               NUMERIC(18,8),
    extra                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE account_snapshot IS '账户快照表';
COMMENT ON COLUMN account_snapshot.snapshot_id IS '账户快照主键';
COMMENT ON COLUMN account_snapshot.run_id IS '关联 strategy_run.run_id';
COMMENT ON COLUMN account_snapshot.snapshot_time IS '快照时间';
COMMENT ON COLUMN account_snapshot.cash IS '现金';
COMMENT ON COLUMN account_snapshot.available_cash IS '可用资金';
COMMENT ON COLUMN account_snapshot.frozen_cash IS '冻结资金';
COMMENT ON COLUMN account_snapshot.margin IS '占用保证金';
COMMENT ON COLUMN account_snapshot.market_value IS '持仓市值';
COMMENT ON COLUMN account_snapshot.realized_pnl IS '已实现盈亏';
COMMENT ON COLUMN account_snapshot.unrealized_pnl IS '浮动盈亏';
COMMENT ON COLUMN account_snapshot.equity IS '总权益';
COMMENT ON COLUMN account_snapshot.nav IS '净值';
COMMENT ON COLUMN account_snapshot.drawdown IS '回撤';
COMMENT ON COLUMN account_snapshot.extra IS '扩展字段，JSONB';
COMMENT ON COLUMN account_snapshot.created_at IS '创建时间';

CREATE INDEX idx_account_snapshot_run_time
    ON account_snapshot(run_id, snapshot_time);

-- =========================================
-- 10. 自动更新时间触发器
-- =========================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_instrument_updated_at
BEFORE UPDATE ON instrument
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_bar_data_updated_at
BEFORE UPDATE ON bar_data
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
