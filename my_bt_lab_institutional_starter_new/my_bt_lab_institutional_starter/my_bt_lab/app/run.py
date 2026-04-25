from __future__ import annotations

import argparse
import logging
from pathlib import Path

from my_bt_lab.config.load import load_yaml_config
from my_bt_lab.engines.factory import run as run_engine
from my_bt_lab.reporting.writer import prepare_run_dir, write_result


def build_argparser():
    p = argparse.ArgumentParser(description="my_bt_lab institutional starter runner")
    p.add_argument("--config", "-c", type=str, required=False, default="my_bt_lab/app/configs/cta.yaml", help="YAML config path")
    p.add_argument("--output", "-o", type=str, required=False, default="runs", help="output root directory")
    p.add_argument("--tag", type=str, default=None, help="run tag appended to output folder name")
    return p


def main():
    args = build_argparser().parse_args()
    cfg, cfg_path = load_yaml_config(args.config)

    project_root = cfg_path.parents[3] if cfg_path.parent.name == "configs" else cfg_path.parent

    out_root = Path(args.output).resolve()
    run_dir = prepare_run_dir(out_root, tag=args.tag or cfg.get("output", {}).get("tag"))

    # log to file + stdout
    log_path = run_dir / "run.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )
    logging.info("Config: %s", cfg_path)
    logging.info("Run dir: %s", run_dir)

    from my_bt_lab.data.cache_cleanup import cleanup_cache
    cleanup_cache(project_root, cfg)
    
    result = run_engine(cfg, cfg_path)

    # quick console
    logging.info("Start Value: %.2f", getattr(result, "start_value", float("nan")))
    logging.info("End Value:   %.2f", getattr(result, "end_value", float("nan")))
    logging.info("DrawDown:    %s", getattr(result, "drawdown", {}))
    logging.info("Trade Stats: %s", getattr(result, "trade_stats", {}))

    write_result(run_dir, cfg, cfg_path, result, project_root=project_root)
    logging.info("Saved outputs to: %s", run_dir)


if __name__ == "__main__":
    main()
