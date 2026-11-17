"""批次状态：批次号、批次键、阶段计数行。

批次键只由输入摘要 + 规则版本 + 参数决定；批次号额外带时间戳，便于人读。
批次创建时把受理所用规则版本钉在批次行上，续跑只认这个版本，外部配置切换不影响它。
"""
from __future__ import annotations

import datetime as dt

from ... import config, manifest


def new_batch(*, batch_key_value: str, input_digest: str, source: str,
              params: dict, rule_version: str,
              resume_of: str | None = None, existing_ids: set | None = None) -> dict:
    # 秒级时间戳后接短随机尾：同秒受理两版规则也不会撞批次号（主键冲突会直接丢批次）
    batch_id = f"SM{dt.datetime.now(config.UTC).strftime('%Y%m%d%H%M%S')}"
    existing_ids = existing_ids or set()
    if batch_id in existing_ids:
        batch_id = f"{batch_id}-{manifest.new_run_id().split('-')[-1]}"
    return {
        "batch_id": batch_id,
        "batch_key": batch_key_value,
        "run_id": manifest.new_run_id(),
        "source": source,
        "status": "running",
        "created_at": dt.datetime.now(config.UTC).isoformat(timespec="seconds"),
        "finished_at": None,
        "input_digest": input_digest,
        "resume_of": resume_of,
        "params": dict(params),
        "rule_version": rule_version,
        "note": "",
    }


def stage_row(stage: str, *, in_count: int = 0, out_count: int = 0,
              quarantine: int = 0, status: str = "done", elapsed_ms: int = 0,
              note: str = "") -> dict:
    return {"stage": stage, "in_count": in_count, "out_count": out_count,
            "quarantine_count": quarantine, "status": status,
            "elapsed_ms": elapsed_ms, "note": note}


def watermark(stages: list) -> dict:
    """水位：最后一个完成的阶段与已处理条数，用于续跑判断。"""
    done = [row for row in stages if row["status"] == "done"]
    if not done:
        return {"stage": "", "out": 0, "index": 0}
    last = done[-1]
    return {"stage": last["stage"], "out": last["out_count"],
            "index": list(config.STAGES).index(last["stage"])
            if last["stage"] in config.STAGES else 0}
