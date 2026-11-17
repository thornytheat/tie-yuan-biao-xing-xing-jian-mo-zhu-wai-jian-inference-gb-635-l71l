"""M5 编排：接入 → 类型 → 剖析 → 键 → 规格单，五段依次计数，最后落库出清单。

整条流水线只有一个入口 run()，批次键、阶段耗时、四账都在同一处收口。

版本与续跑语义：
- 规则包在批次受理时解析并钉在批次行上；新批次取 settings.rule_version 当前版，
  规则版本未登记直接拒绝（RulePackMissing），绝不静默套最新版；
- 每完成一个阶段立刻落「阶段计数行 + 检查点」，中断后 resume() 从最后一个完成阶段
  的下一段继续，沿用原批次号、原 run_id、原账目与原规则版本，不另建任务；
- 续跑时重新读源文件但只用于核对输入摘要：源文件被替换（摘要变化）会在批次备注与
  返回值里显式标 source_changed，已完成阶段不重算，续跑仍基于受理时的检查点；
- 同批次键的完成批次重跑直接回放，重复续跑不新增任何结果。
"""
from __future__ import annotations

import datetime as dt
import json
import time
from decimal import Decimal

from ... import config, errors, manifest, rulepacks
from ..m1_ingest import service as m1_service
from ..m2_contract import service as m2_service
from ..m3_profile import rules as m3_rules
from ..m3_profile import service as m3_service
from ..m4_keys import service as m4_service
from ..m4_keys.service import Modeled
from ..m6_spec import spec_out
from . import checkpoint, resume, state


def _now() -> str:
    return dt.datetime.now(config.UTC).isoformat(timespec="seconds")


def run(*, settings, store, source: str = "sample", params: dict | None = None,
        force: bool = False) -> dict:
    params = dict(params or {"sample_cap": config.MAX_SAMPLE_ROWS_PER_TABLE})
    # 入口先钉规则版本：版本缺失在这里就 422，后面任何阶段都不会静默换版
    pack = rulepacks.current(settings)
    started_at = _now()
    clock = time.perf_counter()

    intake = m1_service.intake(settings)
    inputs = list(intake.inputs)
    input_digest = manifest.digest_parts(intake.digest_parts)
    batch_key_value = manifest.batch_key(rule_version=pack.version,
                                         inputs=inputs, params=params)
    if not force:
        existing = store.find_done_by_key(batch_key_value)
        if existing is not None:
            payload = resume.replay_payload(store, existing)
            payload["input_digest"] = input_digest
            return payload

    batch = state.new_batch(
        batch_key_value=batch_key_value, input_digest=input_digest,
        source=source, params=params, rule_version=pack.version,
        existing_ids={row["batch_id"] for row in store.list_batches(200)})
    store.upsert_batch(batch)

    try:
        result = _pipeline(settings=settings, store=store, batch=batch, pack=pack,
                           intake=intake, input_digest=input_digest, inputs=inputs,
                           params=params, started_at=started_at, clock=clock)
    except Exception as exc:  # noqa: BLE001 - 失败要留痕，再抛给上层
        batch["status"] = "aborted"
        batch["finished_at"] = _now()
        batch["note"] = f"{type(exc).__name__}: {exc}"
        store.upsert_batch(batch)
        raise
    return result


def resume_batch(*, settings, store, batch_id: str) -> dict:
    """中断批次续跑：沿用原批次号 / run_id / 规则版本，从断点阶段的下一段继续。

    规则包按批次钉版解析，而不是看 settings 当前版；钉版缺失即拒绝。
    """
    batch = store.get_batch(batch_id)
    if batch is None:
        raise errors.NotFound(f"批次不存在：{batch_id}", code="B_EMPTY")
    if batch["status"] == "done":
        raise errors.Conflict(f"批次 {batch_id} 已完成，无需续跑",
                              code="B_IDEMPOTENT_REPLAY")

    pinned_version = batch.get("rule_version") or ""
    if not rulepacks.is_registered(pinned_version):
        # 旧批次的规则包被撤掉了：明确拒绝，不许悄悄换成最新版
        raise rulepacks.RulePackMissing(pinned_version or "(空)")
    pack = rulepacks.get(pinned_version)

    stored_stages = store.load_stages(batch_id)
    mark = state.watermark(stored_stages)
    if not mark["stage"]:
        raise errors.Conflict("批次没有可用断点，请重新接入", code="B_RESUME_NO_STATE")

    # 源文件可能已被替换：重新读取只做摘要核对，不覆盖已完成阶段的检查点
    live_intake = m1_service.intake(settings)
    live_digest = manifest.digest_parts(live_intake.digest_parts)
    source_changed = live_digest != batch["input_digest"]

    batch["status"] = "running"
    note = batch.get("note") or ""
    if source_changed:
        batch["note"] = (f"{note}；源文件已替换，已完成阶段按受理时快照续跑"
                         if note else "源文件已替换，已完成阶段按受理时快照续跑")
    store.upsert_batch(batch)

    try:
        result = _pipeline(settings=settings, store=store, batch=batch, pack=pack,
                           intake=live_intake, input_digest=live_digest,
                           inputs=list(live_intake.inputs),
                           params=json.loads(batch.get("params_json") or "{}"),
                           started_at=batch["created_at"],
                           clock=time.perf_counter(), resume_from=mark)
    except Exception as exc:  # noqa: BLE001
        batch["status"] = "aborted"
        batch["finished_at"] = _now()
        batch["note"] = f"{type(exc).__name__}: {exc}"
        store.upsert_batch(batch)
        raise
    result["resumed"] = True
    result["resumed_from"] = mark
    result["source_changed"] = source_changed
    return result


def _pipeline(*, settings, store, batch, pack, intake, input_digest, inputs, params,
              started_at, clock, resume_from: dict | None = None) -> dict:
    """五段流水线。resume_from 给水位时，已完成阶段直接从检查点还原，不重算。"""
    stages = []
    saved_stages = {row["stage"]: row for row in store.load_stages(batch["batch_id"])}
    start_index = resume_from["index"] + 1 if resume_from else 0
    batch_id = batch["batch_id"]

    # ---- 阶段 1：import（续跑时从检查点还原 Intake） ----
    if start_index <= 0:
        mark = time.perf_counter()
        tables_with_sample = len([1 for table in intake.ddl.tables
                                  if intake.rows(table.name)])
        stage_row = state.stage_row(
            "import", in_count=len(intake.ddl.tables), out_count=tables_with_sample,
            quarantine=len(intake.quarantine),
            elapsed_ms=_ms(mark), note=f"分段 {intake.section_count}")
        _commit_stage(store, batch_id, "import", pack.version, input_digest,
                      stage_row, checkpoint.encode_intake(intake))
    else:
        stage_row = _restore_stage(store, batch_id, "import", saved_stages)
        intake = checkpoint.decode_intake(
            store.load_checkpoint(batch_id, "import")["payload"])
    stages.append(stage_row)

    # ---- 阶段 2：contract ----
    if start_index <= 1:
        mark = time.perf_counter()
        coerced = m2_service.build(intake, _settings_with_version(settings, pack.version))
        column_total = sum(len(table.columns) for table in coerced.tables)
        stage_row = state.stage_row(
            "contract", in_count=column_total,
            out_count=len([1 for table in coerced.tables
                           for column in table.columns if column.present]),
            quarantine=len(coerced.quarantine), elapsed_ms=_ms(mark))
        _commit_stage(store, batch_id, "contract", pack.version, input_digest,
                      stage_row, checkpoint.encode_coerced(coerced))
    else:
        stage_row = _restore_stage(store, batch_id, "contract", saved_stages)
        coerced = checkpoint.decode_coerced(
            store.load_checkpoint(batch_id, "contract")["payload"])
    stages.append(stage_row)

    # ---- 阶段 3：profile ----
    if start_index <= 2:
        mark = time.perf_counter()
        profiled = m3_service.build(coerced)
        adopted = len([1 for column in profiled.columns
                       if column["disposition"] == "adopted"])
        stage_row = state.stage_row(
            "profile", in_count=len(profiled.columns), out_count=adopted,
            quarantine=len(profiled.columns) - adopted, elapsed_ms=_ms(mark),
            note=f"冲突 {len(profiled.conflicts)}")
        _commit_stage(store, batch_id, "profile", pack.version, input_digest,
                      stage_row, checkpoint.encode_profiled(profiled))
    else:
        stage_row = _restore_stage(store, batch_id, "profile", saved_stages)
        profiled = checkpoint.decode_profiled(
            store.load_checkpoint(batch_id, "profile")["payload"])
    stages.append(stage_row)

    # ---- 阶段 4：keys ----
    if start_index <= 3:
        mark = time.perf_counter()
        modeled = m4_service.build(coerced, profiled)
        accounts = m3_rules.build(columns=profiled.columns,
                                  row_ledger=profiled.row_ledger,
                                  keys=modeled.keys, star=modeled.star,
                                  metric_sums=profiled.metric_sums)
        confirmed = len([1 for item in modeled.keys
                         if item["status"] in ("confirmed", "inferred")])
        stage_row = state.stage_row(
            "keys", in_count=len(coerced.tables), out_count=confirmed,
            quarantine=len(modeled.quarantine), elapsed_ms=_ms(mark),
            note=f"外键推断 {len([1 for i in modeled.keys if i['status'] == 'inferred'])}")
        # keys 阶段把 modeled/accounts 一并放进检查点，spec 阶段可独立续
        store.upsert_stage_row(batch_id, stage_row)
        store.save_checkpoint(
            batch_id=batch_id, stage="keys", rule_version=pack.version,
            input_digest=input_digest,
            payload={"modeled": _jsonable(modeled), "accounts": accounts})
    else:
        stage_row = _restore_stage(store, batch_id, "keys", saved_stages)
        saved = store.load_checkpoint(batch_id, "keys")["payload"]
        modeled = Modeled(keys=saved["modeled"]["keys"],
                          star=saved["modeled"]["star"],
                          quarantine=saved["modeled"]["quarantine"],
                          codes=saved["modeled"]["codes"])
        accounts = saved["accounts"]
    stages.append(stage_row)

    # ---- 阶段 5：spec + 落库 ----
    if start_index <= 4:
        mark = time.perf_counter()
        spec_rows = spec_out.rows(profiled, modeled, accounts)
        stage_row = state.stage_row(
            "spec", in_count=len(modeled.keys) + len(modeled.star),
            out_count=len(spec_rows), elapsed_ms=_ms(mark))
        # spec 阶段先落计数行与检查点，再做整批事务落库；落库失败也留得住断点
        store.upsert_stage_row(batch_id, stage_row)
        store.save_checkpoint(
            batch_id=batch_id, stage="spec", rule_version=pack.version,
            input_digest=input_digest, payload={"spec_rows": len(spec_rows)})
    else:
        stage_row = _restore_stage(store, batch_id, "spec", saved_stages)
    stages.append(stage_row)

    return _finalize(settings=settings, store=store, batch=batch, pack=pack,
                     profiled=profiled, modeled=modeled, accounts=accounts,
                     stages=stages, params=params, inputs=inputs,
                     started_at=started_at, clock=clock, input_digest=input_digest)


def _settings_with_version(settings, rule_version: str):
    """给下游模块一个钉版 settings：dataclass frozen，用 replace 换版本号。"""
    from dataclasses import replace
    if getattr(settings, "rule_version", None) == rule_version:
        return settings
    return replace(settings, rule_version=rule_version)


def _commit_stage(store, batch_id, stage, rule_version, input_digest,
                  stage_row, payload) -> None:
    store.upsert_stage_row(batch_id, stage_row)
    store.save_checkpoint(batch_id=batch_id, stage=stage,
                          rule_version=rule_version,
                          input_digest=input_digest, payload=payload)


def _restore_stage(store, batch_id, stage, saved_stages) -> dict:
    row = saved_stages.get(stage)
    if row is None:
        raise errors.Conflict(f"阶段 {stage} 缺少断点状态，无法续跑",
                              code="B_RESUME_NO_STATE")
    return state.stage_row(
        row["stage"], in_count=row["in_count"], out_count=row["out_count"],
        quarantine=row["quarantine_count"], status=row["status"],
        elapsed_ms=row["elapsed_ms"], note=row.get("note", ""))


def _jsonable(modeled) -> dict:
    """Modeled 是 dataclass，字段都是 dict/list/Decimal 字符串，转纯 JSON 结构。"""
    return {"keys": list(modeled.keys), "star": list(modeled.star),
            "quarantine": list(modeled.quarantine), "codes": list(modeled.codes)}


def _finalize(*, settings, store, batch, pack, profiled, modeled, accounts,
              stages, params, inputs, started_at, clock, input_digest) -> dict:
    quarantine = (list(_intake_quarantine(store, batch["batch_id"]))
                  + list(profiled.quarantine) + list(modeled.quarantine))
    totals = {
        "accounts_json": json.dumps(accounts, ensure_ascii=False),
        "columns_total": accounts["columns"]["total"],
        "rows_total": accounts["rows"]["total"],
        "metric_total": accounts["metric_total"],
        "quarantine_total": len(quarantine),
        "conflict_total": len(profiled.conflicts),
        "spec_rows": stages[-1]["out_count"],
        "sample_rows": sum(row["total"] for row in profiled.row_ledger),
    }
    finished_at = _now()
    elapsed = int((time.perf_counter() - clock) * 1000)
    batch["status"] = "done"
    batch["finished_at"] = finished_at
    batch["rule_version"] = pack.version
    if batch.get("note"):
        batch["note"] = batch["note"]
    else:
        batch["note"] = ""

    manifest_doc = manifest.build(
        run_id=batch["run_id"], batch_id=batch["batch_id"],
        batch_key_value=batch["batch_key"], settings=_settings_with_version(
            settings, pack.version), inputs=inputs,
        params=params, started_at=started_at, finished_at=finished_at,
        elapsed_ms=elapsed, stages=stages,
        counters={key: str(value) for key, value in totals.items()
                  if key not in ("accounts_json",)},
        input_digest=input_digest)
    manifest_doc["rule_pack"] = pack.as_contract()
    manifest_doc["resume_of"] = batch.get("resume_of") or ""

    tables = [dict(table, role=_role_of(modeled, table["table_name"]),
                   role_reason=_role_code(modeled, table["table_name"]),
                   scd=_scd_of(modeled, table["table_name"]),
                   scd_reason=_scd_code(modeled, table["table_name"]),
                   metric_count=_metric_count(modeled, table["table_name"]),
                   fk_count=_fk_count(modeled, table["table_name"]))
              for table in profiled.tables]
    star_rows = [{
        "table_name": row["table_name"], "role": row["role"],
        "scd": row["scd"], "reason_code": row["role_reason"],
        "metric_columns": [item["column"] for item in row["metric_columns"]],
        "fk_count": row["fk_count"], "degenerate": row["degenerate"],
        "evidence": {**row["evidence"], "role_reason": row["role_reason"],
                     "scd_reason": row["scd_reason"]},
    } for row in modeled.star]
    key_rows = [{
        "kind": item["kind"], "child_table": item["child_table"],
        "child_columns": item["child_columns"], "parent_table": item["parent_table"],
        "parent_columns": item["parent_columns"], "declared": item["declared"],
        "status": item["status"], "inclusion": item["inclusion"],
        "cardinality_ratio": item["cardinality_ratio"],
        "reason_code": item["reason_code"],
        "evidence": {**item["evidence"], "note": item.get("note", "")},
    } for item in modeled.keys]

    store.save_run(
        batch=batch, tables=tables, columns=profiled.columns, keys=key_rows,
        star=star_rows, quarantine=quarantine, conflicts=profiled.conflicts,
        row_ledger=profiled.row_ledger, stages=stages, totals=totals,
        manifest=manifest_doc)

    return {
        "status": "done",
        "replayed": False,
        "resumed": False,
        "source_changed": False,
        "code": "",
        "batch_id": batch["batch_id"],
        "batch_key": batch["batch_key"],
        "run_id": batch["run_id"],
        "rule_version": pack.version,
        "input_digest": input_digest,
        "elapsed_ms": elapsed,
        "stages": stages,
        "accounts": accounts,
        "counts": store.count_rows(batch["batch_id"]),
        "watermark": state.watermark(stages),
        "manifest": manifest_doc,
    }


def _intake_quarantine(store, batch_id: str) -> list:
    """import 阶段的隔离行存在检查点里（Intake 不进业务表），落库时取回。"""
    saved = store.load_checkpoint(batch_id, "import")
    if saved is None:
        return []
    return saved["payload"].get("quarantine", [])


def _ms(mark: float) -> int:
    return int((time.perf_counter() - mark) * 1000)


def _row_of(modeled, table_name: str) -> dict | None:
    lowered = table_name.lower()
    for row in modeled.star:
        if row["table_name"].lower() == lowered:
            return row
    return None


def _role_of(modeled, table_name: str) -> str:
    row = _row_of(modeled, table_name)
    return row["role"] if row else "pending"


def _role_code(modeled, table_name: str) -> str:
    row = _row_of(modeled, table_name)
    return row["role_reason"] if row else ""


def _scd_of(modeled, table_name: str) -> str:
    row = _row_of(modeled, table_name)
    return row["scd"] if row else ""


def _scd_code(modeled, table_name: str) -> str:
    row = _row_of(modeled, table_name)
    return row["scd_reason"] if row else ""


def _metric_count(modeled, table_name: str) -> int:
    row = _row_of(modeled, table_name)
    return len(row["metric_columns"]) if row else 0


def _fk_count(modeled, table_name: str) -> int:
    row = _row_of(modeled, table_name)
    return row["fk_count"] if row else 0


def bundle(store, batch_id: str) -> dict:
    """批次详情：页面与导出共用一次装配，避免每页各拼一套。"""
    batch = store.get_batch(batch_id)
    if batch is None:
        raise errors.NotFound(f"批次不存在：{batch_id}", code="B_EMPTY")
    totals = store.load_totals(batch_id)
    accounts = json.loads(totals.get("accounts_json") or "null")
    return {
        "batch": batch,
        "tables": store.load_tables(batch_id),
        "columns": store.load_columns(batch_id),
        "keys": store.load_keys(batch_id),
        "star": store.load_star(batch_id),
        "quarantine": store.load_quarantine(batch_id),
        "conflicts": store.load_conflicts(batch_id),
        "row_ledger": store.load_row_ledger(batch_id),
        "stages": store.load_stages(batch_id),
        "checkpoints": store.list_checkpoints(batch_id),
        "totals": totals,
        "accounts": accounts,
        "manifest": store.load_manifest(batch_id) or {},
    }


def row(batch: dict, counts: dict | None = None) -> dict:
    """批次行：列表与详情共用，状态文案只在这里写一次。"""
    return {
        "batch_id": batch["batch_id"],
        "batch_key": batch["batch_key"][:12],
        "run_id": batch["run_id"],
        "status": batch["status"],
        "status_label": {"done": "完成", "running": "运行中", "failed": "失败",
                         "aborted": "中断"}.get(batch["status"], batch["status"]),
        "created_at": (batch.get("created_at") or "").replace("T", " ")[:19],
        "finished_at": (batch.get("finished_at") or "").replace("T", " ")[:19],
        "input_digest": (batch.get("input_digest") or "")[:12],
        "rule_version": batch.get("rule_version") or "",
        "resume_of": batch.get("resume_of") or "",
        "counts": counts or {},
        "note": batch.get("note") or "",
    }


def rows(batches: list, store) -> list:
    return [row(batch, store.count_rows(batch["batch_id"])) for batch in batches]


def stage_rows(stages: list) -> list:
    return [
        {"stage": item["stage"],
         "label": config.STAGE_LABELS.get(item["stage"], item["stage"]),
         "in_count": item["in_count"], "out_count": item["out_count"],
         "quarantine_count": item["quarantine_count"], "status": item["status"],
         "elapsed_ms": item["elapsed_ms"], "note": item.get("note", "")}
        for item in stages
    ]


def list_batches(store, limit: int = 30) -> list:
    return rows(store.list_batches(limit), store)


def accounts_of(store, batch_id: str) -> dict:
    totals = store.load_totals(batch_id)
    raw = totals.get("accounts_json")
    if not raw:
        raise errors.NotFound("该批次没有对账结果", code="X_EMPTY_SELECTION")
    accounts = json.loads(raw)
    accounts["rows_page"] = m3_rules.rows_for_page(accounts)
    accounts["batch_id"] = batch_id
    accounts["metric_total"] = str(Decimal(accounts["metric_total"]).quantize(
        config.DECIMAL_QUANT))
    return accounts


def codes_of(rows: list) -> list:
    return sorted({row["code"] for row in rows if row.get("code")})
