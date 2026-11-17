"""M1 入口：触发接入、查看原始输入、导入页。

接入会跑完整条流水线（M2→M4），因为元数据本身没有中间可停留的状态；
落库后由 /api/profile、/api/model 读取。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ... import config, errors, reasons, rulepacks
from ...web import Ctx, get_ctx, render
from ..m3_profile import quarantine as quarantine_mod
from ..m5_batches import service as batch_service
from . import service

router = APIRouter(tags=["M1 元数据接入"])

VALID_SOURCES = ("sample",)


def _params(payload: dict | None) -> tuple[str, dict, str | None]:
    payload = payload or {}
    source = str(payload.get("source") or "sample")
    if source not in VALID_SOURCES:
        raise errors.BadRequest(f"source 只能是 {list(VALID_SOURCES)} 之一",
                                code="E_BAD_REQUEST")
    raw_cap = payload.get("sample_cap", config.MAX_SAMPLE_ROWS_PER_TABLE)
    try:
        cap = int(raw_cap)
    except (TypeError, ValueError):
        raise errors.BadRequest("sample_cap 必须是整数", code="E_BAD_REQUEST") from None
    if not 1 <= cap <= 5000:
        raise errors.BadRequest("sample_cap 必须在 1..5000 之间", code="E_BAD_REQUEST")
    # 显式版本先校验注册表：未登记直接 422，不静默回退
    raw_version = payload.get("rule_version")
    rule_version = str(raw_version).strip() if raw_version else None
    if rule_version:
        rulepacks.get(rule_version)
    return source, {"sample_cap": cap}, rule_version


@router.post("/api/import")
def api_import(payload: dict | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    source, params, rule_version = _params(payload)
    from dataclasses import replace
    settings = (replace(ctx.settings, rule_version=rule_version)
                if rule_version else ctx.settings)
    result = batch_service.run(settings=settings, store=ctx.store, source=source,
                              params=params)
    if not result.get("replayed"):
        result["quarantine_total"] = int(
            ctx.store.load_totals(result["batch_id"]).get("quarantine_total", 0))
    return result


@router.get("/api/intake")
def api_intake(ctx: Ctx = Depends(get_ctx)) -> dict:
    payload = service.summary(ctx.settings)
    batch = ctx.current_batch()
    payload["batch_id"] = batch["batch_id"] if batch else None
    payload["reason_catalog"] = reasons.catalog()[:4]
    return payload


@router.get("/")
def page_index(request: Request, ctx: Ctx = Depends(get_ctx)):
    batch = ctx.current_batch()
    preview = service.summary(ctx.settings)
    payload = {
        "inputs": preview["inputs"],
        "tables": preview["tables"],
        "section_count": preview["section_count"],
        "quarantine_preview": [],
        "distribution": [],
        "accounts": None,
        "manifest": {},
        "totals": {},
    }
    if batch is not None:
        rows = ctx.store.load_quarantine(batch["batch_id"])
        payload["quarantine_preview"] = rows[:8]
        payload["distribution"] = quarantine_mod.distribution(rows)
        payload["manifest"] = ctx.store.load_manifest(batch["batch_id"]) or {}
        payload["totals"] = ctx.store.load_totals(batch["batch_id"])
        try:
            payload["accounts"] = batch_service.accounts_of(ctx.store, batch["batch_id"])
        except errors.NotFound:
            payload["accounts"] = None
    return render(request, "index.html", page="import", page_title="导入与剖析",
                  payload=payload, batch=batch)
