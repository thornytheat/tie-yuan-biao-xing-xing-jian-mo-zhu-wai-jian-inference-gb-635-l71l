"""M5 入口：批次历史、批次详情、续跑、四账明细页。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ... import errors, rulepacks
from ...web import Ctx, get_ctx, render
from . import resume, state
from . import service

router = APIRouter(tags=["M5 版本与重跑"])


def _resolved(ctx: Ctx, batch_id: str | None) -> str:
    batch = ctx.current_batch(batch_id)
    if batch is None:
        raise errors.NotFound("还没有任何批次，请先跑一次接入", code="B_EMPTY")
    return batch["batch_id"]


@router.get("/api/batches")
def api_batches(limit: int = 30, ctx: Ctx = Depends(get_ctx)) -> dict:
    if not 1 <= limit <= 200:
        raise errors.BadRequest("limit 必须在 1..200 之间", code="E_BAD_REQUEST")
    rows = service.list_batches(ctx.store, limit)
    return {"rows": rows, "total": len(rows),
            "rule_version": ctx.settings.rule_version,
            "current_rule_version": rulepacks.CURRENT_VERSION,
            "rule_packs": rulepacks.catalog()}


@router.get("/api/batches/{batch_id}")
def api_batch(batch_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    bundle = service.bundle(ctx.store, batch_id)
    pinned = bundle["batch"].get("rule_version") or ""
    return {
        "batch": service.row(bundle["batch"],
                            ctx.store.count_rows(batch_id)),
        "stages": bundle["stages"],
        "checkpoints": bundle["checkpoints"],
        "accounts": bundle["accounts"],
        "watermark": state.watermark(bundle["stages"]),
        "totals": bundle["totals"],
        "manifest": bundle["manifest"],
        "rule_pack": (rulepacks.get(pinned).as_contract()
                      if rulepacks.is_registered(pinned) else None),
        "rule_pack_missing": not rulepacks.is_registered(pinned) if pinned else False,
    }


@router.post("/api/batches/{batch_id}/resume")
def api_resume(batch_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    allowed, code = resume.can_resume(ctx.store, batch_id)
    if not allowed:
        if code == rulepacks.RULE_PACK_MISSING:
            batch = ctx.store.get_batch(batch_id)
            raise rulepacks.RulePackMissing(
                (batch or {}).get("rule_version") or "(空)")
        raise errors.Conflict(f"批次 {batch_id} 不需要续跑", code=code)
    return service.resume_batch(settings=ctx.settings, store=ctx.store,
                                batch_id=batch_id)


@router.get("/batches")
def page_batches(request: Request, ctx: Ctx = Depends(get_ctx)):
    rows = service.list_batches(ctx.store, 30)
    batch = ctx.current_batch()
    payload = {"rows": rows, "accounts": None, "manifest": {}, "ledger": [],
               "stages": [], "conflicts": [], "checkpoints": [],
               "rule_packs": rulepacks.catalog(),
               "current_rule_version": rulepacks.CURRENT_VERSION,
               "watermark": {"stage": "", "out": 0, "index": 0}}
    if batch is not None:
        bundle = service.bundle(ctx.store, batch["batch_id"])
        payload = {
            "rows": rows,
            "accounts": bundle["accounts"],
            "manifest": bundle["manifest"],
            "ledger": bundle["row_ledger"],
            "stages": bundle["stages"],
            "conflicts": bundle["conflicts"],
            "checkpoints": bundle["checkpoints"],
            "rule_packs": rulepacks.catalog(),
            "current_rule_version": rulepacks.CURRENT_VERSION,
            "watermark": state.watermark(bundle["stages"]),
        }
    return render(request, "batches.html", page="batches", page_title="版本历史",
                  payload=payload, batch=batch)
