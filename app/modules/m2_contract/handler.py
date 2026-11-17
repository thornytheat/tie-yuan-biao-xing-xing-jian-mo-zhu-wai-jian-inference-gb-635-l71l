"""M2 入口：列契约文档 + 单列归一化试跑。

试跑接口用来验证一条规则，不落库；参数不合法直接 4xx，不做兜底。
"""
from __future__ import annotations

from fastapi import APIRouter

from ... import config, errors, rulepacks
from . import coerce, contract, evolve

router = APIRouter(tags=["M2 类型规则"])

VALID_KINDS = {"code", "int", "bigint", "decimal", "date", "timestamp", "text"}


@router.get("/api/contract")
def get_contract(rule_version: str | None = None) -> dict:
    document = contract.document(rule_version)
    document["evolve_rules"] = evolve.rule_rows()
    return document


@router.get("/api/rule-packs")
def get_rule_packs() -> dict:
    """规则包目录：页面核对当前版本与历史版本的渠道归一差异。"""
    return {
        "current_version": rulepacks.CURRENT_VERSION,
        "rows": rulepacks.catalog(),
    }


@router.post("/api/contract/probe")
def probe(payload: dict) -> dict:
    raw_values = payload.get("values")
    kind = str(payload.get("kind") or "").strip()
    if not isinstance(raw_values, list) or not raw_values:
        raise errors.BadRequest("values 必须是非空数组", code="E_BAD_REQUEST")
    if kind and kind not in VALID_KINDS:
        raise errors.BadRequest(f"kind 只能是 {sorted(VALID_KINDS)} 之一",
                                code="E_BAD_REQUEST")
    # 版本缺失在这里就 422，不静默退回最新版
    requested_version = payload.get("rule_version")
    pack = (rulepacks.get(str(requested_version)) if requested_version
            else rulepacks.current())
    values = [str(item) for item in raw_values]
    column = str(payload.get("column") or "").strip()
    width = int(payload.get("width") or 0)
    if not kind:
        kind, codes = coerce.infer_kind(values, str(payload.get("base_type") or ""))
    else:
        codes = []
    normalized, rejected = [], []
    for value in values:
        state = coerce.classify_raw(value)
        if state != "value":
            rejected.append({"raw": value, "code": "Q_SENTINEL_VALUE", "state": state})
            continue
        canonical, _ = coerce.alias_of(value, column, pack)
        if kind == "code":
            text, code = coerce.normalize_code(value, width)
            normalized.append(text)
            if code:
                codes.append(code)
            continue
        if kind in ("int", "bigint", "decimal"):
            number, code = coerce.parse_decimal(value)
            if number is None:
                rejected.append({"raw": value, "code": code, "state": "unparsable"})
                continue
            if kind == "int":
                normalized.append(str(int(number)))
            elif kind == "bigint":
                normalized.append(str(number))
            else:
                normalized.append(str(number.quantize(config.DECIMAL_QUANT)))
            continue
        if kind == "date":
            parsed, code = coerce.parse_date(value)
            if parsed is None:
                rejected.append({"raw": value, "code": code, "state": "unparsable"})
                continue
            normalized.append(parsed.isoformat())
            continue
        if kind == "timestamp":
            parsed, code = coerce.parse_timestamp(value)
            if parsed is None:
                rejected.append({"raw": value, "code": code, "state": "unparsable"})
                continue
            normalized.append(parsed.strftime("%Y-%m-%d %H:%M:%S"))
            continue
        normalized.append(canonical)
        if canonical != value:
            codes.append("K_ALIAS_MERGED")
    return {
        "kind": kind,
        "kind_label": coerce.kind_label(kind),
        "normalized": normalized,
        "rejected": rejected,
        "codes": sorted(set(codes)),
        "contract_version": contract.CONTRACT_VERSION,
        "rule_version": pack.version,
        "channel_note": pack.channel_note,
    }
