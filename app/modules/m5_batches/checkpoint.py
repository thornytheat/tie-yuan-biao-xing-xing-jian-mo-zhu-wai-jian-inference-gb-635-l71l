"""阶段检查点编解码：把 M1/M2 的 dataclass 流水线压成 JSON，再原样还原。

续跑的语义是「从已完成阶段的下一段继续」，所以检查点必须能还原出后续阶段真正
依赖的对象，而不是只存计数：
- import 检查点还原 Intake（Ddl/TableDef/ColumnDef/Section/SampleRow）；
- contract 检查点还原 Coerced（TableRun/ColumnRun/HeaderMap）；
- profile 检查点还原 Profiled（纯 dict/list，但比率与度量和是 Decimal）。

Decimal 一律以字符串落盘、按原量化位还原；dataclass 带 _type 标记重建，
保证中断进程重启后对象行为与首跑一致。
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal

from ..m1_ingest.ddl import ColumnDef, Ddl, TableDef
from ..m1_ingest.service import Intake
from ..m1_ingest.sniff import SampleRow, Section
from ..m2_contract.evolve import HeaderMap
from ..m2_contract.service import Coerced, ColumnRun, TableRun
from ..m3_profile.service import Profiled

# 各阶段产物里需要按 Decimal 还原的字段路径（集合语义，顺序无关）
_DECIMAL_FIELDS = {
    "profile_columns": {
        "null_rate", "unique_rate", "unparsable_rate", "metric_total",
    },
}


def _mark(obj, type_name: str) -> dict:
    data = asdict(obj)
    data["_type"] = type_name
    return data


# ---------- 编码 ----------

def encode_intake(intake: Intake) -> dict:
    tables = []
    for table in intake.ddl.tables:
        columns = [_mark(column, "ColumnDef") for column in table.columns]
        tables.append({
            "_type": "TableDef",
            "name": table.name, "ordinal": table.ordinal,
            "columns": columns, "primary_key": list(table.primary_key),
            "foreign_keys": list(table.foreign_keys),
        })
    sections = {}
    for key, section in intake.sections.items():
        sections[key] = {
            "_type": "Section",
            "name": section.name, "header_line": section.header_line,
            "header": list(section.header),
            "rows": [{"_type": "SampleRow", "line_no": row.line_no,
                      "cells": list(row.cells)} for row in section.rows],
            "dup_headers": section.dup_headers,
            "short_rows": list(section.short_rows),
            "long_rows": list(section.long_rows),
            "delimiter": section.delimiter,
        }
    return {
        "_type": "Intake",
        "ddl": {"_type": "Ddl", "tables": tables, "codes": list(intake.ddl.codes)},
        "inputs": list(intake.inputs),
        "sections": sections,
        "quarantine": list(intake.quarantine),
        "codes": list(intake.codes),
        "section_count": intake.section_count,
    }


def encode_coerced(coerced: Coerced) -> dict:
    tables = []
    for table in coerced.tables:
        columns = [_mark(column, "ColumnRun") for column in table.columns]
        tables.append({
            "_type": "TableRun",
            "name": table.name, "ordinal": table.ordinal,
            "declared_pk": list(table.declared_pk),
            "foreign_keys": list(table.foreign_keys),
            "header_map": _mark(table.header_map, "HeaderMap"),
            "columns": columns, "row_total": table.row_total,
            "valid_rows": list(table.valid_rows),
            "valid_lines": list(table.valid_lines),
            "rows_norm": list(table.rows_norm),
            "null_row_count": table.null_row_count,
            "quarantined_count": table.quarantined_count,
            "case_conflicts": list(table.case_conflicts),
            "column_widenings": list(table.column_widenings),
        })
    return {
        "_type": "Coerced",
        "tables": tables,
        "quarantine": list(coerced.quarantine),
        "codes": list(coerced.codes),
        "rule_version": coerced.rule_version,
    }


def encode_profiled(profiled: Profiled) -> dict:
    payload = {
        "_type": "Profiled",
        "tables": list(profiled.tables),
        "columns": list(profiled.columns),
        "row_ledger": list(profiled.row_ledger),
        "quarantine": list(profiled.quarantine),
        "conflicts": list(profiled.conflicts),
        "metric_sums": profiled.metric_sums,
        "codes": list(profiled.codes),
    }
    # Decimal → 字符串，落盘不丢精度
    for column in payload["columns"]:
        for field_name in _DECIMAL_FIELDS["profile_columns"]:
            if column.get(field_name) is not None and not isinstance(
                    column[field_name], str):
                column[field_name] = str(column[field_name])
    payload["metric_sums"] = {
        table: {name: (str(value) if value is not None else None)
                for name, value in totals.items()}
        for table, totals in profiled.metric_sums.items()}
    return payload


# ---------- 解码 ----------

def _decode_column_def(data: dict) -> ColumnDef:
    return ColumnDef(
        name=data["name"], ordinal=data["ordinal"], raw_type=data["raw_type"],
        base_type=data["base_type"], width=data["width"], precision=data["precision"],
        scale=data["scale"], nullable=data["nullable"], comment=data["comment"],
        inline_ref=tuple(data["inline_ref"]) if data.get("inline_ref") else None,
        inline_pk=data.get("inline_pk", False))


def _decode_table_def(data: dict) -> TableDef:
    table = TableDef(name=data["name"], ordinal=data["ordinal"],
                     primary_key=list(data["primary_key"]),
                     foreign_keys=list(data["foreign_keys"]))
    table.columns = [_decode_column_def(item) for item in data["columns"]]
    return table


def _decode_section(data: dict) -> Section:
    section = Section(name=data["name"], header_line=data["header_line"],
                      header=list(data["header"]),
                      rows=[SampleRow(line_no=row["line_no"], cells=list(row["cells"]))
                            for row in data["rows"]],
                      dup_headers=data["dup_headers"],
                      short_rows=list(data["short_rows"]),
                      long_rows=list(data["long_rows"]),
                      delimiter=data.get("delimiter", ","))
    return section


def decode_intake(payload: dict) -> Intake:
    ddl_data = payload["ddl"]
    ddl = Ddl(tables=[_decode_table_def(item) for item in ddl_data["tables"]],
              codes=list(ddl_data.get("codes", [])))
    intake = Intake(ddl=ddl, inputs=list(payload.get("inputs", [])),
                    quarantine=list(payload.get("quarantine", [])),
                    codes=list(payload.get("codes", [])),
                    section_count=payload.get("section_count", 0))
    intake.sections = {key: _decode_section(data)
                       for key, data in payload.get("sections", {}).items()}
    return intake


def _decode_header_map(data: dict) -> HeaderMap:
    return HeaderMap(table_name=data["table_name"], mapped=list(data["mapped"]),
                     renames=[tuple(item) for item in data["renames"]],
                     unmatched=list(data["unmatched"]),
                     missing=list(data["missing"]))


def _decode_column_run(data: dict) -> ColumnRun:
    return ColumnRun(
        table_name=data["table_name"], name=data["name"], ordinal=data["ordinal"],
        ddl_type=data["ddl_type"], base_type=data["base_type"], width=data["width"],
        precision=data["precision"], scale=data["scale"], nullable=data["nullable"],
        comment=data["comment"], kind=data["kind"], pad_width=data["pad_width"],
        sample_rows=data["sample_rows"], empty_count=data["empty_count"],
        sentinel_count=data["sentinel_count"], unparsable=data["unparsable"],
        present=data["present"], values=list(data["values"]),
        raw_values=list(data["raw_values"]), codes=list(data["codes"]),
        widened_from=data.get("widened_from", ""),
        padded_examples=list(data.get("padded_examples", [])))


def _decode_table_run(data: dict) -> TableRun:
    table = TableRun(
        name=data["name"], ordinal=data["ordinal"],
        declared_pk=list(data["declared_pk"]),
        foreign_keys=list(data["foreign_keys"]),
        header_map=_decode_header_map(data["header_map"]),
        row_total=data["row_total"], valid_rows=list(data["valid_rows"]),
        valid_lines=list(data["valid_lines"]), rows_norm=list(data["rows_norm"]),
        null_row_count=data["null_row_count"],
        quarantined_count=data["quarantined_count"],
        case_conflicts=list(data["case_conflicts"]),
        column_widenings=list(data["column_widenings"]))
    table.columns = [_decode_column_run(item) for item in data["columns"]]
    return table


def decode_coerced(payload: dict) -> Coerced:
    return Coerced(
        tables=[_decode_table_run(item) for item in payload["tables"]],
        quarantine=list(payload["quarantine"]), codes=list(payload["codes"]),
        rule_version=payload.get("rule_version", ""))


def decode_profiled(payload: dict) -> Profiled:
    columns = [dict(column) for column in payload["columns"]]
    for column in columns:
        for field_name in _DECIMAL_FIELDS["profile_columns"]:
            if column.get(field_name) is not None:
                column[field_name] = Decimal(str(column[field_name]))
    metric_sums = {
        table: {name: (Decimal(str(value)) if value is not None else None)
                for name, value in totals.items()}
        for table, totals in payload.get("metric_sums", {}).items()}
    return Profiled(
        tables=list(payload["tables"]), columns=columns,
        row_ledger=list(payload["row_ledger"]),
        quarantine=list(payload["quarantine"]),
        conflicts=list(payload["conflicts"]), metric_sums=metric_sums,
        codes=list(payload.get("codes", [])))


DECODERS = {
    "Intake": decode_intake,
    "Coerced": decode_coerced,
    "Profiled": decode_profiled,
}


def decode(stage: str, payload: dict):
    """按阶段名选解码器；未知阶段直接报错，不猜结构。"""
    decoder = DECODERS.get(stage)
    if decoder is None:
        raise KeyError(f"没有阶段 {stage} 的检查点解码器")
    return decoder(payload)


def json_default(obj):
    """store 落盘兜底：dataclass 转字典、Decimal 转字符串。"""
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"检查点里出现不可序列化的对象：{type(obj)!r}")
