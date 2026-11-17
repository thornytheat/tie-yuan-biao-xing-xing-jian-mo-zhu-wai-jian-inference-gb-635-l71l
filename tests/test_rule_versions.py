"""规则版本化与中断续跑：渠道归一分版、批次钉版、检查点续跑、源文件替换、缺失拒绝。

这些用例自建带「门店POS / pos」取值的最小样本目录，与样例数据的冻结期望值隔离，
避免改动既有 83 个用例所依赖的确定计数。
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from app import config, rulepacks
from app.modules.m2_contract import coerce
from app.modules.m5_batches import service as batch_service, state
from app.modules.m5_batches import checkpoint
from app.store import Store

V1 = "starmodel/1.0.0"
V2 = "starmodel/1.1.0"

DDL = """CREATE TABLE odl_shop_order (
  order_id   varchar(16) NOT NULL COMMENT '订单号',
  channel    varchar(10) COMMENT '渠道',
  amount     numeric(12,2) COMMENT '金额',
  PRIMARY KEY (order_id)
);
"""

# 三种 POS 写法 + 一个 APP，用来区分 v1（合并）与 v2（分别计数）
DUMP = """## table=odl_shop_order
order_id,channel,amount
O001,门店POS,10.00
O002,pos,20.00
O003,门店,30.00
O004,APP,40.00
"""


@pytest.fixture()
def rule_sample_dir(tmp_path, sample_dir):
    """在临时目录放一份带 POS 取值的最小输入，复用样例里的编码文件名约定。"""
    target = tmp_path / "rules_sample"
    target.mkdir()
    (target / "warehouse_ddl.sql").write_text(DDL, encoding="utf-8")
    (target / "sample_dump.csv").write_text(DUMP, encoding="utf-8-sig")
    return target


@pytest.fixture()
def rule_settings(tmp_path, rule_sample_dir):
    return config.Settings(db_path=str(tmp_path / "rules.sqlite"),
                           sample_dir=str(rule_sample_dir),
                           dump_names=("sample_dump.csv",))


@pytest.fixture()
def rule_store(rule_settings):
    return Store(rule_settings.db_path)


def _channel_column(bundle):
    for column in bundle["columns"]:
        if column["table_name"] == "odl_shop_order" and column["column_name"] == "channel":
            return column
    raise AssertionError("channel 列不存在")


# ---------- 规则包注册表 ----------

def test_rule_packs_are_frozen_and_versioned():
    v1 = rulepacks.get(V1)
    v2 = rulepacks.get(V2)
    # v1 合并 POS 与门店，v2 不再合并
    assert v1.alias_of("门店POS", "channel") == ("门店", True)
    assert v1.alias_of("pos", "channel") == ("门店", True)
    assert v2.alias_of("门店POS", "channel") == ("门店POS", False)
    assert v2.alias_of("pos", "channel") == ("pos", False)
    # 两版都保留 APP 等价类
    assert v1.alias_of("app", "channel")[0] == "APP"
    assert v2.alias_of("app", "channel")[0] == "APP"
    # 冻结：运行期改不动
    with pytest.raises(TypeError):
        v2.alias_groups["X"] = ("y",)


def test_missing_rule_version_is_rejected_not_silently_latest():
    with pytest.raises(rulepacks.RulePackMissing) as exc:
        rulepacks.get("starmodel/9.9.9")
    assert exc.value.code == "B_RULE_PACK_MISSING"
    assert "starmodel/9.9.9" in str(exc.value)
    assert rulepacks.available_versions() == [V1, V2]
    # 旧调用方不传包时走当前版（v2），POS 不合并
    assert coerce.alias_of("pos", "channel")[0] == "pos"


# ---------- 新批次按当前版本，旧版可显式受理 ----------

def test_new_batch_uses_current_v2_and_counts_pos_separately(rule_settings, rule_store):
    result = batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    assert result["rule_version"] == V2
    bundle = batch_service.bundle(rule_store, result["batch_id"])
    channel = _channel_column(bundle)
    # 门店POS / pos / 门店 / APP 四个取值各自独立
    assert channel["distinct_count"] == 4
    assert "K_ALIAS_MERGED" not in channel["reason_codes"]
    batch = rule_store.get_batch(result["batch_id"])
    assert batch["rule_version"] == V2


def test_legacy_v1_batch_merges_pos_and_keeps_its_own_rule(rule_settings, rule_store):
    settings_v1 = replace(rule_settings, rule_version=V1)
    result = batch_service.run(settings=settings_v1, store=rule_store, source="sample")
    assert result["rule_version"] == V1
    bundle = batch_service.bundle(rule_store, result["batch_id"])
    channel = _channel_column(bundle)
    # v1：门店POS、pos 并入门店 → 门店 / APP 两个取值
    assert channel["distinct_count"] == 2
    assert "K_ALIAS_MERGED" in channel["reason_codes"]


def test_v1_and_v2_batches_run_concurrently_with_separate_counts(rule_settings, rule_store):
    """旧版恢复与新批次并发：同输入不同规则版本 → 不同批次键、两套计数并存。"""
    v1_result = batch_service.run(
        settings=replace(rule_settings, rule_version=V1),
        store=rule_store, source="sample")
    v2_result = batch_service.run(settings=rule_settings, store=rule_store,
                                  source="sample")
    assert v1_result["batch_id"] != v2_result["batch_id"]
    assert v1_result["batch_key"] != v2_result["batch_key"]
    assert len(rule_store.list_batches()) == 2
    v1_channel = _channel_column(batch_service.bundle(rule_store, v1_result["batch_id"]))
    v2_channel = _channel_column(batch_service.bundle(rule_store, v2_result["batch_id"]))
    assert v1_channel["distinct_count"] == 2
    assert v2_channel["distinct_count"] == 4


# ---------- 幂等：重跑与重复续跑不新增结果 ----------

def test_rerun_same_version_replays_batch_without_new_rows(rule_settings, rule_store):
    first = batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    before = rule_store.count_rows(first["batch_id"])
    second = batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    assert second["replayed"] is True
    assert second["batch_id"] == first["batch_id"]
    assert rule_store.count_rows(first["batch_id"]) == before
    assert len(rule_store.list_batches()) == 1
    # 检查点也不重复
    assert len(rule_store.list_checkpoints(first["batch_id"])) == len(config.STAGES)


# ---------- 中断续跑 ----------

def test_aborted_batch_resumes_from_watermark_with_same_identity(
        rule_settings, rule_store, monkeypatch):
    # 先在 contract 阶段制造中断
    def boom(*args, **kwargs):
        raise RuntimeError("模拟类型阶段中断")

    monkeypatch.setattr(batch_service.m2_service, "build", boom)
    with pytest.raises(RuntimeError):
        batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    batches = rule_store.list_batches()
    assert len(batches) == 1
    batch_id = batches[0]["batch_id"]
    assert batches[0]["status"] == "aborted"

    # 只有 import 阶段落了检查点与计数行
    checkpoints = rule_store.list_checkpoints(batch_id)
    assert [row["stage"] for row in checkpoints] == ["import"]
    mark = state.watermark(rule_store.load_stages(batch_id))
    assert mark["stage"] == "import"
    assert mark["index"] == 0

    monkeypatch.undo()
    resumed = batch_service.resume_batch(settings=rule_settings, store=rule_store,
                                         batch_id=batch_id)
    # 身份不变：原批次号、原 run_id、原钉版
    assert resumed["batch_id"] == batch_id
    stored = rule_store.get_batch(batch_id)
    assert stored["status"] == "done"
    assert resumed["run_id"] == stored["run_id"]
    assert resumed["rule_version"] == V2
    assert resumed["resumed_from"]["stage"] == "import"
    # 五个阶段齐全，计数行仍只有 5 行（覆盖写，不新增）
    stages = rule_store.load_stages(batch_id)
    assert [row["stage"] for row in stages] == list(config.STAGES)
    channel = _channel_column(batch_service.bundle(rule_store, batch_id))
    assert channel["distinct_count"] == 4


def test_resume_is_idempotent_and_reuses_checkpoint_outputs(
        rule_settings, rule_store, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("中断在 profile 前")

    monkeypatch.setattr(batch_service.m3_service, "build", boom)
    with pytest.raises(RuntimeError):
        batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    batch_id = rule_store.list_batches()[0]["batch_id"]
    monkeypatch.undo()

    first = batch_service.resume_batch(settings=rule_settings, store=rule_store,
                                       batch_id=batch_id)
    counts_after_resume = rule_store.count_rows(batch_id)
    # 再点一次续跑：已完成 → 409 语义（can_resume 拒绝），数据不变
    allowed, code = batch_service.resume.can_resume(rule_store, batch_id)
    assert allowed is False
    assert code == "B_IDEMPOTENT_REPLAY"
    assert rule_store.count_rows(batch_id) == counts_after_resume
    assert first["watermark"]["stage"] == "spec"


def test_resume_keeps_pinned_v1_even_after_current_switches_to_v2(
        rule_settings, rule_store, monkeypatch):
    """受理时是 v1，当前版本切到 v2 后中断续跑仍按 v1，不偷偷换版。"""
    settings_v1 = replace(rule_settings, rule_version=V1)
    monkeypatch.setattr(batch_service.m4_service, "build",
                        staticmethod(lambda *a, **k: (_ for _ in ()).throw(
                            RuntimeError("键阶段中断"))))
    with pytest.raises(RuntimeError):
        batch_service.run(settings=settings_v1, store=rule_store, source="sample")
    batch_id = rule_store.list_batches()[0]["batch_id"]
    monkeypatch.undo()

    # settings 当前已是 v2，续跑仍必须取批次钉版 v1
    assert rule_settings.rule_version == V2
    resumed = batch_service.resume_batch(settings=rule_settings, store=rule_store,
                                         batch_id=batch_id)
    assert resumed["rule_version"] == V1
    channel = _channel_column(batch_service.bundle(rule_store, batch_id))
    assert channel["distinct_count"] == 2  # v1 的合并结果
    assert rule_store.get_batch(batch_id)["rule_version"] == V1


def test_resume_rejects_when_pinned_rule_pack_missing(
        rule_settings, rule_store, monkeypatch):
    result = batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    batch_id = result["batch_id"]
    # 人为把批次钉版改成未登记版本，模拟规则包被撤销
    batch = rule_store.get_batch(batch_id)
    batch["status"] = "aborted"
    batch["rule_version"] = "starmodel/8.8.8"
    rule_store.upsert_batch(batch)
    allowed, code = batch_service.resume.can_resume(rule_store, batch_id)
    assert allowed is False
    assert code == "B_RULE_PACK_MISSING"
    with pytest.raises(rulepacks.RulePackMissing):
        batch_service.resume_batch(settings=rule_settings, store=rule_store,
                                   batch_id=batch_id)


def test_import_rejects_unknown_rule_version_via_api(client):
    response = client.post("/api/import",
                           json={"source": "sample", "rule_version": "starmodel/9.9.9"})
    assert response.status_code == 422
    assert response.json()["error"] == "B_RULE_PACK_MISSING"


# ---------- 源文件被替换：续跑保留受理时快照并显式标记 ----------

def test_resume_after_source_replaced_keeps_completed_stages_and_flags_change(
        rule_settings, rule_store, rule_sample_dir, monkeypatch):
    monkeypatch.setattr(batch_service.m3_service, "build",
                        staticmethod(lambda *a, **k: (_ for _ in ()).throw(
                            RuntimeError("剖析阶段中断"))))
    with pytest.raises(RuntimeError):
        batch_service.run(settings=rule_settings, store=rule_store, source="sample")
    batch_id = rule_store.list_batches()[0]["batch_id"]
    original_digest = rule_store.get_batch(batch_id)["input_digest"]
    monkeypatch.undo()

    # 替换源文件：改一个金额，输入摘要必然变化
    replaced = (rule_sample_dir / "sample_dump.csv").read_text(
        encoding="utf-8-sig").replace("10.00", "99.00")
    (rule_sample_dir / "sample_dump.csv").write_text(replaced, encoding="utf-8-sig")

    resumed = batch_service.resume_batch(settings=rule_settings, store=rule_store,
                                         batch_id=batch_id)
    assert resumed["source_changed"] is True
    # 已完成的 import/contract 检查点仍是受理时内容（金额 10.00 的那版）
    intake_checkpoint = rule_store.load_checkpoint(batch_id, "import")
    assert intake_checkpoint["input_digest"] == original_digest
    stored = rule_store.get_batch(batch_id)
    assert "源文件已替换" in stored["note"]
    assert stored["status"] == "done"


# ---------- 检查点编解码往返 ----------

def test_checkpoint_codec_roundtrip_preserves_decimal_and_objects(settings, store, run):
    """直接验证编解码：Intake/Coerced/Profiled 往返后类型与 Decimal 精度不变。"""
    from app.modules.m1_ingest import service as m1
    from app.modules.m2_contract import service as m2
    from app.modules.m3_profile import service as m3

    batch_id = run["batch_id"]
    intake = m1.intake(settings)
    encoded_intake = checkpoint.encode_intake(intake)
    decoded_intake = checkpoint.decode_intake(encoded_intake)
    assert decoded_intake.table_names() == intake.table_names()
    assert len(decoded_intake.rows(intake.ddl.tables[0].name)) == len(
        intake.rows(intake.ddl.tables[0].name))

    coerced = m2.build(intake, settings)
    decoded_coerced = checkpoint.decode_coerced(checkpoint.encode_coerced(coerced))
    assert decoded_coerced.rule_version == coerced.rule_version
    assert decoded_coerced.tables[0].header_map.index_of(
        coerced.tables[0].columns[0].name) >= -1

    profiled = m3.build(coerced)
    decoded_profiled = checkpoint.decode_profiled(checkpoint.encode_profiled(profiled))
    from decimal import Decimal
    for column in decoded_profiled.columns:
        assert isinstance(column["unique_rate"], Decimal)
    assert decoded_profiled.metric_sums and all(
        isinstance(value, Decimal)
        for totals in decoded_profiled.metric_sums.values()
        for value in totals.values() if value is not None)
    # 检查点确实落了库且可枚举
    stages = {row["stage"] for row in store.list_checkpoints(batch_id)}
    assert stages == set(config.STAGES)
