"""渠道归一规则包：可切换、可冻结、可按批次钉版。

设计要点（对应批次续跑需求）：
- 每个规则包是一份冻结快照，版本号一旦登记就不改内容；规则升级只发新版本，不覆盖旧版本；
- 新批次取 settings.rule_version 指向的「当前版本」；中断批次续跑一律按批次受理时
  钉在批次上的版本取包，外部配置怎么切都不影响旧批次；
- 注册表是版本的唯一来源，版本缺失时显式抛 RulePackMissing，绝不静默回退到最新版；
- 规则包目前只管渠道别名等价类，但快照结构预留 thresholds，后续规则升级同机制处理。

v1（starmodel/1.0.0）：「门店POS」与「pos」都并入代表元「门店」；
v2（starmodel/1.1.0）：两者不再合并，分别计数；只保留 APP 与小程序两组等价类。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType

from . import errors

RULE_PACK_MISSING = "B_RULE_PACK_MISSING"


class RulePackMissing(errors.Unprocessable):
    """请求的规则版本在注册表里不存在。

    用 422 而不是 404：请求本身能到达服务端，是服务端规则目录与批次钉版对不上，
    必须人工补登记或切回存在的版本，系统不允许悄悄套最新版。
    """

    def __init__(self, version: str, *, available: list[str] | None = None):
        available = available or []
        super().__init__(
            f"规则版本 {version} 未登记，拒绝执行（不会静默套用最新版）；"
            f"可用版本：{('、'.join(available) or '无')}",
            code=RULE_PACK_MISSING,
            detail={"requested": version, "available": available},
        )
        self.requested_version = version
        self.available_versions = available


@dataclass(frozen=True)
class RulePack:
    """一份不可变的规则快照。

    alias_columns：只有这些列（小写比较）才做别名合并；
    alias_groups：代表元 → 该主体的其他写法，匹配按等值，大小写敏感由组内写法决定；
    thresholds：随包冻结的阈值快照，仅用于清单展示与核对，不参与当前计算。
    """

    version: str
    label: str
    alias_columns: frozenset
    alias_groups: MappingProxyType
    channel_note: str
    thresholds: MappingProxyType = field(default_factory=lambda: MappingProxyType({}))

    def alias_of(self, value: str, column_name: str) -> tuple[str, bool]:
        """同一主体的多个写法合并到代表元；未登记列或未登记取值原样返回。

        返回 (归一值, 是否发生合并)。等价类内匹配先做精确等值，再做忽略大小写等值，
        保证与行序无关、与字典遍历顺序无关。
        """
        if column_name.lower() not in self.alias_columns:
            return value, False
        stripped = (value or "").strip()
        for canonical, variants in self.alias_groups.items():
            if stripped == canonical or stripped in variants:
                return canonical, stripped != canonical
            if stripped.lower() == canonical.lower():
                return canonical, stripped != canonical
        return stripped, False

    def as_contract(self) -> dict:
        """给列契约页/接口用的可 JSON 化视图。"""
        return {
            "rule_version": self.version,
            "label": self.label,
            "alias_columns": sorted(self.alias_columns),
            "alias_groups": {key: list(values)
                             for key, values in self.alias_groups.items()},
            "channel_note": self.channel_note,
        }


def _pack(version: str, label: str, alias_groups: dict, channel_note: str) -> RulePack:
    # 构造时一次性冻结，运行期任何模块都只能读
    frozen_groups = MappingProxyType({
        canonical: tuple(variants) for canonical, variants in alias_groups.items()
    })
    return RulePack(
        version=version,
        label=label,
        alias_columns=frozenset({"channel"}),
        alias_groups=frozen_groups,
        channel_note=channel_note,
        thresholds=MappingProxyType({
            "fk_inclusion_min": "0.95",
            "fk_cardinality_range": "[0.02, 0.95]",
            "scd2_change_ratio": "0.50",
        }),
    )


# v1：受理时的老规则，门店POS / pos 视为同一主体，并入「门店」
_PACK_V1 = _pack(
    "starmodel/1.0.0",
    "渠道归一一期：POS 写法并入门店",
    {
        "APP": ("app", "App", "APP端", "app端"),
        "小程序": ("微信小程序", "微信小程序端"),
        "门店": ("门店POS", "pos"),
    },
    "「门店POS」「pos」与「门店」合并为同一渠道「门店」",
)

# v2：新规则，POS 与门店不再合并；APP、小程序等价类保持不变
_PACK_V2 = _pack(
    "starmodel/1.1.0",
    "渠道归一二期：POS 与门店分别计数",
    {
        "APP": ("app", "App", "APP端", "app端"),
        "小程序": ("微信小程序", "微信小程序端"),
    },
    "「门店POS」「pos」不再并入「门店」，按各自渠道分别计数",
)

# 注册表：版本号 → 冻结规则包。新增版本只许往这里登记，不许改已登记的包。
_REGISTRY: dict[str, RulePack] = {
    _PACK_V1.version: _PACK_V1,
    _PACK_V2.version: _PACK_V2,
}

# 当前版本：新批次（含全新接入与未受理的重跑）默认取它；可被 settings 覆盖。
CURRENT_VERSION = _PACK_V2.version
DEFAULT_VERSION = _PACK_V1.version  # 兼容历史：未显式配置时的兜底仅用于旧库读取


def available_versions() -> list[str]:
    """按版本号升序返回全部已登记版本，页面与报错共用。"""
    return sorted(_REGISTRY)


def get(version: str) -> RulePack:
    """按版本号取冻结规则包；缺失即拒绝，不回退、不猜最新版。"""
    pack = _REGISTRY.get(version)
    if pack is None:
        raise RulePackMissing(version, available=available_versions())
    return pack


def current(settings=None) -> RulePack:
    """新批次使用的当前规则包；settings 未给版本时用注册表的 CURRENT_VERSION。"""
    version = getattr(settings, "rule_version", None) or CURRENT_VERSION
    return get(version)


def is_registered(version: str) -> bool:
    return version in _REGISTRY


def catalog() -> list[dict]:
    """规则包目录：批次页核对「这版规则到底改了什么」。"""
    return [pack.as_contract() | {"is_current": pack.version == CURRENT_VERSION}
            for pack in sorted(_REGISTRY.values(), key=lambda p: p.version)]
