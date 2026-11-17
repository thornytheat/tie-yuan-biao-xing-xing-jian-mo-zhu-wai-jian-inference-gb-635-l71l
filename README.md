# starmodel · 贴源表星型建模与主外键推断

禾丰零售数仓组的贴源层建模辅助工具：读入 DDL 与样本导出件，逐列剖析空值率与唯一率，
推断主键与外键，给出事实表 / 维度表 / SCD 建议，最后输出一份可打印的规格单。

## 能力

- 编码回退（UTF-8 / UTF-8-BOM / GB18030）与 BOM 剥离，分段样本与重复表头识别
- 列类型推断：编码、整数、大整数、数值、日期、时间戳、文本
- 空值率、唯一率、去重数、度量和，全部 Decimal 与固定精度
- 候选主键与外键推断：包含度 + 基数比，声明与数据不一致时判单向引用
- 星型建议：事实 / 维度 / 待定、SCD 类型、退化维度
- 隔离区与冲突表可导出 CSV，运行清单可导出 JSON
- 同输入重跑幂等；四账（列 / 行 / 引用 / 对象）自洽
- 规则包版本化冻结：渠道归一分 `starmodel/1.0.0`（POS 并入门店）与 `starmodel/1.1.0`（POS 分别计数），新批次用当前版、批次受理即钉版，版本缺失明确拒绝不套最新版
- 五阶段检查点：中断后按批次钉版从已完成阶段的下一段续跑，沿用原批次号 / run_id / 账目；源文件被替换只标记不重算已完成段，重复续跑不新增结果
- 变更前评估：下游 SQL 与报表影响面、迁移拓扑序、回滚提示与阻断项

## 模块

| 模块 | 动作 | 入口 |
|---|---|---|
| M1 元数据接入 | 读 DDL 与样本，切分段，逐行诊断 | `POST /api/import` |
| M2 类型规则 | 列契约、阈值、单列归一化试跑、规则包目录 | `GET /api/contract` `GET /api/rule-packs` `POST /api/contract/probe` |
| M3 质量剖析 | 列剖析、隔离区、四账对账 | `GET /api/profile` `GET /api/quarantine` `GET /api/reconcile` |
| M4 键推断与建议 | 主键外键结论、星型对象 | `GET /api/model` |
| M5 版本与重跑 | 批次历史、批次详情、续跑、检查点核对 | `GET /api/batches` `GET /api/batches/{id}` `POST /api/batches/{id}/resume` |
| M6 规格单导出 | 规格单、列规格 CSV、证据 CSV、清单 JSON | `GET /api/spec` |
| M7 建模影响评估 | 变更影响面、迁移顺序、回滚提示、单条变更试算 | `GET /api/impact` `POST /api/impact/propose` |

## 核心规则

1. 候选主键 = 唯一率 1.0000 且无空值；声明主键优先，组合键按 DDL 声明。
2. 外键包含度 = 子表去重取值落在父表内的比例；门槛 0.95，父列必须是单列主键。
3. 声明外键只看包含度；推断外键另要求基数比落在 [0.02, 0.95]。
4. 事实表 = 含度量且外键 ≥ 2 且有主键；被其他表引用的表优先作维度。
5. SCD2 与 SCD1 的分界是「可变更列占比 > 0.50」，没有版本列的维度按 SCD0 处理。
6. 影响级别 = 引用方式 × 优先级 × 变更破坏性：P1 精确引用加破坏性变更判中断，整表引用一律核对。
7. 唯一性先定空值规则：含空值的键免检（与 UNIQUE 一致）或全算，两者结论相反就阻断。
8. 迁移顺序按依赖拓扑排：清洗 → 类型 → 改名 → 键 → 引用；成环整组阻断。

数字化定义与字段含义见 `docs/DATA-DICTIONARY.md`，陷阱与测试映射见 `docs/DEPTH.md`。

## 规则版本与中断续跑

规则包在 `app/rulepacks.py` 登记，版本号一旦登记内容即冻结，升级只发新版本：

| 版本 | 渠道归一（`channel` 列） |
|---|---|
| `starmodel/1.0.0` | `门店POS`、`pos` 并入代表元 `门店`，合并计数 |
| `starmodel/1.1.0`（当前） | `门店POS`、`pos` 不再并入 `门店`，分别计数；APP、小程序等价类不变 |

- **新批次用当前版**：默认按 `config.RULE_VERSION`，也可在接入时传 `rule_version` 显式选版；
  同输入两版产生不同批次键，旧版恢复与新批次可并发、各用各的规则。
- **批次受理即钉版**：版本写进批次行与运行清单；中断续跑只认钉版，外部配置切换不影响旧批次。
- **规则缺失明确拒绝**：请求未登记版本返回 422 `B_RULE_PACK_MISSING`，绝不静默套最新版。
- **检查点续跑**：五个阶段每完成一段立即落检查点；中断后续跑从水位下一段继续，沿用原批次号、
  原 `run_id`、原账目，不另建任务。源文件被替换时重读只核对摘要并标记 `source_changed`，
  已完成阶段按受理时快照保留。重复续跑不新增结果，已完成批次续跑返回 409。
- 批次页（`/batches`）可核对每批钉版规则、渠道归一说明、检查点清单与恢复水位。

```bash
# 显式用旧版受理一批（与当前版批次并存）
curl -s -X POST http://127.0.0.1:18204/api/import -H 'Content-Type: application/json' \
  -d '{"source":"sample","rule_version":"starmodel/1.0.0"}'
# 中断批次续跑（沿用钉版与原批次号）
curl -s -X POST http://127.0.0.1:18204/api/batches/SM20261117122908/resume
# 查看规则包目录
curl -s http://127.0.0.1:18204/api/rule-packs
```

## 启动

```bash
docker compose up -d --build
curl -s http://127.0.0.1:18204/healthz
open http://127.0.0.1:18204/
```

首次打开页面点「运行接入」，或直接：

```bash
curl -s -X POST http://127.0.0.1:18204/api/import -H 'Content-Type: application/json' -d '{"source":"sample"}'
```

可用环境变量 `STARMODEL_RULE_VERSION` 切换新批次默认规则版本（只影响新批次，不动已受理批次）。
重建数据库：`docker compose down -v` 后重新 `up`（数据在命名卷 `starmodel-data`）。

本地跑测试：

```bash
pip install -r requirements.txt
pytest -q
```

## 页面

| 路径 | 页面 |
|---|---|
| `/` | 导入与剖析 |
| `/profile` | 列剖析 |
| `/keys` | 键推断 |
| `/star` | 星型建议 |
| `/batches` | 版本历史 |
| `/impact` | 影响评估 |
| `/spec` | 规格单（可打印） |

## 目录

```text
app/                 main/config/errors/reasons/manifest/store/web + 七个功能模块
app/templates/       base + 7 页
app/static/          theme/layout/components/print.css + 4 个 js
docs/                DEPTH.md  DATA-DICTIONARY.md
sample_data/         DDL + 两个样本导出件（一脏一 GB18030）+ 下游登记表
tests/               conftest + 每模块 1 个文件 + 端到端 + fixtures
```

## 已知不覆盖

见 `docs/DEPTH.md` 最后一节：分段内引号换行、物理 DDL 生成、同义列语义匹配、分区与代理键策略、
下游只算一跳影响。
