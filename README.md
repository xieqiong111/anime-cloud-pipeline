# Anime Cloud Pipeline V5

面向 Linux NAS 的动漫云媒体自动化流水线：以 **MoviePilot** 订阅为发现来源，经 **115 云盘** 下载与 Season 平铺整理、**LitePan** 生成 STRM、补充本地侧车元数据与剧照，最终刷新 **Emby** 媒体库完成入库。支持 **Hermes 不可变 Release 部署** 与 **独立 V5 / Agent Skill 运行**，每个实例保留唯一生产调度所有者。

> 本仓库提供可复现打包、只读上线门禁、状态机与事务审计、季度资源规划、云盘平铺整理及隔离回归测试。真实凭据、NAS 配置、SQLite 状态库与运维脚本均保留在仓库外部。

---

## 核心特性

- **当前季度资格与范围隔离**
  - 通过 `qualify` 严格校验 MoviePilot 订阅：限定授权来源（`admin` / `Bangumi订阅`）、Anime 类型（`genre 16`）以及对应季度首播日期。
  - 通过 `scope-check` 将当前季度可运行身份与范围外历史 `needs_review` 隔离（`outside_declared_scope` 原样保留、不跨季推断或推进）。
- **订阅自动补集与有界快照推进**
  - 普通 `run` 与 `dry-run` 同步核查当前季度合格订阅的已播单集日历，对常规 RSS 未覆盖的已播集按查询预算与游标补搜历史来源。
  - 每次 `run` / `run --resume-only` 仅推进一个有界快照；新提交下载后优先收尾核验已有任务与下游状态，不以“提交成功”代替入库完成。
- **115 云盘 Season 平铺结构与自净**
  - 新下载先在临时 `fid-*` 目录内隔离改名并校验文件 ID、大小与指纹，再以单写者事务移动为 **`作品/Season N/作品 - S01E01.ext`**（视频直接放在 `Season N` 下）。
  - 移动并回读整季清单后，原地删除已核验为空的 `fid-*` 暂存目录（`staging_archive_state: "deleted"`）及空源下载目录。
- **LitePan STRM 双根分离与全量库存证明**
  - 分离 `raw` 与 `published` STRM 根目录并校验内容一致；严格区分媒体文件、STRM、集级 NFO/剧照与剧季共享图片，库存清单不完整或归属冲突时自动阻断提交。
- **侧车容错与首播次日剧照补全**
  - 刚开播剧集若仅缺单集剧照（`<stem>.jpg`）而 NFO 与其余侧车齐备，允许以 `complete_without_episode_image` 完成入库。
  - 每次 `run` 结束后自动为**首播日期早于当天**的已入库集从 TMDB 补齐缺失剧照（不覆盖已有图片，单轮有上限且失败隔离不影响主流程）。
- **季度资源规划与中文化 Emby 展示名**
  - `plan-resources` 只读盘点 8 项季度资源（云盘季度/源/整理目录、本地 raw/published 目录、LitePan 任务与 Webhook 规则、Emby 媒体库）并生成绑定配置与季度的 64 位 `config_revision`。
  - Emby 库显示名自动映射为中文季度名（**一月新番 / 四月新番 / 七月新番 / 十月新番**），云端与磁盘目录保持 `YYYY-MM月新番` 年份前缀；仅在显式双变量授权下执行 `prepare-resources --apply`。
- **事务审计与人工复核**
  - SQLite 持久化单集状态与阶段历史；异常或不确定远程状态进入 `needs_review`，须按原因与 `generation` 做 CAS 核销。

---

## 流程与状态机

```text
MoviePilot 订阅发现与季度筛选
        ↓
  选源与库存核验 → 提交 115 磁链并校验回执 → 云盘文件对账
        ↓
  115 Season 平铺整理（fid-* 改名 → 移入 Season N → 删除空暂存/源目录）
        ↓
  LitePan STRM 生成 → 侧车刮削（次日 TMDB 补剧照）→ Emby 精确身份路径入库
```

核心状态流转：

```text
discovered → download_pending → submitted → cloud_ready
→ organize_pending → organized → strm_pending → strm_ready
→ scrape_pending → metadata_ready → library_pending → library_ready → notified
```

---

## 运行环境要求

- Linux NAS（支持 POSIX `sh`）
- Python **3.12** 或更高版本
- SQLite
- 可用的 MoviePilot、115、LitePan 与 Emby 服务及授权配置

> 在 Hermes 部署模式下，代码 release 目录由部署账号拥有，运行账号仅拥有状态、日志与备份等可变目录；请勿对安装根目录递归执行 `chown`。

---

## 快速开始（Hermes Release 部署）

以下 `<...>` 为占位符，执行前请替换为目标环境实际值：

1. **构建可校验交付包**
   ```sh
   python3 deploy/hermes_release.py build \
     --source . \
     --output "<artifact-output>/anime-cloud-pipeline-hermes.zip" \
     --release-id "<release-id>" \
     --source-revision "<source-revision>"
   ```
2. **解压并校验 SHA-256 清单**
   ```sh
   unzip -q "<release-archive>.zip" -d "<artifact-staging-root>"
   cd "<artifact-staging-root>/anime-cloud-pipeline-hermes"
   sha256sum -c MANIFEST.sha256
   ```
3. **准备外部配置与受限环境文件（`0600`）**
   ```sh
   cp deploy/config.v5.example.json "<config-path>"
   cp deploy/anime-cloud-pipeline.env.example "<env-file>"
   chmod 600 "<env-file>"
   ```
4. **安装并执行只读门禁**
   ```sh
   sh deploy/install.sh \
     --source . \
     --install-root "<install-root>" \
     --config "<config-path>" \
     --env-file "<env-file>" \
     --release-id "<release-id>"

   "<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" doctor
   "<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" preflight
   "<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" migrate --check-only
   "<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" dry-run
   ```

完整部署、激活与回滚说明见 [Hermes NAS 部署手册](references/deployment-hermes.md) 与 [V5 升级指南](docs/V5-UPGRADE.md)；独立原生部署可使用 `scripts/v5_bootstrap.py` 与 `scripts/run_pipeline.py`（见 [Skill 操作契约](SKILL.md)）。

---

## 常用命令

```text
# 只读诊断与预览
anime-cloud-pipeline preflight
anime-cloud-pipeline migrate --check-only
anime-cloud-pipeline qualify
anime-cloud-pipeline scope-check
anime-cloud-pipeline plan-resources
anime-cloud-pipeline dry-run [--details]
anime-cloud-pipeline status
anime-cloud-pipeline reviews list
anime-cloud-pipeline audit
anime-cloud-pipeline cloud-layout
anime-cloud-pipeline cloud-names

# 写入操作（须取得对应授权）
anime-cloud-pipeline run
anime-cloud-pipeline run --resume-only
anime-cloud-pipeline reviews resolve --id <id> --generation <gen> --resolution '<json>'
```

**季度资源授权制备**（独立授权，不会自动触发下载或入库）：

```sh
OCTOP_PIPELINE_ALLOW_RESOURCES=1 \
OCTOP_PIPELINE_RESOURCE_REVISION="<plan-resources-config_revision>" \
"<install-root>/current/bin/anime-cloud-pipeline" prepare-resources --apply
```

---

## 本地测试

```sh
python3.12 -m compileall -q scripts deploy
PYTHONPATH=scripts python3.12 -m unittest discover -s scripts/tests -p 'test*.py'
```

> POSIX 权限、符号链接、ACL、NAS 挂载及定时任务行为须在目标 Linux NAS 上实测验证。

---

## 项目结构

| 路径 | 说明 |
| --- | --- |
| `scripts/pipeline/` | 核心状态机、服务适配、资格与范围隔离、库存归属、Season 平铺、剧照补全与季度资源协调 |
| `scripts/run_pipeline.py` | V5 通用 Python CLI 入口 |
| `scripts/v5_bootstrap.py` / `scripts/v5_plugin_secret.py` | 独立运行环境的只读凭据引导模块 |
| `scripts/hermes_runtime.py` | Hermes 部署入口与环境边界 |
| `scripts/tests/` | 隔离单元测试与回归套件 |
| `deploy/` | 交付打包、安装、回滚工具与脱敏配置样例 |
| `references/`、`docs/V5-UPGRADE.md` | 架构、状态机、季度资源契约、恢复策略与部署文档 |
| `CHANGELOG-V5.md` | V5 版本变更与验证边界记录 |
| `SKILL.md` | Agent 操作契约 |

---

## 安全与运维边界

- **零凭据入库**：严禁在 Git、日志、报告或状态库中记录 Cookie、Token、API Key、播放地址或真实内部服务地址。
- **单一调度所有权**：沿用既有唯一 NAS cron 或原生 agent 定时任务，禁止新增第二套轮询或进程内 `sleep` 等待循环。
- **证据驱动完成判定**：HTTP 200、磁链已提交或单个 STRM 存在均不代表入库完成；必须同时核验云盘路径、STRM 内容一致性、侧车元数据与 Emby 精确身份路径。
- **故障即停与成对回滚**：库存未知、路径越界或身份冲突时立即阻断；代码回滚须与对应 SQLite 备份成对处理。
