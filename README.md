# Anime Cloud Pipeline V5

面向 Linux NAS 的动漫云媒体自动化流水线：以 MoviePilot 订阅作为发现来源，
将下载任务提交到 115 云盘，通过 LitePan 生成 STRM，补充媒体元数据，最后刷新
Emby 媒体库。支持 Hermes 部署和 NAS 原生 agent 调用；每个实例保留已经核实的
唯一生产调度所有者。

> 当前仓库提供可复现构建、不可变 release 安装、原子激活与回滚、只读诊断、
> 状态审计和人工 review 流程。5.4.5 的单集真实下载至 Emby 入库已在授权 NAS
> 上验证；其他部署的挂载、凭据、服务版本及播放仍须单独验收。

## 核心能力

- 单次有界执行：每次 `run` 处理一个快照，每集最多推进一个外部阶段。
- SQLite 持久状态：可恢复、可审计，不把“任务已接受”误判为“媒体已完成”。
- 人工复核：不确定的远程状态进入 `needs_review`，按原因提交结构化处理结果。
- 安全发布：确定性 ZIP、SHA-256 manifest、不可变 release、`current`/`previous` 原子切换。
- 安全配置：凭据只通过受限环境文件注入，不写入 JSON、日志、报告或 Git。
- 单一调度所有权：沿用实例已有的 NAS cron 或原生 agent 任务，不新增第二份调度或进程内睡眠循环。
- 只读上线门禁：`doctor`、`preflight`、`migrate --check-only` 和 `dry-run` 在写入前执行。
- 季度资源准备：先独立核实订阅资格，再盘点和复用资源，仅在授权模式内创建确证缺失的季度资源。
- 严格提交回执：标准磁链 SSP 接口校验成功标志和同一 hash，提交前持久化次数与意图，结果未知时停止自动重提。

## 流程概览

```text
MoviePilot subscription
        ↓
discover → select → submit to 115 → reconcile cloud files
        ↓
organize → LitePan STRM → metadata → Emby refresh → notify
        ↓
SQLite state / audit / needs_review
```

主要状态：

```text
discovered → download_pending → submitted → cloud_ready
→ organize_pending → organized → strm_pending → strm_ready
→ scrape_pending → metadata_ready → library_pending → library_ready → notified
```

## 运行要求

- Linux NAS，支持 POSIX `sh`
- Python 3.12 或更高版本
- SQLite 支持
- 可写的配置、状态、日志和备份目录
- MoviePilot、115、LitePan 与 Emby 的授权配置

release 代码由部署账号管理；运行服务账号仅拥有可变目录。不要对整个安装根目录
执行递归 `chown`，否则会破坏 release 与运行账号之间的权限边界。

## 快速开始

以下命令中的 `<...>` 是语义占位符，执行前必须替换为目标环境中的值；不要复制
开发机或其他 NAS 的路径。

### 1. 构建可验证的交付包

```sh
python3 deploy/hermes_release.py build \
  --source . \
  --output "<artifact-output>/anime-cloud-pipeline-hermes.zip" \
  --release-id "<release-id>" \
  --source-revision "<source-revision>"
```

交付包只包含允许发布的源码、脚本、示例配置和文档，不包含真实配置、环境文件、
数据库、日志、缓存或 Git 元数据。

### 2. 解压并验证

```sh
unzip -q "<release-archive>.zip" -d "<artifact-staging-root>"
cd "<artifact-staging-root>/anime-cloud-pipeline-hermes"
sha256sum -c MANIFEST.sha256
```

### 3. 准备外部配置

将示例复制到安装根目录的 `config/`，再替换语义占位符。凭据只写入权限为
`0600` 或更严格的环境文件，禁止提交到仓库。

```sh
cp deploy/config.v5.example.json "<config-path>"
cp deploy/anime-cloud-pipeline.env.example "<env-file>"
chmod 600 "<env-file>"
```

### 4. 安装但不激活

```sh
sh deploy/install.sh \
  --source . \
  --install-root "<install-root>" \
  --config "<config-path>" \
  --env-file "<env-file>" \
  --release-id "<release-id>"
```

### 5. 执行只读门禁

```sh
"<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" doctor
"<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" preflight
"<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" migrate --check-only
"<install-root>/releases/<release-id>/bin/anime-cloud-pipeline" dry-run
```

通过门禁后，再按照完整部署手册执行显式激活、独立数据库迁移、单条媒体验收和
NAS cron 替换。不要把 HTTP 200、下载任务已提交、单个 STRM 文件存在或媒体库
刷新请求已接受当作端到端完成证据。

完整步骤见 [Hermes NAS 部署手册](references/deployment-hermes.md)。

## 常用命令

安装后统一使用稳定入口：

```text
<install-root>/current/bin/anime-cloud-pipeline version
<install-root>/current/bin/anime-cloud-pipeline doctor
<install-root>/current/bin/anime-cloud-pipeline preflight
<install-root>/current/bin/anime-cloud-pipeline qualify
<install-root>/current/bin/anime-cloud-pipeline plan-resources
<install-root>/current/bin/anime-cloud-pipeline migrate --check-only
<install-root>/current/bin/anime-cloud-pipeline dry-run
<install-root>/current/bin/anime-cloud-pipeline run
<install-root>/current/bin/anime-cloud-pipeline status
<install-root>/current/bin/anime-cloud-pipeline reviews list
<install-root>/current/bin/anime-cloud-pipeline audit
```

`run`、正式 `migrate`、`reviews resolve`、激活、回滚和 cron 修改均可能写入本地
状态或调用外部服务，需要明确授权。

季度资源准备使用独立授权，不会自动启动下载、STRM、刮削或入库。先运行
`qualify`；有合格订阅时运行 `plan-resources`，审阅资源、映射和返回的
`config_revision`，再在本次已授权调用中执行：

```sh
OCTOP_PIPELINE_ALLOW_RESOURCES=1 \
OCTOP_PIPELINE_RESOURCE_REVISION="<fresh-plan-config_revision>" \
"<install-root>/current/bin/anime-cloud-pipeline" prepare-resources --apply
```

两个授权变量只用于该次命令，不写入持久环境文件。完成后重新运行 preflight 和
dry-run；仅媒体本身也已获得授权时，才继续限定范围的 `run`。资源实现当前按
Asia/Shanghai 推导季度，配置使用其他时区时须先完成兼容性审查。
完整资源和管理会话要求见[季度资源契约](references/quarter-resources.md)。

NAS 原生 agent 也可调用同一 Python CLI 和 Skill；保持该实例已有且核实的唯一
调度所有者。不要因本仓库提供 NAS cron 示例而给已有 agent 定时任务新增第二份调度。
本仓库不包含某台 NAS 的 SSH 包装器、私有启动器、配置或审批记录。

## 本地验证

```sh
python3 -m compileall -q scripts deploy
python3 -m unittest discover -s scripts -p 'test*.py'
```

Windows 上的测试不能证明 POSIX 所有权、符号链接、ACL、cron 或 NAS 挂载行为；
这些项目必须在目标 Linux NAS 上验证。

## 文档索引

- [Hermes NAS 部署](references/deployment-hermes.md)
- [本地运行边界](references/deployment-local.md)
- [V5 架构](references/architecture.md)
- [状态机与事务](references/state-machine.md)
- [候选选择](references/candidate-selection.md)
- [恢复策略](references/recovery.md)
- [升级与回滚](docs/V5-UPGRADE.md)
- [V5 变更记录](CHANGELOG-V5.md)
- [Skill 操作契约](SKILL.md)

服务适配文档位于 `references/`：MoviePilot、115、LitePan 和 Emby 各自独立说明。

## 项目结构

| 路径 | 内容 |
| --- | --- |
| `scripts/pipeline/` | 状态机、服务适配、资格与库存门禁、季度资源准备 |
| `scripts/tests/` | 隔离测试和回归用例 |
| `scripts/run_pipeline.py` | 通用 Python CLI |
| `scripts/hermes_runtime.py` | Hermes 部署入口与环境边界 |
| `deploy/` | 安装、验证、回滚工具及配置样例 |
| `references/`、`docs/V5-UPGRADE.md` | 操作契约和升级说明 |
| `SKILL.md` | Agent 使用入口 |

真实配置、部署记录、账户数据和运行日志放在仓库外；配置样例仅使用占位值。

## 安全边界

- 不提交或输出 Cookie、Token、API Key、播放地址、运行时 ID 或真实服务地址。
- 环境文件必须是普通文件，不得是符号链接，且权限不得宽于 `0600`。
- 对远端文件清单不完整、身份不确定或路径证据不足的情况，停止自动推进并进入 review。
- 代码回滚必须与匹配的 SQLite 备份作为同一个 release 单元处理。
- 任何真实写入前，先保存审计证据并确认当前调度器只有一个所有者。
