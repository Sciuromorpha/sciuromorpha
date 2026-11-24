# Sciuromorpha — Personal Data Lake & Media Vault Architecture

> **组织 / 仓库规划**: `github.com/Sciuromorpha/sciuromorpha` (Rust Cargo Workspace Monorepo)
>
> **核心理念**: 一个自托管、轻量无膨胀、控制面与数据面解耦的个人多媒体与数据集中管理平台。

## 1. 项目目标与核心特性

1. **多源内容归档与衍生处理（MVP 第一梯队）**
   * 支持社交媒体/网页媒体抓取（视频、音频、多图/插画、推文等），保留原始标题、作者、URL、发布时间及原始元数据。
   * **规则驱动的衍生资产（Derived Assets）**：支持按来源/标签规则或手动触发，自动从视频剥离音轨转码为 MP3（写入 ID3 元数据）并关联回原条目，支持在检索结果中一键切换“仅听音频”模式；同时支持图片/视频封面缩略图生成与字幕文本抽取。
2. **多副本内容寻址存储（Multi-Replica CAS）**
   * 逻辑条目（`items`）、物理内容（`blobs`，按 SHA-256 去重）与物理副本位置（`blob_replicas`）三层彻底解耦。
   * 支持同一份 Blob 在 **ZFS 本地池、Garage (S3)、加密公有云盘（rclone）、离线磁带（LTO）/冷备硬盘** 之间按策略多副本同步与冷热分层（例如：大体积原画视频落冷备/磁带，轻量 MP3 音轨与缩略图常驻热存储）。
3. **联邦检索（Immich 零冗余接入）**
   * 将现有 **Immich** 实例注册为只读上游存储与元数据提供者，增量同步照片的 GPS 地名、人脸人物、相册/标签与 OCR 文本进入统一索引，不重复拷贝照片本体。
4. **数据血缘、条目互引与两层作者归户（Provenance & Entity Resolution）**
   * 区分“统一自然人/实体（`authors`）”与“跨平台账号/马甲（`author_identities`）”。归档条目绑定到具体平台马甲；合并大小号时仅修改映射关系，历史血缘与原始平台署名零丢失。
   * **条目级引用图谱（Item-to-Item Graph）**：通过 `item_relations` 记录条目间的显式关联（如推文转发 B 站视频、笔记引用 PDF、原图与二创关联）。
5. **可演进的存量补跑与增量合并（Backfill & Meta Merge）**
   * 各微服务回写扩展元数据时采用 JSONB 增量合并语义（`extra_meta || $patch`），互不覆盖。
   * 支持按查询条件筛选存量条目并批量推入 NATS 队列（Backfill），方便未来接入新 Worker（如 Whisper 语音转写、pHash 相似图去重）时无痛补齐历史数据。
6. **高精度中日英检索与细粒度 ACL**
   * **Meilisearch** 专职中日英混排全文检索与多维过滤，**PostgreSQL** 作为 Single Source of Truth。
   * 默认私有，支持通过独立 `peer_tokens`（固定同伴）和 `share_links`（临时分享）授权只读访问；采用“Meilisearch 引擎层过滤 + PG 回查二次鉴权 + 后端流式代理防盗链”三重防护。

## 2. 技术栈与基础设施约束

| 层级 | 选型 | 职责与约束 |
| :--- | :--- | :--- |
| **应用语言** | **Rust** (Cargo Workspace) | 单仓库多二进制微服务，共享协议定义与依赖缓存 |
| **Web / UI** | **Axum** + **Askama** (SSR) + **HTMX** | 模板与静态资源内嵌于 `sciuromorpha-core`，零前端打包工具链 |
| **权威数据库** | **PostgreSQL** (`sqlx`) | 元数据、血缘、作者归户、多副本位置、规则、ACL 的唯一真相源 |
| **检索引擎** | **Meilisearch** | 独立 Index `vault_items`，独立 API Key，严禁直接暴露给前端 |
| **微服务总线** | **NATS (JetStream)** (`async-nats`) | 控制面实时通信：WorkQueue Pull Consumer（慢任务背压）+ 事件流 |
| **热 Cache (Staging)** | **ZFS 共享数据集** / **Garage S3** | 各微服务共享的临时数据面中转区，按 SHA-256 分桶 |
| **运行环境** | **Incus LXC** + **Cloudflare Tunnel** | 开发期单 `dev` 容器多进程；生产期拆独立 LXC，共享 `vault-net` 网桥与 `shift=true` ZFS 挂载 |

## 3. Monorepo 目录结构 (Cargo Workspace)

核心 Rust 服务与 Web UI 统一收拢在主仓库中，编译出多个独立二进制；仅未来的异构端（如浏览器剪藏插件、Python GPU OCR、Flutter 移动端）在 `Sciuromorpha` 组织下独立建库。

```text
sciuromorpha/
├── ARCHITECTURE.md                  # 本架构规格文档
├── Cargo.toml                       # Workspace 定义
├── migrations/                      # PostgreSQL DDL (sqlx migrate)
│   └── 0001_init.sql
└── crates/
    ├── sciuromorpha-common/         # 共享库：NATS 指令/事件协议、PG 领域模型、StagingStore Trait
    │   └── src/
    │       ├── lib.rs
    │       ├── events.rs            # NATS Payload 定义
    │       └── staging.rs           # LocalFs / S3(Garage) 热 Cache 抽象
    ├── sciuromorpha-core/           # 核心网关 + Web UI + 状态机 + ACL + Meilisearch 代理
    │   ├── assets/                  # 嵌入静态资源 (htmx.min.js, app.css)
    │   ├── templates/               # Askama SSR + HTMX 模板 (.html)
    │   │   ├── layout.html
    │   │   ├── index.html           # 检索主页（含“仅听音频”筛选）
    │   │   ├── partials/            # HTMX 局部刷新片段（卡片列表、播放器、投递状态、磁带回迁进度）
    │   │   ├── item_detail.html     # 详情页（视频/纯音频切换、多图浏览、关联条目、副本状态、Immich直达）
    │   │   └── authors.html         # 作者归户与马甲合并管理页
    │   └── src/
    │       ├── main.rs
    │       ├── auth.rs              # Owner (CF Access/Admin) / Peer / Guest 鉴权中间件
    │       ├── routes/              # Web UI, Ingest API, Backfill API, Blob 流式代理路由
    │       ├── search.rs            # Meilisearch 同步与双保险检索
    │       └── state_machine.rs     # 订阅 VAULT_EVT，推进 PG 状态、JSONB Merge、触发规则与多副本同步
    ├── ingest-agent-media/          # MVP Worker：封装 yt-dlp / gallery-dl 抓取社媒落盘热 Cache
    ├── media-agent/                 # MVP Worker：封装 ffmpeg 剥离 MP3、生成缩略图、抽取字幕文本
    ├── storage-agent/               # 存储同步 Worker：ZFS 归档 / Garage S3 / rclone / 磁带回迁
    └── connector-agent-immich/      # Phase 1.5/2：Immich 增量元数据同步连接器
```

## 4. 控制面与数据面分离：热 Cache 与微服务通信

**核心原则**：NATS 仅传递轻量的控制面 JSON 消息（含 `sha256` 与指令），**绝不通过消息总线传输文件本体**；所有文件二进制数据均通过共享的 **热 Cache（Staging Area）** 交换（Claim Check Pattern）。

### 4.1 热 Cache (`StagingStore`) 生命周期

* **存储布局**：按 SHA-256 前四位分桶，例如 `/mnt/staging/ab/cd/abcd...`。
* **同宿主机（默认）**：宿主机 ZFS 数据集（如 `tank/vault-staging`）通过 Incus `disk` 设备（开启 `shift=true` 统一 UID/GID 映射）同时挂载给各 LXC 容器，实现零拷贝 I/O。
* **跨宿主机扩展**：`StagingStore` Trait 同时提供 `S3/Garage` 实现，远端 GPU 或独立磁带机节点可直接通过内网 S3 读写中转区。
* **防误删与 GC 机制**：
  1. **写入与归档（Stage-out）**：新写入的 Blob 在 PG 中标记 `staging_pinned = true`。仅当该 `sha256` 的所有提取/转码任务完成，且在 `blob_replicas` 中满足持久化副本策略（至少 1 份在线持久副本 `status = 'synced'`）后，解除 Pin，由后台 GC 按 LRU 策略清理热 Cache 空间。
  2. **冷数据/磁带回迁（Stage-in）**：访问仅存于离线磁带的 Blob 时，`core` 下发 `stage_in` 指令并返回 HTMX 等待状态；`storage-agent` 挂带恢复文件到热 Cache 后发送完成事件，`core` 为其赋予冷却期租约（如 7 天不淘汰）并流式响应前端。

### 4.2 NATS JetStream 流与 Subject 规范

建立两个 JetStream Stream：
* **`VAULT_CMD`**（`RetentionPolicy::WorkQueue`）：任务派发，各 Agent 使用 **Pull Consumer** 按自身算力主动拉取（`fetch(1)`），长耗时任务期间定期发送 `in_progress()` 续期，天然支持背压与多实例负载均衡。
* **`VAULT_EVT`**（`RetentionPolicy::Limits`）：结果回报与状态广播，由 `sciuromorpha-core` 订阅并驱动状态机。

| Subject | 方向 | 消费方 | 说明 |
| :--- | :--- | :--- | :--- |
| `vault.cmd.ingest.url` | `core` $\rightarrow$ `ingest` | `ingest-agent-media` | 抓取指定 URL（视频/音频/图集）到热 Cache |
| `vault.cmd.media.extract_audio` | `core` $\rightarrow$ `media` | `media-agent` | 从热 Cache 视频剥离 MP3 并写入 ID3 标签 |
| `vault.cmd.media.thumbnail` | `core` $\rightarrow$ `media` | `media-agent` | 生成图片/视频 WebP 缩略图 |
| `vault.cmd.storage.<backend>.replicate` | `core` $\rightarrow$ `storage` | 对应后端 Agent | 将热 Cache Blob 同步至指定存储介质 |
| `vault.cmd.storage.<backend>.stage_in` | `core` $\rightarrow$ `storage` | 对应后端 Agent | 将冷介质/磁带中的 Blob 恢复到热 Cache |
| `vault.evt.ingest.completed` | `ingest` $\rightarrow$ `core` | `sciuromorpha-core` | 抓取完成，上报标准化元数据、作者与 Blob 列表 |
| `vault.evt.media.derivative_ready` | `media` $\rightarrow$ `core` | `sciuromorpha-core` | 衍生 MP3/缩略图/字幕文本准备就绪 |
| `vault.evt.item.meta_patch` | `any agent` $\rightarrow$ `core` | `sciuromorpha-core` | **Meta Merge**：上报局部元数据补丁（如 OCR 文本、pHash、转写字幕），`core` 以 JSONB `||` 增量合并 |
| `vault.evt.storage.synced` | `storage` $\rightarrow$ `core` | `sciuromorpha-core` | 副本落盘完成，回写 `locator` 与 `synced` 状态 |
| `vault.evt.storage.stage_ready` | `storage` $\rightarrow$ `core` | `sciuromorpha-core` | 离线冷文件已恢复至热 Cache，通知前端可播/可下 |

### 4.3 存量数据批量补跑（Backfill）与增量合并机制

* **Meta Merge 语义**：多个独立 Agent 可能并发为同一条 `item` 补充扩展字段（如 `media-agent` 写时长与码率，`whisper-agent` 写转写文本，`phash-agent` 写感知哈希）。`sciuromorpha-core` 在处理 `vault.evt.item.meta_patch` 时，统一采用 PostgreSQL 的局部合并语句（`UPDATE items SET extra_meta = extra_meta || $1::jsonb WHERE id = $2`），避免并发覆盖，并异步触发 Meilisearch 局部重索引。
* **Backfill API（`POST /api/admin/backfill`，仅 Owner 可调）**：
  * 接收筛选条件（如 `source_type`、`item_type`、缺失某个 `role` 或缺失某个 `extra_meta` key）以及目标 `VAULT_CMD` Subject 与参数模板。
  * `sciuromorpha-core` 从 PG 查出符合条件的存量 `item_id` 与对应 `blob_sha256`（若文件不在热 Cache 则按需先触发热副本拉取），以流控方式批量推入 NATS `VAULT_CMD` 队列。
  * 下游新上线的 Worker 通过 Pull Consumer 按自身吞吐匀速消化，实现“先存下来，以后随时用新工具补全元数据”的长期演进能力。

## 5. PostgreSQL 权威数据模型 (`migrations/0001_init.sql`)

```sql
-- ==========================================
-- 1. 存储介质与多副本内容寻址层 (Multi-Replica CAS)
-- ==========================================
CREATE TABLE storage_backends (
    id              TEXT PRIMARY KEY,          -- 'zfs-hot' | 'garage-s3' | 'rclone-gd' | 'lto-tape-01' | 'immich-main'
    driver          TEXT NOT NULL,             -- 'local_fs' | 's3' | 'rclone' | 'offline_tape' | 'immich_api'
    tier            TEXT NOT NULL,             -- 'hot' | 'warm' | 'cold' | 'offline'
    read_priority   INT  NOT NULL DEFAULT 100, -- 数值越小读取优先级越高
    is_online       BOOLEAN NOT NULL DEFAULT TRUE,
    config          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE blobs (
    sha256              TEXT PRIMARY KEY,      -- 64位十六进制 SHA-256
    size_bytes          BIGINT NOT NULL,
    mime_type           TEXT NOT NULL,
    staging_pinned      BOOLEAN NOT NULL DEFAULT TRUE,
    staging_lease_until TIMESTAMPTZ,           -- 冷数据回迁后的冷却期租约
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE blob_replicas (
    blob_sha256      TEXT NOT NULL REFERENCES blobs(sha256) ON DELETE CASCADE,
    backend_id       TEXT NOT NULL REFERENCES storage_backends(id),
    locator          TEXT NOT NULL,            -- ZFS相对路径 / S3 Key / 磁带卷标+tar路径 / Immich asset_id
    status           TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'synced' | 'missing' | 'corrupted'
    is_encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
    last_verified_at TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (blob_sha256, backend_id)
);

-- ==========================================
-- 2. 作者归户两层结构 (Entity Resolution)
-- ==========================================
CREATE TABLE authors (
    id              UUID PRIMARY KEY,          -- 应用层生成 UUIDv7
    display_name    TEXT NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE author_identities (
    id              UUID PRIMARY KEY,
    author_id       UUID NOT NULL REFERENCES authors(id),
    platform        TEXT NOT NULL,             -- 'youtube' | 'bilibili' | 'twitter' | 'pixiv' | 'immich' | 'local'
    platform_uid    TEXT NOT NULL,             -- 平台不可变 ID（防改名）
    handle          TEXT NOT NULL,             -- 当前昵称/用户名
    profile_url     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform, platform_uid)
);
CREATE INDEX idx_author_identities_author ON author_identities(author_id);

-- ==========================================
-- 3. 逻辑归档条目、互引图谱与衍生资产血缘
-- ==========================================
CREATE TABLE items (
    id                  UUID PRIMARY KEY,      -- UUIDv7
    item_type           TEXT NOT NULL,         -- 'video' | 'audio' | 'image_set' | 'photo' | 'social_post' | 'document' | 'note'
    title               TEXT NOT NULL,
    description         TEXT,
    extracted_text      TEXT,                  -- 字幕/OCR/文档抽取纯文本（同步至 Meilisearch）
    source_type         TEXT NOT NULL,         -- 'bilibili' | 'youtube' | 'twitter' | 'pixiv' | 'immich' | 'manual'
    source_url          TEXT,
    source_external_id  TEXT,                  -- 平台侧内容 ID（如 BV号、Tweet ID、Immich asset_id）
    original_created_at TIMESTAMPTZ,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acl_tags            TEXT[] NOT NULL DEFAULT ARRAY['private']::TEXT[],
    extra_meta          JSONB NOT NULL DEFAULT '{}'::jsonb, -- 采用 || 增量合并语义（可存 EXIF、原站标签、未来 pHash 等）
    status              TEXT NOT NULL DEFAULT 'ingesting',  -- 'ingesting' | 'processing' | 'ready' | 'failed'
    UNIQUE (source_type, source_external_id)
);
CREATE INDEX idx_items_acl_tags ON items USING GIN (acl_tags);

-- 条目间显式引用关系（Item-to-Item Graph：推文转发视频、笔记引用文档、相似图/二创关联）
CREATE TABLE item_relations (
    source_item_id  UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    target_item_id  UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    relation_type   TEXT NOT NULL,             -- 'quotes' | 'reposts' | 'references' | 'derived_work' | 'similar_to'
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_item_id, target_item_id, relation_type),
    CHECK (source_item_id <> target_item_id)
);
CREATE INDEX idx_item_relations_target ON item_relations(target_item_id);

CREATE TABLE item_authors (
    item_id         UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    identity_id     UUID NOT NULL REFERENCES author_identities(id),
    role            TEXT NOT NULL DEFAULT 'creator', -- 'creator' | 'reposter' | 'illustrator' | 'depicted'
    PRIMARY KEY (item_id, identity_id, role)
);

CREATE TABLE item_blobs (
    id                  UUID PRIMARY KEY,
    item_id             UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    blob_sha256         TEXT NOT NULL REFERENCES blobs(sha256),
    role                TEXT NOT NULL,         -- 'primary_video' | 'derived_audio' | 'primary_image'
                                               -- 'derived_thumbnail' | 'subtitle' | 'raw_meta'
    derived_from_sha256 TEXT REFERENCES blobs(sha256),
    display_order       INT NOT NULL DEFAULT 0,
    meta                JSONB NOT NULL DEFAULT '{}'::jsonb, -- { "duration_sec": 240, "bitrate_kbps": 192, "codec": "mp3" }
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_item_blobs_item_role ON item_blobs(item_id, role);

-- ==========================================
-- 4. 自动化规则 (转码衍生 & 多副本策略)
-- ==========================================
CREATE TABLE processing_rules (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    -- 例: {"source_type": ["bilibili", "youtube"], "acl_tags_any": ["music"]}
    match_condition JSONB NOT NULL,
    -- 例: {"extract_audio": {"format": "mp3", "bitrate_kbps": 192}, "replica_backends": ["zfs-hot", "garage-s3"]}
    actions         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==========================================
-- 5. ACL 细粒度访问控制
-- ==========================================
CREATE TABLE peer_tokens (
    id               UUID PRIMARY KEY,
    label            TEXT NOT NULL,
    token_hash       TEXT NOT NULL UNIQUE,     -- SHA-256 哈希
    allowed_acl_tags TEXT[] NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE share_links (
    token_hash       TEXT PRIMARY KEY,
    target_item_id   UUID REFERENCES items(id) ON DELETE CASCADE,
    allowed_acl_tags TEXT[],
    expires_at       TIMESTAMPTZ,
    revoked          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (target_item_id IS NOT NULL OR allowed_acl_tags IS NOT NULL)
);
```

## 6. Meilisearch 索引与 ACL 双保险鉴权

### 6.1 `vault_items` 索引配置

```json
{
  "searchableAttributes": [
    "title",
    "author_names",
    "author_handles",
    "description",
    "extracted_text",
    "tags"
  ],
  "filterableAttributes": [
    "acl_tags",
    "item_type",
    "source_type",
    "author_ids",
    "available_roles",
    "original_created_at",
    "captured_at"
  ],
  "sortableAttributes": ["original_created_at", "captured_at"],
  "displayedAttributes": [
    "id", "title", "item_type", "source_type",
    "author_names", "available_roles", "acl_tags", "original_created_at"
  ]
}
```

* **仅听音频过滤**：前端勾选“仅看可听音频”时，服务端向 Meilisearch 附加 `available_roles = "derived_audio"`，列表卡片直接渲染 `<audio src="/blobs/{item_id}/role/derived_audio">` 实现零视频流量播放。

### 6.2 统一 Actor 鉴权与读取路由

所有外部请求经由 Cloudflare Tunnel 进入 `sciuromorpha-core`：
1. **Actor 识别中间件**：
   * **Owner**：通过 Cloudflare Access JWT (`Cf-Access-Jwt-Assertion`) 或本地开发模式 Admin Session 认证，拥有全库读写与管理权限。
   * **Peer**：通过 Magic Link 兑换 `HttpOnly; Secure; SameSite=Lax` Cookie（关联 `peer_tokens`），仅限只读 `allowed_acl_tags` 范围内的条目。
   * **Guest**：通过 URL `/s/{share_token}` 访问（校验 `share_links`），仅限只读单条 `target_item_id` 或指定标签集合。
2. **检索双保险**：`core` 向 Meilisearch 强制注入 `acl_tags IN [...]` 过滤条件 $\rightarrow$ 拿到命中的 `item_id` 列表 $\rightarrow$ 回查 PostgreSQL 校验最新 `acl_tags` 权限后再渲染 HTML。
3. **Blob 防盗链与分层读取路由** (`/blobs/{item_id}/role/{role}` 或 `/blobs/{item_id}/sha256/{hash}`)：
   * 先校验当前 `Actor` 对 `item_id` 的读权限。
   * 优先检查热 Cache 命中；未命中则查询 `blob_replicas` 中 `status = 'synced'` 且 `is_online = true` 的后端按 `read_priority` 路由：
     * `local_fs` (ZFS)：Axum 原生流式输出（支持 HTTP `Range` 分片拖动进度条）。
     * `s3` (Garage)：签发 60 秒短效 Presigned URL 重定向或流式代理。
     * `immich_api`：携带私有 Immich API Key 从内网流式代理缩略图或原图（同伴无需 Immich 账号）。
     * 仅存在于 `offline_tape`：返回 `202 Accepted` 及 HTMX 状态片段，提供“触发磁带回迁”按钮。

## 7. Immich 联邦检索集成规范

* **单向聚合原则**：Immich $\rightarrow$ Sciuromorpha 单向增量同步，不复制照片文件，不将社媒杂图反向挂入 Immich。
* **元数据映射**：
  * `storage_backends` 注册 `id = 'immich-main'`, `driver = 'immich_api'`；`blob_replicas.locator` 存放 Immich `asset_id`。
  * GPS 逆地理编码 $\rightarrow$ `tags` & `extra_meta`；人脸人物（People）$\rightarrow$ `item_authors (role = 'depicted')`；特定共享相册（如 `Share:Friends`）$\rightarrow$ 自动映射为 `acl_tags`；描述/OCR $\rightarrow$ `description` / `extracted_text`。
  * Owner 在 UI 查看 Immich 来源卡片时，额外渲染直达原 Immich 详情页的快捷链接。

## 8. 演进路线与 MVP 落地范围

* **Phase 1 (MVP — 单 Incus `dev` 容器跑通闭环)**：
  1. 初始化 Rust Cargo Workspace + PostgreSQL `0001_init.sql` 迁移（含 `item_relations`）+ NATS JetStream 流初始化。
  2. 实现 `sciuromorpha-core`：Axum + Askama/HTMX 基础界面（URL 投递表单、检索列表、音频/视频播放详情页、作者马甲合并页）、Owner/Peer 基础鉴权、Meilisearch 索引同步、JSONB `Meta Merge` 消费、基础 Backfill 补跑接口、ZFS 热 Cache + 本地 ZFS 归档流式读取。
  3. 实现 `ingest-agent-media`（驱动 `yt-dlp` / `gallery-dl`）与 `media-agent`（驱动 `ffmpeg` 按规则或手动选项剥离 MP3 与缩略图）。
  4. 实现 `storage-agent` 的 `local_fs` (ZFS) 与 `s3` (Garage) 异步副本同步。
* **Phase 2 (联邦、冷备与相似图去重)**：
  * 上线 `connector-agent-immich` 增量同步个人照片元数据。
  * 上线 `storage-agent` 的 `rclone` 加密云盘同步与 `offline_tape` 磁带归档/回迁调度。
  * 引入感知哈希（`pHash/dHash`）Worker，通过 Backfill 补跑存量图片，自动发现 Twitter 压缩图与 Pixiv 原图等跨平台相似图并写入 `item_relations (relation_type = 'similar_to')`。
  * 按微服务拆分为独立 Incus LXC 容器（共享 `vault-net` 网桥与 `shift=true` ZFS 热 Cache）。
* **Phase 3 (全类型补齐)**：
  * 接入文档/网页快照（SingleFile HTML、PDF 文本抽取）与独立 GPU OCR / Whisper Worker（通过 Backfill 补跑历史无字幕视频）。

## 9. 给 IDE Agent (VS Code + Antigravity) 的 Phase 1 初始化指令

在本地新建 `sciuromorpha` 仓库并将本 `ARCHITECTURE.md` 放入根目录后，直接将以下指令发给 IDE Agent 即可开始脚手架搭建：

```markdown
请仔细阅读根目录的 `ARCHITECTURE.md`，按照第 3、4、5、6、8 节的规范，为 `Sciuromorpha` 初始化 Phase 1 (MVP) 的 Rust Cargo Workspace 骨架：

1. **Workspace 与数据库迁移**：
   - 创建根目录 `Cargo.toml`，配置 Workspace 成员：`crates/sciuromorpha-common`、`crates/sciuromorpha-core`、`crates/ingest-agent-media`、`crates/media-agent`、`crates/storage-agent`。
   - 将文档第 5 节的完整 DDL（包含 `item_relations`）写入 `migrations/0001_init.sql`，并补充一条初始化种子数据（插入默认 `zfs-hot` 和 `garage-s3` 两条 `storage_backends` 记录）。
2. **共享协议与热 Cache (`sciuromorpha-common`)**：
   - 在 `events.rs` 中定义 `IngestUrlCmd`、`IngestCompletedEvt`、`ExtractAudioCmd`、`DerivativeReadyEvt`、`ItemMetaPatchEvt`、`ReplicateBlobCmd`、`StorageSyncedEvt` 等 serde 结构体及 NATS Subject 常量。
   - 在 `staging.rs` 中定义 `StagingStore` Trait 并实现 `LocalFsStagingStore`（按 SHA-256 前 4 位两级分桶读写，支持流式写入与哈希计算）。
3. **核心服务 (`sciuromorpha-core`)**：
   - 搭建 Axum + `sqlx` + `async-nats` + `meilisearch-sdk` + `askama` 骨架。
   - 实现启动时自动运行 `sqlx::migrate!()`、自动创建 NATS JetStream `VAULT_CMD` / `VAULT_EVT` Stream 以及初始化 Meilisearch `vault_items` 索引配置。
   - 在状态机中实现对 `vault.evt.item.meta_patch` 的 JSONB 增量合并（`extra_meta = extra_meta || $1`）并触发 Meilisearch 同步；预留 `POST /api/admin/backfill` 接口按条件查询存量条目并批量派发 NATS 任务。
   - 搭建基础 HTMX 页面：主页检索栏（含“仅听音频”过滤开关）、URL 投递表单（含“同时剥离 MP3”勾选框，提交后向 `vault.cmd.ingest.url` 发消息并返回 HTMX 状态卡片）、`/blobs/{item_id}/role/{role}` 支持 HTTP `Range` 的流式代理接口。
4. **MVP Worker 骨架**：
   - `ingest-agent-media`：使用 `async-nats` Pull Consumer 订阅 `vault.cmd.ingest.url`，调用 `yt-dlp --write-info-json` 抓取视频/元数据，将文件存入 `LocalFsStagingStore` 后发布 `vault.evt.ingest.completed`。
   - `media-agent`：使用 Pull Consumer 订阅 `vault.cmd.media.extract_audio`，调用 `ffmpeg` 从热 Cache 剥离 MP3（写入 ID3 title/artist），落盘热 Cache 后发布 `vault.evt.media.derivative_ready`。
   - `storage-agent`：先实现 `local_fs` (ZFS) 副本同步逻辑，监听 `vault.cmd.storage.zfs-hot.replicate` 并将文件归档至目标目录后发布 `vault.evt.storage.synced`。
5. **配置与验证**：
   - 提供 `.env.example`（包含 `DATABASE_URL`、`NATS_URL`、`MEILI_URL`、`MEILI_API_KEY`、`STAGING_DIR`、`ZFS_ARCHIVE_DIR`）。
   - 确保整个 Workspace 通过 `cargo check --workspace` 且无 `unwrap()` 滥用。
```