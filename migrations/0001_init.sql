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

-- ==========================================
-- 6. 初始化种子数据 (默认存储后端)
-- ==========================================
INSERT INTO storage_backends (id, driver, tier, read_priority, is_online, config)
VALUES
    ('zfs-hot', 'local_fs', 'hot', 10, TRUE, '{"description": "Local ZFS hot archive pool"}'::jsonb),
    ('garage-s3', 's3', 'warm', 50, TRUE, '{"bucket": "sciuromorpha-vault", "region": "garage"}'::jsonb)
ON CONFLICT (id) DO NOTHING;
