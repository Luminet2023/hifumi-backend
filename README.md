# Hifumi Backend

本仓库只构建基于 Gin 的 Go API 与容器镜像，不包含 Vue 静态前端。公网基地址固定为：

```text
https://api.luminet.cn/hifumi/
```

反向代理应保留 `/hifumi` 前缀并透传 WebSocket Upgrade；应用监听容器内 `:8080`。OAuth callback 为：

```text
https://api.luminet.cn/hifumi/v1/auth/callback
```

## 二进制命令

同一个 `study-list-api` 二进制提供三个命令：

```text
study-list-api serve
study-list-api migrate
study-list-api healthcheck
```

- `serve` 启动 HTTP/WebSocket API。
- `migrate` 只初始化或升级 MySQL schema；本项目不导入 Cloudflare KV/Durable Object 的旧数据。
- `healthcheck` 请求本机 `/hifumi/healthz`，供 Docker `HEALTHCHECK` 使用。

## 构建与运行

镜像使用 Go 1.26 多阶段构建，最终运行在无 shell 的 distroless 镜像中，并以 UID `65532` 非 root 用户运行。

```bash
docker build \
  --file Dockerfile \
  --tag study-list-api:dev \
  .
```

生产启动前先用同一镜像执行 schema migration：

```bash
docker run --rm --env-file /secure/path/study-list.env \
  study-list-api:dev migrate

docker run --rm --env-file /secure/path/study-list.env \
  --publish 127.0.0.1:8080:8080 \
  study-list-api:dev serve
```

不要把 env 文件提交到仓库，也不要通过 Docker build args 传递 secret。

## 配置

| 环境变量 | 必需 | 默认值/说明 |
| --- | --- | --- |
| `HTTP_ADDR` | 否 | `:8080` |
| `PUBLIC_BASE_URL` | 否 | `https://api.luminet.cn/hifumi/`，必须为无 query/fragment 的 HTTPS URL |
| `FRONTEND_ORIGINS` | 否 | `https://stellafortuna.hifumi.luminet.cn,https://stellafortuna.luminet.cn`，用于 CORS、WebSocket Origin 与 Referer allowlist |
| `FRONTEND_RETURN_URL` | 否 | `https://stellafortuna.luminet.cn/`，OAuth 完成后的前端返回地址 |
| `MYSQL_DSN` | `serve`/`migrate` | MySQL DSN，生产环境应要求 TLS |
| `REDIS_URL` | `serve` | Redis URL；Redis 只承载缓存、限流和跨实例协调，不作为业务真源 |
| `REDIS_KEY_PREFIX` | 否 | `study-list:prod:`；不同环境必须使用不同前缀 |
| `LINUXDO_CLIENT_ID` | `serve` | Linux DO OAuth client ID |
| `LINUXDO_CLIENT_SECRET` | `serve` | Secret，不得写入镜像或仓库 |
| `SESSION_JWT_SECRET` | `serve` | 至少 32 字符，建议 `openssl rand -hex 32` |
| `SESSION_AUDIENCE` | 否 | `stellafortuna` |
| `TRUSTED_PROXY_CIDRS` | 否 | 逗号分隔；仅这些来源可用 `X-Forwarded-For` 影响结构化日志中的 client IP |
| `LOG_LEVEL` | 否 | `info` |

应用启动时只校验 schema 版本，不应由每个 API replica 自动执行 DDL。部署流程必须保证 `migrate` 成功后再启动或滚动更新 `serve`。

公开 API 统一位于 `/hifumi/v1/`，不再包含重复的 `/api` 路径段。Session、logout 和 HTTP sync 请求必须同时携带 allowlist 中的 `Origin` 与 `Referer`，且两者 origin 必须完全一致；CORS preflight 不要求 `Referer`。WebSocket 握手必须携带 allowlist `Origin`，为兼容浏览器可省略 `Referer`，但若携带也必须与 `Origin` 完全一致。健康检查、版本接口和 OAuth callback 不依赖这些浏览器来源头。

OAuth 登录会根据合法 `Referer` 选择两个前端域名之一，并把 return URL 写入签名 state；无效或缺失 `Referer` 时回退到 `FRONTEND_RETURN_URL`。Callback 会再次校验 return URL 的 origin，防止 open redirect；state 验证后的上游、资料或 Session 签名失败也会回到发起登录的前端域名。

同步写入和 `realtime_outbox` 在同一 MySQL 事务提交；独立后台循环在提交后发布 Redis hint，失败会记录重试。Redis 故障不会改变 MySQL 中的 cursor、record、receipt 或用户资料，但会让同步请求、WebSocket 新连接和 `/readyz` 明确失败。

## 本地 Compose 样例

[`deploy/compose.example.yml`](deploy/compose.example.yml) 只用于本地 MySQL、Redis 和 API 联调，不是生产编排文件。先在仓库外创建 env 文件，至少设置：

```dotenv
MYSQL_ROOT_PASSWORD=<random-hex>
MYSQL_PASSWORD=<random-hex>
LINUXDO_CLIENT_ID=<local-oauth-client-id>
LINUXDO_CLIENT_SECRET=<local-oauth-client-secret>
SESSION_JWT_SECRET=<at-least-32-random-characters>
```

其中密码建议只使用随机十六进制字符，以免未经 URL 编码的本地 MySQL DSN 出现歧义。启动：

```bash
docker compose \
  --env-file /secure/path/study-list-local.env \
  --file deploy/compose.example.yml \
  up --build
```

Compose 会等待 MySQL/Redis 健康，执行一次 `migrate`，成功后再启动 API。MySQL 和 Redis 不暴露宿主机端口，API 只监听 `127.0.0.1:8080`。

## GHCR 发布

`.github/workflows/image.yml` 发布私有镜像：

```text
ghcr.io/luminet2023/hifumi-backend
```

- `main` push：生成 `main` 和 `sha-<short-sha>`。
- `v1.2.3` tag：生成 `1.2.3`、`1.2`、`1`、`latest` 和 SHA tag。
- 镜像平台：`linux/amd64`、`linux/arm64`。
- 发布使用仓库 `GITHUB_TOKEN`，只授予 `contents:read`、`packages:write`、`attestations:write`、`id-token:write`。
- BuildKit 始终生成 max provenance 和 SBOM。GitHub `actions/attest` 在公开仓库中生成 registry attestation；当前私有仓库会显式跳过，待平台支持后可设置 `ENABLE_ARTIFACT_ATTESTATION=true` 恢复。

GitHub workflow 不能可靠地把一个已存在且被手工设为 public 的 package 改回 private。首次发布后必须在 GHCR Package settings 确认 `hifumi-backend` 为 **Private** 并继承本仓库权限。

生产服务器应按 digest 拉取，避免可变 tag 漂移：

```bash
docker pull ghcr.io/luminet2023/hifumi-backend@sha256:<digest>
```

私有 GHCR 的服务器拉取凭据只授予 `read:packages`，不得复用发布凭据。回滚时切换到上一个已验证 digest。

生产 Compose 模板位于 `deploy/compose.production.yml`。服务器项目目录的 `.env` 必须设置 `HIFUMI_IMAGE=ghcr.io/luminet2023/hifumi-backend@sha256:<digest>` 及运行时配置，部署时不得改用可变 tag。

## 首次发布顺序

1. 将本分支合并到 `main`，等待 GHCR 多架构镜像、SBOM 和 provenance 完成，记录不可变 digest；仅在仓库支持时等待额外 attestation。
2. 创建空 MySQL database；用该 digest 执行一次 `study-list-api migrate`。
3. 以同一 digest 启动 `serve`，确认 `/hifumi/healthz`、`/hifumi/readyz` 和 `/hifumi/version`。
4. 反向代理必须原样保留 `/hifumi` 前缀并支持 WebSocket Upgrade，不能 rewrite 成根 `/api`。
5. 在 Linux DO 控制台把唯一 callback 设为 `https://api.luminet.cn/hifumi/v1/auth/callback`。
6. 将 Cloudflare Worker 部署为纯静态 Assets Worker，并让已退役的 `/api/*` 路由统一返回 `410 Gone`。
7. 发布带 `VITE_API_BASE_URL=https://api.luminet.cn/hifumi/` 的前端。
8. 验证两个前端 origin 的 OAuth、Session、HTTP 同步、WSS、两标签页 hint 和容器重启恢复。切流后仅允许回滚到已验证的 Go 镜像 digest，不再把 Durable Object 恢复为权威源。

旧 Cloudflare `/api/*` 后端已退役，不再代理、handoff 或接受旧 issuer Session；这些路由统一返回 `410 Gone`。

## 健康检查

- `/hifumi/healthz`：仅检查进程存活，不依赖 MySQL/Redis；Docker healthcheck 使用此端点。
- `/hifumi/readyz`：以短超时检查 MySQL/Redis，供反向代理或编排系统决定是否接流量。

容器关闭时会收到 `SIGTERM`；`serve` 必须先停止接收新请求，再在超时内完成 HTTP/WebSocket 优雅退出。
