# fix1 分支镜像构建

GitHub Actions 的 `Docker Publish` 使用 `GITHUB_TOKEN` 发布到
`ghcr.io/mininglamp-oss/octo-im`，构建 `linux/amd64` 和 `linux/arm64`。
构建文件使用 [41dd1e5 中的 Dockerfile.pr](https://github.com/Mininglamp-OSS/octo-im/blob/41dd1e597a66f1cad44a38e9e4cd4f2d9796695c/Dockerfile.pr)，内容保持一致。
不需要额外配置 Docker Hub 或阿里云凭据。仓库 Actions 策略必须允许
工作流使用 `packages: write`。

## 自动构建

- 向 `v2.2.5-20260422-fix1` 分支 push：生成
  `branch-v2.2.5-20260422-fix1` 标签。
- push `v2.2.5-YYYYMMDD-fix1` 日期版本 tag：生成与 Git tag 完全一致的镜像标签，
  例如 `v2.2.5-20261021-fix1`。日期必须是 8 位数字，也支持 `.2`、`-rc1` 等后缀。
- 每次构建还生成 `sha-<提交 SHA 前 12 位>`，可在 Actions 摘要中查看
  完整提交、镜像标签和 digest。

分支标签随提交更新；需要固定镜像内容时，使用摘要中的 `image@sha256:...`。
该工作流不生成 `latest`、`v2` 等版本别名。

## 手动构建

在 Actions → Docker Publish → Run workflow 中选择已包含本工作流的分支，
填写 `test-*` 标签，例如 `test-fix1-candidate`。也可以运行：

```sh
gh workflow run docker-publish.yml \
  --ref v2.2.5-20260422-fix1 \
  -f tag=test-fix1-candidate
```

正式日期版本通过推送 Git tag 触发；在手动运行的 `tag` 输入框填写
`v2.2.5-20261021-fix1` 会被拒绝。Git tag 所在提交必须包含当前工作流规则。

验证当前开发分支时，将 `--ref` 替换为已推送的开发分支名。
工作流固定 checkout 触发事件的提交，镜像内的版本、提交和日期由该提交生成。

保留 `.github/workflows/docker-publish.yml` 路径，以复用默认分支已注册的
手动入口；GitHub 的手动运行入口要求默认分支存在同路径工作流。
默认分支的输入表单可能与本分支不同，因此建议始终显式填写 `tag`。
参见 [GitHub workflow_dispatch 文档](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_dispatch)。

## 本地验证

```sh
python3 -B -m unittest discover -s test/ci -p 'test_*.py' -v
docker buildx build -f Dockerfile.pr --load -t local/octo-im:test \
  --build-arg "SOURCE_REVISION=$(git rev-parse HEAD)" \
  --build-arg "SOURCE_DATE=$(git log -1 --format=%ct)" \
  --build-arg SOURCE_VERSION=test-local .
```

本地命令按 Docker 当前平台构建，三个 `SOURCE_*` 参数均为必填。
Actions 分别使用 `ubuntu-24.04` 和 `ubuntu-24.04-arm` 原生 runner，
每个 runner 使用 `Dockerfile.pr` 编译并推送对应平台的 digest。
两种架构均成功后，合并任务生成带镜像标签的 manifest，并核对包含 AMD64 和 ARM64。
构建无需 QEMU，也不依赖 `TARGETARCH` 交叉编译参数。
前端使用 Node 20 和 Yarn 1.22.22，依赖使用 Yarn lockfile；Go 为 `1.23.0`。
构建上下文不包含 `.git`、`.context` 或宿主机的 `node_modules`，
由专用的 `Dockerfile.pr.dockerignore` 排除这些路径，
仅将前端编译结果复制到 Go 构建阶段。
