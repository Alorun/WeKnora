# 最小 Go DataSource 插件模板

这个目录是一份可复制到主仓外开发的教学模板。它只依赖公开的
`pkg/plugin/sdk` 和生成的协议类型，不导入 `internal/...`，也不包含
Runtime、Docker 或权限实现。模板固定产生一个 `hello.txt` 文档，演示稳定的
`external_id`、`revision`、opaque Cursor 和最终 Checkpoint；实际插件通常只需替换
`source.go` 中的数据读取与变化判断。

当前 SDK 尚未单独发布。合并前开发使用脚本临时创建 `go.work`，不会修改
`go.mod` 或写入个人路径：

```bash
cp -R /path/to/WeKnora/examples/plugins/datasource-go ./my-datasource
cd my-datasource
./scripts/with-sdk.sh /path/to/WeKnora go test ./...
./scripts/with-sdk.sh /path/to/WeKnora go vet ./...
./scripts/build.sh /path/to/WeKnora "$PWD/dist/example.hello"
```

构建结果只有 `plugin.yaml` 和 `bin/plugin-linux-amd64`。将插件复制到其他目录后，
以上命令仍然成立；脚本传入的 WeKnora 路径必须是当前 SDK 源码所在仓库。

复制后应同步修改 `plugin.yaml` 与 `main.go` 中的插件 ID、版本、extension ID 和
capabilities。入口必须保持包内相对路径。`plugin.yaml` 的资源预算是 Host
校验的上限，不是让插件选择 Docker 参数的入口。

实现约束：

- `ListResources` 暴露可选择资源，`ResolveAncestors` 保持资源选择一致；
- `Sync` 对同一旧 Cursor 和同一内容产生相同事件身份；
- 原始文件通过 `DocumentUpsert.content` 发送，不在插件内解析、切块或索引；
- 只有完整成功枚举后才发送删除与最终 Checkpoint，任何发送错误必须返回；
- Host 原样保存 `cursor_json`，其内部格式由插件负责版本化；
- 不要设置 `replaces_subtree` / `subtree_keep`，V1 Host 会拒绝非默认值；
- 只使用 Host 授权的 `/data/source`，不要接受宿主路径、Docker 参数或网络凭据。

完整的递归目录扫描、文件安全打开和删除 Diff 示例位于独立 Local Directory
插件；本模板有意不复制那套实现。正式开发与部署流程见
`website-docs/06-development/04-datasource-plugins.md`。
