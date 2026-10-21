# linkage-gateway

面向国网智巡视频联动协议的独立 Go 网关：接收 UDP 点表（`0x43`）和变位（`0x55`）报文，使用 SQLite 保存点位、WVP 通道绑定及事件，通过 REST API 和 WebSocket 提供联动信息。

网关不依赖 SSV Agent，也不负责媒体播放；前端根据显式绑定的设备和通道 ID 调用 WVP 点播。

## 快速启动

需要 Go `1.27+`，在网关目录执行：

```bash
go run ./cmd/linkage-gateway --config config/linkage-gateway.example.yaml
```

默认 UDP `127.0.0.1:9301`，HTTP `127.0.0.1:8766`，WebSocket `/ws/linkage`，数据库 `data/linkage.db`。开发端口避开闭源参考服务 `ai_server:9300`。

部署前按需复制配置模板到 `config/linkage-gateway.yaml`，调整监听地址和 `web.allowed_origins`；启动时通过 `--config` 指定。本地配置和运行数据不提交。

## 验证

```bash
go test ./...
go vet ./...
```

## 文档

- [技术规格](docs/spec.md)：配置、接口与项目边界。
- [实施计划](docs/plan.md)：完成项及待验证项，真实视频播放与黑盒对照仍待验证。
- [协议记录](docs/protocol.md)：字段、原始报文 fixture 和回放脚本说明。
