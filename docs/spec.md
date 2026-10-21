# linkage-gateway 技术规格

## 1. 目标

实现一个独立的设备联动网关，复现并承接以下链路：

```text
UDP 0x43 点表上传
        ↓
保存站序号 + 监控索引号对应的设备点表
        ↓
UDP 0x55 设备变位
        ↓
匹配点表和显式配置的视频通道绑定
        ↓
保存联动事件
        ↓
WebSocket 推送给 wvp-web-ui
        ↓
前端调用 WVP 点播并弹出视频界面
```

网关不直接播放视频、不直接调用 WVP 点播接口，也不写入 SSV Agent 的 `EventLedger`。

## 2. 项目边界

本项目是独立 Go 仓库，远端为 `https://github.com/ssv-develop-team/linkage-gateway.git`。本机独立开发目录为 `/home/Tiger/Documents/code/web/linkage-gateway`；`wvp-web-ui` 通过 `external/linkage-gateway` Git submodule 引入并锁定具体提交。

子模块只分发源码，不改变 HTTP/WebSocket 契约，也不随前端启动或构建自动运行。运行数据和本地配置不纳入 Git；已有独立实例无需搬迁或重复启动。

### 网关负责

- 监听 UDP 并接收原始应用层负载；
- 解析 `0x43` 点表上传报文；
- 解析 `0x55` 变位报文；
- 持久化点表、视频绑定和联动事件；
- 提供点表、绑定和事件 REST API；
- 通过 WebSocket 推送实时联动事件；
- 持久化未匹配、未绑定、禁用及重复事件；解析失败写入结构化日志。

### 网关不负责

- 监听或改造本机闭源 `ai_server`；
- 直接调用 WVP 的 `/api/play/start` 或 `/api/play/stop`；
- 处理 RTSP、FLV、WebRTC 或播放器生命周期；
- 把设备联动事件伪装成 SSV 视觉检测事件；
- 通过设备名称、实物 ID 或整数数据库 ID 猜测 WVP 通道。

## 3. 技术栈

- Go 1.27 系列；
- `net.ListenUDP` 接收 UDP；
- 标准库 `net/http` 提供 HTTP；
- `github.com/coder/websocket` 提供 WebSocket；
- `database/sql` + `modernc.org/sqlite`；
- `gopkg.in/yaml.v3` 读取配置；
- `golang.org/x/text/encoding/simplifiedchinese` 处理 GB2312/GB18030 文本；
- `log/slog` 记录结构化日志；
- 第一阶段不引入 Redis、ORM、Gin 或其他消息队列。

## 4. 运行形态

一个进程同时提供：

```text
UDP      127.0.0.1:9301
HTTP     127.0.0.1:8766
WebSocket /ws/linkage
```

本机 `ai_server` 当前占用 `0.0.0.0:9300`。`9300` 作为黑盒参考端口，网关开发联调默认使用 `9301`。只有在停止或隔离 `ai_server` 后，网关才可以改用 `9300`。

## 5. 配置

提交配置模板：

`config/linkage-gateway.example.yaml`

本机配置：

`config/linkage-gateway.yaml`

本机配置不提交。`version` 当前必须为 `1`。加载顺序为：

```text
默认值 → `--config` 指定的 YAML 覆盖 → 环境变量覆盖 → 校验
```

配置只保存监听地址、数据库路径、协议解码选项、去重窗口和日志选项。点表及视频绑定属于运行时数据，保存到 SQLite。

## 6. 协议输入

协议回放源：

`/home/Tiger/Downloads/send_fixed_port.py`

脚本 SHA-256：

`a9bf1237317b5328637595c16a3259d0ea79327e2d8dbdbc77817e4855b36055`

已知输入：

- `0x43` 报文长度为 `237` 字节，包含 GB2312 编码的设备资源信息配置文本；
- 当前点表包含站序号 `1`、监控索引号 `1`、设备名 `zh/110KV..12140.地刀`；
- `0x55` 报文长度为 `28` 字节，当前 payload 对应点位 `1_1` 的遥信变位；
- UDP payload 不包含 Ethernet/IP/UDP 头。

字段偏移、校验方式、fixture 摘要和本地解析结果记录于 `docs/protocol.md`。`0x43`、`0x55` 的长度字段与累加校验已按附录 F 和固定样本实现；当前仍未进行 `ai_server:9300` 黑盒对照。

以下事项仍待确认，不提前假设：

- `0x43` 是完整快照还是增量更新，以及是否删除本批次缺失的旧点；
- `0x43` 多帧重组和起始传输位置的字节序；
- `0x55` 监控索引号大于 `255` 时的字节序；
- CP56 年份世纪映射和部署时区；
- 重复变位的准确去重窗口；
- 是否需要支持遥控预置动作报文。

## 7. 数据模型

第一阶段使用独立数据库：

`data/linkage.db`

核心表：

### `point_imports`

保存每次点表上传批次和原始报文摘要。

### `point_table`

保存当前有效点表，以 `station_no + monitor_index` 作为业务唯一键，业务层表示为 `point_key`，例如 `1_1`。

至少包含：

- `point_key`；
- `station_no`；
- `monitor_index`；
- `device_name`；
- `device_type`；
- `physical_id`；
- `linkage_enabled`；
- `import_id`；
- `updated_at`。

### `point_bindings`

保存点位到 WVP 视频通道的显式绑定，支持一个点位绑定多个通道。

至少包含：

- `point_key`；
- `wvp_device_id`；
- `wvp_channel_id`；
- `channel_name`；
- `enabled`；
- `display_order`。

`wvp_device_id` 和 `wvp_channel_id` 必须由明确配置提供，不能由网关推断。

### `linkage_events`

保存收到的变位事实和处理结果。

至少包含：

- `event_id`；
- `sequence`；
- `point_key`；
- `event_type`；
- `property`；
- `value`；
- `event_time`；
- `received_at`；
- `raw_frame_hash`；
- `match_status`；
- `broadcast_status`。

## 8. REST 接口

```text
GET  /healthz
GET  /readyz

GET  /api/v1/linkage/points
GET  /api/v1/linkage/points/{point_key}

GET  /api/v1/linkage/bindings
GET  /api/v1/linkage/bindings?point_key=1_1
PUT  /api/v1/linkage/bindings/{point_key}

GET  /api/v1/linkage/events?after=0&limit=50
GET  /api/v1/linkage/events/{event_id}
```

`GET /api/v1/linkage/events` 使用 `after` 序号游标和 `limit` 分页；默认分别为 `0`、`50`，`limit` 范围为 `1..500`。绑定查询不带 `point_key` 时返回全部绑定；提供 `point_key` 时仅返回该点位的绑定。绑定 PUT 使用完整替换语义，JSON 结构为：

```json
{
  "bindings": [
    {
      "deviceId": "wvp-device-id",
      "channelId": "wvp-channel-id",
      "name": "联动摄像机",
      "enabled": true,
      "displayOrder": 0
    }
  ]
}
```

设备 ID、通道 ID 必须显式填写。错误响应采用 `{"code":"...","error":"..."}`。

## 9. WebSocket 消息

端点：

```text
/ws/linkage
```

实时消息采用 JSON：

```json
{
  "type": "linkage_event",
  "schemaVersion": 1,
  "eventId": "evt-20261019-000001",
  "sequence": 42,
  "pointKey": "1_1",
  "stationNo": 1,
  "monitorIndex": 1,
  "deviceName": "zh/110KV..12140.地刀",
  "eventType": "遥信",
  "property": "变位",
  "value": "合位",
  "eventTime": "2026-10-19T15:42:40.382+08:00",
  "receivedAt": "2026-10-19T15:42:41.000+08:00",
  "matchStatus": "matched",
  "channels": [
    {
      "deviceId": "wvp-device-id",
      "channelId": "wvp-channel-id",
      "name": "联动摄像机"
    }
  ]
}
```

WebSocket 会推送每个非重复联动事件，包括 `unmatched`、`disabled`、`unbound` 等状态；前端仅在 `matchStatus` 为 `matched` 且 `channels` 非空时自动弹出视频。断线补偿通过 REST 查询，历史事件默认不自动弹出视频。消息结构版本由 `schemaVersion` 标识。

## 10. 黑盒验证

`ai_server` 是闭源参考实现，不作为网关运行时依赖。验证时使用同一份 `send_fixed_port.py` 输入：

1. 发送第 `2` 段，观察 `ai_server` 的点表接受效果；
2. 发送第 `1` 段，观察 `1_1` 变位联动效果；
3. 对网关使用同样 payload；
4. 对比点表记录、事件匹配、WebSocket 消息和前端视频弹窗结果。

对比以网络行为和可观察结果为准，不读取或假设闭源实现内部代码。
