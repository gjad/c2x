# c2x

轻量的 Linux 主机监控与服务管理工具,Rust 编写,包含两个可执行文件:

- **server**:汇总所有 client 的状态,提供网页看板。
- **client**:部署在被监控主机上,主动连接 server。

client 主动发起 gRPC 双向流长连接,即使在 NAT 后面也能被 server 管理。

## 功能

**监控**
- client 每秒上报 CPU、内存、各挂载点硬盘使用情况。
- server 能感知 client 在线或掉线(心跳 + 超时,约 10 秒内发现)。
- 以 client 的来源 IP 作为唯一标识,在 config 里设置显示名称。

**服务管理**(systemd)
- client 在 config 中列出允许管理的服务,server 网页可查看状态,并启动、停止、重启。
- 服务守护:被守护的服务一旦停止,client 自动重启它(带退避)。在网页上手动停止会暂停守护,手动启动后恢复。

**日志**
- server 记录 client 上下线、服务启停操作、守护重启,每天一个文件,保留 30 天。

**网页**(内嵌页面,无需构建)
- 列表页:每个 client 一行,SSE 每秒刷新,显示状态、CPU / 内存、硬盘、服务、最近 5 条日志。
- 详情页(点击行在新窗口打开):实时状态、服务控制、历史折线图(1 小时到 30 天)、分页操作日志。
- 离线的 client 可以删除,需输入管理密码。

**存储**
- 每分钟保存一个采样,每个 client 每天一个 JSON,保留 30 天。

## 安全

- 传输加密:消息使用 ChaCha20-Poly1305,密钥由 token 派生;token 不在网络上明文传输。
- client 只能用 server 允许的 token 连接。
- client 只执行自己 config 中列出的服务,不会执行任意命令。
- 网页没有登录,通过 HTTP 访问。服务控制接口要求自定义请求头以防跨站请求,删除 client 需要管理密码。
- **网页能启停服务,请只在内网开放,或在前面加 nginx 做 HTTPS 和访问控制。**

## 构建与运行

需要 Rust 工具链,不需要安装 `protoc`。

```sh
cargo build --release
./target/release/server server.toml
./target/release/client client.toml
```

网页默认地址:`http://<server>:8080`。client 需要有权限调用 `systemctl`(通常以 root 运行)。

## 配置

**server.toml**

```toml
grpc_listen = "0.0.0.0:50051"
http_listen = "0.0.0.0:8080"
data_dir = "data"                  # 历史曲线数据
log_dir = "logs"                   # 事件日志
tokens = ["change-me-token-1"]     # 允许连接的 token 列表
admin_password = "change-me"       # 删除离线 client 的密码,留空则禁用删除
```

**client.toml**

```toml
server = "http://127.0.0.1:50051"
name = "host-1"                    # 网页上显示的名称
token = "change-me-token-1"        # 必须在 server 的 tokens 中
services = ["nginx.service"]       # 允许 server 查看并控制的服务
guard = ["nginx.service"]          # 需要守护的服务,必须是 services 的子集
```

## 目录结构

```
proto/monitor.proto     通信协议
src/lib.rs, crypto.rs   协议代码与加密
src/bin/client.rs       client
src/bin/server/         server(gRPC、网页接口、存储、日志)
src/bin/server/*.html   网页页面(编译时内嵌)
```
