# Java-gRPC — gRPC 通訊與 Netty 網路程式設計 PoC

用 Spring Boot 實作 gRPC server/client，並以 Netty 練習二進位封包解析、黏包／半包處理、讀取閒置逾時與群組廣播。此專案用來驗證通訊流程及底層網路處理的核心概念。

## 專案範圍

| 範例 | 實作內容 | 執行入口 |
|---|---|---|
| gRPC 通訊 | Unary、Server Streaming、跨 server 轉發、Deadline、例外映射 | Spring Boot + grpcurl |
| 封包解析 | Netty `ByteBuf` 解析自訂 header 與 UTF-8 body | `POST /api/packets/parse` |
| 黏包／半包 | 長度欄位解碼、封包 decoder/encoder pipeline | `FrameDecoderDemo.main()` |
| 閒置逾時 | `IdleStateHandler`、模擬心跳與關閉閒置 channel | `IdleStateHandlerDemo.main()` |
| 群組廣播 | `ChannelGroup`、room 篩選、channel 關閉後自動移除 | `ChannelGroupDemo.main()` |

三個 Netty Demo 使用 `EmbeddedChannel` 在程式內驗證 pipeline，沒有啟動獨立的 TCP socket server。

## 技術棧

| 項目 | 版本 |
|---|---|
| Java | 21 |
| Spring Boot | 4.0.6 |
| gRPC | 1.75.0 |
| grpc-spring-boot-starter | 3.1.0.RELEASE |
| Protocol Buffers | 3.25.5 |
| Netty | buffer / codec / handler；版本由 Maven dependency management 管理 |
| Lombok | — |

## 功能說明

### 1. Unary RPC — `SayHello`

最基本的 gRPC 模式：一個 request，一個 response。

示範重點：
- `@GrpcService` 註冊 gRPC service
- 透過 `@GrpcAdvice` + `@GrpcExceptionHandler` 做集中式例外處理，service 只需 throw 業務 exception，不需手動呼叫 `responseObserver.onError()`

```
client ──SayHello(name)──▶ server:9090
       ◀──Hello, {name}──
```

例外情境：

| 輸入 | 丟出的 Exception | gRPC Status |
|---|---|---|
| name 為空字串 | `HelloBadRequestException` | `INVALID_ARGUMENT` |
| name = "unknown" | `HelloNotFoundException` | `NOT_FOUND` |

### 2. Server Streaming RPC — `SayHelloStream`

一個 request，server 持續推送多個 response。適合進度回報、即時推播等場景。

此範例 server 會連續推送 5 則訊息後結束。

```
client ──SayHelloStream(name)──▶ server:9090
       ◀──Hello, {name} [1/5]──
       ◀──Hello, {name} [2/5]──
       ...
       ◀──Hello, {name} [5/5]──
```

### 3. gRPC-to-gRPC 轉發 — `ForwardSayHello`

示範 gRPC server 作為 client 呼叫另一個 gRPC server，模擬微服務鏈式呼叫。

```
client ──ForwardSayHello(name)──▶ server:9090 ──SayHello(name)──▶ server:9091
       ◀──────────────── Hello, {name} ─────────────────────────◀─
```

實作重點：
- `@GrpcClient` 注入下游 stub（runtime reflection 注入，非 Spring bean）
- `application-server2.yaml` profile 讓同一個 jar 以不同 port 起兩個 instance

### 4. Deadline 控制與上游傳播

呼叫下游 gRPC 時，Deadline 有兩種來源：

| 情境 | 策略 |
|---|---|
| 上游 caller 有設 deadline | `Context.current().getDeadline()` 直接沿用，整條 chain 共用同一個絕對超時時間點 |
| 上游 caller 無 deadline（如 grpcurl 直接測試）| fallback：`Deadline.after(3, TimeUnit.SECONDS)` 保護下游不無限等待 |

下游呼叫沿用同一個 deadline，以限制整條呼叫鏈的等待時間。RPC 逾時或取消不代表下游業務運算一定立即停止；耗時工作仍需自行檢查取消狀態。

### 5. 全域例外處理 — `@GrpcAdvice`

`GlobalGrpcExceptionHandler` 集中管理所有 gRPC 例外對應：

```
業務 Exception ──▶ @GrpcAdvice ──▶ gRPC Status (回傳給 client)
```

`catch (StatusRuntimeException e)` 的 `onError(e)` 直接傳原始 exception，可保留下游回傳的 trailers（錯誤細節 metadata），完整轉發給最終 caller。

### 6. HTTP API（非 gRPC）— `POST /api/packets/parse`

除了 gRPC，也提供 REST API 來做 Netty `ByteBuf` 封包解析練習。

封包格式（big-endian）：

```
| magic(2 bytes) | version(1 byte) | bodyLength(2 bytes) | body(N bytes, UTF-8) |
```

實作重點：
- `readableBytes()`：讀取前先確認資料長度
- `markReaderIndex()/resetReaderIndex()`：封包不足時回滾讀取位置
- `readUnsignedShort()/readUnsignedByte()`：避免 signed 型別造成負值誤判

### 7. Netty Pipeline — 黏包／半包

`FrameDecoderDemo` 組合以下 pipeline：

```text
LengthFieldBasedFrameDecoder → PacketFrameDecoder → PacketFrameEncoder
```

- 長度欄位位於 offset 3、佔 2 bytes；header 共 5 bytes，保留完整 frame 給 parser。
- 黏包：一次輸入兩個串接封包，應解出兩個 `PacketMessage`。
- 半包：先輸入前 4 bytes，應無訊息輸出；補齊剩餘資料後才解出封包。
- Encoder 依 UTF-8 body 的 byte 長度寫入 header，示範訊息與二進位資料的轉換。

### 8. 讀取閒置逾時與模擬心跳

`IdleStateHandlerDemo` 設定 3 秒 reader idle，透過 `EmbeddedChannel` 模擬時間前進：每 2 秒收到資料時保持連線；停止收到資料後觸發 `READER_IDLE`，由 handler 關閉 channel。這裡的心跳是模擬收到資料，未另行定義心跳封包協定。

### 9. ChannelGroup 群組廣播

`ChannelGroupDemo` 示範所有 channel 廣播、透過 channel attribute 記錄 room 並篩選接收者，以及 channel 關閉後自動從群組移除。每個 `EmbeddedChannel` 使用獨立的 `DefaultChannelId`，避免識別碼相同導致加入群組失敗。

---

## 啟動方式

### 環境需求

- JDK 21（專案編譯目標）
- Maven（或使用專案內附的 `mvnw`）

### 編譯

```bash
./mvnw compile
```

Windows PowerShell：

```powershell
.\mvnw.cmd compile
```

`protobuf-maven-plugin` 會在 compile 階段自動將 `src/main/proto/hello.proto` 編譯成 Java stub（輸出至 `target/generated-sources/`）。

### 啟動兩個 Server

在專案根目錄開啟兩個終端機，分別執行：

```powershell
# 終端機 1：gRPC 9090、HTTP 8080
.\mvnw.cmd spring-boot:run

# 終端機 2：gRPC 9091、HTTP 8081
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=server2" "-Dspring-boot.run.arguments=--server.port=8081"
```

Linux / macOS：

```bash
# 終端機 1
./mvnw spring-boot:run

# 終端機 2
./mvnw spring-boot:run -Dspring-boot.run.profiles=server2 '-Dspring-boot.run.arguments=--server.port=8081'
```

`server2` profile 只改 gRPC port；第二個 instance 必須另外指定 HTTP port，避免兩個 Spring Boot instance 同時占用 8080。9090 的轉發 client 以 plaintext 連到 `localhost:9091`。

### 執行 Netty Demo

在 IDE 匯入 Maven 專案，完成編譯後，直接執行 `com.example.javagrpc.netty` 下的三個 Demo `main()`。觀察 console 中的封包數量、idle 事件、channel 是否關閉，以及各 channel 收到的廣播訊息；不需要先啟動 Spring Boot。

---

## 測試方式

使用 [grpcurl](https://github.com/fullstorydev/grpcurl) 測試（需先安裝）。

以下 grpcurl 與 curl 指令使用 Bash 的 JSON 引號語法；Windows HTTP 測試可使用後面的 PowerShell 範例。

```bash
# Unary
grpcurl -plaintext -d '{"name":"Alice"}' localhost:9090 HelloService/SayHello

# Server Streaming
grpcurl -plaintext -d '{"name":"Alice"}' localhost:9090 HelloService/SayHelloStream

# Forward（需同時啟動 9090 和 9091）
grpcurl -plaintext -d '{"name":"Alice"}' localhost:9090 HelloService/ForwardSayHello

# 觸發 INVALID_ARGUMENT
grpcurl -plaintext -d '{"name":""}' localhost:9090 HelloService/SayHello

# 觸發 NOT_FOUND
grpcurl -plaintext -d '{"name":"unknown"}' localhost:9090 HelloService/SayHello
```

HTTP API 測試（非 gRPC）：

```bash
curl -i -X POST http://localhost:8080/api/packets/parse -H "Content-Type: application/json" -d '{"hexPacket":"CA FE 01 00 05 68 65 6C 6C 6F"}'

curl -i -X POST http://localhost:8080/api/packets/parse -H "Content-Type: application/json" -d '{"hexPacket":"CAFE0100056869"}'
```

Windows PowerShell：

```powershell
$body = @{ hexPacket = 'CA FE 01 00 05 68 65 6C 6C 6F' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/packets/parse' -ContentType 'application/json' -Body $body
```

第一個封包應回傳 `magic=51966`、`version=1`、`bodyLength=5`、`body="hello"`。第二個封包宣告 5 bytes，但 body 只有 2 bytes，會進入解析錯誤處理。

### 自動化測試與範圍

```powershell
.\mvnw.cmd test
```

Linux / macOS 使用 `./mvnw test`。

- `PacketParseControllerTest`：直接呼叫 controller，驗證有效封包欄位及無效封包拋出的 `ResponseStatusException`；未經 HTTP/MVC 流程。
- `JavaGRpcApplicationTests`：Spring Boot context 啟動測試。
- `NettyPacketParserTest.java` 目前是空檔案；三個 Netty Demo 是可執行示範，尚未轉為 JUnit 測試。

## 目前限制

- Parser 會讀取 magic 與 version，但未驗證固定 magic 或支援的協定版本。
- HTTP parser 一次解析一個封包，額外資料透過 note 提示；多封包拆解由 frame decoder 示範。
- `parsePacket(ByteBuf)` 的輸入 buffer 生命週期由呼叫端管理。
- Controller 的解析錯誤 handler 目前只回傳 `BAD_PACKET` body，未顯式設定 HTTP status；現有單元測試不能證明實際 HTTP 回應為 400。測試 API 時應同時查看 status 與 body。
- gRPC plaintext 與 `EmbeddedChannel` 範例用於本機 PoC，尚未包含正式 TCP server、TLS 與完整端到端測試。

---

## 專案結構重點

```
src/main/proto/hello.proto          # Proto 定義（service + message）
src/main/java/.../HelloServiceImpl  # 三個 RPC 實作
src/main/java/.../NettyPacketParser # ByteBuf 封包解析核心
src/main/java/.../api/PacketParseController # 非 gRPC HTTP API
src/main/java/.../netty/            # Frame、IdleStateHandler、ChannelGroup Demo
src/main/java/.../GlobalGrpcExceptionHandler  # @GrpcAdvice 集中例外處理
src/main/java/.../exception/        # 業務 Exception 定義
src/main/resources/application.yaml           # 主設定（port 9090）
src/main/resources/application-server2.yaml  # 第二 server profile（port 9091）
```

## 關鍵設計決策

- **`grpc-bom` in `<dependencyManagement>`**：統一 gRPC runtime 與 `protoc-gen-grpc-java` 使用的版本至 1.75.0，避免傳遞依賴的版本差異。
- **`javax.annotation-api`**：生成的 stub 使用 `@javax.annotation.Generated`，因此顯式加入對應 annotation dependency。
