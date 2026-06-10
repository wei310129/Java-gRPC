# Java gRPC — Spring Boot 範例專案

用 Spring Boot 實作 gRPC server/client 的學習專案，涵蓋基本 Unary RPC、Server Streaming、跨 server 轉發、Deadline 控制以及全域例外處理。

## 技術棧

| 項目 | 版本 |
|---|---|
| Java | 21 |
| Spring Boot | 4.0.6 |
| gRPC | 1.75.0 |
| grpc-spring-boot-starter | 3.1.0.RELEASE |
| Protocol Buffers | 3.25.5 |
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

當上游 deadline 到期時，gRPC Context 會自動取消，下游呼叫也會一併中斷，不會繼續執行。

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

---

## 啟動方式

### 環境需求

- JDK 21+
- Maven（或使用專案內附的 `mvnw`）

### 編譯

```bash
./mvnw compile
```

`protobuf-maven-plugin` 會在 compile 階段自動將 `src/main/proto/hello.proto` 編譯成 Java stub（輸出至 `target/generated-sources/`）。

### 啟動 Server

```bash
# 啟動 port 9090（預設）
./mvnw spring-boot:run

# 啟動 port 9091（測試 ForwardSayHello 用）
./mvnw spring-boot:run -Dspring-boot.run.profiles=server2
```

---

## 測試方式

使用 [grpcurl](https://github.com/fullstorydev/grpcurl) 測試（需先安裝）。

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
curl -X POST http://localhost:8080/api/packets/parse -H "Content-Type: application/json" -d '{"hexPacket":"CA FE 01 00 05 68 65 6C 6C 6F"}'

curl -X POST http://localhost:8080/api/packets/parse -H "Content-Type: application/json" -d '{"hexPacket":"CAFE0100056869"}'
```

---

## 專案結構重點

```
src/main/proto/hello.proto          # Proto 定義（service + message）
src/main/java/.../HelloServiceImpl  # 三個 RPC 實作
src/main/java/.../NettyPacketParser # ByteBuf 封包解析核心
src/main/java/.../api/PacketParseController # 非 gRPC HTTP API
src/main/java/.../GlobalGrpcExceptionHandler  # @GrpcAdvice 集中例外處理
src/main/java/.../exception/        # 業務 Exception 定義
src/main/resources/application.yaml           # 主設定（port 9090）
src/main/resources/application-server2.yaml  # 第二 server profile（port 9091）
```

## 關鍵設計決策

- **`grpc-bom` in `<dependencyManagement>`**：`grpc-server-spring-boot-starter:3.1.0` 拉入的 gRPC runtime 版本與 `protoc-gen-grpc-java:1.75.0` 生成的 stub 不相容，透過 BOM 統一覆蓋所有 gRPC 傳遞依賴至 1.75.0。
- **`javax.annotation-api`**：Java 21 移除了 `javax` namespace，但 protoc-gen-grpc-java 生成的程式碼仍依賴 `@javax.annotation.Generated`，需手動補上。
