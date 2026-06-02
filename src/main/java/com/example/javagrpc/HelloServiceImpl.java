package com.example.javagrpc;

import com.example.javagrpc.grpc.HelloRequest;
import com.example.javagrpc.grpc.HelloResponse;
import com.example.javagrpc.grpc.HelloServiceGrpc;
import com.example.javagrpc.exception.HelloBadRequestException;
import com.example.javagrpc.exception.HelloNotFoundException;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
public class HelloServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {

    // @GrpcClient 由 Spring 在 runtime 透過 reflection 注入，IDE 靜態分析看不到賦值所以誤報 unused
    @SuppressWarnings("unused")
    @GrpcClient("hello-service-9091")
    private HelloServiceGrpc.HelloServiceBlockingStub helloService9091;

    /**
     * Unary RPC：一個 request，一個 response。
     * 示範 @GrpcAdvice 的 exception handling：
     *   - blank name  → HelloBadRequestException → Status.INVALID_ARGUMENT
     *   - "unknown"   → HelloNotFoundException   → Status.NOT_FOUND
     */
    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        log.info("sayHello");
        if (request.getName().isBlank()) {
            throw new HelloBadRequestException("name must not be blank");
        }
        if (request.getName().equalsIgnoreCase("unknown")) {
            throw new HelloNotFoundException(request.getName());
        }
        HelloResponse response = HelloResponse.newBuilder()
                .setMessage("Hello, " + request.getName())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Server Streaming RPC：一個 request，server 持續推送多個 response。
     * 適合進度回報、即時推播等場景。
     */
    @Override
    public void sayHelloStream(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        for (int i = 1; i <= 5; i++) {
            HelloResponse response = HelloResponse.newBuilder()
                    .setMessage("Hello, " + request.getName() + " [" + i + "/5]")
                    .build();
            responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
    }

    /**
     * Forward RPC：收到 request 後轉發給 9091 的 HelloService。
     * 示範 gRPC-to-gRPC 呼叫與 deadline 傳遞。
     *
     * Deadline 策略：
     *   - 上游有 deadline → 直接沿用，確保整條 chain 共用同一個超時時間點，
     *     避免下游在上游已超時後還繼續執行。
     *   - 上游無 deadline（e.g. 直接用 grpcurl 測試）→ fallback 到 3 秒保護，
     *     防止下游無限等待。
     *
     * onError 直接傳原始 StatusRuntimeException 而非重新包裝，
     * 是為了保留下游回傳的 trailers（錯誤細節 metadata），完整轉發給 caller。
     */
    @Override
    public void forwardSayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        log.info("forwardSayHello");
        try {
            Deadline upstreamDeadline = Context.current().getDeadline();
            Deadline effectiveDeadline = upstreamDeadline != null
                    ? upstreamDeadline
                    : Deadline.after(3, TimeUnit.SECONDS);
            HelloResponse response = helloService9091
                    .withDeadline(effectiveDeadline)
                    .sayHello(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            log.error("forwardSayHello failed: {}", e.getStatus());
            responseObserver.onError(e);
        }
    }
}
