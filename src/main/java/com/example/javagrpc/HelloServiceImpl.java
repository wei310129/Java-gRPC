package com.example.javagrpc;

import com.example.javagrpc.grpc.HelloRequest;
import com.example.javagrpc.grpc.HelloResponse;
import com.example.javagrpc.grpc.HelloServiceGrpc;
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

    @SuppressWarnings("unused")
    @GrpcClient("hello-service-9091")
    private HelloServiceGrpc.HelloServiceBlockingStub helloService9091;

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        log.info("sayHello");
        HelloResponse response = HelloResponse.newBuilder()
                .setMessage("Hello, " + request.getName())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

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

    @Override
    public void forwardSayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        log.info("forwardSayHello");
        try {
            // 如果還有上游可以拿上游的dealine來繼續往下傳，實現整條rpc路徑的超時控制
//            Deadline inherited = Context.current().getDeadline();
//            helloService9091.withDeadline(inherited).sayHello(request);
//            HelloResponse response = helloService9091
//                    .withDeadline(Deadline.after(3, TimeUnit.SECONDS)) // 定義絕對時間來限制下游rpc超時
//                    .sayHello(request);
            HelloResponse response = helloService9091
                    .withDeadlineAfter(3, TimeUnit.SECONDS) // 直接建立或刷新超時
                    .sayHello(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            log.error("forwardSayHello failed: {}", e.getStatus());
            responseObserver.onError(Status.fromThrowable(e).asRuntimeException());
        }
    }
}
