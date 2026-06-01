package com.example.javagrpc;

import com.example.javagrpc.grpc.HelloRequest;
import com.example.javagrpc.grpc.HelloResponse;
import com.example.javagrpc.grpc.HelloServiceGrpc;
import io.grpc.stub.StreamObserver;
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
        // 呼叫 9091 的 SayHello，再把結果回傳給 caller
        HelloResponse response = helloService9091.sayHello(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
