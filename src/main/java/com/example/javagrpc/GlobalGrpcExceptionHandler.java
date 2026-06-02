package com.example.javagrpc;

import com.example.javagrpc.exception.HelloBadRequestException;
import com.example.javagrpc.exception.HelloNotFoundException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@Slf4j
@GrpcAdvice
public class GlobalGrpcExceptionHandler {

    @GrpcExceptionHandler(HelloNotFoundException.class)
    public StatusRuntimeException handleNotFound(HelloNotFoundException e) {
        log.warn("Not found: {}", e.getMessage());
        return Status.NOT_FOUND
                .withDescription(e.getMessage())
                .asRuntimeException();
    }

    @GrpcExceptionHandler(HelloBadRequestException.class)
    public StatusRuntimeException handleBadRequest(HelloBadRequestException e) {
        log.warn("Bad request: {}", e.getMessage());
        return Status.INVALID_ARGUMENT
                .withDescription(e.getMessage())
                .asRuntimeException();
    }

    @GrpcExceptionHandler(Exception.class)
    public StatusRuntimeException handleUnexpected(Exception e) {
        log.error("Unexpected error", e);
        return Status.INTERNAL
                .withDescription("Internal server error")
                .asRuntimeException();
    }
}
