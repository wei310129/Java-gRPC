package com.example.javagrpc.exception;

public class HelloBadRequestException extends RuntimeException {
    public HelloBadRequestException(String message) {
        super(message);
    }
}
