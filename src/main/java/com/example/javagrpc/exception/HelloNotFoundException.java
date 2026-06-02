package com.example.javagrpc.exception;

public class HelloNotFoundException extends RuntimeException {
    public HelloNotFoundException(String name) {
        super("User not found: " + name);
    }
}
