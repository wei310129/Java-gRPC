package com.example.javagrpc.netty;

/**
 * decoder 輸出 / encoder 輸入的中介資料結構。
 *
 * <p>對應 {@link com.example.javagrpc.NettyPacketParser} 的封包格式：
 * {@code [ magic(2 bytes) | version(1 byte) | bodyLength(2 bytes) | body(N bytes, UTF-8) ] }。
 * {@code bodyLength} 由 {@link PacketFrameEncoder} 依 {@code body} 的 UTF-8 位元組長度自動計算。
 */
public record PacketMessage(int magic, int version, String body) {
}
