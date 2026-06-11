package com.example.javagrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.charset.StandardCharsets;

/**
 * 將 {@link PacketMessage} 編碼為 wire format：
 * {@code [ magic(2 bytes) | version(1 byte) | bodyLength(2 bytes) | body(N bytes, UTF-8) ] }。
 *
 * <p>bodyLength 由 body 的 UTF-8 位元組長度即時計算，呼叫端不需自行填寫。
 */
public class PacketFrameEncoder extends MessageToByteEncoder<PacketMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PacketMessage msg, ByteBuf out) {
        byte[] bodyBytes = msg.body().getBytes(StandardCharsets.UTF_8);
        out.writeShort(msg.magic());
        out.writeByte(msg.version());
        out.writeShort(bodyBytes.length);
        out.writeBytes(bodyBytes);
    }
}
