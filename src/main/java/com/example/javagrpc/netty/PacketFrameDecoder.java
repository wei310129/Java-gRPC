package com.example.javagrpc.netty;

import com.example.javagrpc.NettyPacketParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/**
 * 將 {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder} 切出的完整 frame
 * 解析為 {@link PacketMessage}。
 *
 * <p>frame 邊界（黏包/半包）已由前一個 handler（LengthFieldBasedFrameDecoder）處理，
 * 這裡收到的 buf 一定是「剛好一個完整封包」，可直接交給
 * {@link NettyPacketParser#parsePacket(ByteBuf)} 解析欄位。
 */
public class PacketFrameDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        NettyPacketParser.ParsedPacket parsed = NettyPacketParser.parsePacket(msg);
        out.add(new PacketMessage(parsed.magic(), parsed.version(), parsed.body()));
    }
}
