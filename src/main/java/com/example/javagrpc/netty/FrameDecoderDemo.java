package com.example.javagrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 用 {@link EmbeddedChannel} 示範 {@link LengthFieldBasedFrameDecoder} 如何處理
 * 黏包（sticky packet）與半包（half packet）。
 *
 * <p>直接執行本類別的 {@code main} 方法即可在 console 觀察結果，不需要真的開 TCP 連線。
 *
 * <p>封包格式：{@code [ magic(2) | version(1) | bodyLength(2) | body(N) ] }，
 * {@code bodyLength} 欄位位於 offset 3、長度 2 bytes，且其值就是「frame 長度 - 5」，
 * 因此 {@code lengthAdjustment=0}、{@code initialBytesToStrip=0}（保留 header 給
 * {@link PacketFrameDecoder} 解析）。
 *
 * <h2>為什麼要練習這個技術</h2>
 * TCP 是「位元組流」，沒有訊息邊界：作業系統的 socket buffer 何時觸發一次
 * {@code read}、一次 {@code read} 會帶回多少 bytes，完全不對應到應用層的一筆訊息。
 * 結果就是兩種典型問題：
 * <ul>
 *   <li><b>黏包</b>：連續傳送的多筆訊息被合併在同一次 read 裡。</li>
 *   <li><b>半包</b>：一筆訊息被拆成多次 read 才收完。</li>
 * </ul>
 * 任何「自訂二進位協議」的伺服器/客戶端都一定要解決這個問題，否則資料解析會錯位、丟資料。
 *
 * <h2>不可取代性</h2>
 * <ul>
 *   <li><b>正確性無法靠「多讀幾次」蒙混過去</b>：必須有明確規則判斷「目前累積的 bytes
 *       是否已構成一個完整訊息」，否則在高流量下遲早會解析錯位。</li>
 *   <li><b>自己手刻容易出錯且難維護</b>：要正確處理「索引回滾（mark/reset）」、
 *       「累積緩衝區（cumulation buffer）」、「frame 過長保護」等細節；
 *       {@link LengthFieldBasedFrameDecoder} 是 Netty 已經被大量專案驗證過的標準實作，
 *       用設定取代重寫，大幅降低出錯機率。</li>
 *   <li><b>效能</b>：相較於「逐 byte 掃描尋找分隔符」的協議設計，長度前綴可以
 *       O(1) 直接定位下一筆訊息的邊界，不需要掃描整段 body 內容。</li>
 * </ul>
 *
 * <h2>適用場景</h2>
 * <ul>
 *   <li>自訂二進位協議的 TCP server/client（IM、遊戲伺服器、IoT 裝置通訊、交易/行情協議）。</li>
 *   <li>RPC 框架的傳輸層（例如 Dubbo、gRPC 底層、自製微服務間二進位協議）。</li>
 *   <li>任何「先讀固定長度 header 取得 body 長度，再讀 body」的長度前綴（length-prefixed）協議。</li>
 * </ul>
 * <p>不適用：協議本身是文字型且以分隔符結尾（如 HTTP/1.1、Redis RESP、以換行分隔的協議），
 * 這類情況應改用 {@code DelimiterBasedFrameDecoder} 或對應協議的現成 codec。
 */
public class FrameDecoderDemo {

    /** header 中 bodyLength 欄位的 offset：magic(2) + version(1)。 */
    private static final int LENGTH_FIELD_OFFSET = 3;

    /** bodyLength 欄位長度。 */
    private static final int LENGTH_FIELD_LENGTH = 2;

    private static final int MAX_FRAME_LENGTH = 65535;

    private FrameDecoderDemo() {
    }

    public static void main(String[] args) {
        demoStickyPackets();
        System.out.println();
        demoHalfPacket();
    }

    /** 兩個封包黏在一起，一次送進 pipeline，decoder 應切出兩個訊息。 */
    private static void demoStickyPackets() {
        System.out.println("=== 黏包 (sticky packet) ===");
        EmbeddedChannel channel = newChannel();

        ByteBuf packetA = encode(channel, new PacketMessage(0xCAFE, 1, "ab"));
        ByteBuf packetB = encode(channel, new PacketMessage(0xCAFE, 1, "cdef"));

        ByteBuf combined = Unpooled.buffer();
        combined.writeBytes(packetA);
        combined.writeBytes(packetB);
        packetA.release();
        packetB.release();

        System.out.println("一次送出 " + combined.readableBytes() + " bytes（兩個封包黏在一起）");
        channel.writeInbound(combined);

        printDecoded(channel, "第一個訊息");
        printDecoded(channel, "第二個訊息");
    }

    /** 一個封包被拆成兩次送進 pipeline，decoder 應該等資料齊全才輸出。 */
    private static void demoHalfPacket() {
        System.out.println("=== 半包 (half packet) ===");
        EmbeddedChannel channel = newChannel();

        ByteBuf full = encode(channel, new PacketMessage(0xCAFE, 1, "hello"));
        byte[] bytes = new byte[full.readableBytes()];
        full.readBytes(bytes);
        full.release();

        // 第一次只送出前 4 bytes，header（5 bytes）都還沒收完。
        System.out.println("第一次送出前 4 bytes（header 還不完整）");
        channel.writeInbound(Unpooled.wrappedBuffer(bytes, 0, 4));
        printDecoded(channel, "此時 readInbound()");

        // 補上剩餘 bytes，frame 才會被完整解析出來。
        System.out.println("送出剩餘 " + (bytes.length - 4) + " bytes");
        channel.writeInbound(Unpooled.wrappedBuffer(bytes, 4, bytes.length - 4));
        printDecoded(channel, "此時 readInbound()");
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, 0, 0),
                new PacketFrameDecoder(),
                new PacketFrameEncoder());
    }

    /** 透過 outbound pipeline 把 PacketMessage 編碼成 ByteBuf，方便組出測試用的 wire bytes。 */
    private static ByteBuf encode(EmbeddedChannel channel, PacketMessage message) {
        channel.writeOutbound(message);
        return channel.readOutbound();
    }

    private static void printDecoded(EmbeddedChannel channel, String label) {
        PacketMessage decoded = channel.readInbound();
        System.out.println(label + " -> " + decoded);
    }
}
