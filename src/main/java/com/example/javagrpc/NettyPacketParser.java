package com.example.javagrpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * Netty ByteBuf 封包解析練習（教學版）。
 *
 * <p>封包格式（Big-Endian）：
 *
 * <pre>
 * [ magic(2 bytes) | version(1 byte) | bodyLength(2 bytes) | body(N bytes, UTF-8) ]
 * </pre>
 *
 * <p>重點：
 * <ul>
 *   <li>先驗證資料長度再讀取，避免越界。</li>
 *   <li>使用 mark/reset 示範「半包時回滾 readerIndex」。</li>
 *   <li>使用 unsigned 讀法避免 signed 型別誤判。</li>
 * </ul>
 */
public final class NettyPacketParser {

    /**
     * 固定 header 長度：2(magic) + 1(version) + 2(bodyLength) = 5 bytes。
     */
    private static final int HEADER_SIZE = 5;

    private NettyPacketParser() {
    }

    /**
     * 將 hex 字串解析為封包欄位。
     *
     * @param hexPacket 可接受「CA FE」、「0xCA 0xFE」、「CAFE」三種常見輸入型式
     */
    public static ParsedPacket parseHexPacket(String hexPacket) {
        // 1) 先做最外層防呆：null/blank 不進入解析流程。
        if (hexPacket == null || hexPacket.isBlank()) {
            throw new IllegalArgumentException("hexPacket must not be blank");
        }

        // 2) 先 normalize，讓輸入格式一致（拿掉 0x 與空白）。
        String normalizedHex = normalizeHex(hexPacket);

        // 3) Hex 兩個字元 = 1 byte。
        //    若字元數是奇數，代表最後半個 byte 缺失（不可能組成完整 byte），必須直接拒絕。
        //    (len & 1) 等價於 len % 2：
        //      - 結果 0 代表偶數
        //      - 結果 1 代表奇數
        //    所以 (normalizedHex.length() & 1) != 0 => 長度為奇數 => 非法 hex 長度。
        if ((normalizedHex.length() & 1) != 0) {
            throw new IllegalArgumentException("hexPacket length must be even");
        }

        // 4) 將 hex 字串轉為原始 bytes，例如 "CAFE" -> [0xCA, 0xFE]。
        byte[] raw;
        try {
            raw = HexFormat.of().parseHex(normalizedHex);
        } catch (IllegalArgumentException ex) {
            // parseHex 會在出現非法字元（非 0-9A-Fa-f）時丟 IllegalArgumentException。
            throw new IllegalArgumentException("hexPacket contains invalid hex characters", ex);
        }

        // 5) wrappedBuffer 是 zero-copy：不複製 byte[]，直接包成 ByteBuf。
        ByteBuf buf = Unpooled.wrappedBuffer(raw);

        // readableBytes = writerIndex - readerIndex。
        // 讀 header 前先確認至少有 5 bytes，避免 readUnsigned... 直接越界。
        if (buf.readableBytes() < HEADER_SIZE) {
            throw new IllegalArgumentException("packet too short: need at least 5 bytes header");
        }

        // 先標記目前 readerIndex（通常是 0），後續若偵測到半包可回滾。
        buf.markReaderIndex();

        // readUnsignedXXX：
        // - readUnsignedShort() 讀 2 bytes，範圍 0..65535
        // - readUnsignedByte()  讀 1 byte，範圍 0..255
        // 避免 Java signed byte/short 轉成負值造成判斷錯誤。
        int magic = buf.readUnsignedShort();
        int version = buf.readUnsignedByte();
        int bodyLength = buf.readUnsignedShort();

        // header 宣告的 bodyLength 不可大於「目前剩餘可讀長度」，否則是半包或壞包。
        if (buf.readableBytes() < bodyLength) {
            // 回滾到 mark 的位置，示範 decoder 在串流場景下可等待更多資料再重試。
            buf.resetReaderIndex();
            throw new IllegalArgumentException(
                    "packet body incomplete: expected " + bodyLength + " bytes, but only "
                            + buf.readableBytes() + " bytes available");
        }

        // 讀取 body；讀完後 readerIndex 會往前推進 bodyLength。
        byte[] bodyBytes = new byte[bodyLength];
        buf.readBytes(bodyBytes);
        String body = new String(bodyBytes, StandardCharsets.UTF_8);

        // 若還有剩餘 bytes，代表封包後面還黏了資料（trailing bytes）。
        // 這在實務上可能是「黏包」或額外噪音，先用 note 呈現，方便你觀察。
        String note = buf.isReadable()
                ? "trailing bytes detected: " + buf.readableBytes()
                : "packet parsed successfully";

        return new ParsedPacket(magic, version, bodyLength, body, note);
    }

    /**
     * 把輸入的 hex 字串標準化。
     *
     * <p>處理規則：
     * <ol>
     *   <li>移除所有 "0x" / "0X" 前綴（例如 "0xCA 0xFE" -> "CA FE"）</li>
     *   <li>移除所有空白字元（空白、tab、換行）</li>
     * </ol>
     *
     * <p>範例：
     * <ul>
     *   <li>"0xCA 0xFE" -> "CAFE"</li>
     *   <li>"CA FE" -> "CAFE"</li>
     *   <li>"CA\nFE" -> "CAFE"</li>
     * </ul>
     */
    private static String normalizeHex(String value) {
        // replace 是字面替換：把所有 0x / 0X 去掉。
        String noPrefix = value.replace("0x", "").replace("0X", "");

        // regex "\\s+" = 一個以上空白字元（space/tab/newline）。
        // 全部移除後，讓後續 parseHex 只看到純十六進位字元。
        return noPrefix.replaceAll("\\s+", "");
    }

    public record ParsedPacket(int magic, int version, int bodyLength, String body, String note) {
    }
}
