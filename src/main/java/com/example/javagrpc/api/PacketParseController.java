package com.example.javagrpc.api;

import com.example.javagrpc.NettyPacketParser;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/packets")
public class PacketParseController {

    // HTTP 練習入口：把 hex 封包交給 ByteBuf parser，回傳解析後欄位。
    @PostMapping("/parse")
    public PacketParseResponse parse(@Valid @RequestBody PacketParseRequest request) {
        try {
            NettyPacketParser.ParsedPacket parsed = NettyPacketParser.parseHexPacket(request.hexPacket());
            // 顯式映射可保持 API 穩定，避免 parser 內部型別調整直接外溢到 HTTP 合約。
            return new PacketParseResponse(
                    parsed.magic(),
                    parsed.version(),
                    parsed.bodyLength(),
                    parsed.body(),
                    parsed.note());
        } catch (IllegalArgumentException e) {
            // parser 的輸入錯誤統一轉為 400，對前端語意更清楚。
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    // 只針對本 controller 的例外做回傳格式統一。
    @ExceptionHandler
    public PacketErrorResponse handle(ResponseStatusException e) {
        if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
            return new PacketErrorResponse("BAD_PACKET", e.getReason());
        }
        return new PacketErrorResponse("UNEXPECTED", "Unexpected error");
    }

    // hexPacket 可含空白或 0x 前綴；@NotBlank 先擋空輸入。
    public record PacketParseRequest(@NotBlank String hexPacket) {
    }

    // bodyLength 為 header 宣告值；note 用來提示是否有 trailing bytes。
    public record PacketParseResponse(int magic, int version, int bodyLength, String body, String note) {
    }

    public record PacketErrorResponse(String code, String message) {
    }
}


