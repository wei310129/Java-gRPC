package com.example.javagrpc.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class PacketParseControllerTest {

    private final PacketParseController controller = new PacketParseController();

    @Test
    void parse_shouldReturnParsedPacket() {
        PacketParseController.PacketParseResponse response = controller.parse(
                new PacketParseController.PacketParseRequest("CA FE 01 00 05 68 65 6C 6C 6F"));

        assertEquals(0xCAFE, response.magic());
        assertEquals(1, response.version());
        assertEquals(5, response.bodyLength());
        assertEquals("hello", response.body());
    }

    @Test
    void parse_shouldThrow400WhenPacketInvalid() {
        ResponseStatusException ex = assertThrows(
                ResponseStatusException.class,
                () -> controller.parse(new PacketParseController.PacketParseRequest("CAFE0100056869")));

        assertEquals(400, ex.getStatusCode().value());
    }
}
