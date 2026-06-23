package dev.nicolas.swim;

import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CodecTest {

    private final Codec codec = new Codec(1400);
    private final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 7001);

    @Test
    void pingRoundTrip() throws Exception {
        Message.Ping ping = new Message.Ping(1L, List.of());
        byte[] bytes = codec.encode(ping);
        Message decoded = codec.decode(bytes);
        assertInstanceOf(Message.Ping.class, decoded);
        assertEquals(ping.seqNo(), ((Message.Ping) decoded).seqNo());
        assertEquals(ping.piggyback(), ((Message.Ping) decoded).piggyback());
    }

    @Test
    void ackRoundTrip() throws Exception {
        Message.Ack ack = new Message.Ack(42L, List.of());
        byte[] bytes = codec.encode(ack);
        Message decoded = codec.decode(bytes);
        assertInstanceOf(Message.Ack.class, decoded);
        assertEquals(ack.seqNo(), ((Message.Ack) decoded).seqNo());
    }

    @Test
    void pingReqRoundTrip() throws Exception {
        Message.PingReq req = new Message.PingReq(7L, "node-2", addr, List.of());
        byte[] bytes = codec.encode(req);
        Message decoded = codec.decode(bytes);
        assertInstanceOf(Message.PingReq.class, decoded);
        Message.PingReq decodedReq = (Message.PingReq) decoded;
        assertEquals(req.seqNo(), decodedReq.seqNo());
        assertEquals(req.targetId(), decodedReq.targetId());
        assertEquals(req.targetAddress(), decodedReq.targetAddress());
    }

    @Test
    void messageWithPiggybackRoundTrip() throws Exception {
        Update u = new Update("node-3", addr, MemberState.SUSPECT, 2);
        Message.Ping ping = new Message.Ping(5L, List.of(u));
        byte[] bytes = codec.encode(ping);
        Message decoded = codec.decode(bytes);
        Message.Ping decodedPing = (Message.Ping) decoded;
        assertEquals(1, decodedPing.piggyback().size());
        Update du = decodedPing.piggyback().get(0);
        assertEquals(u.id(), du.id());
        assertEquals(u.state(), du.state());
        assertEquals(u.incarnation(), du.incarnation());
        assertEquals(u.address(), du.address());
    }

    @Test
    void encodeThrowsWhenExceedsMtu() {
        Codec smallCodec = new Codec(10);
        Message.Ping ping = new Message.Ping(1L, List.of());
        assertThrows(IllegalArgumentException.class, () -> smallCodec.encode(ping));
    }
}
