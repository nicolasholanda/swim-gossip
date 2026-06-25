package dev.nicolas.swim;

import dev.nicolas.swim.event.EventType;
import dev.nicolas.swim.sim.InProcessTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class IndirectProbeTest {

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    private static MembershipList membershipOf(InetSocketAddress... addrs) {
        MembershipList ml = new MembershipList(System.nanoTime());
        char id = 'A';
        for (InetSocketAddress a : addrs) {
            ml.add(new Member(String.valueOf(id++), a, MemberState.ALIVE, 0));
        }
        return ml;
    }

    @Test
    void indirectAckRescuesTargetWhenDirectPathBlocked() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 20001);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 20002);
        InetSocketAddress addrC = new InetSocketAddress("127.0.0.1", 20003);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(300))
                .withDirectAckTimeout(Duration.ofMillis(80))
                .withIndirectFanout(2);

        InProcessTransport tA = new InProcessTransport(addrA, 1L);
        InProcessTransport tB = new InProcessTransport(addrB, 2L);
        InProcessTransport tC = new InProcessTransport(addrC, 3L);
        Codec codec = new Codec(config.maxMessageBytes());

        tA.blockPeer(addrB);
        tB.blockPeer(addrA);

        FailureDetectorTest.RecordingSink sinkA = new FailureDetectorTest.RecordingSink();
        SwimNode nodeA = new SwimNode("A", tA, codec, membershipOf(addrA, addrB, addrC), config, sinkA);
        SwimNode nodeB = new SwimNode("B", tB, codec, membershipOf(addrA, addrB, addrC), config, dev.nicolas.swim.event.EventSink.NOOP);
        SwimNode nodeC = new SwimNode("C", tC, codec, membershipOf(addrA, addrB, addrC), config, dev.nicolas.swim.event.EventSink.NOOP);

        try {
            nodeA.start();
            nodeB.start();
            nodeC.start();

            assertTrue(waitFor(() -> sinkA.has(EventType.PING_REQ), 2000),
                    "A should send PING_REQ when direct path to B fails");
            assertTrue(waitFor(() -> sinkA.has(EventType.INDIRECT_ACK), 2000),
                    "A should receive INDIRECT_ACK via C");
        } finally {
            nodeA.close();
            nodeB.close();
            nodeC.close();
        }
    }

    @Test
    void noIndirectAckWhenTargetIsFullyDown() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 20011);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 20012);
        InetSocketAddress addrC = new InetSocketAddress("127.0.0.1", 20013);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(300))
                .withDirectAckTimeout(Duration.ofMillis(80))
                .withIndirectFanout(2);

        InProcessTransport tA = new InProcessTransport(addrA, 1L);
        InProcessTransport tC = new InProcessTransport(addrC, 3L);
        Codec codec = new Codec(config.maxMessageBytes());

        FailureDetectorTest.RecordingSink sinkA = new FailureDetectorTest.RecordingSink();
        SwimNode nodeA = new SwimNode("A", tA, codec, membershipOf(addrA, addrB, addrC), config, sinkA);
        SwimNode nodeC = new SwimNode("C", tC, codec, membershipOf(addrA, addrB, addrC), config, dev.nicolas.swim.event.EventSink.NOOP);

        try {
            nodeA.start();
            nodeC.start();

            assertTrue(waitFor(() -> sinkA.has(EventType.PING_REQ), 2000),
                    "A should attempt PING_REQ after direct timeout");
            assertTrue(waitFor(() -> sinkA.events.stream()
                    .anyMatch(e -> e.type() == EventType.TIMEOUT
                            && "B".equals(e.target())
                            && "final".equals(e.meta().get("phase"))), 2000),
                    "A should emit final TIMEOUT for B when no INDIRECT_ACK arrives");
            assertFalse(sinkA.has(EventType.INDIRECT_ACK), "no INDIRECT_ACK expected");
        } finally {
            nodeA.close();
            nodeC.close();
        }
    }
}
