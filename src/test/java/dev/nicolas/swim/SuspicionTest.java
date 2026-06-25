package dev.nicolas.swim;

import dev.nicolas.swim.event.EventType;
import dev.nicolas.swim.sim.InProcessTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SuspicionTest {

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    @Test
    void suspectTransitionsToDeadAfterTimeout() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 21001);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 21002);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(150))
                .withDirectAckTimeout(Duration.ofMillis(40))
                .withSuspectTimeout(Duration.ofMillis(300))
                .withIndirectFanout(0);

        InProcessTransport tA = new InProcessTransport(addrA, 1L);
        Codec codec = new Codec(config.maxMessageBytes());

        MembershipList memA = new MembershipList(1L);
        memA.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memA.add(new Member("B", addrB, MemberState.ALIVE, 0));

        FailureDetectorTest.RecordingSink sinkA = new FailureDetectorTest.RecordingSink();
        SwimNode nodeA = new SwimNode("A", tA, codec, memA, config, sinkA);

        try {
            nodeA.start();

            assertTrue(waitFor(() -> sinkA.has(EventType.SUSPECT, "B"), 2000),
                    "A should mark B as SUSPECT after probe fully fails");
            assertEquals(MemberState.SUSPECT, memA.get("B").orElseThrow().state());

            assertTrue(waitFor(() -> sinkA.has(EventType.DEAD, "B"), 2000),
                    "A should transition B to DEAD after T_suspect");
            assertEquals(MemberState.DEAD, memA.get("B").orElseThrow().state());
        } finally {
            nodeA.close();
        }
    }

    @Test
    void selfRefuteBumpsIncarnationAndPropagatesAlive() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 21011);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 21012);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(80))
                .withDirectAckTimeout(Duration.ofMillis(30))
                .withSuspectTimeout(Duration.ofMillis(5000));

        InProcessTransport tA = new InProcessTransport(addrA, 1L);
        InProcessTransport tB = new InProcessTransport(addrB, 2L);
        Codec codec = new Codec(config.maxMessageBytes());

        MembershipList memA = new MembershipList(1L);
        memA.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memA.add(new Member("B", addrB, MemberState.ALIVE, 0));

        MembershipList memB = new MembershipList(2L);
        memB.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memB.add(new Member("B", addrB, MemberState.ALIVE, 0));

        FailureDetectorTest.RecordingSink sinkA = new FailureDetectorTest.RecordingSink();
        FailureDetectorTest.RecordingSink sinkB = new FailureDetectorTest.RecordingSink();
        SwimNode nodeA = new SwimNode("A", tA, codec, memA, config, sinkA);
        SwimNode nodeB = new SwimNode("B", tB, codec, memB, config, sinkB);

        try {
            nodeA.start();
            nodeB.start();

            nodeA.applyUpdate(new Update("B", addrB, MemberState.SUSPECT, 0));
            assertTrue(sinkA.has(EventType.SUSPECT, "B"), "A should record SUSPECT after injection");

            assertTrue(waitFor(() -> sinkB.events.stream().anyMatch(e ->
                    e.type() == EventType.ALIVE && "B".equals(e.target())
                            && Boolean.TRUE.equals(e.meta().get("refute"))), 2000),
                    "B should self-refute with bumped incarnation");
            assertTrue(nodeB.selfIncarnation() > 0, "B's incarnation should be bumped");

            assertTrue(waitFor(() -> {
                Member b = memA.get("B").orElse(null);
                return b != null && b.state() == MemberState.ALIVE && b.incarnation() > 0;
            }, 2000), "A should learn B's refuted ALIVE state");
        } finally {
            nodeA.close();
            nodeB.close();
        }
    }
}
