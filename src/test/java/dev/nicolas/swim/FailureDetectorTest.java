package dev.nicolas.swim;

import dev.nicolas.swim.event.Event;
import dev.nicolas.swim.event.EventSink;
import dev.nicolas.swim.event.EventType;
import dev.nicolas.swim.sim.InProcessTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FailureDetectorTest {

    static class RecordingSink implements EventSink {
        final List<Event> events = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void accept(Event event) {
            events.add(event);
        }

        boolean has(EventType type) {
            synchronized (events) {
                return events.stream().anyMatch(e -> e.type() == type);
            }
        }

        boolean has(EventType type, String target) {
            synchronized (events) {
                return events.stream().anyMatch(e -> e.type() == type && target.equals(e.target()));
            }
        }
    }

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    @Test
    void steadyPingAndAck() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 19001);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 19002);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(100))
                .withDirectAckTimeout(Duration.ofMillis(50));

        InProcessTransport transportA = new InProcessTransport(addrA, 1L);
        InProcessTransport transportB = new InProcessTransport(addrB, 2L);
        Codec codec = new Codec(config.maxMessageBytes());

        MembershipList memA = new MembershipList(1L);
        memA.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memA.add(new Member("B", addrB, MemberState.ALIVE, 0));

        MembershipList memB = new MembershipList(2L);
        memB.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memB.add(new Member("B", addrB, MemberState.ALIVE, 0));

        RecordingSink sinkA = new RecordingSink();
        SwimNode nodeA = new SwimNode("A", transportA, codec, memA, config, sinkA);
        SwimNode nodeB = new SwimNode("B", transportB, codec, memB, config, EventSink.NOOP);

        try {
            nodeA.start();
            nodeB.start();

            assertTrue(waitFor(() -> sinkA.has(EventType.PING, "B"), 1000), "expected PING to B");
            assertTrue(waitFor(() -> sinkA.has(EventType.ACK), 1000), "expected ACK");
            assertFalse(sinkA.has(EventType.TIMEOUT), "should not have timed out");
        } finally {
            nodeA.close();
            nodeB.close();
        }
    }

    @Test
    void timeoutWhenPeerNotResponding() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 19011);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 19012);

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(100))
                .withDirectAckTimeout(Duration.ofMillis(50));

        InProcessTransport transportA = new InProcessTransport(addrA, 1L);
        Codec codec = new Codec(config.maxMessageBytes());

        MembershipList memA = new MembershipList(1L);
        memA.add(new Member("A", addrA, MemberState.ALIVE, 0));
        memA.add(new Member("B", addrB, MemberState.ALIVE, 0));

        RecordingSink sinkA = new RecordingSink();
        SwimNode nodeA = new SwimNode("A", transportA, codec, memA, config, sinkA);

        try {
            nodeA.start();

            assertTrue(waitFor(() -> sinkA.has(EventType.PING, "B"), 1000), "expected PING to B");
            assertTrue(waitFor(() -> sinkA.has(EventType.TIMEOUT, "B"), 1000), "expected TIMEOUT for B");
            assertFalse(sinkA.has(EventType.ACK), "should not have received ACK");
        } finally {
            nodeA.close();
        }
    }
}
