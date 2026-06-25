package dev.nicolas.swim;

import dev.nicolas.swim.event.EventSink;
import dev.nicolas.swim.sim.InProcessTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DisseminationTest {

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    @Test
    void selectPrefersLowestSendCountFirst() {
        SwimConfig config = SwimConfig.defaults().withDisseminationLambda(3);
        MembershipList ml = new MembershipList(0L);
        for (int i = 0; i < 8; i++) {
            ml.add(new Member("n" + i, new InetSocketAddress("127.0.0.1", 7000 + i),
                    MemberState.ALIVE, 0));
        }
        Disseminator d = new Disseminator(config, ml);

        Update u1 = new Update("x1", new InetSocketAddress("127.0.0.1", 8001), MemberState.ALIVE, 1);
        Update u2 = new Update("x2", new InetSocketAddress("127.0.0.1", 8002), MemberState.ALIVE, 1);

        d.add(u1);
        d.select(10_000);
        d.select(10_000);

        d.add(u2);
        List<Update> picked = d.select(10_000);

        assertEquals(u2.id(), picked.get(0).id(), "lowest-count update must be first");
    }

    @Test
    void evictsAfterLambdaTimesCeilLog2N() {
        SwimConfig config = SwimConfig.defaults().withDisseminationLambda(2);
        MembershipList ml = new MembershipList(0L);
        for (int i = 0; i < 8; i++) {
            ml.add(new Member("n" + i, new InetSocketAddress("127.0.0.1", 7000 + i),
                    MemberState.ALIVE, 0));
        }
        Disseminator d = new Disseminator(config, ml);

        int threshold = d.evictionThreshold();
        assertEquals(2 * (int) Math.ceil(Math.log(8) / Math.log(2)), threshold);

        Update u = new Update("x", new InetSocketAddress("127.0.0.1", 8001), MemberState.ALIVE, 1);
        d.add(u);

        int seen = 0;
        for (int i = 0; i < 100; i++) {
            List<Update> selected = d.select(10_000);
            if (selected.stream().anyMatch(up -> up.id().equals("x"))) seen++;
            if (d.size() == 0) break;
        }
        assertTrue(seen <= threshold, "selected " + seen + " times, threshold=" + threshold);
        assertEquals(0, d.size(), "update should be evicted after threshold");
    }

    @Test
    void replacesEntryOnHigherIncarnation() {
        SwimConfig config = SwimConfig.defaults();
        MembershipList ml = new MembershipList(0L);
        ml.add(new Member("n1", new InetSocketAddress("127.0.0.1", 7001), MemberState.ALIVE, 0));
        ml.add(new Member("n2", new InetSocketAddress("127.0.0.1", 7002), MemberState.ALIVE, 0));
        Disseminator d = new Disseminator(config, ml);

        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 8001);
        d.add(new Update("x", addr, MemberState.SUSPECT, 1));
        d.add(new Update("x", addr, MemberState.ALIVE, 2));

        assertEquals(1, d.size());
        Update picked = d.select(10_000).get(0);
        assertEquals(2, picked.incarnation());
        assertEquals(MemberState.ALIVE, picked.state());
    }

    @Test
    void byteBudgetCapsSelection() {
        SwimConfig config = SwimConfig.defaults();
        MembershipList ml = new MembershipList(0L);
        ml.add(new Member("n1", new InetSocketAddress("127.0.0.1", 7001), MemberState.ALIVE, 0));
        Disseminator d = new Disseminator(config, ml);

        for (int i = 0; i < 10; i++) {
            d.add(new Update("x" + i, new InetSocketAddress("127.0.0.1", 8000 + i),
                    MemberState.ALIVE, 1));
        }

        List<Update> selected = d.select(250);
        assertTrue(selected.size() <= 2,
                "at most 2 updates fit in 250 bytes given 100-byte estimate, got " + selected.size());
    }

    @Test
    void updateReachesAllNodesInCluster() throws Exception {
        int n = 4;
        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(60))
                .withDirectAckTimeout(Duration.ofMillis(25));
        Codec codec = new Codec(config.maxMessageBytes());

        List<InetSocketAddress> addrs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            addrs.add(new InetSocketAddress("127.0.0.1", 22000 + i));
        }

        List<SwimNode> nodes = new ArrayList<>();
        List<MembershipList> mems = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MembershipList ml = new MembershipList(i + 1);
            for (int j = 0; j < n; j++) {
                ml.add(new Member("N" + j, addrs.get(j), MemberState.ALIVE, 0));
            }
            mems.add(ml);
            InProcessTransport t = new InProcessTransport(addrs.get(i), i + 100);
            nodes.add(new SwimNode("N" + i, t, codec, ml, config, EventSink.NOOP));
        }

        try {
            nodes.forEach(SwimNode::start);

            InetSocketAddress newcomerAddr = new InetSocketAddress("127.0.0.1", 22999);
            Update gossip = new Update("X", newcomerAddr, MemberState.ALIVE, 5);
            nodes.get(0).applyUpdate(gossip);

            for (int i = 0; i < n; i++) {
                final int idx = i;
                assertTrue(waitFor(() -> mems.get(idx).get("X")
                                .map(m -> m.incarnation() == 5 && m.state() == MemberState.ALIVE)
                                .orElse(false), 3000),
                        "node N" + idx + " did not learn gossip about X");
            }
        } finally {
            nodes.forEach(SwimNode::close);
        }
    }
}
