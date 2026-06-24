package dev.nicolas.swim;

import dev.nicolas.swim.sim.InProcessTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransportTest {

    @Test
    void inProcessTransportRoundTrip() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 18001);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 18002);
        InProcessTransport a = new InProcessTransport(addrA, 1L);
        InProcessTransport b = new InProcessTransport(addrB, 2L);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<byte[]> received = new AtomicReference<>();
            AtomicReference<InetSocketAddress> sender = new AtomicReference<>();
            b.onReceive((from, data) -> {
                sender.set(from);
                received.set(data);
                latch.countDown();
            });

            a.send(addrB, new byte[]{1, 2, 3});
            assertTrue(latch.await(1, TimeUnit.SECONDS), "message not delivered");
            assertArrayEquals(new byte[]{1, 2, 3}, received.get());
            assertEquals(addrA, sender.get());
        } finally {
            a.close();
            b.close();
        }
    }

    @Test
    void inProcessTransportDropsAllOnPartition() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 18011);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 18012);
        InProcessTransport a = new InProcessTransport(addrA, 1L);
        InProcessTransport b = new InProcessTransport(addrB, 2L);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            b.onReceive((from, data) -> latch.countDown());
            b.setPartitioned(true);

            a.send(addrB, new byte[]{1});
            assertFalse(latch.await(200, TimeUnit.MILLISECONDS), "message should be dropped");
        } finally {
            a.close();
            b.close();
        }
    }

    @Test
    void inProcessTransportBlocksSpecificPeer() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 18021);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 18022);
        InetSocketAddress addrC = new InetSocketAddress("127.0.0.1", 18023);
        InProcessTransport a = new InProcessTransport(addrA, 1L);
        InProcessTransport b = new InProcessTransport(addrB, 2L);
        InProcessTransport c = new InProcessTransport(addrC, 3L);

        try {
            CountDownLatch latchB = new CountDownLatch(1);
            CountDownLatch latchC = new CountDownLatch(1);
            b.onReceive((from, data) -> latchB.countDown());
            c.onReceive((from, data) -> latchC.countDown());

            a.blockPeer(addrB);
            a.send(addrB, new byte[]{1});
            a.send(addrC, new byte[]{2});

            assertFalse(latchB.await(200, TimeUnit.MILLISECONDS), "blocked peer received");
            assertTrue(latchC.await(500, TimeUnit.MILLISECONDS), "unblocked peer missed");
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    void udpTransportRoundTrip() throws Exception {
        InetSocketAddress addrA = new InetSocketAddress("127.0.0.1", 0);
        InetSocketAddress addrB = new InetSocketAddress("127.0.0.1", 0);
        UdpTransport a = new UdpTransport(addrA, 1400);
        UdpTransport b = new UdpTransport(addrB, 1400);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<byte[]> received = new AtomicReference<>();
            b.onReceive((from, data) -> {
                received.set(data);
                latch.countDown();
            });

            a.send(b.localAddress(), new byte[]{9, 8, 7});
            assertTrue(latch.await(2, TimeUnit.SECONDS), "UDP packet not delivered");
            assertArrayEquals(new byte[]{9, 8, 7}, received.get());
        } finally {
            a.close();
            b.close();
        }
    }
}
