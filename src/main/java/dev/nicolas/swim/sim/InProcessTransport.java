package dev.nicolas.swim.sim;

import dev.nicolas.swim.Transport;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class InProcessTransport implements Transport {

    private static final Map<InetSocketAddress, InProcessTransport> NETWORK = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService SCHED = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "inproc-net");
        t.setDaemon(true);
        return t;
    });

    private final InetSocketAddress local;
    private final Random random;
    private final long minLatencyMs;
    private final long maxLatencyMs;
    private final double dropRate;
    private final Set<InetSocketAddress> blockedPeers = ConcurrentHashMap.newKeySet();
    private final AtomicReference<BiConsumer<InetSocketAddress, byte[]>> handler = new AtomicReference<>();
    private volatile boolean partitioned = false;
    private volatile boolean closed = false;

    public InProcessTransport(InetSocketAddress local, long seed,
                              long minLatencyMs, long maxLatencyMs, double dropRate) {
        this.local = local;
        this.random = new Random(seed);
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
        this.dropRate = dropRate;
        NETWORK.put(local, this);
    }

    public InProcessTransport(InetSocketAddress local, long seed) {
        this(local, seed, 0L, 0L, 0.0);
    }

    public void setPartitioned(boolean p) {
        this.partitioned = p;
    }

    public void blockPeer(InetSocketAddress peer) {
        blockedPeers.add(peer);
    }

    public void unblockPeer(InetSocketAddress peer) {
        blockedPeers.remove(peer);
    }

    @Override
    public void send(InetSocketAddress dest, byte[] payload) {
        if (closed || partitioned || blockedPeers.contains(dest)) return;
        if (dropRate > 0 && random.nextDouble() < dropRate) return;
        InProcessTransport target = NETWORK.get(dest);
        if (target == null || target.closed || target.partitioned || target.blockedPeers.contains(local)) return;

        byte[] copy = payload.clone();
        long delay = minLatencyMs;
        if (maxLatencyMs > minLatencyMs) {
            delay += (long) (random.nextDouble() * (maxLatencyMs - minLatencyMs));
        }
        if (delay <= 0) {
            target.deliver(local, copy);
        } else {
            SCHED.schedule(() -> target.deliver(local, copy), delay, TimeUnit.MILLISECONDS);
        }
    }

    private void deliver(InetSocketAddress from, byte[] payload) {
        if (closed) return;
        BiConsumer<InetSocketAddress, byte[]> h = handler.get();
        if (h != null) h.accept(from, payload);
    }

    @Override
    public void onReceive(BiConsumer<InetSocketAddress, byte[]> h) {
        handler.set(h);
    }

    @Override
    public InetSocketAddress localAddress() {
        return local;
    }

    @Override
    public void close() {
        closed = true;
        NETWORK.remove(local);
    }
}
