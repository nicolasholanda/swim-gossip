package dev.nicolas.swim.sim;

import dev.nicolas.swim.Codec;
import dev.nicolas.swim.Member;
import dev.nicolas.swim.MemberState;
import dev.nicolas.swim.MembershipList;
import dev.nicolas.swim.SwimConfig;
import dev.nicolas.swim.SwimNode;
import dev.nicolas.swim.event.EventSink;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

public class Cluster implements AutoCloseable {

    private static final AtomicInteger PORT_BASE = new AtomicInteger(30000);

    private final List<SwimNode> nodes;
    private final List<InProcessTransport> transports;
    private final List<MembershipList> memberships;
    private final List<InetSocketAddress> addresses;

    private Cluster(List<SwimNode> nodes, List<InProcessTransport> transports,
                    List<MembershipList> memberships, List<InetSocketAddress> addresses) {
        this.nodes = nodes;
        this.transports = transports;
        this.memberships = memberships;
        this.addresses = addresses;
    }

    public static Cluster create(int n, SwimConfig config) {
        return create(n, config, idx -> EventSink.NOOP);
    }

    public static Cluster create(int n, SwimConfig config, IntFunction<EventSink> sinkFactory) {
        int base = PORT_BASE.getAndAdd(n);
        Codec codec = new Codec(config.maxMessageBytes());

        List<InetSocketAddress> addresses = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            addresses.add(new InetSocketAddress("127.0.0.1", base + i));
        }

        List<MembershipList> memberships = new ArrayList<>(n);
        List<InProcessTransport> transports = new ArrayList<>(n);
        List<SwimNode> nodes = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            MembershipList ml = new MembershipList(i + 1L);
            for (int j = 0; j < n; j++) {
                ml.add(new Member(nodeId(j), addresses.get(j), MemberState.ALIVE, 0));
            }
            memberships.add(ml);
            InProcessTransport t = new InProcessTransport(addresses.get(i), i + 100L);
            transports.add(t);
            nodes.add(new SwimNode(nodeId(i), t, codec, ml, config, sinkFactory.apply(i)));
        }

        return new Cluster(nodes, transports, memberships, addresses);
    }

    public static String nodeId(int idx) {
        return "N" + idx;
    }

    public void startAll() {
        nodes.forEach(SwimNode::start);
    }

    public SwimNode node(int idx) {
        return nodes.get(idx);
    }

    public InProcessTransport transport(int idx) {
        return transports.get(idx);
    }

    public MembershipList membership(int idx) {
        return memberships.get(idx);
    }

    public InetSocketAddress address(int idx) {
        return addresses.get(idx);
    }

    public int size() {
        return nodes.size();
    }

    public List<SwimNode> nodes() {
        return List.copyOf(nodes);
    }

    @Override
    public void close() {
        for (SwimNode node : nodes) {
            try { node.close(); } catch (Exception ignored) {}
        }
    }
}
