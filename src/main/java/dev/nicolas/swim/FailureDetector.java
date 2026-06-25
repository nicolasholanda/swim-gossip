package dev.nicolas.swim;

import dev.nicolas.swim.event.Event;
import dev.nicolas.swim.event.EventSink;
import dev.nicolas.swim.event.EventType;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FailureDetector {

    @FunctionalInterface
    public interface MessageSender {
        void send(InetSocketAddress dest, Message msg);
    }

    private record PendingPing(long seqNo, Member target) {}

    private record RelayContext(long originalSeqNo, InetSocketAddress originalRequester, String targetId) {}

    private final String selfId;
    private final MembershipList membership;
    private final SwimConfig config;
    private final EventSink eventSink;
    private final MessageSender sender;
    private final Map<Long, PendingPing> pending = new ConcurrentHashMap<>();
    private final Map<Long, RelayContext> relayingFor = new ConcurrentHashMap<>();
    private final AtomicLong seqGen = new AtomicLong();
    private final ScheduledExecutorService scheduler;

    public FailureDetector(String selfId, MembershipList membership, SwimConfig config,
                           EventSink eventSink, MessageSender sender) {
        this.selfId = selfId;
        this.membership = membership;
        this.config = config;
        this.eventSink = eventSink;
        this.sender = sender;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "fd-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        long periodMs = config.protocolPeriod().toMillis();
        scheduler.scheduleAtFixedRate(this::tick, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public void tick() {
        Optional<Member> target = membership.nextPingTarget(selfId);
        target.ifPresent(this::probe);
    }

    private void probe(Member target) {
        long seqNo = seqGen.incrementAndGet();
        pending.put(seqNo, new PendingPing(seqNo, target));

        sender.send(target.address(), new Message.Ping(seqNo, List.of()));
        eventSink.accept(Event.of(EventType.PING, selfId, target.id(), Map.of("seq", seqNo)));

        scheduler.schedule(() -> onDirectTimeout(seqNo),
                config.directAckTimeout().toMillis(), TimeUnit.MILLISECONDS);
        scheduler.schedule(() -> onFinalTimeout(seqNo),
                config.protocolPeriod().toMillis(), TimeUnit.MILLISECONDS);
    }

    protected void onDirectTimeout(long seqNo) {
        PendingPing pp = pending.get(seqNo);
        if (pp == null) return;

        eventSink.accept(Event.of(EventType.TIMEOUT, selfId, pp.target.id(),
                Map.of("seq", seqNo, "phase", "direct")));

        int k = config.indirectFanout();
        List<Member> relays = membership.kRandomOthers(selfId, k + 1).stream()
                .filter(m -> !m.id().equals(pp.target.id()))
                .limit(k)
                .toList();

        for (Member relay : relays) {
            sender.send(relay.address(),
                    new Message.PingReq(seqNo, pp.target.id(), pp.target.address(), List.of()));
            eventSink.accept(Event.of(EventType.PING_REQ, selfId, relay.id(),
                    Map.of("seq", seqNo, "via", pp.target.id())));
        }
    }

    protected void onFinalTimeout(long seqNo) {
        PendingPing pp = pending.remove(seqNo);
        if (pp != null) {
            eventSink.accept(Event.of(EventType.TIMEOUT, selfId, pp.target.id(),
                    Map.of("seq", seqNo, "phase", "final")));
        }
    }

    public void onPing(InetSocketAddress from, Message.Ping ping) {
        sender.send(from, new Message.Ack(ping.seqNo(), List.of()));
    }

    public void onAck(InetSocketAddress from, Message.Ack ack) {
        PendingPing pp = pending.remove(ack.seqNo());
        if (pp != null) {
            boolean indirect = !pp.target.address().equals(from);
            if (indirect) {
                Optional<Member> via = membership.getAll().stream()
                        .filter(m -> m.address().equals(from))
                        .findFirst();
                eventSink.accept(Event.of(EventType.INDIRECT_ACK, pp.target.id(), selfId,
                        Map.of("seq", ack.seqNo(), "via", via.map(Member::id).orElse(from.toString()))));
            } else {
                eventSink.accept(Event.of(EventType.ACK, pp.target.id(), selfId,
                        Map.of("seq", ack.seqNo())));
            }
            return;
        }

        RelayContext relay = relayingFor.remove(ack.seqNo());
        if (relay != null) {
            sender.send(relay.originalRequester(),
                    new Message.Ack(relay.originalSeqNo(), List.of()));
        }
    }

    public void onPingReq(InetSocketAddress from, Message.PingReq req) {
        long relaySeq = seqGen.incrementAndGet();
        relayingFor.put(relaySeq, new RelayContext(req.seqNo(), from, req.targetId()));
        sender.send(req.targetAddress(), new Message.Ping(relaySeq, List.of()));

        scheduler.schedule(() -> relayingFor.remove(relaySeq),
                config.protocolPeriod().toMillis(), TimeUnit.MILLISECONDS);
    }

    protected ScheduledExecutorService scheduler() {
        return scheduler;
    }

    protected EventSink eventSink() {
        return eventSink;
    }

    protected String selfId() {
        return selfId;
    }

    protected MembershipList membership() {
        return membership;
    }

    protected SwimConfig config() {
        return config;
    }

    protected MessageSender sender() {
        return sender;
    }
}
