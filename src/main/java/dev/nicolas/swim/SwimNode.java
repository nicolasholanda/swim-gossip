package dev.nicolas.swim;

import dev.nicolas.swim.event.Event;
import dev.nicolas.swim.event.EventSink;
import dev.nicolas.swim.event.EventType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SwimNode implements AutoCloseable {

    private final String id;
    private final Transport transport;
    private final Codec codec;
    private final MembershipList membership;
    private final SwimConfig config;
    private final EventSink eventSink;
    private final FailureDetector failureDetector;

    private volatile int selfIncarnation;
    private final List<Update> pendingUpdates = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, ScheduledFuture<?>> suspectTimers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService suspectScheduler;

    public SwimNode(String id, Transport transport, Codec codec, MembershipList membership,
                    SwimConfig config, EventSink eventSink) {
        this.id = id;
        this.transport = transport;
        this.codec = codec;
        this.membership = membership;
        this.config = config;
        this.eventSink = eventSink;
        this.selfIncarnation = membership.get(id).map(Member::incarnation).orElse(0);
        this.suspectScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "swim-suspect-" + id);
            t.setDaemon(true);
            return t;
        });

        EventSink internalSink = e -> {
            observeEvent(e);
            eventSink.accept(e);
        };

        this.failureDetector = new FailureDetector(id, membership, config, internalSink, this::sendMessage);
        transport.onReceive(this::onReceive);
    }

    public void start() {
        failureDetector.start();
    }

    public String id() {
        return id;
    }

    public MembershipList membership() {
        return membership;
    }

    public Transport transport() {
        return transport;
    }

    public EventSink eventSink() {
        return eventSink;
    }

    public FailureDetector failureDetector() {
        return failureDetector;
    }

    public int selfIncarnation() {
        return selfIncarnation;
    }

    public void applyUpdate(Update u) {
        applyUpdates(List.of(u));
    }

    private void observeEvent(Event e) {
        if (e.type() == EventType.TIMEOUT && "final".equals(e.meta().get("phase"))) {
            membership.get(e.target()).ifPresent(this::markSuspect);
        }
    }

    private void markSuspect(Member target) {
        Member suspected = target.withState(MemberState.SUSPECT);
        if (membership.merge(suspected)) {
            Update u = new Update(target.id(), target.address(), MemberState.SUSPECT, target.incarnation());
            pendingUpdates.add(u);
            eventSink.accept(Event.of(EventType.SUSPECT, id, target.id(),
                    Map.of("incarnation", target.incarnation())));
            scheduleSuspectTimer(target.id(), target.incarnation());
        }
    }

    private void scheduleSuspectTimer(String memberId, int suspectedIncarnation) {
        ScheduledFuture<?> prev = suspectTimers.put(memberId,
                suspectScheduler.schedule(
                        () -> onSuspectTimeout(memberId, suspectedIncarnation),
                        config.suspectTimeout().toMillis(), TimeUnit.MILLISECONDS));
        if (prev != null) prev.cancel(false);
    }

    private void cancelSuspectTimer(String memberId) {
        ScheduledFuture<?> t = suspectTimers.remove(memberId);
        if (t != null) t.cancel(false);
    }

    private void onSuspectTimeout(String memberId, int suspectedIncarnation) {
        suspectTimers.remove(memberId);
        Member current = membership.get(memberId).orElse(null);
        if (current == null) return;
        if (current.state() == MemberState.SUSPECT && current.incarnation() <= suspectedIncarnation) {
            Member dead = current.withState(MemberState.DEAD);
            if (membership.merge(dead)) {
                Update u = new Update(memberId, current.address(), MemberState.DEAD, current.incarnation());
                pendingUpdates.add(u);
                eventSink.accept(Event.of(EventType.DEAD, id, memberId,
                        Map.of("incarnation", current.incarnation())));
            }
        }
    }

    private void applyUpdates(List<Update> updates) {
        for (Update u : updates) {
            if (u.id().equals(id)) {
                handleSelfUpdate(u);
            } else {
                handlePeerUpdate(u);
            }
        }
    }

    private void handleSelfUpdate(Update u) {
        if ((u.state() == MemberState.SUSPECT || u.state() == MemberState.DEAD)
                && u.incarnation() >= selfIncarnation) {
            int newInc = u.incarnation() + 1;
            selfIncarnation = newInc;
            InetSocketAddress addr = membership.get(id).map(Member::address).orElseGet(transport::localAddress);
            Member refuted = new Member(id, addr, MemberState.ALIVE, newInc);
            membership.merge(refuted);
            Update refute = new Update(id, addr, MemberState.ALIVE, newInc);
            pendingUpdates.add(refute);
            eventSink.accept(Event.of(EventType.ALIVE, id, id,
                    Map.of("incarnation", newInc, "refute", true)));
        }
    }

    private void handlePeerUpdate(Update u) {
        Member incoming = new Member(u.id(), u.address(), u.state(), u.incarnation());
        if (membership.merge(incoming)) {
            pendingUpdates.add(u);
            switch (u.state()) {
                case ALIVE -> {
                    eventSink.accept(Event.of(EventType.ALIVE, id, u.id(),
                            Map.of("incarnation", u.incarnation())));
                    cancelSuspectTimer(u.id());
                }
                case SUSPECT -> {
                    eventSink.accept(Event.of(EventType.SUSPECT, id, u.id(),
                            Map.of("incarnation", u.incarnation())));
                    scheduleSuspectTimer(u.id(), u.incarnation());
                }
                case DEAD -> {
                    eventSink.accept(Event.of(EventType.DEAD, id, u.id(),
                            Map.of("incarnation", u.incarnation())));
                    cancelSuspectTimer(u.id());
                }
            }
        }
    }

    private void onReceive(InetSocketAddress from, byte[] bytes) {
        try {
            Message msg = codec.decode(bytes);
            applyUpdates(piggybackOf(msg));
            dispatch(from, msg);
        } catch (IOException e) {
            System.err.println("[" + id + "] decode failed: " + e.getMessage());
        }
    }

    private void dispatch(InetSocketAddress from, Message msg) {
        switch (msg) {
            case Message.Ping p -> failureDetector.onPing(from, p);
            case Message.Ack a -> failureDetector.onAck(from, a);
            case Message.PingReq pr -> failureDetector.onPingReq(from, pr);
        }
    }

    private List<Update> piggybackOf(Message m) {
        return switch (m) {
            case Message.Ping p -> p.piggyback();
            case Message.Ack a -> a.piggyback();
            case Message.PingReq pr -> pr.piggyback();
        };
    }

    private void sendMessage(InetSocketAddress dest, Message msg) {
        Message withPiggyback = attachPiggyback(msg);
        try {
            transport.send(dest, codec.encode(withPiggyback));
        } catch (IOException e) {
            System.err.println("[" + id + "] encode failed: " + e.getMessage());
        }
    }

    private Message attachPiggyback(Message msg) {
        if (pendingUpdates.isEmpty()) return msg;
        List<Update> drained;
        synchronized (pendingUpdates) {
            if (pendingUpdates.isEmpty()) return msg;
            drained = new ArrayList<>(pendingUpdates);
            pendingUpdates.clear();
        }
        List<Update> combined = new ArrayList<>(piggybackOf(msg));
        combined.addAll(drained);
        return switch (msg) {
            case Message.Ping p -> new Message.Ping(p.seqNo(), combined);
            case Message.Ack a -> new Message.Ack(a.seqNo(), combined);
            case Message.PingReq pr -> new Message.PingReq(pr.seqNo(), pr.targetId(), pr.targetAddress(), combined);
        };
    }

    @Override
    public void close() {
        failureDetector.stop();
        suspectScheduler.shutdownNow();
        transport.close();
    }
}
