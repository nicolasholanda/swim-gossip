package dev.nicolas.swim;

import dev.nicolas.swim.event.EventSink;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SwimNode implements AutoCloseable {

    private final String id;
    private final Transport transport;
    private final Codec codec;
    private final MembershipList membership;
    private final SwimConfig config;
    private final EventSink eventSink;
    private final FailureDetector failureDetector;

    public SwimNode(String id, Transport transport, Codec codec, MembershipList membership,
                    SwimConfig config, EventSink eventSink) {
        this.id = id;
        this.transport = transport;
        this.codec = codec;
        this.membership = membership;
        this.config = config;
        this.eventSink = eventSink;
        this.failureDetector = new FailureDetector(id, membership, config, eventSink, this::sendMessage);

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

    private void onReceive(InetSocketAddress from, byte[] bytes) {
        try {
            Message msg = codec.decode(bytes);
            dispatch(from, msg);
        } catch (IOException e) {
            System.err.println("[" + id + "] decode failed: " + e.getMessage());
        }
    }

    private void dispatch(InetSocketAddress from, Message msg) {
        switch (msg) {
            case Message.Ping p -> failureDetector.onPing(from, p);
            case Message.Ack a -> failureDetector.onAck(from, a);
            case Message.PingReq pr -> { /* implemented in indirect probing */ }
        }
    }

    private void sendMessage(InetSocketAddress dest, Message msg) {
        try {
            transport.send(dest, codec.encode(msg));
        } catch (IOException e) {
            System.err.println("[" + id + "] encode failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        failureDetector.stop();
        transport.close();
    }
}
