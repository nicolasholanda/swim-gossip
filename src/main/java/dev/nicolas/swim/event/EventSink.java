package dev.nicolas.swim.event;

public interface EventSink {

    void accept(Event event);

    EventSink NOOP = event -> {};
}
