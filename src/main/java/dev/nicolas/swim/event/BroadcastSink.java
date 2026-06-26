package dev.nicolas.swim.event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class BroadcastSink implements EventSink {

    private final List<Consumer<Event>> subscribers = new CopyOnWriteArrayList<>();

    public void subscribe(Consumer<Event> subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe(Consumer<Event> subscriber) {
        subscribers.remove(subscriber);
    }

    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public void accept(Event event) {
        for (Consumer<Event> sub : subscribers) {
            try {
                sub.accept(event);
            } catch (Exception ignored) {
            }
        }
    }
}
