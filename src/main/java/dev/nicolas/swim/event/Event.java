package dev.nicolas.swim.event;

import java.util.Map;

public record Event(
        long timestampMillis,
        EventType type,
        String source,
        String target,
        Map<String, Object> meta) {

    public static Event of(EventType type, String source, String target) {
        return new Event(System.currentTimeMillis(), type, source, target, Map.of());
    }

    public static Event of(EventType type, String source, String target, Map<String, Object> meta) {
        return new Event(System.currentTimeMillis(), type, source, target, meta);
    }
}
