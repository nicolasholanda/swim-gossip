package dev.nicolas.swim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Disseminator {

    private static final int ESTIMATED_UPDATE_SIZE_BYTES = 100;

    private static final class Entry {
        final Update update;
        int sendCount;

        Entry(Update update) {
            this.update = update;
        }
    }

    private final SwimConfig config;
    private final MembershipList membership;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public Disseminator(SwimConfig config, MembershipList membership) {
        this.config = config;
        this.membership = membership;
    }

    public synchronized void add(Update u) {
        Entry existing = entries.get(u.id());
        if (existing == null || shouldReplace(u, existing.update)) {
            entries.put(u.id(), new Entry(u));
        }
    }

    private static boolean shouldReplace(Update incoming, Update existing) {
        if (incoming.incarnation() != existing.incarnation()) {
            return incoming.incarnation() > existing.incarnation();
        }
        return priority(incoming.state()) > priority(existing.state());
    }

    private static int priority(MemberState s) {
        return switch (s) {
            case DEAD -> 3;
            case SUSPECT -> 2;
            case ALIVE -> 1;
        };
    }

    public synchronized List<Update> select(int byteBudget) {
        List<Entry> sorted = new ArrayList<>(entries.values());
        sorted.sort(Comparator.comparingInt(e -> e.sendCount));

        List<Update> selected = new ArrayList<>();
        int bytesUsed = 0;
        for (Entry e : sorted) {
            if (bytesUsed + ESTIMATED_UPDATE_SIZE_BYTES > byteBudget) break;
            selected.add(e.update);
            bytesUsed += ESTIMATED_UPDATE_SIZE_BYTES;
            e.sendCount++;
        }

        evict();
        return selected;
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized int sendCountOf(String memberId) {
        Entry e = entries.get(memberId);
        return e == null ? 0 : e.sendCount;
    }

    public int evictionThreshold() {
        int n = Math.max(2, membership.size());
        return (int) (config.disseminationLambda() * Math.ceil(Math.log(n) / Math.log(2)));
    }

    private void evict() {
        int threshold = evictionThreshold();
        entries.values().removeIf(e -> e.sendCount > threshold);
    }
}
