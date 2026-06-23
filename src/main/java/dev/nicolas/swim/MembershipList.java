package dev.nicolas.swim;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MembershipList {

    private final Map<String, Member> members = new ConcurrentHashMap<>();
    private final Random random;
    private final Deque<String> roundRobinQueue = new ArrayDeque<>();

    public MembershipList(long seed) {
        this.random = new Random(seed);
    }

    public void add(Member member) {
        members.put(member.id(), member);
    }

    public boolean merge(Member update) {
        boolean[] changed = {false};
        members.compute(update.id(), (id, current) -> {
            if (current == null || overrides(update, current)) {
                changed[0] = true;
                return update;
            }
            return current;
        });
        return changed[0];
    }

    private boolean overrides(Member incoming, Member current) {
        return switch (incoming.state()) {
            case DEAD -> true;
            case SUSPECT -> incoming.incarnation() > current.incarnation()
                    || (incoming.incarnation() == current.incarnation() && current.state() == MemberState.ALIVE);
            case ALIVE -> incoming.incarnation() > current.incarnation();
        };
    }

    public Optional<Member> get(String id) {
        return Optional.ofNullable(members.get(id));
    }

    public List<Member> getAll() {
        return List.copyOf(members.values());
    }

    public Optional<Member> nextPingTarget(String selfId) {
        while (true) {
            while (!roundRobinQueue.isEmpty()) {
                String id = roundRobinQueue.poll();
                Member m = members.get(id);
                if (m != null && !id.equals(selfId) && m.state() != MemberState.DEAD) {
                    return Optional.of(m);
                }
            }
            List<String> candidates = members.keySet().stream()
                    .filter(id -> !id.equals(selfId))
                    .filter(id -> members.get(id).state() != MemberState.DEAD)
                    .collect(Collectors.toCollection(ArrayList::new));
            if (candidates.isEmpty()) return Optional.empty();
            Collections.shuffle(candidates, random);
            roundRobinQueue.addAll(candidates);
        }
    }

    public List<Member> kRandomOthers(String selfId, int k) {
        List<Member> candidates = members.values().stream()
                .filter(m -> !m.id().equals(selfId))
                .filter(m -> m.state() != MemberState.DEAD)
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(candidates, random);
        return Collections.unmodifiableList(candidates.subList(0, Math.min(k, candidates.size())));
    }

    public int size() {
        return members.size();
    }
}
