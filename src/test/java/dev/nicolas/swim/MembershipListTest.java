package dev.nicolas.swim;

import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MembershipListTest {

    static final InetSocketAddress ADDR_A = new InetSocketAddress("127.0.0.1", 7001);
    static final InetSocketAddress ADDR_B = new InetSocketAddress("127.0.0.1", 7002);
    static final InetSocketAddress ADDR_C = new InetSocketAddress("127.0.0.1", 7003);

    private MembershipList list() {
        return new MembershipList(42L);
    }

    @Test
    void newMemberIsAddedOnMerge() {
        MembershipList ml = list();
        Member m = new Member("a", ADDR_A, MemberState.ALIVE, 0);
        assertTrue(ml.merge(m));
        assertEquals(m, ml.get("a").orElseThrow());
    }

    @Test
    void suspectOverridesAliveAtSameIncarnation() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 1));
        assertTrue(ml.merge(new Member("a", ADDR_A, MemberState.SUSPECT, 1)));
        assertEquals(MemberState.SUSPECT, ml.get("a").orElseThrow().state());
    }

    @Test
    void aliveDoesNotOverrideSuspectAtSameIncarnation() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.SUSPECT, 1));
        assertFalse(ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 1)));
        assertEquals(MemberState.SUSPECT, ml.get("a").orElseThrow().state());
    }

    @Test
    void aliveOverridesSuspectWithHigherIncarnation() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.SUSPECT, 1));
        assertTrue(ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 2)));
        assertEquals(MemberState.ALIVE, ml.get("a").orElseThrow().state());
    }

    @Test
    void deadOverridesAlive() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 5));
        assertTrue(ml.merge(new Member("a", ADDR_A, MemberState.DEAD, 0)));
        assertEquals(MemberState.DEAD, ml.get("a").orElseThrow().state());
    }

    @Test
    void deadOverridesSuspect() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.SUSPECT, 3));
        assertTrue(ml.merge(new Member("a", ADDR_A, MemberState.DEAD, 0)));
        assertEquals(MemberState.DEAD, ml.get("a").orElseThrow().state());
    }

    @Test
    void lowerIncarnationIgnored() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 5));
        assertFalse(ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 3)));
        assertEquals(5, ml.get("a").orElseThrow().incarnation());
    }

    @Test
    void roundRobinVisitsAllMembersOnce() {
        MembershipList ml = list();
        ml.merge(new Member("b", ADDR_A, MemberState.ALIVE, 0));
        ml.merge(new Member("c", ADDR_B, MemberState.ALIVE, 0));
        ml.merge(new Member("d", ADDR_C, MemberState.ALIVE, 0));

        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            ml.nextPingTarget("a").ifPresent(m -> seen.add(m.id()));
        }
        assertEquals(Set.of("b", "c", "d"), seen);
    }

    @Test
    void roundRobinRestartsAfterEpochExhausted() {
        MembershipList ml = list();
        ml.merge(new Member("b", ADDR_A, MemberState.ALIVE, 0));
        ml.merge(new Member("c", ADDR_B, MemberState.ALIVE, 0));

        Set<String> epoch1 = new HashSet<>();
        epoch1.add(ml.nextPingTarget("a").orElseThrow().id());
        epoch1.add(ml.nextPingTarget("a").orElseThrow().id());

        Set<String> epoch2 = new HashSet<>();
        epoch2.add(ml.nextPingTarget("a").orElseThrow().id());
        epoch2.add(ml.nextPingTarget("a").orElseThrow().id());

        assertEquals(Set.of("b", "c"), epoch1);
        assertEquals(Set.of("b", "c"), epoch2);
    }

    @Test
    void kRandomOthersReturnsAtMostK() {
        MembershipList ml = list();
        ml.merge(new Member("b", ADDR_A, MemberState.ALIVE, 0));
        ml.merge(new Member("c", ADDR_B, MemberState.ALIVE, 0));
        ml.merge(new Member("d", ADDR_C, MemberState.ALIVE, 0));

        List<Member> others = ml.kRandomOthers("a", 2);
        assertEquals(2, others.size());
        assertFalse(others.stream().anyMatch(m -> m.id().equals("a")));
    }

    @Test
    void kRandomOthersExcludesSelf() {
        MembershipList ml = list();
        ml.merge(new Member("a", ADDR_A, MemberState.ALIVE, 0));
        ml.merge(new Member("b", ADDR_B, MemberState.ALIVE, 0));

        List<Member> others = ml.kRandomOthers("a", 10);
        assertFalse(others.stream().anyMatch(m -> m.id().equals("a")));
    }

    @Test
    void kRandomOthersExcludesDeadMembers() {
        MembershipList ml = list();
        ml.merge(new Member("b", ADDR_A, MemberState.ALIVE, 0));
        ml.merge(new Member("c", ADDR_B, MemberState.DEAD, 0));

        List<Member> others = ml.kRandomOthers("a", 10);
        assertEquals(1, others.size());
        assertEquals("b", others.get(0).id());
    }
}
