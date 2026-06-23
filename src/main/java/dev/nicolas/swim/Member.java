package dev.nicolas.swim;

import java.net.InetSocketAddress;

public record Member(String id, InetSocketAddress address, MemberState state, int incarnation) {

    public Member withState(MemberState newState) {
        return new Member(id, address, newState, incarnation);
    }

    public Member withIncarnation(int newIncarnation) {
        return new Member(id, address, state, newIncarnation);
    }

    public Member refute() {
        return new Member(id, address, MemberState.ALIVE, incarnation + 1);
    }
}
