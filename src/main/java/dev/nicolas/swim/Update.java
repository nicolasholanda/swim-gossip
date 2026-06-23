package dev.nicolas.swim;

import java.net.InetSocketAddress;

public record Update(String id, InetSocketAddress address, MemberState state, int incarnation) {}
