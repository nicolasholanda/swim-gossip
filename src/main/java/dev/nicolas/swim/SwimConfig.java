package dev.nicolas.swim;

import java.time.Duration;

public record SwimConfig(
        Duration protocolPeriod,
        Duration directAckTimeout,
        int indirectFanout,
        Duration suspectTimeout,
        int disseminationLambda,
        int maxMessageBytes,
        long randomSeed) {

    public static SwimConfig defaults() {
        return new SwimConfig(
                Duration.ofMillis(1000),
                Duration.ofMillis(400),
                3,
                Duration.ofMillis(5000),
                3,
                1400,
                42L);
    }

    public SwimConfig withProtocolPeriod(Duration period) {
        return new SwimConfig(period, directAckTimeout, indirectFanout,
                suspectTimeout, disseminationLambda, maxMessageBytes, randomSeed);
    }

    public SwimConfig withDirectAckTimeout(Duration timeout) {
        return new SwimConfig(protocolPeriod, timeout, indirectFanout,
                suspectTimeout, disseminationLambda, maxMessageBytes, randomSeed);
    }

    public SwimConfig withIndirectFanout(int k) {
        return new SwimConfig(protocolPeriod, directAckTimeout, k,
                suspectTimeout, disseminationLambda, maxMessageBytes, randomSeed);
    }

    public SwimConfig withSuspectTimeout(Duration timeout) {
        return new SwimConfig(protocolPeriod, directAckTimeout, indirectFanout,
                timeout, disseminationLambda, maxMessageBytes, randomSeed);
    }

    public SwimConfig withDisseminationLambda(int lambda) {
        return new SwimConfig(protocolPeriod, directAckTimeout, indirectFanout,
                suspectTimeout, lambda, maxMessageBytes, randomSeed);
    }

    public SwimConfig withMaxMessageBytes(int bytes) {
        return new SwimConfig(protocolPeriod, directAckTimeout, indirectFanout,
                suspectTimeout, disseminationLambda, bytes, randomSeed);
    }

    public SwimConfig withRandomSeed(long seed) {
        return new SwimConfig(protocolPeriod, directAckTimeout, indirectFanout,
                suspectTimeout, disseminationLambda, maxMessageBytes, seed);
    }
}
