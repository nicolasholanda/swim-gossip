package dev.nicolas.swim;

import dev.nicolas.swim.sim.Cluster;
import dev.nicolas.swim.sim.Scenario;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ConvergenceTest {

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    @Test
    void eightNodeClusterConvergesAfterKillingOne() throws Exception {
        int n = 8;
        int victim = 0;

        SwimConfig config = SwimConfig.defaults()
                .withProtocolPeriod(Duration.ofMillis(100))
                .withDirectAckTimeout(Duration.ofMillis(40))
                .withSuspectTimeout(Duration.ofMillis(400))
                .withIndirectFanout(2);

        try (Cluster cluster = Cluster.create(n, config)) {
            cluster.startAll();
            Thread.sleep(300);

            String victimId = Cluster.nodeId(victim);
            Scenario scenario = new Scenario(cluster);

            long start = System.currentTimeMillis();
            scenario.kill(victim);

            for (int i = 0; i < n; i++) {
                if (i == victim) continue;
                final int idx = i;
                assertTrue(waitFor(() -> cluster.membership(idx).get(victimId)
                                .map(m -> m.state() == MemberState.DEAD)
                                .orElse(false), 10_000),
                        "node " + Cluster.nodeId(idx) + " did not mark " + victimId + " as DEAD");
            }

            long elapsed = System.currentTimeMillis() - start;
            long bound = config.suspectTimeout().toMillis()
                    + (long) (n + 1) * config.protocolPeriod().toMillis();

            System.out.println("[ConvergenceTest] kill-one convergence: " + elapsed
                    + "ms (bound " + bound + "ms, N=" + n + ")");

            assertTrue(elapsed < bound,
                    "convergence took " + elapsed + "ms, exceeded bound " + bound + "ms");
        }
    }
}
