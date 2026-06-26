package dev.nicolas.swim.sim;

public class Scenario {

    private final Cluster cluster;

    public Scenario(Cluster cluster) {
        this.cluster = cluster;
    }

    public Scenario kill(int idx) {
        cluster.transport(idx).close();
        return this;
    }

    public Scenario partition(int idx) {
        cluster.transport(idx).setPartitioned(true);
        return this;
    }

    public Scenario heal(int idx) {
        cluster.transport(idx).setPartitioned(false);
        return this;
    }

    public Scenario blockLink(int from, int to) {
        cluster.transport(from).blockPeer(cluster.address(to));
        return this;
    }

    public Scenario unblockLink(int from, int to) {
        cluster.transport(from).unblockPeer(cluster.address(to));
        return this;
    }

    public Scenario pause(long millis) throws InterruptedException {
        Thread.sleep(millis);
        return this;
    }
}
