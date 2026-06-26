# swim-gossip

A working implementation of the **SWIM** membership protocol — the gossip-based
failure detector that powers HashiCorp Serf and Consul — written in plain Java
21 with a browser-based visualizer that shows ping/ack/gossip exchanges and
node-state transitions in real time.

## What SWIM is

SWIM (*Scalable Weakly-consistent Infection-style Membership*) is a peer
membership protocol that scales sub-linearly with cluster size. Each node:

1. **Probes** a random peer every protocol period `T` (direct PING → ACK).
2. **Falls back to indirect probing** through `k` other peers (PING_REQ) when
   the direct path fails — surviving transient packet loss without false
   positives.
3. **Suspects** then **declares dead** a peer that no path can reach, after a
   refutation window `T_suspect` in which the suspected node can bump its
   incarnation number and broadcast `ALIVE` to clear itself.
4. **Disseminates** state changes by piggybacking updates on regular probe
   messages, with a per-update send-count cap of `λ · ⌈log₂ N⌉`.

The result: failure detection in O(1) time per node, dissemination in
O(log N) rounds, no central coordinator.

## Architecture

```
            ┌────────────────────────┐
            │       SwimNode         │
            │  ─────────────────────│
            │   Disseminator        ─┼── piggyback updates on every send
            │   FailureDetector    ─┼── PING / PING_REQ / suspicion timer
            │   MembershipList     ─┼── §3.3 merge rules, shuffled RR
            └────────┬───────────────┘
                     │ Message (Ping | Ack | PingReq) + List<Update>
                     ▼
            ┌────────────────────────┐
            │       Transport        │
            │  UDP   |  InProcess    │
            └────────┬───────────────┘
                     │
                     ▼
              EventSink ──► Jsonl │ Broadcast ──► SseServer ──► viz/
```

## Tech stack

- Java 21 (records, sealed interfaces, pattern matching)
- Maven, single module
- Jackson for message and event JSON
- JUnit 5 (in-process cluster harness for integration tests)
- JDK `com.sun.net.httpserver` for the live SSE bridge
- Standalone HTML / canvas / vanilla JS visualizer (no build step)

## Running

### Build

```
mvn package
```

Produces a shaded jar at `target/swim-gossip-0.1.0-SNAPSHOT.jar`.

### Replay a recorded run

```
# write events to a file
java -jar target/swim-gossip-0.1.0-SNAPSHOT.jar cluster \
    --nodes 5 --port-base 7000 --duration 10s --kill-at 5s \
    --out viz/sample-events.jsonl

# open the visualizer (use any static server, e.g.)
python -m http.server --directory viz 8000
# then visit http://localhost:8000/
```

### Watch a cluster live

```
# start cluster with SSE endpoint on :8080
java -jar target/swim-gossip-0.1.0-SNAPSHOT.jar cluster \
    --nodes 5 --port-base 7000 --duration 60s --kill-at 10s \
    --live --port 8080

# in another shell, serve viz/ statically
python -m http.server --directory viz 8000

# open with ?live to switch to EventSource
open http://localhost:8000/?live=http://localhost:8080/events
```

## Convergence

With defaults (`T=1s`, `T_direct=400ms`, `T_suspect=5s`, `k=3`,
`λ=3`) the `ConvergenceTest` harness shrinks `T` for speed and verifies
that killing 1 of N=8 nodes leads to consensus on `DEAD` across all
survivors within `T_suspect + (N+1)·T` — typically well under 2 seconds
on a developer laptop.

## Layout

```
src/main/java/dev/nicolas/swim/
  Member, MemberState, MembershipList, SwimConfig
  Message, Update, Codec
  Transport, UdpTransport
  FailureDetector, Disseminator, SwimNode, Main
  event/   Event, EventType, EventSink, JsonlFileSink, BroadcastSink, SseServer
  sim/     InProcessTransport, Cluster, Scenario
viz/       index.html, viz.js, styles.css, sample-events.jsonl
```
