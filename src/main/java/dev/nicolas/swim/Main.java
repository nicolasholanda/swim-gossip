package dev.nicolas.swim;

import dev.nicolas.swim.event.BroadcastSink;
import dev.nicolas.swim.event.EventSink;
import dev.nicolas.swim.event.JsonlFileSink;
import dev.nicolas.swim.event.SseServer;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "cluster" -> runCluster(parseOptions(args, 1));
            default -> printUsage();
        }
    }

    private static void runCluster(Map<String, String> opts) throws Exception {
        int n = Integer.parseInt(opts.getOrDefault("--nodes", "5"));
        int portBase = Integer.parseInt(opts.getOrDefault("--port-base", "7000"));
        long durationMs = parseDuration(opts.getOrDefault("--duration", "10s"));
        long killAtMs = opts.containsKey("--kill-at") ? parseDuration(opts.get("--kill-at")) : -1L;
        String outPath = opts.get("--out");

        SwimConfig config = SwimConfig.defaults();
        Codec codec = new Codec(config.maxMessageBytes());

        JsonlFileSink fileSink = outPath != null ? new JsonlFileSink(Path.of(outPath)) : null;
        SseServer sseServer = null;
        EventSink sink;
        if (opts.containsKey("--live")) {
            BroadcastSink broadcast = new BroadcastSink();
            if (fileSink != null) broadcast.subscribe(fileSink::accept);
            int ssePort = Integer.parseInt(opts.getOrDefault("--port", "8080"));
            sseServer = new SseServer(ssePort, broadcast);
            sseServer.start();
            System.out.println("[cluster] SSE on http://localhost:" + sseServer.port() + "/events");
            sink = broadcast;
        } else {
            sink = fileSink != null ? fileSink : EventSink.NOOP;
        }

        List<InetSocketAddress> addresses = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            addresses.add(new InetSocketAddress("127.0.0.1", portBase + i));
        }

        List<SwimNode> nodes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MembershipList ml = new MembershipList(i + 1L);
            for (int j = 0; j < n; j++) {
                ml.add(new Member("N" + j, addresses.get(j), MemberState.ALIVE, 0));
            }
            UdpTransport t = new UdpTransport(addresses.get(i), config.maxMessageBytes());
            nodes.add(new SwimNode("N" + i, t, codec, ml, config, sink));
        }

        long start = System.currentTimeMillis();
        try {
            nodes.forEach(SwimNode::start);
            System.out.println("[cluster] started " + n + " nodes on UDP " + portBase + "..." + (portBase + n - 1));

            if (killAtMs >= 0) {
                Thread.sleep(killAtMs);
                System.out.println("[cluster] killing N0 at t=" + killAtMs + "ms");
                nodes.get(0).close();
            }

            long elapsed = System.currentTimeMillis() - start;
            long remaining = Math.max(0, durationMs - elapsed);
            Thread.sleep(remaining);
        } finally {
            for (SwimNode node : nodes) {
                try { node.close(); } catch (Exception ignored) {}
            }
            if (sseServer != null) {
                try { sseServer.close(); } catch (Exception ignored) {}
            }
            if (fileSink != null) {
                try { fileSink.close(); } catch (Exception ignored) {}
            }
            System.out.println("[cluster] shut down after " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    private static Map<String, String> parseOptions(String[] args, int from) {
        Map<String, String> out = new HashMap<>();
        for (int i = from; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) continue;
            String value = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true";
            out.put(key, value);
        }
        return out;
    }

    private static long parseDuration(String s) {
        String t = s.trim().toLowerCase(Locale.ROOT);
        if (t.endsWith("ms")) return Long.parseLong(t.substring(0, t.length() - 2).trim());
        if (t.endsWith("s")) return Long.parseLong(t.substring(0, t.length() - 1).trim()) * 1000L;
        return Long.parseLong(t);
    }

    private static void printUsage() {
        System.out.println("""
                Usage:
                  swim-gossip cluster --nodes N [--port-base 7000] [--duration 10s] [--kill-at 5s] [--out events.jsonl] [--live --port 8080]
                """);
    }
}
