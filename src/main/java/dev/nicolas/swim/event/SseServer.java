package dev.nicolas.swim.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class SseServer implements AutoCloseable {

    private static final byte[] KEEPALIVE = ": keepalive\n\n".getBytes(StandardCharsets.UTF_8);
    private static final long KEEPALIVE_TIMEOUT_SECONDS = 15;

    private final HttpServer server;
    private final BroadcastSink sink;
    private final ObjectMapper mapper = new ObjectMapper();

    public SseServer(int port, BroadcastSink sink) throws IOException {
        this.sink = sink;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/events", this::handleEvents);
        server.createContext("/health", this::handleHealth);
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "sse-server");
            t.setDaemon(true);
            return t;
        }));
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        byte[] body = ("{\"subscribers\":" + sink.subscriberCount() + "}")
                .getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private void handleEvents(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("Connection", "keep-alive");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, 0);
        OutputStream os = ex.getResponseBody();

        LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<>();
        Consumer<Event> sub = queue::offer;
        sink.subscribe(sub);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                Event e = queue.poll(KEEPALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (e == null) {
                    os.write(KEEPALIVE);
                } else {
                    String json = mapper.writeValueAsString(e);
                    os.write(("data: " + json + "\n\n").getBytes(StandardCharsets.UTF_8));
                }
                os.flush();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
        } finally {
            sink.unsubscribe(sub);
            try { os.close(); } catch (IOException ignored) {}
            ex.close();
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
