package dev.nicolas.swim.event;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class JsonlFileSink implements EventSink, AutoCloseable {

    private final BufferedWriter writer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();
    private volatile boolean closed = false;

    public JsonlFileSink(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) Files.createDirectories(parent);
        this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void accept(Event event) {
        if (closed) return;
        synchronized (lock) {
            if (closed) return;
            try {
                writer.write(mapper.writeValueAsString(event));
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                System.err.println("JsonlFileSink write failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            writer.close();
        }
    }
}
