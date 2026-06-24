package dev.nicolas.swim;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public interface Transport extends AutoCloseable {

    void send(InetSocketAddress dest, byte[] payload);

    void onReceive(BiConsumer<InetSocketAddress, byte[]> handler);

    InetSocketAddress localAddress();

    @Override
    void close();
}
