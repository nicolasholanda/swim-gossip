package dev.nicolas.swim;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class UdpTransport implements Transport {

    private final DatagramSocket socket;
    private final InetSocketAddress localAddress;
    private final int maxBytes;
    private final AtomicReference<BiConsumer<InetSocketAddress, byte[]>> handler = new AtomicReference<>();
    private final Thread receiver;
    private volatile boolean running = true;

    public UdpTransport(InetSocketAddress bind, int maxBytes) throws SocketException {
        this.socket = new DatagramSocket(bind);
        this.localAddress = (InetSocketAddress) socket.getLocalSocketAddress();
        this.maxBytes = maxBytes;
        this.receiver = new Thread(this::receiveLoop, "udp-receiver-" + localAddress.getPort());
        this.receiver.setDaemon(true);
        this.receiver.start();
    }

    @Override
    public void send(InetSocketAddress dest, byte[] payload) {
        try {
            socket.send(new DatagramPacket(payload, payload.length, dest));
        } catch (IOException e) {
            throw new RuntimeException("UDP send to " + dest + " failed", e);
        }
    }

    @Override
    public void onReceive(BiConsumer<InetSocketAddress, byte[]> h) {
        handler.set(h);
    }

    @Override
    public InetSocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public void close() {
        running = false;
        socket.close();
    }

    private void receiveLoop() {
        byte[] buf = new byte[maxBytes];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        while (running) {
            try {
                socket.receive(packet);
                byte[] data = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), 0, data, 0, packet.getLength());
                BiConsumer<InetSocketAddress, byte[]> h = handler.get();
                if (h != null) {
                    h.accept((InetSocketAddress) packet.getSocketAddress(), data);
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("UDP receive error: " + e.getMessage());
                }
            }
        }
    }
}
