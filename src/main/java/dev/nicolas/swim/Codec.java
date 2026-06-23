package dev.nicolas.swim;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.net.InetSocketAddress;

public final class Codec {

    private final ObjectMapper mapper;
    private final int maxBytes;

    public Codec(int maxBytes) {
        this.maxBytes = maxBytes;
        this.mapper = buildMapper();
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper om = new ObjectMapper();
        SimpleModule m = new SimpleModule();
        m.addSerializer(InetSocketAddress.class, new InetSocketAddressSerializer());
        m.addDeserializer(InetSocketAddress.class, new InetSocketAddressDeserializer());
        om.registerModule(m);
        return om;
    }

    public byte[] encode(Message msg) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(msg);
        if (bytes.length > maxBytes) {
            throw new IllegalArgumentException(
                    "encoded message is " + bytes.length + " bytes, exceeds MTU of " + maxBytes);
        }
        return bytes;
    }

    public Message decode(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Message.class);
    }

    private static final class InetSocketAddressSerializer extends JsonSerializer<InetSocketAddress> {
        @Override
        public void serialize(InetSocketAddress value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeString(value.getHostString() + ":" + value.getPort());
        }
    }

    private static final class InetSocketAddressDeserializer extends JsonDeserializer<InetSocketAddress> {
        @Override
        public InetSocketAddress deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getText();
            int lastColon = text.lastIndexOf(':');
            return new InetSocketAddress(text.substring(0, lastColon),
                    Integer.parseInt(text.substring(lastColon + 1)));
        }
    }
}
