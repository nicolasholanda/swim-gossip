package dev.nicolas.swim;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.net.InetSocketAddress;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Message.Ping.class, name = "PING"),
        @JsonSubTypes.Type(value = Message.Ack.class, name = "ACK"),
        @JsonSubTypes.Type(value = Message.PingReq.class, name = "PING_REQ")
})
public sealed interface Message permits Message.Ping, Message.Ack, Message.PingReq {

    List<Update> piggyback();

    record Ping(long seqNo, List<Update> piggyback) implements Message {}

    record Ack(long seqNo, List<Update> piggyback) implements Message {}

    record PingReq(long seqNo, String targetId, InetSocketAddress targetAddress, List<Update> piggyback)
            implements Message {}
}
