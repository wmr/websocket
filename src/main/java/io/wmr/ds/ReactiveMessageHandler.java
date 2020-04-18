package io.wmr.ds;

import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class ReactiveMessageHandler implements WebSocketHandler {

    @NotNull
    private static byte[] initArray() {
        final var bytes = new byte[2 * 1024 * 1024];
        for (int i = 0; i != bytes.length; ++i) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }


    @NotNull
    private static byte[] temper(byte[] data, Integer idx) {
        data[idx] = idx.byteValue();
        return data;
    }




    @NotNull
    public static WebSocketMessage binaryMessage(WebSocketSession session, byte[] payload) {
        return session.binaryMessage(
            (bufferFactory) -> bufferFactory.wrap(payload));
    }

    public static byte[] eofMessage() {
        return new byte[]{};
    }


    @Override
    @NotNull
    public Mono<Void> handle(WebSocketSession session) {

        final var bytes = initArray();

        return session.send(
            Flux.range(0, 10000)
                .map(idx -> temper(bytes, idx))
                .concatWith(Mono.just(eofMessage()))
                .map(payload -> binaryMessage(session, payload)))
            .and(session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .log());
    }


}
