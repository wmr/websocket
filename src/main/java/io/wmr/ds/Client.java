package io.wmr.ds;

import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import reactor.core.publisher.Mono;

import java.net.URI;

public class Client {

    @Option(
        names = {"-u", "--uri"},
        description = "servier uri", defaultValue = "ws://localhost:9090/data")
    String uri;

    public static void main(String[] args) {

        var clientArgs = parseArgs(args);

        final ReactorNettyWebSocketClient client
            = new ReactorNettyWebSocketClient();

        client.setMaxFramePayloadLength(Integer.MAX_VALUE);
        client.execute(
            URI.create(clientArgs.uri),
            session -> session.send(
                Mono.just(session.textMessage("give me my dataz! yez?")))
                .thenMany(session.receive()
                    .map(WebSocketMessage::getPayload)
                    .buffer(1000)
                    .log())
                .then())
            .block();
    }

    public static Client parseArgs(String[] args) {
        final var clientArgs = new Client();
        new CommandLine(clientArgs).parseArgs(args);
        return clientArgs;
    }
}
