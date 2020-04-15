package io.wmr.ds;

import static java.net.http.HttpClient.Version.HTTP_2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.common.base.Stopwatch;

public class Client {
    public static void main(String[] args)
            throws URISyntaxException, IOException, InterruptedException {

        final var watch = Stopwatch.createStarted();
        final var request = HttpRequest.newBuilder(new URI("http://localhost:9090/orc"))
                .header("Accept", "application/cbor")
                .version(HTTP_2)
                .GET()
                .build();

        var response = HttpClient.newBuilder()
                .build()
                .send(request, HttpResponse.BodyHandlers.ofByteArrayConsumer(it -> {
                    Integer len = it.map(chunk -> chunk.length)
                            .orElse(0);
                    System.out.print(len);
                }));


//
//        int total = 0;
//        try {
//            while (true) {
//                total += parser.readValueAs(List.class).size();
//            }
//        }
//        catch(Exception e) {
//            System.out.println(total);
//            System.out.println(e);
//            System.out.println("" + watch.elapsed(TimeUnit.SECONDS));
//        }

    }
}
