package io.wmr.ds;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Spliterator;

import static java.lang.System.arraycopy;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

class DataHandlerTest {

    @Test
    void data() throws IOException {
        final var mapper = new CBORMapper();
        final var chunk1 = mapper.writeValueAsBytes(List.of("asdf", 42, "jkl"));
        final var chunk2 = mapper.writeValueAsBytes(List.of("asdf", 42, "jkl"));
        final var all = new byte[chunk1.length + chunk2.length];
        arraycopy(chunk1, 0, all, 0, chunk1.length);
        arraycopy(chunk2, 0, all, chunk1.length, chunk2.length);

        var parser = mapper.getFactory().createParser(all);
        var listIterator = parser.readValuesAs(List.class);
        var targetStream = stream(
                spliteratorUnknownSize(listIterator, Spliterator.ORDERED),
                false);

        targetStream.forEachOrdered(System.out::println);
    }
}