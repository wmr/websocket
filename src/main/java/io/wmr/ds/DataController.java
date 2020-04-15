package io.wmr.ds;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.ion.IonFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.wmr.ds.OrcUtils.stringify;
import static io.wmr.ds.OrcUtils.writeToGenerator;
import static org.springframework.http.MediaType.*;

public class DataController {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    @GetMapping(value = "/test", produces = APPLICATION_CBOR_VALUE)
    public Map<String, Object> test() {
        return Map.of("test", "stuff");
    }

    @GetMapping(value = "/read")
    public String dataRead() throws IOException {
        var out = new OutputStream() {

            @Override
            public void write(int b) throws IOException {

            }
        };
        streamingBody(new CBORFactory()).writeTo(out);
        return "hi";
    }

    @GetMapping(value = "/avro", produces = APPLICATION_JSON_VALUE)
    public StreamingResponseBody dataAvro() throws IOException {
        final DataFileReader dfr = new DataFileReader(Paths.get("/Users/wmr/Src/python/chunked/data.avro").toFile(),
                new GenericDatumReader<>());

        return out -> {
            final GenericData.Record record = new GenericData.Record(dfr.getSchema());
            final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(dfr.getSchema(), out);
            final DatumWriter<Object> writer = new GenericDatumWriter<>(dfr.getSchema());
            while (dfr.hasNext()) {
                dfr.next(record);
                writer.write(record, jsonEncoder);
                out.flush();
            }
        };

    }

    @GetMapping(value = "/orc", produces = APPLICATION_CBOR_VALUE)
    @ResponseBody
    public void dataOrcCbor(HttpServletResponse response) throws IOException {
        ServletOutputStream out = response.getOutputStream();
        response.setContentType("application/cbor");
        StreamingResponseBody streamingResponseBody = streamingBody(new CBORFactory());
        streamingResponseBody.writeTo(out);
    }

    @GetMapping(value = "/orc", produces = APPLICATION_JSON_VALUE)
    public StreamingResponseBody dataOrcJson() throws IOException {
        return streamingBody(new JsonFactory());
    }

    @GetMapping(value = "/orc", produces = "application/x-ion")
    public StreamingResponseBody dataOrcIon() throws IOException {
        return streamingBody(new IonFactory());
    }

    @GetMapping(value = "/orc", produces = "application/x-avro")
    public StreamingResponseBody dataOrcAvro() throws IOException {
        return streamingBody(new AvroFactory());
    }

    ResponseBodyEmitter bodyEmitter(final JsonFactory jsonFactory) {
        final ResponseBodyEmitter emitter = new ResponseBodyEmitter();
        Runnable runnable = () -> {
            try (final Reader reader = OrcFile.createReader(
                    new Path("/Users/wmr/Src/python/chunked", "data.orc"),
                    OrcFile.readerOptions(new Configuration()))) {

                try (final RecordReader rows = reader.rows()) {
                    final VectorizedRowBatch batch = reader
                            .getSchema()
                            .createRowBatch();

                    while (rows.nextBatch(batch)) {
                        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
                             final JsonGenerator generator = jsonFactory.createGenerator(out)) {

                            jsonFactory.createGenerator(out);
                            writeToGenerator(generator, batch);
                            emitter.send(out.toByteArray(), APPLICATION_CBOR);
                        }
                    }
                    emitter.complete();
                }


            } catch (final IOException e) {
                emitter.completeWithError(e);
            }
        };

        executorService.execute(runnable);

        return emitter;
    }

    StreamingResponseBody streamingBody(final JsonFactory jsonFactory) {
        try (final Reader reader = OrcFile.createReader(
                new Path("/Users/wmr/Src/python/chunked", "data.orc"),
                OrcFile.readerOptions(new Configuration()))) {

            return out -> {
                try (final JsonGenerator generator = jsonFactory.createGenerator(out);
                     final RecordReader rows = reader.rows()) {

                    final VectorizedRowBatch batch = reader
                            .getSchema()
                            .createRowBatch();

                    while (rows.nextBatch(batch)) {
                        writeToGenerator(generator, batch);
                        generator.flush();
                    }
                }
            };
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @GetMapping(value = "/orcstr", produces = APPLICATION_CBOR_VALUE)
    public StreamingResponseBody dataOrcString() throws IOException {

        final Reader reader = OrcFile.createReader(
                new Path("/Users/wmr/Src/python/chunked", "data.orc"),
                OrcFile.readerOptions(new Configuration()));

        final RecordReader rows = reader.rows();
        final VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        return out -> {
            while (rows.nextBatch(batch)) {
                out.write(stringify(batch));
                out.flush();
            }
        };

    }
}
