package io.wmr.ds;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static io.reactivex.rxjava3.core.BackpressureStrategy.BUFFER;
import static io.reactivex.rxjava3.schedulers.Schedulers.computation;
import static io.reactivex.rxjava3.schedulers.Schedulers.io;

@Component
public class DataMessageHandler extends BinaryWebSocketHandler {

    public static void genValue(JsonGenerator generator, int i, ColumnVector cv) throws IOException {
        if (ColumnVector.Type.LONG == cv.type) {
            generator.writeNumber(((LongColumnVector) cv).vector[i]);
        }
        if (ColumnVector.Type.BYTES == cv.type) {
            generator.writeString(((BytesColumnVector) cv).toString(i));
        }
    }

    public static void writeToGenerator(JsonGenerator generator, VectorizedRowBatch batch) throws IOException {
        generator.writeStartArray();

        if (batch.size == 0) {
            return;
        }

        if (batch.selectedInUse) {
            for (int j = 0; j < batch.size; j++) {
                int i = batch.selected[j];

                generator.writeStartArray();
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];
                    if (cv != null) {

                        genValue(generator, i, cv);
                    }
                }
                generator.writeEndArray();
            }
        } else {
            for (int i = 0; i < batch.size; i++) {
                generator.writeStartArray();
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];
                    if (cv != null) {
                        genValue(generator, i, cv);
                    }
                }
                generator.writeEndArray();
            }
        }
        generator.writeEndArray();
    }

    Flowable<byte[]> dataFlowable(final JsonFactory jsonFactory) {
        return Flowable.create(emitter -> {
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
                            generator.flush();
                            emitter.onNext(out.toByteArray());
                        }
                    }
                    emitter.onNext(new byte[]{});
                    emitter.onComplete();
                }
            } catch (final IOException e) {
                emitter.onError(e);
                emitter.onComplete();
            }
        }, BUFFER);
    }


    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        dataFlowable(new CBORFactory())
                .observeOn(io())
                .subscribeOn(computation())
                .map(BinaryMessage::new)
                .subscribe(session::sendMessage);
    }

}
