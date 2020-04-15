package io.wmr.ds;

import static io.reactivex.rxjava3.core.BackpressureStrategy.BUFFER;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.common.base.Stopwatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.junit.jupiter.api.Test;

import io.reactivex.rxjava3.core.Flowable;
import static org.assertj.core.api.Assertions.*;

public class DataControllerTest {

    @Test
    public void createOrc() throws IOException {
        var schema = TypeDescription
                .fromString("struct<col0:int,col1:string,col2:string,col3:string,col4:string>");

        var opts = OrcFile.writerOptions(new Configuration())
                .compress(CompressionKind.ZSTD)
                .setSchema(schema);

        try (var writer = OrcFile.createWriter(
                new Path("/Users/wmr/Src/python/chunked", "data-zstd.orc"),
                opts)) {

            var batch = schema.createRowBatch();

            var iterations = 20_000_000;
            for (int i=0; i!= iterations; ++i) {
                var uuid = UUID.randomUUID().toString();
                addRow(writer, batch, List.of(i, uuid + "1", uuid + "2", uuid + "3", uuid + "4"));
            }

            if (batch.size != 0) {
                writer.addRowBatch(batch);
            }
        }
    }

    private void addRow(Writer writer, VectorizedRowBatch batch, List<Object> data) throws IOException {
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        int row = batch.size++;

        LongColumnVector col0 = (LongColumnVector) batch.cols[0];
        BytesColumnVector col1 = (BytesColumnVector) batch.cols[1];
        BytesColumnVector col2 = (BytesColumnVector) batch.cols[2];
        BytesColumnVector col3 = (BytesColumnVector) batch.cols[3];
        BytesColumnVector col4 = (BytesColumnVector) batch.cols[4];

        col0.vector[row] = Integer.toUnsignedLong((int)data.get(0));
        col1.setVal(row, String.valueOf(data.get(1)).getBytes());
        col2.setVal(row, String.valueOf(data.get(1)).getBytes());
        col3.setVal(row, String.valueOf(data.get(1)).getBytes());
        col4.setVal(row, String.valueOf(data.get(1)).getBytes());

    }

    @Test
    public void test() throws IOException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Reader reader = OrcFile.createReader(
                new Path("/Users/wmr/Src/python/chunked", "data.orc"),
                OrcFile.readerOptions(new Configuration()));

        final RecordReader rows = reader.rows();
        final VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        final CBORMapper cborMapper = new CBORMapper();

        final AtomicInteger idx = new AtomicInteger(0);
        Flowable.create(emitter -> {
            while (rows.nextBatch(batch)) {
                emitter.onNext(cborMapper.writeValueAsBytes(batch.stringify("").getBytes()));
            }
        }, BUFFER).subscribe(it -> {});

        System.out.println(idx.intValue());
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

}