package io.wmr.ds;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.split;

public class C2O {
    @Option(
        names = {"-u", "--uri"},
        description = "servier uri",
        defaultValue = "/Users/wmr/go/src/c2o/data.csv.gz")
    String source;

    public static void main(String[] args) {
        final var c2o = new C2O();
        new CommandLine(c2o).parseArgs(args);

        final var watch = Stopwatch.createStarted();

        String[] header = null;
        try (final var stream = Files.newInputStream(Paths.get(c2o.source));
             final var gzipStream = new GZIPInputStream(stream);
             final var reader = new BufferedReader(
                 new InputStreamReader(gzipStream, UTF_8))) {

            TypeDescription schemaDef = null;
            VectorizedRowBatch batch = null;
            Writer writer = null;
            String line;
            while ((line = reader.readLine()) != null) {
                final var record = split(line, ',');
                if (header == null) {
                    header = record;
                    continue;
                }
                if (schemaDef == null) {
                    schemaDef = inferSchema(header, record);
                    writer = createWriter(schemaDef);
                    batch = schemaDef.createRowBatch();
                }

                var idx = batch.size++;

                for (int j =0; j!= record.length; ++j) {
                    ColumnVector col = batch.cols[j];
                    switch (col.type) {
                        case LONG:
                            ((LongColumnVector)col).vector[idx] = Long.parseLong(record[j]);
                            break;
                        case BYTES:
                            ((BytesColumnVector)col).setVal(idx, record[j].getBytes());
                    }
                }


                if (batch.size == batch.getMaxSize() - 1) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
            writer.close();
            System.out.println("total time spent: " + watch.elapsed(SECONDS));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    private static String inferType(String data) {
        try {
            Integer.parseInt(data);
            return "int";

        } catch (NumberFormatException e) {
            return "string";
        }
    }

    private static Writer createWriter(TypeDescription schema) {
        var conf = new Configuration();
        try {
            return OrcFile.createWriter(
                new Path("/Users/wmr/go/src/c2o/out.java.orc"),
                OrcFile.writerOptions(conf).setSchema(schema));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static TypeDescription inferSchema(String[] header, String[] record) {
        String[] types = new String[record.length];
        for (int idx = 0; idx != record.length; ++idx) {
            types[idx] = header[idx] + ":" + inferType(record[idx]);
        }
        return TypeDescription
            .fromString("struct<" + join(types, ',') + ">");
    }
}
