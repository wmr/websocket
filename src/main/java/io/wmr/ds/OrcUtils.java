package io.wmr.ds;

import com.fasterxml.jackson.core.JsonGenerator;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

import static io.reactivex.rxjava3.core.BackpressureStrategy.BUFFER;

public class OrcUtils {

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

    public static void genValue(JsonGenerator generator, int i, ColumnVector cv) throws IOException {
        if (ColumnVector.Type.LONG == cv.type) {
            generator.writeNumber(((LongColumnVector) cv).vector[i]);
        }
        if (ColumnVector.Type.BYTES == cv.type) {
            generator.writeString(((BytesColumnVector) cv).toString(i));
        }
    }

    public static Object[] objectify(VectorizedRowBatch batch) {

        if (batch.size == 0) {
            return new Object[]{};
        }

        Object[] all = new Object[batch.size];

        if (batch.selectedInUse) {

            for (int j = 0; j < batch.size; j++) {
                int i = batch.selected[j];

                Object[] row = new Object[batch.numCols];
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];
                    if (cv != null) {
                        row[i] = asObject(cv, i);
                    }
                }
                all[j] = row;
            }
        } else {
            for (int i = 0; i < batch.size; i++) {
                Object[] row = new Object[batch.numCols];
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];

                    if (cv != null) {
                        row[k] = asObject(cv, i);
                    }
                }
                all[i] = row;
            }
        }
        return all;
    }


    public static byte[] stringify(VectorizedRowBatch batch) {

        if (batch.size == 0) {
            return "".getBytes();
        }

        StringBuilder b = new StringBuilder();
        if (batch.selectedInUse) {
            for (int j = 0; j < batch.size; j++) {
                int i = batch.selected[j];

                b.append('[');
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];
                    if (k > 0) {
                        b.append(", ");
                    }
                    if (cv != null) {
                        try {
                            cv.stringifyValue(b, i);
                        } catch (Exception ex) {
                            b.append("<invalid>");
                        }
                    }
                }
                b.append(']');
                if (j < batch.size - 1) {
                    b.append('\n');
                }
            }
        } else {
            for (int i = 0; i < batch.size; i++) {
                b.append('[');
                for (int k = 0; k < batch.projectionSize; k++) {
                    int projIndex = batch.projectedColumns[k];
                    ColumnVector cv = batch.cols[projIndex];
                    if (k > 0) {
                        b.append(", ");
                    }
                    if (cv != null) {
                        try {
                            cv.stringifyValue(b, i);
                        } catch (Exception ex) {
                            b.append("<invalid>");
                        }
                    }
                }
                b.append(']');
                if (i < batch.size - 1) {
                    b.append('\n');
                }
            }
        }
        return b.toString().getBytes();
    }


    private static Object asObject(ColumnVector cv, int i) {

        if (ColumnVector.Type.LONG == cv.type) {
            return ((LongColumnVector) cv).vector[i];
        }
        if (ColumnVector.Type.BYTES == cv.type) {
            return ((BytesColumnVector) cv).toString(i);
        }
        return null;
    }

    public static Flowable<Object[]> flowableFrom(Reader reader) {
        return Flowable.create(emitter -> {
            try (final RecordReader rows = reader.rows()) {
                final VectorizedRowBatch batch = reader.getSchema()
                        .createRowBatch();

                while (rows.nextBatch(batch)) {
                    emitter.onNext(objectify(batch));
                }
            } catch (IOException e) {
                emitter.onError(e);
            } finally {
                emitter.onComplete();
            }
        }, BUFFER);

    }
}
