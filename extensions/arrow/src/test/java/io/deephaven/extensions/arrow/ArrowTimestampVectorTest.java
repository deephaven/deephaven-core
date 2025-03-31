//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.arrow;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.mutable.MutableInt;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ArrowTimestampVectorTest {
    private static final List<Long> expectedRows;
    private static final Instant[] expectedValues;
    static {
        expectedRows = Arrays.asList(
                0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L,
                16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L, 25L);
        expectedValues = new Instant[expectedRows.size()];
        int numBatchRows = expectedRows.size() / 2;

        long next = DateTimeUtils.epochNanos(
                ZonedDateTime.of(2024, 3, 1, 8, 30, 0, 0, ZoneId.of("UTC")));
        long step_size = 60L * 1_000_000_000L;
        for (int ii = 0; ii < numBatchRows; ++ii, next += step_size) {
            if (ii % 4 == 3) {
                expectedValues[ii] = null;
                continue;
            }
            expectedValues[ii] = DateTimeUtils.epochNanosToInstant(next);
        }

        for (int ii = 0; ii < numBatchRows; ++ii, next += step_size) {
            expectedValues[ii + numBatchRows] = expectedValues[ii];
        }
    }

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static void generateTable() throws Exception {
        final int rowCount = expectedRows.size() / 2;
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // Create four timestamp vectors.
        TimeStampSecVector tsSec = new TimeStampSecVector("ts_sec", allocator);
        TimeStampMilliVector tsMilli = new TimeStampMilliVector("ts_milli", allocator);
        TimeStampMicroVector tsMicro = new TimeStampMicroVector("ts_micro", allocator);
        TimeStampNanoVector tsNano = new TimeStampNanoVector("ts_nano", allocator);

        // Allocate space for each vector.
        tsSec.allocateNew(rowCount);
        tsMilli.allocateNew(rowCount);
        tsMicro.allocateNew(rowCount);
        tsNano.allocateNew(rowCount);

        for (int ii = 0; ii < rowCount; ii++) {
            if (expectedValues[ii] == null) {
                tsSec.setNull(ii);
                tsMilli.setNull(ii);
                tsMicro.setNull(ii);
                tsNano.setNull(ii);
            } else {
                long next = DateTimeUtils.epochNanos(expectedValues[ii]);
                Assert.assertEquals(0, next % 1_000_000_000L);
                tsSec.setSafe(ii, next / 1_000_000_000L);
                tsMilli.setSafe(ii, next / 1_000_000L);
                tsMicro.setSafe(ii, next / 1_000L);
                tsNano.setSafe(ii, next);
            }
        }

        // Inform each vector how many values were set.
        tsSec.setValueCount(rowCount);
        tsMilli.setValueCount(rowCount);
        tsMicro.setValueCount(rowCount);
        tsNano.setValueCount(rowCount);

        // Build a schema from the fields of our vectors.
        List<Field> fields = new ArrayList<>();
        fields.add(tsSec.getField());
        fields.add(tsMilli.getField());
        fields.add(tsMicro.getField());
        fields.add(tsNano.getField());
        Schema schema = new Schema(fields);

        // Assemble the vectors into a VectorSchemaRoot.
        List<FieldVector> vectors = new ArrayList<>();
        vectors.add(tsSec);
        vectors.add(tsMilli);
        vectors.add(tsMicro);
        vectors.add(tsNano);
        VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, rowCount);

        // Write out the data as a Feather V2 (Arrow IPC) file.
        try (FileOutputStream fos = new FileOutputStream("/tmp/timestamp_vectors.arrow");
                WritableByteChannel channel = Channels.newChannel(fos)) {

            // ArrowFileWriter writes out the Arrow IPC file format,
            // which is the basis for Feather V2.
            ArrowFileWriter writer = new ArrowFileWriter(root, /* dictionaryProvider */ null, channel);
            writer.start();
            writer.writeBatch();
            // write it twice cause we need to test multiple batch logic
            writer.writeBatch();
            writer.end();
            writer.close();
            System.out.println("Data successfully written to timestamps.arrow");
        } finally {
            // Release all allocated memory.
            root.close();
            allocator.close();
        }
    }

    private static QueryTable loadTable() throws Exception {
        // noinspection ConstantConditions;
        final File dataFile =
                new File(ArrowTimestampVectorTest.class.getResource("/timestamp_vectors.arrow").getFile());
        return ArrowWrapperTools.readFeather(dataFile.getPath());
    }

    @Test
    public void testReadArrowFile() throws Exception {
        final QueryTable table = loadTable();
        Assert.assertEquals(expectedValues.length, table.intSize());

        // check that the expected rows are present;
        final List<Long> actualRows = new ArrayList<>();
        table.getRowSet().forAllRowKeys(actualRows::add);
        Assert.assertEquals(expectedRows, actualRows);

        final ColumnSource<?>[] columns = table.getColumnSources().toArray(ColumnSource[]::new);
        Assert.assertEquals(4, columns.length);
        for (int ii = 0; ii < columns.length; ++ii) {
            final ColumnSource<?> cs = columns[ii];
            Assert.assertEquals(Instant.class, cs.getType());

            ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
            final MutableInt pos = new MutableInt();
            table.getRowSet().forAllRowKeys(
                    rowKey -> Assert.assertEquals(expectedValues[pos.getAndIncrement()], cs.get(rowKey)));
            Assert.assertEquals(2, ArrowWrapperTools.Shareable.numBlocksLoaded());
        }
    }

    @Test
    public void testFillChunk() throws Exception {
        final QueryTable table = loadTable();

        final ColumnSource<?>[] columns = table.getColumnSources().toArray(ColumnSource[]::new);
        Assert.assertEquals(4, columns.length);
        for (final ColumnSource<?> cs : columns) {
            Assert.assertEquals(Instant.class, cs.getType());

            try (final ChunkSource.FillContext fillContext = cs.makeFillContext(table.intSize());
                    final WritableObjectChunk<Instant, Values> chunk =
                            WritableObjectChunk.makeWritableChunk(table.intSize())) {

                ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
                cs.fillChunk(fillContext, chunk, table.getRowSet());
                Assert.assertEquals(2, ArrowWrapperTools.Shareable.numBlocksLoaded());

                for (int ii = 0; ii < expectedValues.length; ++ii) {
                    Assert.assertEquals(expectedValues[ii], chunk.get(ii));
                }
            }
        }
    }
}
