//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.arrow;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static io.deephaven.engine.table.impl.SnapshotTestUtils.verifySnapshotBarrageMessage;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static java.util.Arrays.asList;

public class ArrowWrapperToolsTest {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    @Test
    public void testReadMultiArrowFile() {
        // noinspection ConstantConditions
        File file = new File(this.getClass().getResource("/three_vectors.arrow").getFile());
        String path = file.getPath();
        Table table = ArrowWrapperTools.readFeather(path);
        Collection<? extends ColumnSource<?>> columnSources = table.getColumnSources();

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 2);
        builder.appendRange(4, 4);
        builder.appendRange(8, 9);
        try (final RowSet expectedRowSet = builder.build()) {
            Assert.assertEquals(expectedRowSet, table.getRowSet());
        }

        final ColumnSource<String> column0 = table.getColumnSource("name");
        try (final ChunkSource.FillContext fillContext = column0.makeFillContext(table.intSize());
                final WritableObjectChunk<String, Values> chunk =
                        WritableObjectChunk.makeWritableChunk(table.intSize())) {
            ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
            column0.fillChunk(fillContext, chunk, table.getRowSet());
            Assert.assertEquals(3, ArrowWrapperTools.Shareable.numBlocksLoaded());

            final String[] expectedValues = new String[] {
                    "hi",
                    "that some one is you",
                    null,
                    "I'm fine",
                    "It is great",
                    "See you soon"
            };
            for (int ii = 0; ii < expectedValues.length; ++ii) {
                Assert.assertEquals(expectedValues[ii], chunk.get(ii));
                Assert.assertEquals(expectedValues[ii], column0.get(table.getRowSet().get(ii)));
            }
        }

        final ColumnSource<Double> column1 = table.getColumnSource("salary");
        try (final ChunkSource.FillContext fillContext = column1.makeFillContext(table.intSize());
                final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(table.intSize())) {
            ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
            column1.fillChunk(fillContext, chunk, table.getRowSet());
            Assert.assertEquals(3, ArrowWrapperTools.Shareable.numBlocksLoaded());

            final double[] expectedValues = new double[] {
                    8.98,
                    0,
                    QueryConstants.NULL_DOUBLE,
                    0.94,
                    7434.744,
                    -385.943,
            };

            for (int ii = 0; ii < expectedValues.length; ++ii) {
                Assert.assertEquals(expectedValues[ii], chunk.get(ii), 1e-5);
                Assert.assertEquals(expectedValues[ii], column1.getDouble(table.getRowSet().get(ii)), 1e-5);
            }
        }

        // noinspection unchecked
        final ColumnSource<Integer> column2 = table.getColumnSource("age");
        try (final ChunkSource.FillContext fillContext = column2.makeFillContext(table.intSize());
                WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(table.intSize())) {
            ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
            column2.fillChunk(fillContext, chunk, table.getRowSet());
            Assert.assertEquals(3, ArrowWrapperTools.Shareable.numBlocksLoaded());

            final int[] expectedValues = new int[] {
                    465,
                    5,
                    QueryConstants.NULL_INT,
                    0,
                    1922,
                    -63,
            };

            for (int ii = 0; ii < expectedValues.length; ++ii) {
                Assert.assertEquals(expectedValues[ii], chunk.get(ii));
                Assert.assertEquals(expectedValues[ii], column2.getInt(table.getRowSet().get(ii)));
            }
        }
    }

    @Test
    public void testHandleLargeMultiVectorFile() {
        File file = new File("file.arrow");
        try {
            final int totalAmount = 10000;
            final int stepSize = 1024;
            final Table expected = generateMultiVectorFile(file.getPath(), totalAmount / 2, totalAmount);
            final Table table = ArrowWrapperTools.readFeather(file.getPath());
            List<? extends ColumnSource<?>> columnSources = new ArrayList<>(table.getColumnSources());

            final ChunkSource.FillContext[] fcs = new ChunkSource.FillContext[columnSources.size()];
            // noinspection unchecked
            final WritableChunk<Values>[] writableChunks = (WritableChunk<Values>[]) columnSources.stream()
                    .map(cs -> cs.getChunkType().makeWritableChunk(stepSize)).toArray(WritableChunk[]::new);

            try (SafeCloseableArray<ChunkSource.FillContext> ignored1 = new SafeCloseableArray<>(fcs);
                    SafeCloseableArray<WritableChunk<Values>> ignored2 = new SafeCloseableArray<>(writableChunks);
                    SharedContext sharedContext = SharedContext.makeSharedContext();
                    RowSequence.Iterator iterator = table.getRowSet().getRowSequenceIterator()) {
                for (int ii = 0; ii < fcs.length; ++ii) {
                    fcs[ii] = columnSources.get(ii).makeFillContext(stepSize, sharedContext);
                }
                ArrowWrapperTools.Shareable.resetNumBlocksLoaded();

                int i = 0;
                while (iterator.hasMore()) {
                    RowSequence nextOrderedKeysWithLength = iterator.getNextRowSequenceWithLength(stepSize);
                    for (int ii = 0; ii < fcs.length; ii++) {
                        columnSources.get(ii).fillChunk(fcs[ii], writableChunks[ii], nextOrderedKeysWithLength);
                    }
                    for (int ii = 0; ii < writableChunks[0].size(); ii++) {
                        for (WritableChunk<Values> writableChunk : writableChunks) {
                            if (writableChunk instanceof WritableObjectChunk) {
                                Assert.assertEquals(Integer.toString(i + ii),
                                        writableChunk.asWritableObjectChunk().get(ii));
                            } else if (writableChunk instanceof WritableIntChunk) {
                                Assert.assertEquals(i + ii, writableChunk.asWritableIntChunk().get(ii));
                            } else {
                                Assert.assertEquals((i + ii) / 10.0, writableChunk.asWritableDoubleChunk().get(ii),
                                        (i + ii) / 1000.0);
                            }
                        }
                    }
                    i += stepSize;
                }
                Assert.assertEquals(6, ArrowWrapperTools.Shareable.numBlocksLoaded());
                assertTableEquals(expected, table);
            }
        } finally {
            // noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testHandleLargeMultiVectorFile_InterleaveWithGet() {
        File file = new File("file.arrow");
        try {
            final int totalAmount = 10000;
            final int stepSize = 1024;
            generateMultiVectorFile(file.getPath(), totalAmount / 2, totalAmount);
            Table table = ArrowWrapperTools.readFeather(file.getPath());
            List<? extends ColumnSource<?>> columnSources = new ArrayList<>(table.getColumnSources());

            final ChunkSource.FillContext[] fcs = new ChunkSource.FillContext[columnSources.size()];
            // noinspection unchecked
            final WritableChunk<Values>[] writableChunks = (WritableChunk<Values>[]) columnSources.stream()
                    .map(cs -> cs.getChunkType().makeWritableChunk(stepSize)).toArray(WritableChunk[]::new);

            try (SafeCloseableArray<ChunkSource.FillContext> ignored1 = new SafeCloseableArray<>(fcs);
                    SafeCloseableArray<WritableChunk<Values>> ignored2 = new SafeCloseableArray<>(writableChunks);
                    SharedContext sharedContext = SharedContext.makeSharedContext();
                    RowSequence.Iterator iterator = table.getRowSet().getRowSequenceIterator()) {
                for (int ii = 0; ii < fcs.length; ++ii) {
                    fcs[ii] = columnSources.get(ii).makeFillContext(stepSize, sharedContext);
                }
                ArrowWrapperTools.Shareable.resetNumBlocksLoaded();

                int i = 0;
                while (iterator.hasMore()) {
                    if (i < 4096) {
                        for (int ii = 0; ii < fcs.length; ii++) {
                            // no block switch
                            columnSources.get(ii).get(1024);
                        }
                    }

                    if (i > 5120) {
                        for (int ii = 0; ii < fcs.length; ii++) {
                            // no block switch
                            columnSources.get(ii).get(9000);
                        }
                    }

                    if (i == 5120) {
                        for (int ii = 0; ii < fcs.length; ii++) {
                            // block switch
                            columnSources.get(ii).get(0);
                        }
                    }

                    if (i == 1024) {
                        for (int ii = 0; ii < fcs.length; ii++) {
                            // block switch
                            columnSources.get(ii).get(9000);
                        }
                    }

                    RowSequence rowSeqStep = iterator.getNextRowSequenceWithLength(stepSize);
                    for (int ii = 0; ii < fcs.length; ii++) {
                        columnSources.get(ii).fillChunk(fcs[ii], writableChunks[ii], rowSeqStep);
                    }
                    for (int ii = 0; ii < writableChunks[0].size(); ii++) {
                        for (WritableChunk<Values> writableChunk : writableChunks) {
                            if (writableChunk instanceof WritableObjectChunk) {
                                Assert.assertEquals(Integer.toString(i + ii),
                                        writableChunk.asWritableObjectChunk().get(ii));
                            } else if (writableChunk instanceof WritableIntChunk) {
                                Assert.assertEquals(i + ii, writableChunk.asWritableIntChunk().get(ii));
                            } else {
                                Assert.assertEquals((i + ii) / 10.0, writableChunk.asWritableDoubleChunk().get(ii),
                                        (i + ii) / 1000.0);
                            }
                        }
                    }
                    i += stepSize;
                }
                Assert.assertEquals(10, ArrowWrapperTools.Shareable.numBlocksLoaded());
            }
        } finally {
            // noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testConcurrentSnapshots() {
        File file = new File("file.arrow");
        try {
            final int totalAmount = 10000;
            final Table expected = generateMultiVectorFile(file.getPath(), totalAmount / 2, totalAmount);
            final QueryTable readback = ArrowWrapperTools.readFeather(file.getPath());

            final Thread[] threads = new Thread[10];
            final BarrageMessage[] results = new BarrageMessage[10];
            try (final SafeCloseable ignored1 = () -> SafeCloseable.closeAll(results)) {
                // Lets simulate 10 threads trying to snapshot the table at the same time.
                // Each thread will start up and wait for the barrier, then they will all attempt
                // a snapshot at the same time and poke the countdown latch to release the test thread
                // Then we'll validate all the results and life will be great
                final CyclicBarrier barrier = new CyclicBarrier(10);
                final CountDownLatch latch = new CountDownLatch(10);
                final ExecutionContext executionContext = ExecutionContext.getContext();
                for (int ii = 0; ii < 10; ii++) {
                    final int threadNo = ii;
                    threads[ii] = new Thread(() -> {
                        try (final SafeCloseable ignored2 = executionContext.open()) {
                            barrier.await();
                            // noinspection resource
                            results[threadNo] = ConstructSnapshot.constructBackplaneSnapshot(new Object(), readback);
                        } catch (InterruptedException | BrokenBarrierException e) {
                            throw new RuntimeException(e);
                        } finally {
                            latch.countDown();
                        }
                    });
                    threads[ii].start();
                }

                latch.await();
                for (int ii = 0; ii < 10; ii++) {
                    verifySnapshotBarrageMessage(results[ii], expected);
                }
                readback.close();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            file.delete();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Table generateMultiVectorFile(final String path, final int batchSize, final int totalAmount) {
        try (BufferAllocator allocator = new RootAllocator()) {
            Field strs = new Field("strs", FieldType.nullable(new ArrowType.Utf8()), null);
            Field ints = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Field doubles = new Field("doubles",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Schema schemaPerson = new Schema(asList(strs, ints, doubles));
            try (
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)) {
                VarCharVector strVector = (VarCharVector) vectorSchemaRoot.getVector("strs");
                IntVector intVector = (IntVector) vectorSchemaRoot.getVector("ints");
                Float8Vector float8Vector = (Float8Vector) vectorSchemaRoot.getVector("doubles");

                File file = new File(path);
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        ArrowFileWriter writer =
                                new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())) {
                    writer.start();
                    int counter = 0;
                    while (counter < totalAmount) {
                        strVector.allocateNew(batchSize);
                        intVector.allocateNew(batchSize);
                        float8Vector.allocateNew(batchSize);
                        for (int i = 0; i < batchSize; i++) {
                            intVector.set(i, counter);
                            strVector.set(i, Integer.toString(counter).getBytes());
                            float8Vector.set(i, counter / 10.0);
                            vectorSchemaRoot.setRowCount(batchSize);
                            counter++;
                        }
                        writer.writeBatch();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // now generate a comparison table
            return TableTools.emptyTable(totalAmount)
                    .update("strs=Long.toString(ii)",
                            "ints=(int)ii",
                            "doubles=ii/10.0d");
        }
    }
}
