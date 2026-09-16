//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import com.google.protobuf.CodedInputStream;
import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.BarrageSubscriptionImpl.BarrageDataMarshaller;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.vectors.IntVectorColumnWrapper;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.GrpcMarshallingException;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.IntVector;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;

/**
 * Barrage round-trip coverage for column-type serialization: chunk types, non-Java-serializable types, nested arrays
 * and vectors, and dictionary-encoded columns.
 */
@Category(OutOfBandTest.class)
public class BarrageMessageTypeRoundTripTest extends BarrageMessageRoundTripTestBase {

    public void testAllUniqueChunkTypeColumnSourcesWithValidityBuffers() {
        testAllUniqueChunkTypeColumnSources(false);
    }

    public void testAllUniqueChunkTypeColumnSourcesWithDeephavenNulls() {
        testAllUniqueChunkTypeColumnSources(true);
    }

    private void testAllUniqueChunkTypeColumnSources(final boolean useDeephavenNulls) {
        this.useDeephavenNulls = useDeephavenNulls;

        final int MAX_STEPS = 100;
        for (int size : new int[] {10, 1000, 10000}) {
            SharedProducerForAllClients helper =
                    new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                        @Override
                        public void createTable() {
                            // note we use column name `Sym` instead of `objCol` for the groupBy op in #createNuggets
                            columnInfo = initColumnInfos(
                                    new String[] {"longCol", "intCol", "Sym", "byteCol", "doubleCol", "floatCol",
                                            "shortCol", "charCol", "boolCol", "strArrCol", "datetimeCol",
                                            "bytePrimArray", "intPrimArray"},
                                    new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                                    new IntGenerator(10, 100, 0.1),
                                    new SetGenerator<>("a", "b", "c", "d"), // covers object
                                    new ByteGenerator((byte) 0, (byte) 127, 0.1),
                                    new DoubleGenerator(100.1, 200.1, 0.1),
                                    new FloatGenerator(100.1f, 200.1f, 0.1),
                                    new ShortGenerator((short) 0, (short) 20000, 0.1),
                                    new CharGenerator('a', 'z', 0.1),
                                    new BooleanGenerator(0.2),
                                    new SetGenerator<>(new String[] {"a", "b"}, new String[] {"0", "1"},
                                            new String[] {}, null),
                                    new UnsortedInstantGenerator(
                                            DateTimeUtils.parseInstant("2020-02-14T00:00:00 NY"),
                                            DateTimeUtils.parseInstant("2020-02-25T00:00:00 NY")),
                                    // uses var binary encoding
                                    new ByteArrayGenerator(Byte.MIN_VALUE, Byte.MAX_VALUE, 0, 32),
                                    // uses var list encoding
                                    new IntArrayGenerator(0, Integer.MAX_VALUE, 0, 32));
                            sourceTable = getTable(size / 4, random, columnInfo);
                        }
                    };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getRowSet().lastRowKey());
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified =
                            RowSetFactory.fromRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified().writableCast().retain(helper.sourceTable.getRowSet());
                }
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        }
    }

    public void testAllUniqueNonJavaSerRoundTripTypesWithValidityBuffers() {
        testAllUniqueNonJavaSerRoundTripTypes(false);
    }

    public void testAllUniqueNonJavaSerRoundTripTypesWithDeephavenNulls() {
        testAllUniqueNonJavaSerRoundTripTypes(true);
    }

    private void testAllUniqueNonJavaSerRoundTripTypes(final boolean useDeephavenNulls) {
        this.useDeephavenNulls = useDeephavenNulls;

        final int MAX_STEPS = 100;
        for (int size : new int[] {10, 1000, 10000}) {
            SharedProducerForAllClients helper =
                    new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                        @Override
                        public void createTable() {
                            // note we use column name `Sym` instead of `objCol` for the groupBy op in #createNuggets
                            columnInfo = initColumnInfos(
                                    new String[] {"longCol", "intCol", "Sym", "byteCol", "doubleCol", "floatCol",
                                            "shortCol", "charCol", "boolCol", "strCol", "strArrCol", "datetimeCol",
                                            "zdtCol", "localTimeCol", "localDateCol"},
                                    new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                                    new IntGenerator(10, 100, 0.1),
                                    new SetGenerator<>("a", "b", "c", "d"), // covers strings
                                    new ByteGenerator((byte) 0, (byte) 127, 0.1),
                                    new DoubleGenerator(100.1, 200.1, 0.1),
                                    new FloatGenerator(100.1f, 200.1f, 0.1),
                                    new ShortGenerator((short) 0, (short) 20000, 0.1),
                                    new CharGenerator('a', 'z', 0.1),
                                    new BooleanGenerator(0.2),
                                    new StringGenerator(),
                                    new SetGenerator<>(new String[] {"a", "b"}, new String[] {"0", "1"},
                                            new String[] {}, null),
                                    new UnsortedInstantGenerator(
                                            DateTimeUtils.parseInstant("2020-02-14T00:00:00 NY"),
                                            DateTimeUtils.parseInstant("2020-02-25T00:00:00 NY")),
                                    new SetGenerator<>(
                                            DateTimeUtils.parseInstant("2025-11-13T00:00:00 NY")
                                                    .atZone(ZoneId.of("UTC")),
                                            DateTimeUtils.parseInstant("2025-11-14T00:00:00 NY")
                                                    .atZone(ZoneId.of("UTC")),
                                            null),
                                    new SetGenerator<>(
                                            LocalTime.of(10, 30, 45),
                                            LocalTime.of(14, 15, 30),
                                            LocalTime.of(22, 45, 0),
                                            null),
                                    new SetGenerator<>(
                                            LocalDate.of(2025, 11, 13),
                                            LocalDate.of(2025, 11, 14),
                                            LocalDate.of(2025, 11, 15),
                                            null));
                            sourceTable = getTable(size / 4, random, columnInfo);
                        }
                    };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getRowSet().lastRowKey());
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified =
                            RowSetFactory.fromRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified().writableCast().retain(helper.sourceTable.getRowSet());
                }
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        }
    }

    public void testNestedArrays() {
        // Two cases that aren't expected to work in the java client, since it doesn't have enough type info:
        // * triply nested groupBy(), where we have ObjectVector of ObjectVector of some vector
        // * double nested groupBy() where the inner type is not a primitive
        final Table queryTable = TableTools.emptyTable(1)
                // .update("triplevector_int=i", "triplevector_string=``").groupBy()
                .update("doublevector_int=i"/* , "doublevector_string=``" */).groupBy()
                .update("vector_int=i", "vector_string=``").groupBy()
                .update("array_int=new int[]{i}", "doublearray_int=new int[][] {{i}}",
                        "array_string=new String[]{``}",
                        "doublearray_string=new String[][]{{``}}");
        queryTable.setRefreshing(true);
        final BitSet allColumns = new BitSet(queryTable.getColumnSourceMap().size());
        for (int i = 0; i < queryTable.getColumnSourceMap().size(); i++) {
            allColumns.set(i);
        }
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);

        final RemoteClient remoteClient =
                remoteNugget.newClient(RowSetFactory.fromRange(0, 0), allColumns, "snapshot");
        remoteClient.setTransform(t -> {
            // Based on the column type, if we see an array turn it into a string. While this seems to mask the types
            // that we're getting over the wire, it doesn't function correctly without those types having arrived, so we
            // should still be correctly validating the table.
            return (QueryTable) t.updateView(t.getDefinition().getColumnNameMap().entrySet().stream()
                    .map(entry -> {
                        String colName = entry.getKey();
                        Class<?> type = entry.getValue().getDataType();
                        if (type.isArray()) {
                            return Selectable.of(
                                    ColumnName.of(colName),
                                    RawString
                                            .of("io.deephaven.server.barrage.BarrageMessageTypeRoundTripTest.arrayToString("
                                                    + colName + ")"));
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
        });
        // Obtain snapshot of original viewport.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("snapshot");
    }

    public void testFailingNestedVectors() {
        // Same as testNestedArrays, except we verify that we get expected exceptions for cases where BarrageTable can't
        // handle the type
        final Table failingTable = TableTools.emptyTable(1)
                .update("triplevector_int=i", "triplevector_string=``").groupBy()
                .update("doublevector_string=``").groupBy()
                .groupBy();

        failingTable.setRefreshing(true);
        for (int i = 0; i < failingTable.getColumnSourceMap().size(); i++) {
            BitSet oneColumn = new BitSet(failingTable.getColumnSourceMap().size());
            oneColumn.set(i);
            final RemoteNugget remoteNugget = new RemoteNugget(() -> failingTable);

            final RemoteClient remoteClient =
                    remoteNugget.newClient(RowSetFactory.fromRange(0, 0), oneColumn, "snapshot");
            flushProducerTable();
            remoteNugget.flushClientEvents();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

            assertNotNull(remoteClient.dummyObserver.failure);
            assertTrue(remoteClient.dummyObserver.failure.getCause() instanceof GrpcMarshallingException);
            assertTrue(remoteClient.dummyObserver.failure.getCause().getCause() instanceof StatusRuntimeException);
            StatusRuntimeException sre =
                    (StatusRuntimeException) remoteClient.dummyObserver.failure.getCause().getCause();
            assertTrue(sre.getMessage().contains("Column with type java.lang.Object cannot be serialized directly"));
        }
    }

    @ScriptApi
    public static String arrayToString(int[] arr) {
        return Arrays.toString(arr);
    }

    @ScriptApi
    public static String arrayToString(Object[] arr) {
        return Arrays.deepToString(arr);
    }

    /**
     * Builds a pojo {@link Schema} based on the natural schema of {@code def} except that the named column is annotated
     * with an Arrow {@link DictionaryEncoding} (Int32 index, dict id 0).
     * <p>
     * Accepts a {@link TableDefinition} rather than a live {@link Table} to avoid calling
     * {@link Table#getAttributes()}, which would publish the attribute map and prevent subsequent modification of the
     * attributes.
     */
    private static Schema buildDictEncodedSchema(final TableDefinition def, final String dictColumnName) {
        // Build the natural schema using an empty attributes map so getAttributes() is never called on the live table.
        final Schema natural = BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, def, Map.of(), false);
        final List<Field> fields = natural.getFields().stream().map(f -> {
            if (!dictColumnName.equals(f.getName())) {
                return f;
            }
            return new Field(f.getName(),
                    new FieldType(f.isNullable(), f.getType(),
                            new DictionaryEncoding(0L, false, new ArrowType.Int(32, true)),
                            f.getMetadata()),
                    f.getChildren());
        }).collect(Collectors.toList());
        return new Schema(fields, natural.getCustomMetadata());
    }

    /**
     * Builds a pojo {@link Schema} based on the natural schema of {@code def} except that the named column is doubly
     * encoded as {@code RunEndEncoded<Dictionary<...>>}: the parent is run-end encoded (Int32 run_ends) and its
     * {@code values} child carries an Arrow {@link DictionaryEncoding} (Int32 index, dict id 0).
     */
    private static Schema buildReeDictEncodedSchema(final TableDefinition def, final String colName) {
        final Schema natural = BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, def, Map.of(), false);
        final List<Field> fields = natural.getFields().stream().map(f -> {
            if (!colName.equals(f.getName())) {
                return f;
            }
            // values child: the original value field annotated with a dictionary encoding
            final Field values = new Field("values",
                    new FieldType(f.isNullable(), f.getType(),
                            new DictionaryEncoding(0L, false, new ArrowType.Int(32, true)), f.getMetadata()),
                    f.getChildren());
            final Field runEnds = new Field("run_ends",
                    new FieldType(false, new ArrowType.Int(32, true), null), Collections.emptyList());
            return new Field(f.getName(),
                    new FieldType(false, new ArrowType.RunEndEncoded(), null, f.getMetadata()),
                    List.of(runEnds, values));
        }).collect(Collectors.toList());
        return new Schema(fields, natural.getCustomMetadata());
    }

    /**
     * Creates a two-column refreshing QueryTable ({@code Sym} String + {@code intCol} int) and annotates {@code Sym} as
     * a doubly-encoded {@code RunEndEncoded<Dictionary<...>>} column via {@link Table#BARRAGE_SCHEMA_ATTRIBUTE}. The
     * low-cardinality {@code Sym} generator produces both runs (favoring REE) and few distinct values (favoring the
     * dictionary), exercising the combined encoding.
     */
    private QueryTable makeReeDictTable(final int initialSize, final Random random,
            final ColumnInfo<?, ?>[] columnInfoOut) {
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c"),
                new IntGenerator(0, 100));
        System.arraycopy(columnInfo, 0, columnInfoOut, 0, columnInfo.length);
        final QueryTable table = getTable(initialSize, random, columnInfo);
        table.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, buildReeDictEncodedSchema(table.getDefinition(), "Sym"));
        return table;
    }

    /**
     * Full ticking subscription over a doubly-encoded {@code RunEndEncoded<Dictionary<...>>} column. Across many steps
     * this exercises the nested dictionary's delta batches (new distinct values shipped as append-only DictionaryBatch
     * messages preceding each RecordBatch) and the reader's run-expansion of dictionary indices.
     */
    public void testReeDictionaryEncodedFullSubscriptionTicking() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeReeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "full-ree-dict");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    /**
     * Two full subscribers on the same producer share a single {@code DictionaryWriterRegistry}; the nested dictionary
     * of a {@code RunEndEncoded<Dictionary<...>>} column must still round-trip for both.
     */
    public void testReeDictionaryEncodedSharedProducer() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(1);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeReeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "full-ree-dict-1");
        nugget.newClient(null, allCols, "full-ree-dict-2");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    /**
     * Regression test: a subscription whose initial snapshot carries <em>zero rows</em> (e.g. a freshly created,
     * still-empty ticking table) must still emit an initial {@code isDelta=false} {@code DictionaryBatch} defining the
     * nested dictionary's id <em>before</em> the first {@code RecordBatch} that references it. Strict Arrow consumers
     * (such as the C++ client) fail or hang when the first RecordBatch references an undefined dictionary id, even with
     * zero rows.
     *
     * <p>
     * The Java reader is deliberately lenient here — {@code DictionaryChunkReader} skips dictionary resolution entirely
     * for a zero-row batch — so a plain round-trip cannot detect the defect. Instead this test asserts the wire-level
     * invariant directly, using the per-message {@link MessageHeader} types recorded by {@link DummyObserver}. Without
     * the empty-batch dictionary-registration threading in
     * {@code ChunkWriter.getEmptyInputStream(options, dictionaryRegistry)} (and its callers), the initial flush
     * contains no DictionaryBatch at all and this test fails.
     */
    public void testReeDictionaryEncodedEmptyInitialSnapshot() {
        final int steps = 10;
        final int size = 100;
        final Random random = new Random(2);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeReeDictTable(0, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(null, allCols, "ree-dict-empty-initial");

        // Deliver the initial snapshot; the table has no rows yet.
        flushProducerTable();

        final List<Byte> headerTypes = client.dummyObserver.observedHeaderTypes;
        final int firstDictionaryBatch = headerTypes.indexOf(MessageHeader.DictionaryBatch);
        final int firstRecordBatch = headerTypes.indexOf(MessageHeader.RecordBatch);
        assertTrue("initial snapshot must contain a RecordBatch", firstRecordBatch >= 0);
        assertTrue("initial snapshot of a dictionary-encoded subscription must emit a DictionaryBatch defining the "
                + "dictionary id before the first RecordBatch that references it, even when the snapshot has zero "
                + "rows; strict Arrow consumers (e.g. the C++ client) fail or hang otherwise. Observed header types: "
                + headerTypes,
                firstDictionaryBatch >= 0 && firstDictionaryBatch < firstRecordBatch);

        // The (lenient) Java replica should also be a valid empty table at this point.
        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        nugget.validate("initial empty snapshot");

        // Now grow the table from empty and confirm the stream stays healthy end-to-end.
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    /**
     * Builds a schema where {@code col1Name} and {@code col2Name} both carry {@link DictionaryEncoding} with the same
     * dictionary id (0). Two columns sharing a single id means a single {@code DictionaryBatch} per update covers both.
     */
    private static Schema buildSharedDictEncodedSchema(
            final TableDefinition def, final String col1Name, final String col2Name) {
        final Schema natural = BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, def, Map.of(), false);
        final DictionaryEncoding sharedEncoding =
                new DictionaryEncoding(0L, false, new ArrowType.Int(32, true));
        final List<Field> fields = natural.getFields().stream().map(f -> {
            if (!col1Name.equals(f.getName()) && !col2Name.equals(f.getName())) {
                return f;
            }
            return new Field(f.getName(),
                    new FieldType(f.isNullable(), f.getType(), sharedEncoding, f.getMetadata()),
                    f.getChildren());
        }).collect(Collectors.toList());
        return new Schema(fields, natural.getCustomMetadata());
    }

    /**
     * Creates a two-column refreshing QueryTable ({@code Sym} String + {@code intCol} int) and annotates {@code Sym}
     * with a dictionary encoding via {@link Table#BARRAGE_SCHEMA_ATTRIBUTE}. Returns the column-info array so callers
     * can drive incremental updates via {@link GenerateTableUpdates}.
     */
    private QueryTable makeDictTable(final int initialSize, final Random random,
            final ColumnInfo<?, ?>[] columnInfoOut) {
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c"),
                new IntGenerator(0, 100));
        System.arraycopy(columnInfo, 0, columnInfoOut, 0, columnInfo.length);
        final QueryTable table = getTable(initialSize, random, columnInfo);
        table.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, buildDictEncodedSchema(table.getDefinition(), "Sym"));
        return table;
    }

    /**
     * Creates a three-column refreshing QueryTable ({@code Sym1} String + {@code Sym2} String + {@code intCol} int)
     * where both string columns share dictionary id=0 via {@link #buildSharedDictEncodedSchema}.
     */
    private QueryTable makeSharedDictTable(final int initialSize, final Random random,
            final ColumnInfo<?, ?>[] columnInfoOut) {
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(
                new String[] {"Sym1", "Sym2", "intCol"},
                new SetGenerator<>("a", "b", "c"),
                new SetGenerator<>("x", "y", "z"),
                new IntGenerator(0, 100));
        System.arraycopy(columnInfo, 0, columnInfoOut, 0, columnInfo.length);
        final QueryTable table = getTable(initialSize, random, columnInfo);
        table.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE,
                buildSharedDictEncodedSchema(table.getDefinition(), "Sym1", "Sym2"));
        return table;
    }

    public void testDictionaryEncodedFullSubscriptionTicking() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "full-dict");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    public void testDictionaryEncodedSharedDictionaryAcrossFullSubscribers() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(1);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        // Two full subscribers on the same producer share a single DictionaryWriterRegistry
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "full-dict-1");
        nugget.newClient(null, allCols, "full-dict-2");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    /**
     * Verifies that two columns sharing the same Arrow dictionary id (id=0) are correctly encoded and decoded through a
     * full ticking subscription. The shared {@link io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry} must
     * emit exactly one {@code DictionaryBatch} per id per update even though two columns reference it, and both columns
     * must decode to their correct values.
     */
    public void testDictionaryEncodedSharedIdAcrossColumns() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(5);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[3];
        final QueryTable sourceTable = makeSharedDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "shared-id-full");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    public void testDictionaryEncodedViewportSubscriptionTicking() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(2);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        // Viewport subscriber gets its own private DictionaryWriterRegistry
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(RowSetFactory.fromRange(0, size / 10), allCols, "viewport-dict");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    public void testDictionaryEncodedGrowingSubscription() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(3);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        // Start as viewport subscription.
        nugget.newClient(RowSetFactory.fromRange(0, size / 10), allCols, "growing-dict");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Run a few steps as viewport
        for (int step = 0; step < 5; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("viewport step " + step);
        }

        // Add a full subscription after a few viewport steps.
        nugget.newClient(null, allCols, "growing-dict-full");

        // Run remaining steps with both clients active.
        for (int step = 5; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("mixed step " + step);
        }
    }

    /**
     * Verifies that when the cumulative dictionary size exceeds the live row count, the server resets the dictionary
     * (emitting {@code isDelta=false}) so the client stays consistent. Tests both full and viewport subscriptions.
     */
    public void testDictionaryEncodedOverflowCompaction() {
        // Use 50 distinct values so the dictionary fills up quickly.
        final String[] symValues = new String[50];
        for (int i = 0; i < symValues.length; i++) {
            symValues[i] = "val" + i;
        }

        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        // Start large enough that all 50 values are likely represented in the initial snapshot.
        final QueryTable sourceTable = makeDictTableWithManyValues(200, symValues, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Subscribe both a full client and a viewport client.
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allCols, "overflow-full");
        nugget.newClient(RowSetFactory.fromRange(0, 9), allCols, "overflow-viewport");

        // Flush the initial snapshot.
        flushProducerTable();
        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        nugget.validate("initial snapshot");

        // Remove most rows: keep only the first 5 live rows. The dictionary still has ~50 entries,
        // but the live row count drops to 5 — triggering overflow on the next propagation.
        final RowSet rowsToKeep = RowSetFactory.fromRange(0, 4);
        final RowSet rowsToRemove = sourceTable.getRowSet().minus(rowsToKeep);
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(sourceTable, rowsToRemove);
            sourceTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    rowsToRemove.copy(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY));
        });
        rowsToRemove.close();

        // propagateToSubscribers detects overflow (dict.size()~50 > liveRowCount=5) and resets the shared state.
        flushProducerTable();
        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        nugget.validate("after mass removal - overflow triggered");

        // Run a few more normal updates to confirm the dictionary rebuilds and correctness is maintained.
        final Random random = new Random(99);
        for (int step = 0; step < 10; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, 20, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("post-overflow step " + step);
        }
    }

    /**
     * Verifies that a viewport subscription with a dictionary-encoded column correctly updates its local
     * {@link io.deephaven.extensions.barrage.chunk.LocalDictionaryWriterState} when the viewport is shifted to rows
     * that contain previously-unseen dictionary values.
     *
     * <p>
     * This exercises the code path in {@code BarrageMessageProducer#propagateSnapshotForSubscription} that is reached
     * after {@link RemoteClient#setViewport} triggers a new snapshot: the snapshot uses the subscription's private
     * {@link io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry}, which must emit a fresh
     * {@code isDelta=false} DictionaryBatch for the new window of values.
     */
    public void testDictionaryEncodedViewportChange() {
        final int steps = 20;
        final int size = 100;
        final Random random = new Random(4);
        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo<?, ?>[2];
        final QueryTable sourceTable = makeDictTable(size / 4, random, columnInfo);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        // Start with a narrow viewport at the head of the table.
        final int vpSize = size / 10;
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(RowSetFactory.fromRange(0, vpSize - 1), allCols, "vp-dict");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Run a few steps with the initial viewport, then shift the viewport every few steps.
        // Shifting into new rows forces the producer to snapshot those rows; the dictionary must
        // include any values in the new window that the client has not previously seen.
        long vpStart = 0;
        for (int step = 0; step < steps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));

            // Every 4 steps, shift the viewport forward by the viewport size so it lands on a
            // completely disjoint set of rows — maximizing the chance of encountering new Sym values.
            if (step > 0 && step % 4 == 0) {
                vpStart = (vpStart + vpSize) % Math.max(1, sourceTable.size() - vpSize);
                final long finalVpStart = vpStart;
                client.setViewport(RowSetFactory.fromRange(finalVpStart, finalVpStart + vpSize - 1));
            }

            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    // ---- Dictionary-encoding ticking tests ----

    /**
     * Creates a two-column refreshing QueryTable with many distinct Sym values (one per row by construction) so the
     * dictionary accumulates quickly, enabling reliable overflow testing.
     */
    QueryTable makeDictTableWithManyValues(final int initialSize, final String[] symValues,
            final ColumnInfo<?, ?>[] columnInfoOut) {
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>(symValues),
                new IntGenerator(0, 1000));
        System.arraycopy(columnInfo, 0, columnInfoOut, 0, columnInfo.length);
        final QueryTable table = getTable(initialSize, new Random(42), columnInfo);
        table.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, buildDictEncodedSchema(table.getDefinition(), "Sym"));
        return table;
    }
}
