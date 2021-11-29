package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.FileUtils;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.stringset.ArrayStringSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.stringset.StringSet;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.TableWithDefaults;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.TableMap;
import io.deephaven.parquet.table.layout.DeephavenNestedPartitionLayout;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.engine.table.impl.select.ReinterpretedColumn;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.codec.BigIntegerCodec;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.parquet.table.layout.DeephavenNestedPartitionLayout.PARQUET_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * High-level unit tests for {@link RegionedColumnSource} implementations of
 * {@link ColumnSource#fillChunk(ColumnSource.FillContext, WritableChunk, RowSequence)}.
 */
@Category(OutOfBandTest.class)
public class TestChunkedRegionedOperations {

    private static final long TABLE_SIZE = 100_000;
    private static final long STRIPE_SIZE = TABLE_SIZE / 10;

    private QueryScope originalScope;
    private File dataDirectory;

    private Table expected;
    private Table actual;

    public static final class SimpleSerializable implements Serializable {

        private final long value;
        private byte valueByte;

        public SimpleSerializable(final long value) {
            this.value = value;
            valueByte = (byte) value;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final SimpleSerializable that = (SimpleSerializable) other;
            return value == that.value && valueByte == that.valueByte;
        }
    }

    public static final class SimpleExternalizable implements Externalizable {

        private long value;
        private byte valueByte;

        @SuppressWarnings("unused")
        public SimpleExternalizable(final long value) {
            this.value = value;
            valueByte = (byte) value;
        }

        public SimpleExternalizable() {}

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final SimpleExternalizable that = (SimpleExternalizable) other;
            return value == that.value && valueByte == that.valueByte;
        }

        @Override
        public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
            out.writeLong(value);
            out.writeByte(valueByte);
        }

        @Override
        public void readExternal(@NotNull final ObjectInput in) throws IOException {
            value = in.readLong();
            valueByte = in.readByte();
        }
    }

    @Before
    public void setUp() throws Exception {
        originalScope = QueryScope.getScope();
        final QueryScope queryScope = new QueryScope.StandaloneImpl();
        Arrays.stream(originalScope.getParams(originalScope.getParamNames()))
                .forEach(p -> queryScope.putParam(p.getName(), p.getValue()));
        queryScope.putParam("nowNanos", DateTimeUtils.currentTime().getNanos());
        queryScope.putParam("letters",
                IntStream.range('A', 'A' + 64).mapToObj(c -> new String(new char[] {(char) c})).toArray(String[]::new));
        queryScope.putParam("emptySymbolSet", new ArrayStringSet());
        queryScope.putParam("stripeSize", STRIPE_SIZE);
        QueryScope.setScope(queryScope);

        QueryLibrary.resetLibrary();
        QueryLibrary.importClass(BigInteger.class);
        QueryLibrary.importClass(StringSet.class);
        QueryLibrary.importClass(ArrayStringSet.class);
        QueryLibrary.importClass(SimpleSerializable.class);
        QueryLibrary.importClass(SimpleExternalizable.class);
        QueryLibrary.importStatic(BooleanUtils.class);

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofLong("II"),
                ColumnDefinition.ofString("PC").withPartitioning(),
                ColumnDefinition.ofByte("B"),
                ColumnDefinition.ofChar("C"),
                ColumnDefinition.ofShort("S"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofFloat("F"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofBoolean("Bl"),
                ColumnDefinition.ofString("Sym"),
                ColumnDefinition.ofString("Str"),
                ColumnDefinition.ofTime("DT"),
                ColumnDefinition.fromGenericType("SymS", StringSet.class),
                ColumnDefinition.fromGenericType("Ser", SimpleSerializable.class),
                ColumnDefinition.fromGenericType("Ext", SimpleExternalizable.class),
                ColumnDefinition.fromGenericType("Fix", BigInteger.class),
                ColumnDefinition.fromGenericType("Var", BigInteger.class));
        final ParquetInstructions parquetInstructions = new ParquetInstructions.Builder()
                .addColumnCodec("Fix", BigIntegerCodec.class.getName(), "4")
                .addColumnCodec("Var", BigIntegerCodec.class.getName())
                .useDictionary("Sym", true)
                .setMaximumDictionaryKeys(100) // Force "Str" to use non-dictionary encoding
                .build();

        final Table inputData = ((QueryTable) TableTools.emptyTable(TABLE_SIZE)
                .update(
                        "II   = ii")
                .updateView(
                        "PC   = Long.toString((long) (II / stripeSize))",
                        "B    = II % 1000  == 0  ? NULL_BYTE   : (byte)  II",
                        "C    = II % 27    == 26 ? NULL_CHAR   : (char)  ('A' + II % 27)",
                        "S    = II % 30000 == 0  ? NULL_SHORT  : (short) II",
                        "I    = II % 512   == 0  ? NULL_INT    : (int)   II",
                        "L    = II % 1024  == 0  ? NULL_LONG   :         II",
                        "F    = II % 2048  == 0  ? NULL_FLOAT  : (float) (II * 0.25)",
                        "D    = II % 4096  == 0  ? NULL_DOUBLE :         II * 1.25",
                        "Bl   = II % 8192  == 0  ? null        :         II % 2 == 0",
                        "Sym  = II % 64    == 0  ? null        :         Long.toString(II % 1000)",
                        "Str  = II % 128   == 0  ? null        :         Long.toString(II)",
                        "DT   = II % 256   == 0  ? null        :         new DateTime(nowNanos + II)",
                        "SymS = (StringSet) new ArrayStringSet(letters[((int) II) % 64], letters[(((int) II) + 7) % 64])",
                        "Ser  = II % 1024  == 0  ? null        : new SimpleSerializable(II)",
                        "Ext  = II % 1024  == 0  ? null        : new SimpleExternalizable(II)",
                        "Fix  = Sym == null      ? null        : new BigInteger(Sym, 10)",
                        "Var  = Str == null      ? null        : new BigInteger(Str, 10)"))
                                .withDefinitionUnsafe(definition);
        // TODO: Add (Fixed|Variable)WidthObjectCodec columns

        final Table inputMissingData = ((QueryTable) TableTools.emptyTable(TABLE_SIZE)
                .update(
                        "II   = ii")
                .updateView(
                        "PC   = `N` + Long.toString((long) (II / stripeSize))",
                        "B    = NULL_BYTE",
                        "C    = NULL_CHAR",
                        "S    = NULL_SHORT",
                        "I    = NULL_INT",
                        "L    = NULL_LONG",
                        "F    = NULL_FLOAT",
                        "D    = NULL_DOUBLE",
                        "Bl   = (Boolean) null",
                        "Sym  = (String) null",
                        "Str  = (String) null",
                        "DT   = (DateTime) null",
                        "SymS = (StringSet) null",
                        "Ser  = (SimpleSerializable) null",
                        "Ext  = (SimpleExternalizable) null",
                        "Fix  = (BigInteger) null",
                        "Var  = (BigInteger) null")).withDefinitionUnsafe(definition);

        dataDirectory = Files.createTempDirectory(Paths.get(""), "TestChunkedRegionedOperations-").toFile();
        dataDirectory.deleteOnExit();

        final TableDefinition partitionedDataDefinition = new TableDefinition(inputData.getDefinition());

        final TableDefinition partitionedMissingDataDefinition =
                new TableDefinition(inputData.view("PC", "II").getDefinition());

        final String tableName = "TestTable";

        final TableMap partitionedInputData = inputData.partitionBy("PC");
        ParquetTools.writeParquetTables(
                partitionedInputData.values().toArray(TableWithDefaults.ZERO_LENGTH_TABLE_ARRAY),
                partitionedDataDefinition.getWritable(),
                parquetInstructions,
                Arrays.stream(partitionedInputData.getKeySet())
                        .map(pcv -> new File(dataDirectory,
                                "IP" + File.separator + "P" + pcv + File.separator + tableName + File.separator
                                        + PARQUET_FILE_NAME))
                        .toArray(File[]::new),
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY);

        final TableMap partitionedInputMissingData = inputMissingData.view("PC", "II").partitionBy("PC");
        ParquetTools.writeParquetTables(
                partitionedInputMissingData.values().toArray(TableWithDefaults.ZERO_LENGTH_TABLE_ARRAY),
                partitionedMissingDataDefinition.getWritable(),
                parquetInstructions,
                Arrays.stream(partitionedInputMissingData.getKeySet())
                        .map(pcv -> new File(dataDirectory,
                                "IP" + File.separator + "P" + pcv + File.separator + tableName + File.separator
                                        + PARQUET_FILE_NAME))
                        .toArray(File[]::new),
                CollectionUtil.ZERO_LENGTH_STRING_ARRAY);

        expected = TableTools
                .merge(
                        inputData.updateView("PC = `P` + PC"),
                        inputMissingData.updateView("PC = `P` + PC"))
                .updateView(
                        "Bl_R = booleanAsByte(Bl)",
                        "DT_R = nanos(DT)");

        actual = ParquetTools.readPartitionedTable(
                DeephavenNestedPartitionLayout.forParquet(dataDirectory, tableName, "PC", null),
                ParquetInstructions.EMPTY,
                partitionedDataDefinition).updateView(
                        new ReinterpretedColumn<>("Bl", Boolean.class, "Bl_R", byte.class),
                        new ReinterpretedColumn<>("DT", DateTime.class, "DT_R", long.class))
                .coalesce();
    }

    @After
    public void tearDown() throws Exception {

        if (expected != null) {
            expected.releaseCachedResources();
        }
        if (actual != null) {
            actual.releaseCachedResources();
        }

        QueryScope.setScope(originalScope);
        QueryLibrary.resetLibrary();

        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            int tries = 0;
            boolean success = false;
            do {
                try {
                    FileUtils.deleteRecursively(dataDirectory);
                    success = true;
                } catch (Exception e) {
                    System.gc();
                    tries++;
                }
            } while (!success && tries < 10);
            TestCase.assertTrue(success);
        }
    }

    @Test
    public void testEqual() {
        assertTableEquals(expected, actual);
    }

    private static void assertChunkWiseEquals(@NotNull final Table expected, @NotNull final Table actual,
            final int chunkCapacity) {
        boolean first = true;
        assertEquals(expected.size(), actual.size());
        try (final SafeCloseableList closeables = new SafeCloseableList();
                final RowSequence.Iterator expectedIterator = expected.getRowSet().getRowSequenceIterator();
                final RowSequence.Iterator actualIterator = actual.getRowSet().getRowSequenceIterator()) {
            final ChunkType[] chunkTypes = expected.getDefinition().getColumnStream().map(ColumnDefinition::getDataType)
                    .map(ChunkType::fromElementType).toArray(ChunkType[]::new);
            final Equals[] equals = Arrays.stream(chunkTypes).map(Equals::make).toArray(Equals[]::new);

            // noinspection unchecked
            final WritableChunk<Values>[] expectedChunks = Arrays.stream(chunkTypes)
                    .map(ct -> ct.makeWritableChunk(chunkCapacity)).toArray(WritableChunk[]::new);
            // noinspection unchecked
            final WritableChunk<Values>[] actualChunks = Arrays.stream(chunkTypes)
                    .map(ct -> ct.makeWritableChunk(chunkCapacity)).toArray(WritableChunk[]::new);

            final ColumnSource[] expectedSources =
                    expected.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
            final ColumnSource[] actualSources =
                    actual.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

            final ColumnSource.FillContext[] expectedContexts =
                    Arrays.stream(expectedSources).map(cs -> closeables.add(cs.makeFillContext(chunkCapacity)))
                            .toArray(ColumnSource.FillContext[]::new);
            final ColumnSource.FillContext[] actualContexts =
                    Arrays.stream(actualSources).map(cs -> closeables.add(cs.makeFillContext(chunkCapacity)))
                            .toArray(ColumnSource.FillContext[]::new);

            assertEquals(expectedChunks.length, expectedContexts.length);
            assertEquals(actualChunks.length, actualContexts.length);

            while (expectedIterator.hasMore()) {
                assertTrue(actualIterator.hasMore());
                final RowSequence expectedKeys = expectedIterator.getNextRowSequenceWithLength(chunkCapacity);
                final RowSequence actualKeys = actualIterator.getNextRowSequenceWithLength(chunkCapacity);
                for (int ci = 0; ci < expectedChunks.length; ++ci) {
                    final Equals equal = equals[ci];
                    final WritableChunk<Values> expectedChunk = expectedChunks[ci];
                    final WritableChunk<Values> actualChunk = actualChunks[ci];

                    if (first) {
                        // Let's exercise the legacy get code, too, while we're in here

                        ((AbstractColumnSource) expectedSources[ci]).defaultFillChunk(expectedContexts[ci],
                                expectedChunk, expectedKeys);
                        ((AbstractColumnSource) actualSources[ci]).defaultFillChunk(actualContexts[ci], actualChunk,
                                actualKeys);

                        assertEquals(expectedChunk.size(), actualChunk.size());
                        for (int ei = 0; ei < expectedChunk.size(); ++ei) {
                            assertTrue(equal.equals(expectedChunk, actualChunk, ei));
                        }
                    }

                    assertEquals(expectedKeys.size(), actualKeys.size());

                    expectedSources[ci].fillChunk(expectedContexts[ci], expectedChunk, expectedKeys);
                    actualSources[ci].fillChunk(actualContexts[ci], actualChunk, actualKeys);

                    assertEquals(expectedKeys.size(), expectedChunk.size());
                    assertEquals(actualKeys.size(), actualChunk.size());

                    assertEquals(expectedChunk.size(), actualChunk.size());
                    for (int ei = 0; ei < expectedChunk.size(); ++ei) {
                        assertTrue(equal.equals(expectedChunk, actualChunk, ei));
                    }
                }
                first = false;
            }
        }
    }

    @FunctionalInterface
    private interface Equals {

        boolean equals(@NotNull Chunk<Values> expected, @NotNull Chunk<Values> actual, int index);

        static Equals make(@NotNull final ChunkType chunkType) {
            switch (chunkType) {
                case Boolean:
                    return (e, a, i) -> e.asBooleanChunk().get(i) == a.asBooleanChunk().get(i);
                case Byte:
                    return (e, a, i) -> e.asByteChunk().get(i) == a.asByteChunk().get(i);
                case Char:
                    return (e, a, i) -> e.asCharChunk().get(i) == a.asCharChunk().get(i);
                case Int:
                    return (e, a, i) -> e.asIntChunk().get(i) == a.asIntChunk().get(i);
                case Short:
                    return (e, a, i) -> e.asShortChunk().get(i) == a.asShortChunk().get(i);
                case Long:
                    return (e, a, i) -> e.asLongChunk().get(i) == a.asLongChunk().get(i);
                case Float:
                    return (e, a, i) -> e.asFloatChunk().get(i) == a.asFloatChunk().get(i);
                case Double:
                    return (e, a, i) -> e.asDoubleChunk().get(i) == a.asDoubleChunk().get(i);
                case Object:
                    return (e, a, i) -> Objects.equals(e.asObjectChunk().get(i), a.asObjectChunk().get(i));
            }
            throw new IllegalArgumentException("Unknown ChunkType " + chunkType);
        }
    }

    @Test
    public void testFullTableFullChunks() {
        assertChunkWiseEquals(expected, actual, expected.intSize());
    }

    @Test
    public void testFullTableNormalChunks() {
        assertChunkWiseEquals(expected, actual, 4096);
    }

    @Test
    public void testFullTableSmallChunks() {
        assertChunkWiseEquals(expected, actual, 8);
    }

    @Test
    public void testHalfDenseTableFullChunks() {
        assertChunkWiseEquals(expected.where("(ii / 100) % 2 == 0"), actual.where("(ii / 100) % 2 == 0"),
                expected.intSize());
    }

    @Test
    public void testHalfDenseTableNormalChunks() {
        assertChunkWiseEquals(expected.where("(ii / 100) % 2 == 0"), actual.where("(ii / 100) % 2 == 0"), 4096);
    }

    @Test
    public void testHalfDenseTableSmallChunks() {
        assertChunkWiseEquals(expected.where("(ii / 100) % 2 == 0"), actual.where("(ii / 100) % 2 == 0"), 8);
    }

    @Test
    public void testSparseTableFullChunks() {
        assertChunkWiseEquals(expected.where("ii % 2 == 0"), actual.where("ii % 2 == 0"), expected.intSize());
    }

    @Test
    public void testSparseTableNormalChunks() {
        assertChunkWiseEquals(expected.where("ii % 2 == 0"), actual.where("ii % 2 == 0"), 4096);
    }

    @Test
    public void testSparseTableSmallChunks() {
        assertChunkWiseEquals(expected.where("ii % 2 == 0"), actual.where("ii % 2 == 0"), 8);
    }

    @Test
    public void testEqualSymbols() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/949): Uncomment this once we write encoding stats
        // //noinspection unchecked
        // final SymbolTableSource<String> symbolTableSource = (SymbolTableSource<String>)
        // actual.getColumnSource("Sym");
        //
        // assertTrue(symbolTableSource.hasSymbolTable(actual.build()));
        // final Table symbolTable = symbolTableSource.getStaticSymbolTable(actual.build(), false);
        //
        // assertTableEquals(expected.view("PC", "Sym").where("Sym != null").firstBy("PC", "Sym").dropColumns("PC"),
        // symbolTable.view("Sym = Symbol").where("Sym != null"));
        //
        // final Table joined = actual
        // .updateView(new ReinterpretedColumn<>("Sym", String.class, "SymId", long.class))
        // .where("SymId != NULL_LONG") // Symbol tables don't explicitly map the null ID
        // .exactJoin(symbolTable, "SymId=" + SymbolTableSource.ID_COLUMN_NAME, "DictionarySym=" +
        // SymbolTableSource.SYMBOL_COLUMN_NAME);
        // final Table joinedBad = joined.where("Sym != DictionarySym");
        // TestCase.assertTrue(joinedBad.isEmpty());
    }
}
