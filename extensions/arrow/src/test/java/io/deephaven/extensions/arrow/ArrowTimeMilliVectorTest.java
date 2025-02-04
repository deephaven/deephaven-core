//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateArrowColumnSourceTests or "./gradlew generateArrowColumnTestSources" to regenerate
//
// @formatter:off
package io.deephaven.extensions.arrow;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.mutable.MutableInt;
import java.io.File;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ArrowTimeMilliVectorTest {
    private static final List<Long> expectedRows = Arrays.asList(0L, 1L, 2L, 4L, 8L, 9L);

    private static final LocalTime[] expectedValues = new LocalTime[] {java.time.LocalTime.of(0, 0, 0, 864000000), java.time.LocalTime.of(0, 0, 0, 908000000), null, java.time.LocalTime.of(0, 0, 0, 0), java.time.LocalTime.of(0, 14, 34, 300000000), java.time.LocalTime.of(21, 59, 29, 26000000)};

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static QueryTable loadTable() {
        //noinspection ConstantConditions;
        final File dataFile = new File(ArrowTimeMilliVectorTest.class.getResource("/time_milli_vector.arrow").getFile());
        return ArrowWrapperTools.readFeather(dataFile.getPath());
    }

    @Test
    public void testReadArrowFile() {
        final QueryTable table = loadTable();
        Assert.assertEquals(expectedValues.length, table.intSize());

        // check that the expected rows are present;
        final List<Long> actualRows = new ArrayList<>();
        table.getRowSet().forAllRowKeys(actualRows::add);
        Assert.assertEquals(expectedRows, actualRows);

        Assert.assertEquals(1, table.getColumnSources().size());
        // noinspection OptionalGetWithoutIsPresent, unchecked;
        final ColumnSource<LocalTime> cs = (ColumnSource<LocalTime>)table.getColumnSources().stream().findFirst().get();

        ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
        final MutableInt pos = new MutableInt();
        table.getRowSet().forAllRowKeys(rowKey -> Assert.assertEquals(expectedValues[pos.getAndIncrement()], cs.get(rowKey)));
        Assert.assertEquals(3, ArrowWrapperTools.Shareable.numBlocksLoaded());
    }

    @Test
    public void testFillChunk() {
        final QueryTable table = loadTable();

        // noinspection OptionalGetWithoutIsPresent, unchecked;
        final ColumnSource<LocalTime> cs = (ColumnSource<LocalTime>)table.getColumnSources().stream().findFirst().get();

        try (final ChunkSource.FillContext fillContext = cs.makeFillContext(table.intSize());
            final WritableObjectChunk<LocalTime, Values> chunk = WritableObjectChunk.makeWritableChunk(table.intSize())) {

            ArrowWrapperTools.Shareable.resetNumBlocksLoaded();
            cs.fillChunk(fillContext, chunk, table.getRowSet());
            Assert.assertEquals(3, ArrowWrapperTools.Shareable.numBlocksLoaded());

            for (int ii = 0; ii < expectedValues.length; ++ii) {
                Assert.assertEquals(expectedValues[ii], chunk.get(ii));
            }
        }
    }
}
