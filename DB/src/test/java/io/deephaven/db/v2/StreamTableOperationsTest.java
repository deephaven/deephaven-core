package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.ShiftAwareListener.Update;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.RedirectionIndex;
import io.deephaven.db.v2.utils.WrappedIndexRedirectionIndexImpl;
import io.deephaven.qst.table.EmptyTable;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Unit tests that exercise optimized operations for stream tables.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class StreamTableOperationsTest extends JUnit4QueryTableTestBase {

    private static final long INPUT_SIZE = 100_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 10_000;

    private final Table source = Table.of(EmptyTable.of(INPUT_SIZE)
        .update("Sym = Long.toString(ii % 1000) + `_Sym`")
        .update("Price = ii / 100 - (ii % 100)")
        .update("Size = (long) (ii / 50 - (ii % 50))"));


    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Execute a table operator.
     *
     * @param operator The operator to apply
     * @param windowed Whether the stream table index should be a sliding window (if {@code true})
     *        or zero-based (if {@code false})
     * @param expectStreamResult Whether the result is expected to be a stream table
     */
    private void doOperatorTest(@NotNull final UnaryOperator<Table> operator,
        final boolean windowed, final boolean expectStreamResult) {
        final QueryTable normal =
            new QueryTable(Index.FACTORY.getEmptyIndex(), source.getColumnSourceMap());
        normal.setRefreshing(true);

        final Index streamInternalIndex;
        final Map<String, ? extends ColumnSource> streamSources;
        if (windowed) {
            streamInternalIndex = null;
            streamSources = source.getColumnSourceMap();
        } else {
            // Redirecting so we can present a zero-based Index from the stream table
            streamInternalIndex = Index.FACTORY.getEmptyIndex();
            final RedirectionIndex streamRedirections =
                new WrappedIndexRedirectionIndexImpl(streamInternalIndex);
            // noinspection unchecked
            streamSources = source.getColumnSourceMap().entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    (entry -> new ReadOnlyRedirectedColumnSource(streamRedirections,
                        entry.getValue())),
                    Assert::neverInvoked,
                    LinkedHashMap::new));
        }
        final QueryTable stream = new QueryTable(Index.FACTORY.getEmptyIndex(), streamSources);
        stream.setRefreshing(true);
        stream.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        TstUtils.assertTableEquals(normal, stream);

        final Table expected = operator.apply(normal);
        final Table streamExpected = operator.apply(stream);
        TstUtils.assertTableEquals(expected, streamExpected);
        TestCase.assertEquals(expectStreamResult, ((BaseTable) streamExpected).isStream());

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
            LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
            new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();

        int step = 0;
        long usedSize = 0;
        Index normalLastInserted = Index.CURRENT_FACTORY.getEmptyIndex();
        Index streamLastInserted = Index.CURRENT_FACTORY.getEmptyIndex();
        while (usedSize < INPUT_SIZE) {
            final long refreshSize = Math.min(INPUT_SIZE - usedSize, refreshSizes.nextLong());
            final Index normalStepInserted = refreshSize == 0
                ? Index.CURRENT_FACTORY.getEmptyIndex()
                : Index.CURRENT_FACTORY.getIndexByRange(usedSize, usedSize + refreshSize - 1);
            final Index streamStepInserted = streamInternalIndex == null ? normalStepInserted
                : refreshSize == 0
                    ? Index.CURRENT_FACTORY.getEmptyIndex()
                    : Index.CURRENT_FACTORY.getIndexByRange(0, refreshSize - 1);

            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            try {
                final Index finalNormalLastInserted = normalLastInserted;
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (normalStepInserted.nonempty() || finalNormalLastInserted.nonempty()) {
                        normal.getIndex().update(normalStepInserted, finalNormalLastInserted);
                        normal.notifyListeners(new Update(normalStepInserted,
                            finalNormalLastInserted, Index.CURRENT_FACTORY.getEmptyIndex(),
                            IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
                final Index finalStreamLastInserted = streamLastInserted;
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (streamStepInserted.nonempty() || finalStreamLastInserted.nonempty()) {
                        if (streamInternalIndex != null) {
                            streamInternalIndex.clear();
                            streamInternalIndex.insert(normalStepInserted);
                        }
                        stream.getIndex().clear();
                        stream.getIndex().insert(streamStepInserted);
                        stream.notifyListeners(new Update(streamStepInserted,
                            finalStreamLastInserted, Index.CURRENT_FACTORY.getEmptyIndex(),
                            IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
            } finally {
                LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            }
            try {
                TstUtils.assertTableEquals(expected, streamExpected);
            } catch (ComparisonFailure e) {
                System.err.printf("FAILURE: step %d, previousUsedSize %d, refreshSize %d%n", step,
                    usedSize, refreshSize);
                throw e;
            }

            ++step;
            usedSize += refreshSize;
            normalLastInserted = normalStepInserted;
            streamLastInserted = streamStepInserted;
        }
    }

    @Test
    public void testSortOneColumn() {
        doOperatorTest(table -> table.sort("Sym"), false, true);
    }

    @Test
    public void testSortMultipleColumns() {
        doOperatorTest(table -> table.sort("Price", "Sym"), false, true);
    }

    @Test
    public void testSortOneColumnWindowed() {
        doOperatorTest(table -> table.sort("Sym"), true, true);
    }

    @Test
    public void testSortMultipleColumnsWindowed() {
        doOperatorTest(table -> table.sort("Price", "Sym"), true, true);
    }
}
