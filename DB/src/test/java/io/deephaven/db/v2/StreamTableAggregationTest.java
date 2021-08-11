package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.ShiftAwareListener.Update;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.qst.table.EmptyTable;
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

import static io.deephaven.db.v2.by.ComboAggregateFactory.*;

/**
 * Unit tests that exercise special aggregation semantics for stream tables.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
public class StreamTableAggregationTest extends JUnit4QueryTableTestBase {

    private static final long INPUT_SIZE = 100_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 10_000;

    private final Table source = Table.of(EmptyTable.of(INPUT_SIZE)
            .update("Sym = Long.toString(ii % 1000) + `_Sym`")
            .update("Price = ii / 100 - (ii % 100)")
            .update("Size = (long) (ii / 50 - (ii % 50))")
    );
    private final Table sourceGroupedOnSym = source.by("Sym").ungroup().flatten();

    {
        //noinspection unchecked
        final AbstractColumnSource<String> groupingSource = (AbstractColumnSource<String>) sourceGroupedOnSym.getColumnSource("Sym");
        groupingSource.setGroupToRange(AbstractColumnSource.getValueToRangeMap(sourceGroupedOnSym.getIndex(), groupingSource).entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                (final Map.Entry<String, long[]> entry) -> Index.CURRENT_FACTORY.getIndexByRange(entry.getValue()[0], entry.getValue()[1] - 1),
                Assert::neverInvoked,
                LinkedHashMap::new
        )));
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void doOperatorTest(@NotNull final UnaryOperator<Table> operator, final boolean grouped, final boolean expectStreamResult) {
        final QueryTable normal = new QueryTable(Index.FACTORY.getEmptyIndex(), (grouped ? sourceGroupedOnSym : source).getColumnSourceMap());
        normal.setRefreshing(true);

        final QueryTable stream = new QueryTable(Index.FACTORY.getEmptyIndex(), (grouped ? sourceGroupedOnSym : source).getColumnSourceMap());
        stream.setRefreshing(true);
        stream.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        TstUtils.assertTableEquals(normal, stream);

        final Table expected = operator.apply(normal);
        final Table actual = operator.apply(stream);
        TstUtils.assertTableEquals(expected, actual);
        TestCase.assertEquals(expectStreamResult, ((BaseTable) actual).isStream());

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)
        ).iterator();

        long usedSize = 0;
        Index lastInserted = Index.CURRENT_FACTORY.getEmptyIndex();
        while (usedSize < INPUT_SIZE) {
            final long refreshSize = Math.min(INPUT_SIZE - usedSize, refreshSizes.nextLong());
            final Index stepInserted = refreshSize == 0
                    ? Index.CURRENT_FACTORY.getEmptyIndex()
                    : Index.CURRENT_FACTORY.getIndexByRange(usedSize, usedSize + refreshSize - 1);

            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            try {
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (stepInserted.nonempty()) {
                        normal.getIndex().insert(stepInserted);
                        normal.notifyListeners(new Update(stepInserted, Index.CURRENT_FACTORY.getEmptyIndex(), Index.CURRENT_FACTORY.getEmptyIndex(), IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
                final Index finalLastInserted = lastInserted;
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (stepInserted.nonempty() || finalLastInserted.nonempty()) {
                        stream.getIndex().update(stepInserted, finalLastInserted);
                        stream.notifyListeners(new Update(stepInserted, finalLastInserted, Index.CURRENT_FACTORY.getEmptyIndex(), IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
                TstUtils.assertTableEquals(expected, actual);
            } finally {
                LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            }
            TstUtils.assertTableEquals(expected, actual);

            usedSize += refreshSize;
            lastInserted = stepInserted;
        }
    }

    @Test
    public void testFirstBy() {
        doOperatorTest(table -> table.firstBy("Sym"), false, false);
    }

    @Test
    public void testFirstByNoKeys() {
        doOperatorTest(Table::firstBy, false, false);
    }

    @Test
    public void testFirstByGrouped() {
        doOperatorTest(table -> table.firstBy("Sym"), true, false);
    }

    @Test
    public void testLastBy() {
        doOperatorTest(table -> table.lastBy("Sym"), false, false);
    }

    @Test
    public void testLastByNoKeys() {
        doOperatorTest(Table::lastBy, false, false);
    }

    @Test
    public void testLastByGrouped() {
        doOperatorTest(table -> table.lastBy("Sym"), true, false);
    }

    @Test
    public void testMinBy() {
        doOperatorTest(table -> table.minBy("Sym"), false, false);
    }

    @Test
    public void testMinByNoKeys() {
        doOperatorTest(Table::minBy, false, false);
    }

    @Test
    public void testMinByGrouped() {
        doOperatorTest(table -> table.minBy("Sym"), true, false);
    }

    @Test
    public void testMaxBy() {
        doOperatorTest(table -> table.maxBy("Sym"), false, false);
    }

    @Test
    public void testMaxByNoKeys() {
        doOperatorTest(Table::maxBy, false, false);
    }

    @Test
    public void testMaxByGrouped() {
        doOperatorTest(table -> table.maxBy("Sym"), true, false);
    }

    @Test
    public void testMedianBy() {
        doOperatorTest(table -> table.medianBy("Sym"), false, false);
    }

    @Test
    public void testMedianByNoKeys() {
        doOperatorTest(Table::medianBy, false, false);
    }

    @Test
    public void testMedianByGrouped() {
        doOperatorTest(table -> table.medianBy("Sym"), true, false);
    }

    @Test
    public void testComboBy() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastPrice=Price", "LastSize=Size"),
                AggMin("MinPrice=Price", "MinSize=Size"),
                AggMax("MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedPrice=Price", "MedSize=Size")
        ), "Sym"), false, false);
    }

    @Test
    public void testComboByNoKeys() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstSym=Sym", "FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastSym=Sym", "LastPrice=Price", "LastSize=Size"),
                AggMin("MinSym=Sym", "MinPrice=Price", "MinSize=Size"),
                AggMax("MaxSym=Sym", "MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedSym=Sym", "MedPrice=Price", "MedSize=Size")
        )), false, false);
    }

    @Test
    public void testComboByGrouped() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastPrice=Price", "LastSize=Size"),
                AggMin("MinPrice=Price", "MinSize=Size"),
                AggMax("MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedPrice=Price", "MedSize=Size")
        ), "Sym"), true, false);
    }
}
