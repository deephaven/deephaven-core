package io.deephaven.generic.region;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.engine.table.impl.select.SimulationClock;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link AppendOnlyFixedSizePageRegionChar} and its replicas.
 */
@Category(OutOfBandTest.class)
public class AppendOnlyFixedSizePageRegionTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testCorrectness() {
        final Instant startTime = Instant.now();
        final Instant endTime = DateTimeUtils.plus(startTime, 1_000_000_000L);
        final SimulationClock clock = new SimulationClock(startTime, endTime, 100_000_000L);
        final TimeTable[] timeTables = new TimeTable[] {
                new TimeTable(ExecutionContext.getContext().getUpdateGraph(), clock, startTime, 1000, false),
                new TimeTable(ExecutionContext.getContext().getUpdateGraph(), clock, startTime, 10000, false),
                new TimeTable(ExecutionContext.getContext().getUpdateGraph(), clock, startTime, 100000, false)
        };
        final Table[] withTypes = addTypes(timeTables);
        final DependentRegistrar dependentRegistrar = new DependentRegistrar(withTypes);
        final Table expected = makeMerged(withTypes);
        final Table actual = makeRegioned(dependentRegistrar, withTypes);
        System.out.println("Initial start time: " + clock.instantNanos());
        TstUtils.assertTableEquals(expected, actual);
        clock.start();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        while (!clock.done()) {
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.advance();
                for (final TimeTable timeTable : timeTables) {
                    timeTable.run();
                }
                dependentRegistrar.run();
            });
            System.out.println("Cycle start time: " + clock.instantNanos());
            TstUtils.assertTableEquals(expected, actual);
        }
    }

    private static Table[] addTypes(@NotNull final Table... tables) {
        return Arrays.stream(tables).map(AppendOnlyFixedSizePageRegionTest::addTypes).toArray(Table[]::new);
    }

    private static Table addTypes(@NotNull final Table table) {
        return table.updateView(
                "B    = ii % 1000  == 0  ? NULL_BYTE   : (byte)  ii",
                "C    = ii % 27    == 26 ? NULL_CHAR   : (char)  ('A' + ii % 27)",
                "S    = ii % 30000 == 0  ? NULL_SHORT  : (short) ii",
                "I    = ii % 512   == 0  ? NULL_INT    : (int)   ii",
                "L    = ii % 1024  == 0  ? NULL_LONG   :         ii",
                "F    = ii % 2048  == 0  ? NULL_FLOAT  : (float) (ii * 0.25)",
                "D    = ii % 4096  == 0  ? NULL_DOUBLE :         ii * 1.25",
                "Bl   = ii % 8192  == 0  ? null        :         ii % 2 == 0",
                "Str  = ii % 128   == 0  ? null        :         Long.toString(ii)");
    }

    private static Table makeMerged(@NotNull final Table... constituents) {
        return TableTools.merge(constituents);
    }

    private static Table makeRegioned(@NotNull final UpdateSourceRegistrar registrar,
            @NotNull final Table... constituents) {
        assertThat(constituents).isNotNull();
        assertThat(constituents).isNotEmpty();

        return new SimpleSourceTable(
                constituents[0].getDefinition(),
                "Test SimpleSourceTable",
                RegionedTableComponentFactoryImpl.INSTANCE,
                new TableBackedTableLocationProvider(registrar, constituents),
                registrar).coalesce();
    }

    private static final class DependentRegistrar implements UpdateSourceRegistrar, Runnable {

        private final NotificationQueue.Dependency[] dependencies;

        private final List<Runnable> dependentSources = new ArrayList<>();

        private DependentRegistrar(@NotNull final NotificationQueue.Dependency... dependencies) {
            this.dependencies = dependencies;
            ExecutionContext.getContext().getUpdateGraph().addSource(this);
        }

        @Override
        public synchronized void addSource(@NotNull final Runnable updateSource) {
            dependentSources.add(updateSource);
        }

        @Override
        public synchronized void removeSource(@NotNull final Runnable updateSource) {
            dependentSources.remove(updateSource);
        }

        @Override
        public void requestRefresh() {
            ExecutionContext.getContext().getUpdateGraph().requestRefresh();
        }

        @Override
        public void run() {
            ExecutionContext.getContext().getUpdateGraph().addNotification(new AbstractNotification(false) {
                @Override
                public boolean canExecute(final long step) {
                    synchronized (DependentRegistrar.this) {
                        return Arrays.stream(dependencies).allMatch(dependency -> dependency.satisfied(step));
                    }
                }

                @Override
                public void run() {
                    synchronized (DependentRegistrar.this) {
                        final int sourcesSize = dependentSources.size();
                        // Run the sources in reverse order, because the location listeners will be registered
                        // after the
                        for (int si = sourcesSize - 1; si >= 0; --si) {
                            dependentSources.get(si).run();
                        }
                    }
                }
            });
        }
    }

    private static final class TableBackedTableLocationProvider extends AbstractTableLocationProvider {

        private final UpdateSourceRegistrar registrar;

        private TableBackedTableLocationProvider(
                @NotNull final UpdateSourceRegistrar registrar,
                @NotNull final Table... tables) {
            super(StandaloneTableKey.getInstance(), false);
            this.registrar = registrar;
            final MutableInt nextId = new MutableInt();
            Arrays.stream(tables)
                    .map(table -> (QueryTable) table.coalesce().withAttributes(Map.of("ID", nextId.getAndIncrement())))
                    .peek(table -> Assert.assertion(table.isAppendOnly(), "table is append only"))
                    .map(TableBackedTableLocationKey::new)
                    .forEach(this::handleTableLocationKey);
        }

        @Override
        public void refresh() {}

        @Override
        protected @NotNull TableLocation makeTableLocation(@NotNull TableLocationKey locationKey) {
            return new TableBackedTableLocation(registrar, (TableBackedTableLocationKey) locationKey);
        }
    }

    private static final class TableBackedTableLocationKey implements ImmutableTableLocationKey {

        private static final String NAME = TableBackedTableLocationKey.class.getSimpleName();

        private final QueryTable table;

        private TableBackedTableLocationKey(@NotNull final QueryTable table) {
            this.table = table;
        }

        @Override
        public String getImplementationName() {
            return NAME;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append(NAME).append('[').append(table).append(']');
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public int compareTo(@NotNull final TableLocationKey other) {
            // noinspection DataFlowIssue
            return Integer.compare(
                    (int) table.getAttribute("ID"),
                    (int) ((TableBackedTableLocationKey) other).table.getAttribute("ID"));
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(table);
        }

        @Override
        public boolean equals(@Nullable final Object other) {
            return other == this ||
                    (other instanceof TableBackedTableLocationKey
                            && ((TableBackedTableLocationKey) other).table == table);
        }

        @Override
        public <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(
                @NotNull final String partitionKey) {
            throw new UnknownPartitionKeyException(partitionKey, this);
        }

        @Override
        public Set<String> getPartitionKeys() {
            return Collections.emptySet();
        }
    }

    private static final class TableBackedTableLocation extends AbstractTableLocation {

        private final UpdateSourceRegistrar registrar;

        private Runnable token;

        private TableBackedTableLocation(
                @NotNull final UpdateSourceRegistrar registrar,
                @NotNull TableBackedTableLocationKey tableLocationKey) {
            super(StandaloneTableKey.getInstance(), tableLocationKey, tableLocationKey.table.isRefreshing());
            this.registrar = registrar;
        }

        private QueryTable table() {
            return ((TableBackedTableLocationKey) getKey()).table;
        }

        @Override
        protected void activateUnderlyingDataSource() {
            registrar.addSource(token = this::refresh); // handleUpdate ignores "unchanged" state
            refresh();
            activationSuccessful(token);
        }

        @Override
        protected void deactivateUnderlyingDataSource() {
            registrar.removeSource(token);
            token = null;
        }

        @Override
        protected <T> boolean matchSubscriptionToken(final T token) {
            return token == this.token;
        }

        @Override
        public void refresh() {
            if (table().isFailed()) {
                if (token == null) {
                    throw new TableDataException("Can't refresh from a failed table");
                } else {
                    activationFailed(token, new TableDataException("Can't maintain subscription to a failed table"));
                }
            } else {
                handleUpdate(table().getRowSet().copy(), -1L);
            }
        }

        @Override
        protected @NotNull ColumnLocation makeColumnLocation(@NotNull final String name) {
            return new TableBackedColumnLocation(this, name);
        }

    }

    private static final class TableBackedColumnLocation
            extends AbstractColumnLocation
            implements AppendOnlyRegionAccessor<Values> {

        private static final int PAGE_SIZE = 1 << 16;

        private final ColumnSource<?> columnSource;

        private TableBackedColumnLocation(
                @NotNull final TableBackedTableLocation tableLocation,
                @NotNull final String name) {
            super(tableLocation, name);
            columnSource = tableLocation.table().getDefinition().getColumnNameMap().containsKey(name)
                    ? ReinterpretUtils.maybeConvertToPrimitive(tableLocation.table().getColumnSource(name))
                    : null;
        }

        @Override
        public boolean exists() {
            return columnSource != null;
        }

        @Override
        public <METADATA_TYPE> @Nullable METADATA_TYPE getMetadata(
                @NotNull final ColumnDefinition<?> columnDefinition) {
            return null;
        }

        @Override
        public ColumnRegionChar<Values> makeColumnRegionChar(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionChar<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionByte<Values> makeColumnRegionByte(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionByte<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionShort<Values> makeColumnRegionShort(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionShort<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionInt<Values> makeColumnRegionInt(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionInt<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionLong<Values> makeColumnRegionLong(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionLong<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionFloat<Values> makeColumnRegionFloat(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionFloat<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull final ColumnDefinition<?> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionDouble<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
                @NotNull final ColumnDefinition<TYPE> columnDefinition) {
            return new AppendOnlyFixedSizePageRegionObject<>(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, this);
        }

        @Override
        public long size() {
            return getTableLocation().getSize();
        }

        @Override
        public void readChunkPage(
                final long firstRowPosition,
                final int minimumSize,
                @NotNull final WritableChunk<Values> destination) {
            // @formatter:off
            try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(minimumSize);
                 final RowSequence rows =
                         RowSequenceFactory.forRange(firstRowPosition, firstRowPosition + minimumSize - 1)) {
                // @formatter:on
                columnSource.fillChunk(fillContext, destination, rows);
            }
        }
    }
}
