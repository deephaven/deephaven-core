package io.deephaven.db.v2;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.impl.*;
import io.deephaven.db.v2.select.MatchFilter;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.regioned.*;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.deephaven.db.tables.utils.TableTools.*;

/**
 * Unit tests for {@link PartitionAwareSourceTable} with many partition types.
 */
public class TestPartitioningColumns {

    @Test
    public void testEverything() {
        final Table input = newTable(
                charCol("Ch", 'C', 'A', 'B'),
                byteCol("By", (byte) 1, (byte) 2, (byte) 3),
                shortCol("Sh", (short) 1, (short) 2, (short) 3),
                intCol("In", 1 << 20, 2 << 20, 3 << 20),
                longCol("Lo", 1L << 36, 2L << 36, 3L << 36),
                floatCol("Fl", 0.1f, 0.2f, 0.3f),
                doubleCol("Do", 0.1, 0.2, 0.3),
                dateTimeCol("DT", DBDateTime.now(), DBTimeUtils.plus(DBDateTime.now(), 1),
                        DBTimeUtils.plus(DBDateTime.now(), 2)),
                stringCol("St", "ABC", "DEF", "GHI"),
                col("Bo", Boolean.TRUE, Boolean.FALSE, Boolean.TRUE));

        final RecordingLocationKeyFinder<SimpleTableLocationKey> recordingLocationKeyFinder =
                new RecordingLocationKeyFinder<>();
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        final String[] partitionKeys = input.getDefinition().getColumnNamesArray();
        // noinspection unchecked
        final ColumnSource<? extends Comparable<?>>[] partitionValueSources =
                input.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        final int numColumns = partitionValueSources.length;
        input.getIndex().forAllLongs((final long indexKey) -> {
            for (int ci = 0; ci < numColumns; ++ci) {
                partitions.put(partitionKeys[ci], partitionValueSources[ci].get(indexKey));
            }
            recordingLocationKeyFinder.accept(new SimpleTableLocationKey(partitions));
        });

        final TableDefinition resultDefinition = new TableDefinition(input.getDefinition().getColumnStream()
                .map(ColumnDefinition::withPartitioning).collect(Collectors.toList()));
        final Table result = new PartitionAwareSourceTable(resultDefinition, "TestPartitioningColumns",
                RegionedTableComponentFactoryImpl.INSTANCE,
                new PollingTableLocationProvider<>(
                        StandaloneTableKey.getInstance(),
                        recordingLocationKeyFinder,
                        (tk, tlk, rs) -> {
                            final DummyTableLocation tl = new DummyTableLocation(tk, tlk);
                            tl.handleUpdate(Index.CURRENT_FACTORY.getFlatIndex(1), 1L);
                            return tl;
                        },
                        null),
                null);

        final Table expected = input.sort(input.getDefinition().getColumnNamesArray());

        TstUtils.assertTableEquals(expected, result);

        final SelectFilter[] filters = input.getDefinition().getColumnStream()
                .map(cd -> new MatchFilter(cd.getName(), (Object) null)).toArray(SelectFilter[]::new);
        TstUtils.assertTableEquals(expected.where(filters), result.where(filters));

        TstUtils.assertTableEquals(expected.selectDistinct(), result.selectDistinct());
    }

    private static final class DummyTableLocation extends AbstractTableLocation {

        protected DummyTableLocation(@NotNull final TableKey tableKey,
                @NotNull final TableLocationKey tableLocationKey) {
            super(tableKey, tableLocationKey, false);
        }

        @Override
        public void refresh() {

        }

        @NotNull
        @Override
        protected ColumnLocation makeColumnLocation(@NotNull String name) {
            return new ColumnLocation() {
                @NotNull
                @Override
                public TableLocation getTableLocation() {
                    return DummyTableLocation.this;
                }

                @NotNull
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public boolean exists() {
                    throw new UnsupportedOperationException();
                }

                @Nullable
                @Override
                public <METADATA_TYPE> METADATA_TYPE getMetadata(@NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionChar<Attributes.Values> makeColumnRegionChar(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionByte<Attributes.Values> makeColumnRegionByte(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionShort<Attributes.Values> makeColumnRegionShort(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionInt<Attributes.Values> makeColumnRegionInt(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionLong<Attributes.Values> makeColumnRegionLong(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionFloat<Attributes.Values> makeColumnRegionFloat(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnRegionDouble<Attributes.Values> makeColumnRegionDouble(
                        @NotNull ColumnDefinition<?> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <TYPE> ColumnRegionObject<TYPE, Attributes.Values> makeColumnRegionObject(
                        @NotNull ColumnDefinition<TYPE> columnDefinition) {
                    throw new UnsupportedOperationException();
                }

            };
        }
    }
}
