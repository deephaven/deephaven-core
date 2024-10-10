//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.locations.impl.*;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.rowset.RowSetFactory;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.deephaven.engine.util.TableTools.*;

/**
 * Unit tests for {@link PartitionAwareSourceTable} with many partition types.
 */
public class TestPartitioningColumns {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

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
                instantCol("DT", DateTimeUtils.now(), DateTimeUtils.plus(DateTimeUtils.now(), 1),
                        DateTimeUtils.plus(DateTimeUtils.now(), 2)),
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
        input.getRowSet().forAllRowKeys((final long indexKey) -> {
            for (int ci = 0; ci < numColumns; ++ci) {
                partitions.put(partitionKeys[ci], partitionValueSources[ci].get(indexKey));
            }
            recordingLocationKeyFinder.accept(new SimpleTableLocationKey(partitions));
        });

        final TableDefinition resultDefinition = TableDefinition.of(input.getDefinition().getColumnStream()
                .map(ColumnDefinition::withPartitioning).collect(Collectors.toList()));
        final Table result = new PartitionAwareSourceTable(resultDefinition, "TestPartitioningColumns",
                RegionedTableComponentFactoryImpl.INSTANCE,
                new PollingTableLocationProvider<>(
                        StandaloneTableKey.getInstance(),
                        recordingLocationKeyFinder,
                        (tk, tlk, rs) -> {
                            final DummyTableLocation tl = new DummyTableLocation(tk, tlk);
                            tl.handleUpdate(RowSetFactory.flat(1), 1L);
                            return tl;
                        },
                        null,
                        TableUpdateMode.STATIC,
                        TableUpdateMode.STATIC),
                null);

        for (String colName : partitionKeys) {
            final DataIndex fullIndex = DataIndexer.getDataIndex(result, colName);
            Assert.neqNull(fullIndex, "fullIndex");

            final ColumnSource<?>[] columns = new ColumnSource<?>[] {result.getColumnSource(colName)};

            final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup(columns);
            final ColumnSource<RowSet> fullIndexRowSetColumn = fullIndex.rowSetColumn();

            ChunkSource.WithPrev<?> tableKeys = DataIndexUtils.makeBoxedKeySource(columns);

            // Iterate through the entire source table and verify the lookup row set is valid and contains this row.
            try (final RowSet.Iterator rsIt = result.getRowSet().iterator();
                    final CloseableIterator<Object> keyIt =
                            ChunkedColumnIterator.make(tableKeys, result.getRowSet())) {

                while (rsIt.hasNext() && keyIt.hasNext()) {
                    final long rowKey = rsIt.nextLong();
                    final Object key = keyIt.next();

                    // Verify the row sets at the lookup keys match.
                    final long fullRowKey = fullIndexRowKeyLookup.apply(key, false);
                    Assert.geqZero(fullRowKey, "fullRowKey");

                    final RowSet fullRowSet = fullIndexRowSetColumn.get(fullRowKey);
                    Assert.neqNull(fullRowSet, "fullRowSet");

                    Assert.eqTrue(fullRowSet.containsRange(rowKey, rowKey), "fullRowSet.containsRange(rowKey, rowKey)");
                }
            }
        }

        final Table expected = input.sort(input.getDefinition().getColumnNamesArray());

        TstUtils.assertTableEquals(expected, result);

        final List<WhereFilter> filters = input.getDefinition().getColumnStream()
                .map(cd -> new MatchFilter(MatchType.Regular, cd.getName(), (Object) null))
                .collect(Collectors.toList());
        TstUtils.assertTableEquals(expected.where(Filter.and(filters)), result.where(Filter.and(filters)));

        TstUtils.assertTableEquals(expected.selectDistinct(), result.selectDistinct());
    }
}
