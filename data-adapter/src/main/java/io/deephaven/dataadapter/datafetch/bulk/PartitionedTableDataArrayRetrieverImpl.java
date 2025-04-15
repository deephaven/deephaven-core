//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import gnu.trove.list.TLongList;
import io.deephaven.base.verify.Assert;
import io.deephaven.dataadapter.ContextHolder;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;

public final class PartitionedTableDataArrayRetrieverImpl extends AbstractTableDataArrayRetrieverImpl {

    private final ColumnSource<Table> constituentColumn;

    public PartitionedTableDataArrayRetrieverImpl(
            final PartitionedTable thePartitionedTable,
            final List<String> colNamesToRetrieve) {
        super(colNamesToRetrieve, thePartitionedTable.constituentDefinition());

        Assert.neqNull(thePartitionedTable, "thePartitionedTable");
        Assert.neqNull(colNamesToRetrieve, "colNamesToRetrieve");
        Assert.gtZero(colNamesToRetrieve.size(), "colNamesToRetrieve.size()");

        final String constituentColName = thePartitionedTable.constituentColumnName();
        constituentColumn = thePartitionedTable.table().getColumnSource(constituentColName, Table.class);

        final TableDefinition constituentDefinition = thePartitionedTable.constituentDefinition();

        // Ensure that all columns to retrieve are present in the constituent table
        constituentDefinition.checkHasColumns(colNamesToRetrieve);
    }

    private ColumnSource<?>[] getColumnSources(Table constituent) {
        final List<String> columnNames = getColumnNames();
        final int nCols = columnNames.size();
        final ColumnSource<?>[] constituentColSources = new ColumnSource[nCols];
        for (int i = 0; i < nCols; i++) {
            constituentColSources[i] = constituent.getColumnSource(columnNames.get(i));
        }
        return constituentColSources;
    }

    /**
     * @param usePrev                            Whether to retrieve prev data instead of current
     * @param partitionedTableConstituentsRowSet Row set of partitioned table constituents to retrieve data from
     * @param dataArrs                           Arrays to populate with table data (created by {@link #createDataArrays})
     * @param rowKeysList                        List to populate with row keys of the constituents from which rows were retrieved
     */
    @Override
    public void fillDataArrays(final boolean usePrev,
                               final RowSet partitionedTableConstituentsRowSet,
                               final Object[] dataArrs,
                               final TLongList rowKeysList) {
        // rowKeysList must be empty to start
        Assert.eqZero(rowKeysList.size(), "rowKeysList.size()");

        final int nConstituentsToRetrieve = partitionedTableConstituentsRowSet.intSize();
        final Table[] constituents = new Table[nConstituentsToRetrieve];
        final int[] constituentSizes = new int[nConstituentsToRetrieve];

        // First, determine the total number of rows to be retrieved across all constituents
        int totalSize = 0;
        {
            int constituentIdx = 0;
            for (RowSet.Iterator iter = partitionedTableConstituentsRowSet.iterator(); iter.hasNext(); ) {
                long nextConstituentRowKey = iter.nextLong();
                final Table nextConstituent = Objects.requireNonNull(constituentColumn.get(nextConstituentRowKey));
                final int constituentIntSize = nextConstituent.intSize();

                constituents[constituentIdx] = nextConstituent;
                constituentSizes[constituentIdx] = constituentIntSize;

                // Update the rowKeysList with this constituent's row key, for all slots in the dataArrs that will be filled with this constituent's data
                rowKeysList.fill(totalSize, totalSize + constituentIntSize, nextConstituentRowKey);

                totalSize = Math.addExact(totalSize, constituentIntSize);

                constituentIdx++;
            }
        }

        // Create the actual data arrays to be filled
        final @NotNull Object[] actualDataArrays = createDataArrays(totalSize);

        // Populate the passed-in dataArrs with the correctly-sized arrays from actualDataArrays
        System.arraycopy(actualDataArrays, 0, dataArrs, 0, actualDataArrays.length);

        // Iterate over the constituents and fill the data arrays with their data
        for (int constituentIdx = 0, dataArrOffset = 0; constituentIdx < constituents.length; constituentIdx++) {
            final Table nextConstituent = constituents[constituentIdx];
            final TrackingRowSet constituentRowSet = nextConstituent.getRowSet();

            final ColumnSource<?>[] columnSources = getColumnSources(nextConstituent);

            final int chunkSize = Math.min(MAX_CHUNK_SIZE, constituentRowSet.intSize());
            try (final ContextHolder contextHolder = new ContextHolder(chunkSize, columnSources)) {
                fillDataArrays(usePrev, constituentRowSet, columnSources, dataArrs, null, contextHolder, dataArrOffset);
            }

            dataArrOffset += constituentSizes[constituentIdx];
        }
    }

}
