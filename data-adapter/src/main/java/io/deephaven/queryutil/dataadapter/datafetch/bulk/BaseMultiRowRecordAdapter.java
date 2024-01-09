package io.deephaven.queryutil.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.queryutil.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.queryutil.dataadapter.rec.json.JsonRecordAdapterGenerator;

/**
 * Adapter to convert multiple rows of table data into instances of {@code T}.
 * <p>
 * The {@link DefaultMultiRowRecordAdapter} implementation will work for all datatypes. More efficient implementations
 * can be created manually (like {@link ExampleGeneratedMultiRowDataArrayRetriever}) or generated (as
 * {@link JsonRecordAdapterGenerator} does for JSON {@code ObjectNode} records).
 *
 * @param <T> The record data type.
 */
public abstract class BaseMultiRowRecordAdapter<T> implements MultiRowRecordAdapter<T> {

    /**
     * The runtime type of new records (determined from {@link RecordAdapterDescriptor#getEmptyRecord()
     * descriptor.getEmptyRecord()}).
     */
    protected final Class<T> recordType;

    protected final int nCols;

    /**
     * Utility to retrieve chunks of data from column sources.
     */
    protected final TableDataArrayRetriever tableDataArrayRetriever;

    public BaseMultiRowRecordAdapter(final Table sourceTable, final RecordAdapterDescriptor<T> descriptor) {
        // noinspection unchecked
        this(
                TableDataArrayRetriever.makeDefault(descriptor.getColumnAdapters().keySet().stream()
                        .map(sourceTable::getColumnSource).toArray(ColumnSource[]::new)),
                descriptor.getColumnAdapters().size(),
                (Class<T>) descriptor.getEmptyRecord().getClass());
    }

    public BaseMultiRowRecordAdapter(final TableDataArrayRetriever tableDataArrayRetriever, final int nCols,
            Class<T> recordType) {
        this.nCols = nCols;
        this.tableDataArrayRetriever = tableDataArrayRetriever;
        this.recordType = recordType;
    }

    public TableDataArrayRetriever getTableDataArrayRetriever() {
        return tableDataArrayRetriever;
    }

    public T[] createRecordsFromData(final Object[] recordDataArrs, final int nRecords) {
        Assert.eq(recordDataArrs.length, "recordDataArrs.length", nCols, "nCols");
        final T[] results = createEmptyRecordsArr(nRecords);

        // Create the records
        for (int ii = 0; ii < results.length; ii++) {
            results[ii] = createEmptyRecord();
        }

        populateRecords(results, recordDataArrs);

        return results;
    }
}
