//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterGenerator;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter to convert multiple rows of table data into instances of {@code T}.
 * <p>
 * The {@link DefaultMultiRowRecordAdapter} implementation will work for all datatypes. More efficient implementations
 * can be created manually or generated (as {@link JsonRecordAdapterGenerator} does for JSON {@code ObjectNode}
 * records).
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

    public BaseMultiRowRecordAdapter(final RecordAdapterDescriptor<T> descriptor,
            final TableDataArrayRetriever tableDataArrayRetriever) {
        // noinspection unchecked
        this(
                tableDataArrayRetriever,
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

    public T[] createRecordsFromData(@NotNull final Object[] recordDataArrs, final int nRecords) {
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
