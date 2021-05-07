package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.RowSetter;

/**
 * An interface for RowSetters that obtain their values from JSON objects.
 */
public interface JsonRecordSetter {
    /**
     * A RowSetter that will set a value obtained from a {@link JsonRecord} as indicated in the
     * record by the key. The setter and the value it sets will be matched to the type of the
     * column.
     * @param record The {@link JsonRecord} from which to get the value.
     * @param key The property name by which to retrieve the value from the record.
     * @param setter The {@link io.deephaven.tablelogger.RowSetter} for which the set method will be called.
     */
    void set (JsonRecord record, String key, RowSetter setter);
}
