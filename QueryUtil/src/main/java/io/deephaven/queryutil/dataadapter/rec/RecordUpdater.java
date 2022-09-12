package io.deephaven.queryutil.dataadapter.rec;

/**
 * Updates one field of a record of type {@code} R with a value of type {@code C}.
 * <p>
 * See {@link RecordUpdaters} for utility methods to create {@code RecordUpdater} instances.
 */
public interface RecordUpdater<R, C> {

    /**
     * Returns the source type {@code C} for this record updater (i.e. the data type that is taken as an input
     * to update a field of the record of type {@code R}).
     *
     * @return The supported source type of this record updater.
     */
    Class<C> getSourceType();

    // default updateRecord methods that throw exceptions. subclasses can implement for supported types.

    default void updateRecord(R record, C colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithChar(R record, char colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithByte(R record, byte colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithShort(R record, short colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithInt(R record, int colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithFloat(R record, float colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithLong(R record, long colValue) {
        throw new UnsupportedOperationException();
    }

    default void updateRecordWithDouble(R record, double colValue) {
        throw new UnsupportedOperationException();
    }

}
