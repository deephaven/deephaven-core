package io.deephaven.db.v2.tuples;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.v2.sources.WritableSource;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for classes that know how to export the elements of a given tuple type. Currently supports element-wise
 * export to a {@link WritableSource} without unnecessary boxing, or full export to a "{@link SmartKey}" with the
 * necessary boxing.
 */
public interface TupleExporter<TUPLE_TYPE> {

    /**
     * Export a single element from the tuple, identified by its element index, to the destination index key of the
     * supplied writable source.
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param tuple The tuple to export an element from
     * @param elementIndex The element index to export
     * @param writableSource The destination
     * @param destinationIndexKey The destination index key
     */
    <ELEMENT_TYPE> void exportElement(TUPLE_TYPE tuple, int elementIndex,
            @NotNull WritableSource<ELEMENT_TYPE> writableSource, long destinationIndexKey);

    /**
     * Export a single element from the tuple, identified by its element index, to an Object
     * 
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param tuple The tuple to export an element from
     * @param elementIndex The element index to export
     */
    Object exportElement(TUPLE_TYPE tuple, int elementIndex);

    /**
     * Export a single element from the tuple, identified by its element index, to an Object. If the tuple has been
     * internally reinterpreted, return the reinterpreted value.
     *
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param tuple The tuple to export an element from
     * @param elementIndex The element index to export
     */
    default Object exportElementReinterpreted(TUPLE_TYPE tuple, int elementIndex) {
        return exportElement(tuple, elementIndex);
    }

    /**
     * <p>
     * Export this tuple's element list as a key suitable for the {@link io.deephaven.db.v2.TableMap}s resulting from
     * {@link io.deephaven.db.tables.Table#byExternal}.
     * <p>
     * For the empty tuple this is a unsupported.
     * <p>
     * For singles, this is the (boxed) sole element itself.
     * <p>
     * For doubles and longer, this is a newly-allocated "{@link SmartKey}".
     *
     * @param tuple The tuple to export all elements from
     * @return The new smart key
     */
    Object exportToExternalKey(TUPLE_TYPE tuple);
}
