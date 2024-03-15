//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for classes that know how to export the elements of a given tuple type. Currently, supports element-wise
 * export to a {@link WritableColumnSource} without unnecessary boxing.
 */
public interface TupleExporter<TUPLE_TYPE> {

    /**
     * Export a single element from the tuple, identified by its element index, to the destination row key of the
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
     * @param destinationIndexKey The destination row key
     */
    <ELEMENT_TYPE> void exportElement(TUPLE_TYPE tuple, int elementIndex,
            @NotNull WritableColumnSource<ELEMENT_TYPE> writableSource, long destinationIndexKey);

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
}
