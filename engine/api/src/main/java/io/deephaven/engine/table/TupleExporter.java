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
     * Export a single element from the tuple, identified by its element index, to an Object.
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
     * @return The exported element, boxed as an Object as needed
     */
    Object exportElement(TUPLE_TYPE tuple, int elementIndex);


    /**
     * Fill an Object[] with all element from the tuple.
     *
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param dest The destination Object[]
     * @param tuple The tuple to export an element from
     */
    void exportAllTo(Object[] dest, TUPLE_TYPE tuple);

    /**
     * Fill an Object[] with all element from the tuple, mapping the tuple elements to the destination array using the
     * provided int[] map. This map contains the destination index for each tuple element in order.
     * <p>
     * Providing map == [1, 2, 0] means that the 0th element of the tuple will be written in dest[1], the 1st element of
     * the tuple will be written in dest[2], and the 2nd element of the tuple will be written in dest[0].
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param dest The destination Object[]
     * @param tuple The tuple to export an element from
     * @param map Instructions where to write each tuple element in `dest`
     */
    default void exportAllTo(Object[] dest, TUPLE_TYPE tuple, int[] map) {
        // Ignore the map in the default implementation
        exportAllTo(dest, tuple);
    }

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
     * @return The exported element, reinterpreted if internally reinterpreted, boxed as an Object as needed
     */
    default Object exportElementReinterpreted(TUPLE_TYPE tuple, int elementIndex) {
        return exportElement(tuple, elementIndex);
    }

    /**
     * Fill an Object[] with all element from the tuple. If the tuple has been internally reinterpreted, will fill with
     * reinterpreted values.
     *
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param dest The destination Object[]
     * @param tuple The tuple to export an element from
     */
    default void exportAllReinterpretedTo(Object[] dest, TUPLE_TYPE tuple) {
        exportAllTo(dest, tuple);
    }

    /**
     * Fill an Object[] with all element from the tuple, mapping the tuple elements to the destination array using the
     * provided int[] map. This map contains the destination index for each tuple element in order. will fill with
     * reinterpreted values.
     *
     * <p>
     * Providing map == [1, 2, 0] means that the 0th element of the tuple will be written in dest[1], the 1st element of
     * the tuple will be written in dest[2], and the 2nd element of the tuple will be written in dest[0].
     * <p>
     * For the empty tuple, this is unsupported.
     * <p>
     * For singles, this will copy the sole element, possibly in boxed form.
     * <p>
     * For doubles and longer, this will copy the specified element without any unnecessary boxing.
     *
     * @param dest The destination Object[]
     * @param tuple The tuple to export an element from
     * @param map Instructions where to write each tuple element in `dest`
     */
    default void exportAllReinterpretedTo(Object[] dest, TUPLE_TYPE tuple, int[] map) {
        // Ignore the map in the default implementation
        exportAllReinterpretedTo(dest, tuple);
    }

    @FunctionalInterface
    interface ExportElementFunction<TUPLE_TYPE> {

        /**
         * Export a single element from the tuple, identified by its element index, to an Object. This interface is
         * intended to be compatible with both {@link TupleExporter#exportElement(Object, int)} and
         * {@link TupleExporter#exportElementReinterpreted(Object, int)}, and consequently does not specify whether the
         * result will be reinterpreted.
         * 
         * @param tuple The tuple to export an element from
         * @param elementIndex The element index to export
         * @return The exported element, boxed as an Object as needed
         */
        Object exportElement(TUPLE_TYPE tuple, int elementIndex);
    }
}
