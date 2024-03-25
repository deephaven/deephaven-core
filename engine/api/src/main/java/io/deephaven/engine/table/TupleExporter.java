//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for classes that know how to export the elements of a given tuple type. Currently, supports element-wise
 * export to a {@link WritableColumnSource} without unnecessary boxing, and possibly-boxing export as {code Object}.
 */
public interface TupleExporter<TUPLE_TYPE> {

    /**
     * Get the number of elements in tuples supported by this TupleExporter.
     *
     * @return The number of elements in tuples supported by this TupleExporter
     */
    int tupleLength();

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
    <ELEMENT_TYPE> void exportElement(
            @NotNull TUPLE_TYPE tuple,
            int elementIndex,
            @NotNull WritableColumnSource<ELEMENT_TYPE> writableSource,
            long destinationIndexKey);

    /**
     * Export a single element (identified by {@code elementIndex}) from the tuple, boxing as necessary.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param tuple The tuple to export an element from
     * @param elementIndex The element index to export
     * @return The exported element, boxed when necessary
     */
    Object exportElement(@NotNull TUPLE_TYPE tuple, int elementIndex);

    /**
     * Fill an {@code Object[]} with all elements from the tuple, boxing as necessary.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param dest The destination {@code Object[]}
     * @param tuple The tuple to export from
     */
    default void exportAllTo(final Object @NotNull [] dest, @NotNull final TUPLE_TYPE tuple) {
        final int length = tupleLength();
        for (int ei = 0; ei < length; ++ei) {
            dest[ei] = exportElement(tuple, ei);
        }
    }

    /**
     * Fill an {@code Object[]} with all elements from the tuple, boxing as necessary, mapping the tuple elements to the
     * destination array using the provided {@code int[]} {code map}. This map contains the destination index for each
     * tuple element in order.
     * <p>
     * Providing {@code map = new int{1, 2, 0}} means that the 0th element of the tuple will be written in
     * {@code dest[1]}, the 1st element of the tuple will be written in {@code dest[2]}, and the 2nd element of the
     * tuple will be written in {@code dest[0]}.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param dest The destination {@code Object[]}
     * @param tuple The tuple to export from
     * @param map Instructions where to write each tuple element in {@code dest}
     */
    default void exportAllTo(
            final Object @NotNull [] dest,
            @NotNull final TUPLE_TYPE tuple,
            final int @NotNull [] map) {
        final int length = tupleLength();
        for (int ei = 0; ei < length; ++ei) {
            dest[map[ei]] = exportElement(tuple, ei);
        }
    }

    /**
     * Export a single element (identified by {@code elementIndex}) from the tuple, boxing as necessary. If the tuple
     * has been internally reinterpreted, return the reinterpreted value.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param tuple The tuple to export an element from
     * @param elementIndex The element index to export
     * @return The exported element, reinterpreted if internally reinterpreted, boxed when necessary
     */
    default Object exportElementReinterpreted(@NotNull final TUPLE_TYPE tuple, final int elementIndex) {
        return exportElement(tuple, elementIndex);
    }

    /**
     * Fill an {@code Object[]} with all element from the tuple, boxing as necessary. If the tuple has been internally
     * reinterpreted, will fill with reinterpreted values.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param dest The destination {@code Object[]}
     * @param tuple The tuple to export from
     */
    default void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final TUPLE_TYPE tuple) {
        final int length = tupleLength();
        for (int ei = 0; ei < length; ++ei) {
            dest[ei] = exportElementReinterpreted(tuple, ei);
        }
    }

    /**
     * Fill an Object[] with all element from the tuple, boxing as necessary, mapping the tuple elements to the
     * destination array using the provided int[] map. This map contains the destination index for each tuple element in
     * order. will fill with reinterpreted values.
     * <p>
     * Providing {@code map = new int{1, 2, 0}} means that the 0th element of the tuple will be written in
     * {@code dest[1]}, the 1st element of the tuple will be written in {@code dest[2]}, and the 2nd element of the
     * tuple will be written in {@code dest[0]}.
     * <p>
     * For the empty tuple, this is unsupported.
     *
     * @param dest The destination {@code Object[]}
     * @param tuple The tuple to export from
     * @param map Instructions where to write each tuple element in {@code dest}
     */
    default void exportAllReinterpretedTo(
            final Object @NotNull [] dest,
            @NotNull final TUPLE_TYPE tuple,
            final int @NotNull [] map) {
        final int length = tupleLength();
        for (int ei = 0; ei < length; ++ei) {
            dest[map[ei]] = exportElementReinterpreted(tuple, ei);
        }
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
         * @return The exported element, boxed when necessary
         */
        Object exportElement(@NotNull TUPLE_TYPE tuple, int elementIndex);
    }
}
