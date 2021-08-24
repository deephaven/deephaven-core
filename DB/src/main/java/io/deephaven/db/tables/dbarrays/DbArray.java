/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.qst.type.DbGenericArrayType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public interface DbArray<T> extends DbArrayBase<DbArray<T>> {

    long serialVersionUID = 2691131699080413017L;

    static <T> DbGenericArrayType<DbArray, T> type(GenericType<T> genericType) {
        return DbGenericArrayType.of(DbArray.class, genericType);
    }

    T get(long i);
    DbArray<T> subArray(long fromIndexInclusive, long toIndexExclusive);
    DbArray<T> subArrayByPositions(long [] positions);
    T[] toArray();
    Class<T> getComponentType();

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    T getPrev(long offset);

    @Override
    default Chunk<Attributes.Values> toChunk()  {
        return ObjectChunk.chunkWrap(toArray());
    }

    DbArray<T> getDirect();

    @Override
    default void fillChunk(WritableChunk destChunk) {
        destChunk.asWritableObjectChunk().copyFromTypedArray(toArray(), 0, destChunk.size(), (int)size());
    }

    static String defaultValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : val.toString();
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param array       The DbArray to convert to a String
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    static String toString(@NotNull final DbArray<?> array, final int prefixLength) {
        if (array.isEmpty()) {
            return "[]";
        }

        final Function<Object, String> valToString = DbArrayBase.classToHelper(array.getComponentType());

        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(array.size(), prefixLength);
        builder.append(valToString.apply(array.get(0)));
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(',').append(valToString.apply(array.get(ei)));
        }
        if (displaySize == array.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link Object#equals(Object)}.
     *
     * @param aArray The LHS of the equality test (always a DbArray)
     * @param b      The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DbArray aArray, @Nullable final Object b) {
        if (aArray == b) {
            return true;
        }
        if (!(b instanceof DbArray)) {
            return false;
        }
        final DbArray bArray = (DbArray) b;
        final long size = aArray.size();
        if (size != bArray.size()) {
            return false;
        }
        for (long ei = 0; ei < size; ++ei) {
            if (!Objects.equals(aArray.get(ei), bArray.get(ei))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link Object#hashCode()}. Follows the pattern in
     * {@link Arrays#hashCode(Object[])}.
     *
     * @param array The DbArray to hash
     * @return The hash code
     */
    static int hashCode(@NotNull final DbArray array) {
        final long size = array.size();
        int result = 1;
        for (long ei = 0; ei < size; ++ei) {
            result = 31 * result + Objects.hashCode(array.get(ei));
        }
        return result;
    }

    /**
     * Base class for all "indirect" DbArray implementations.
     */
    abstract class Indirect<T> implements DbArray<T> {

        private static final long serialVersionUID = 1L;

        @Override
        public final String toString() {
            return DbArray.toString(this, 10);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public final boolean equals(Object obj) {
            return DbArray.equals(this, obj);
        }

        @Override
        public final int hashCode() {
            return DbArray.hashCode(this);
        }

        protected final Object writeReplace() {
            return new DbArrayDirect<>(toArray());
        }
    }
}
