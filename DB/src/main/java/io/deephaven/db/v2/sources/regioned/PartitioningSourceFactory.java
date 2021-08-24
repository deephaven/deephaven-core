package io.deephaven.db.v2.sources.regioned;

import org.jetbrains.annotations.NotNull;

class PartitioningSourceFactory {

    /**
     * Get a partitioning {@link RegionedColumnSource} for the supplied {@code dataType}.
     *
     * @param dataType The data type expected for partition values
     * @return A new partitioning {@link RegionedColumnSource}
     */
    static <DATA_TYPE> RegionedColumnSource<DATA_TYPE> makePartitioningSource(
        @NotNull final Class<DATA_TYPE> dataType) {
        final RegionedColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new RegionedColumnSourceObject.Partitioning<>(dataType);
        } else if (dataType == char.class || dataType == Character.class) {
            result = new RegionedColumnSourceChar.Partitioning();
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new RegionedColumnSourceByte.Partitioning();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new RegionedColumnSourceShort.Partitioning();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new RegionedColumnSourceInt.Partitioning();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new RegionedColumnSourceLong.Partitioning();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new RegionedColumnSourceFloat.Partitioning();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new RegionedColumnSourceDouble.Partitioning();
        } else {
            result = new RegionedColumnSourceObject.Partitioning<>(dataType);
        }
        // noinspection unchecked
        return (RegionedColumnSource<DATA_TYPE>) result;
    }
}
