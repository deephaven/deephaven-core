//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.util.PartitionParser;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The name and data type of one key-value partitioning column, recorded so that a reader does not have to guess the
 * type back from the directory name.
 *
 * <p>
 * A partitioning column's values live in the directory path rather than in any file's parquet schema, so without this
 * the only thing a reader has to go on is the key text. Inference from text does not round trip: a {@code String}
 * column whose values all happen to be a single character reads back as {@code char}, one whose values look like
 * integers reads back as {@code int} -- and {@code "01"} then reads back as {@code 1}, which cannot reproduce its own
 * directory name.
 *
 * <p>
 * {@link #dataType()} holds a type name rather than a {@link Class}, and is resolved through
 * {@link PartitionParser#lookup(Class, Class)}'s supported set rather than by loading an arbitrary class. An
 * unrecognized name leaves {@link #columnDefinition()} empty and the reader falls back to inference, which is also what
 * happens for files written before this field existed.
 */
@Immutable
@SimpleStyle
@JsonSerialize(as = ImmutablePartitioningColumnInfo.class)
@JsonDeserialize(as = ImmutablePartitioningColumnInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class PartitioningColumnInfo {

    @Parameter
    public abstract String columnName();

    /**
     * @return The column's data type, as produced by {@link Class#getName()}
     */
    @Parameter
    public abstract String dataType();

    @Check
    final void checkColumnName() {
        ColumnName.of(columnName());
    }

    /**
     * Resolve this record back to a partitioning {@link ColumnDefinition}.
     *
     * @return The column definition, or {@code null} if {@link #dataType()} does not name a type that partition values
     *         can be parsed to
     */
    @Nullable
    public final ColumnDefinition<?> columnDefinition() {
        final Class<?> dataType = SUPPORTED_TYPES.get(dataType());
        if (dataType == null) {
            return null;
        }
        return ColumnDefinition.fromGenericType(columnName(), dataType, null,
                ColumnDefinition.ColumnType.Partitioning);
    }

    public static PartitioningColumnInfo of(@NotNull final ColumnDefinition<?> columnDefinition) {
        return ImmutablePartitioningColumnInfo.of(columnDefinition.getName(),
                columnDefinition.getDataType().getName());
    }

    public static PartitioningColumnInfo of(@NotNull final String columnName, @NotNull final String dataType) {
        return ImmutablePartitioningColumnInfo.of(columnName, dataType);
    }

    /**
     * The types a partition value can be parsed to, keyed by {@link Class#getName()}. Deliberately an allow-list built
     * from {@link PartitionParser}: resolving {@link #dataType()} by loading the named class would let file metadata
     * name any class on the classpath, and a type with no parser could not be read back anyway.
     */
    private static final java.util.Map<String, Class<?>> SUPPORTED_TYPES = buildSupportedTypes();

    private static java.util.Map<String, Class<?>> buildSupportedTypes() {
        final java.util.Map<String, Class<?>> supported = new java.util.HashMap<>();
        for (final Class<?> candidate : new Class<?>[] {
                String.class,
                Boolean.class, boolean.class,
                Character.class, char.class,
                Byte.class, byte.class,
                Short.class, short.class,
                Integer.class, int.class,
                Long.class, long.class,
                Float.class, float.class,
                Double.class, double.class,
                java.math.BigInteger.class,
                java.math.BigDecimal.class,
                java.time.Instant.class,
                java.time.LocalDate.class,
                java.time.LocalTime.class,
                java.time.ZonedDateTime.class}) {
            if (PartitionParser.lookup(candidate, null) != null) {
                supported.put(candidate.getName(), candidate);
            }
        }
        return java.util.Map.copyOf(supported);
    }
}
