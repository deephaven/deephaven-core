//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.util.annotations.InternalUseOnly;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A simple {@link ParquetColumnResolver} implementation from a {@link Map}.
 */
@Value.Immutable
@BuildableStyle
public abstract class ParquetColumnResolverMap implements ParquetColumnResolver {

    public static Builder builder() {
        return ImmutableParquetColumnResolverMap.builder();
    }

    abstract Map<String, List<String>> mapUnsafe();

    /**
     * A map from Deephaven column name to Parquet path.
     */
    public final Map<String, List<String>> map() {
        return mapUnsafe();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Equivalent to {@code Optional.ofNullable(map().get(columnName))}.
     */
    @Override
    public Optional<List<String>> of(String columnName) {
        return Optional.ofNullable(map().get(columnName));
    }

    public interface Builder {

        // Ideally, not part of the public interface.
        // See https://github.com/immutables/immutables/issues/1534
        @InternalUseOnly
        Builder putMapUnsafe(String key, List<String> value);

        default Builder putMap(String key, List<String> value) {
            return putMapUnsafe(key, List.copyOf(value));
        }

        ParquetColumnResolverMap build();
    }
}
