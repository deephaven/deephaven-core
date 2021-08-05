package io.deephaven.db.v2.parquet.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * <p>Representation class for per-column type information stored in key-value metadata for Deephaven-written Parquet
 * files.
 * <p>Currently only used for columns requiring non-default deserialization or type selection
 */
@Value.Immutable
@BuildableStyle
@JsonSerialize(as = ImmutableColumnTypeInfo.class)
@JsonDeserialize(as = ImmutableColumnTypeInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class ColumnTypeInfo {

    /**
     * @return The column name
     */
    public abstract String columnName();

    /**
     * @return Optional codec info for binary types
     */
    public abstract Optional<CodecInfo> codec();

    /**
     * Values for the {@link #specialType()} field.
     */
    public enum SpecialType {
        StringSet,
        Vector
    }

    /**
     * @return Optional type hint for complex types
     */
    public abstract Optional<SpecialType> specialType();

    @Value.Check
    final void checkColumnName() {
        if (columnName().isEmpty()) {
            throw new IllegalArgumentException("Empty column name");
        }
    }

    public static ColumnTypeInfo.Builder builder() {
        return ImmutableColumnTypeInfo.builder();
    }

    public interface Builder {

        Builder columnName(String columnName);

        Builder codec(CodecInfo codec);

        Builder specialType(SpecialType specialType);

        ColumnTypeInfo build();
    }
}
