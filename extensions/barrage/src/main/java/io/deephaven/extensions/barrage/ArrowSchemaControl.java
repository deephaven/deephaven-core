//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;

import java.util.Map;

/**
 * Immutable control object for Arrow schema generation. Allows callers to assign specific {@link ColumnEncoding}s to
 * named columns, overriding any auto-detected encodings applied by
 * {@link io.deephaven.extensions.barrage.util.BarrageUtil#schemaFromTable}.
 *
 * <p>
 * Use {@link #builder()} to construct an instance, or {@link #DEFAULT} when no overrides are needed.
 */
@Immutable
@BuildableStyle
@Style(depluralize = true)
public abstract class ArrowSchemaControl {

    /** Default instance with no column-encoding overrides. */
    public static final ArrowSchemaControl DEFAULT = ImmutableArrowSchemaControl.builder().build();

    public static Builder builder() {
        return ImmutableArrowSchemaControl.builder();
    }

    /**
     * Returns the map of column-name to {@link ColumnEncoding} overrides.
     */
    public abstract Map<String, ColumnEncoding> encodings();

    // TODO: consider adding builder methods for Union<> and Map<> column types (vs. creating Arrow-compatible schema
    // objects)

    public interface Builder {
        Builder putEncoding(String columnName, ColumnEncoding encoding);

        Builder putAllEncodings(Map<String, ? extends ColumnEncoding> encodings);

        ArrowSchemaControl build();
    }
}
