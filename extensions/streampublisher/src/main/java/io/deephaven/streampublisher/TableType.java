package io.deephaven.streampublisher;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.annotations.SingletonStyle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Type for the result {@link Table} returned by kafka consumers.
 */
public interface TableType {

    static Blink blink() {
        return Blink.of();
    }

    static Append append() {
        return Append.of();
    }

    static Ring ring(int capacity) {
        return Ring.of(capacity);
    }

    /**
     * Map "Python-friendly" table type name to a {@link TableType}. Supported values are:
     * <ol>
     * <li>{@code "blink"}</li>
     * <li>{@code "stream"} (deprecated; use {@code "blink"})</li>
     * <li>{@code "append"}</li>
     * <li>{@code "ring:<capacity>"} where capacity is a integer number specifying the maximum number of trailing rows
     * to include in the result</li>
     * </ol>
     *
     * @param typeName The friendly name
     * @return The mapped {@link TableType}
     */
    @ScriptApi
    static TableType friendlyNameToTableType(@NotNull final String typeName) {
        final String[] split = typeName.split(":");
        switch (split[0].trim()) {
            case "blink":
            case "stream": // TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this
                if (split.length != 1) {
                    throw unexpectedType(typeName, null);
                }
                return blink();
            case "append":
                if (split.length != 1) {
                    throw unexpectedType(typeName, null);
                }
                return append();
            case "ring":
                if (split.length != 2) {
                    throw unexpectedType(typeName, null);
                }
                try {
                    return ring(Integer.parseInt(split[1].trim()));
                } catch (NumberFormatException e) {
                    throw unexpectedType(typeName, e);
                }
            default:
                throw unexpectedType(typeName, null);
        }
    }

    private static IllegalArgumentException unexpectedType(@NotNull final String typeName, @Nullable Exception cause) {
        return new IllegalArgumentException("Unexpected type format \"" + typeName
                + "\", expected \"blink\", \"append\", or \"ring:<capacity>\"", cause);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Blink blink);

        T visit(Append append);

        T visit(Ring ring);
    }

    /**
     * <p>
     * Consume data into an in-memory blink table, which will present only newly-available rows to downstream
     * operations and visualizations.
     * <p>
     * See {@link Table#BLINK_TABLE_ATTRIBUTE} for a detailed explanation of blink table semantics, and
     * {@link BlinkTableTools} for related tooling.
     */
    @Immutable
    @SingletonStyle
    abstract class Blink implements TableType {

        public static Blink of() {
            return ImmutableBlink.of();
        }

        @Override
        public final <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * Consume data into an in-memory append-only table.
     *
     * @see BlinkTableTools#blinkToAppendOnly(Table)
     */
    @Immutable
    @SingletonStyle
    abstract class Append implements TableType {

        public static Append of() {
            return ImmutableAppend.of();
        }

        @Override
        public final <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * Consume data into an in-memory ring table.
     *
     * @see RingTableTools#of(Table, int)
     */
    @Immutable
    @SimpleStyle
    abstract class Ring implements TableType {

        public static Ring of(int capacity) {
            return ImmutableRing.of(capacity);
        }

        @Parameter
        public abstract int capacity();

        @Override
        public final <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }
}
