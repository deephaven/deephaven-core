//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Describes how a {@link FunctionGeneratedTableFactory} should build and refresh a function-generated table.
 *
 * <p>
 * Exactly one of {@link #tableSupplier()} or {@link #retainingLastTableSupplier()} must be provided. The former always
 * produces a new table on each invocation; the latter may return an empty {@link Optional}, in which case the
 * previously produced result is retained for the next cycle rather than being regenerated.
 *
 * <p>
 * At most one of {@link #refreshInterval()} or {@link #dependencies()} may be provided. A {@link #refreshInterval()}
 * drives periodic (wall-clock) refreshes; {@link #dependencies()} drives refreshes when at least one of the listed
 * tables ticks. If neither is provided, the supplier is invoked exactly once at construction and the result is static.
 */
@Immutable
@BuildableStyle
public abstract class FunctionGeneratedTableSpec {

    public static Builder builder() {
        return ImmutableFunctionGeneratedTableSpec.builder();
    }

    /**
     * A supplier that always produces a new table to be reflected in the result. Mutually exclusive with
     * {@link #retainingLastTableSupplier()}.
     *
     * @return the table supplier
     */
    public abstract Optional<Supplier<Table>> tableSupplier();

    /**
     * A supplier that may not produce a new table; when it returns an empty {@link Optional}, the previous result is
     * retained for the next cycle. Mutually exclusive with {@link #tableSupplier()}.
     *
     * @return the retaining-last table supplier
     */
    public abstract Optional<Supplier<Optional<Table>>> retainingLastTableSupplier();

    /**
     * The wall-clock interval at which to invoke the supplier. Mutually exclusive with {@link #dependencies()}.
     *
     * @return the refresh interval
     */
    public abstract Optional<Duration> refreshInterval();

    /**
     * The tables that, when any of them ticks, cause the supplier to be invoked in a dependency-respecting way.
     * Mutually exclusive with {@link #refreshInterval()}.
     *
     * @return the dependency tables
     */
    public abstract List<Table> dependencies();

    /**
     * When {@code true} (the default), the generated data is copied into the result table using a flat, contiguous
     * {@link io.deephaven.engine.rowset.RowSet}, exactly as the legacy {@link FunctionGeneratedTableFactory} does. When
     * {@code false}, the result delegates directly to the generated table's column sources via
     * {@link io.deephaven.engine.table.impl.sources.SwitchColumnSource}, avoiding the copy and adopting the generated
     * table's own {@link io.deephaven.engine.rowset.RowSet} shape.
     *
     * @return whether to copy the generated data into the result
     */
    @Default
    public boolean copyData() {
        return true;
    }

    /**
     * When {@code true}, the result is presented as a {@link Table#BLINK_TABLE_ATTRIBUTE blink table}: each cycle
     * removes the previous rows and adds the newly generated rows, and downstream operations treat the result as
     * retaining only the current cycle's rows. Defaults to {@code false}. Requires a refresh trigger (a
     * {@link #refreshInterval()} or {@link #dependencies()}).
     *
     * @return whether to present the result as a blink table
     */
    @Default
    public boolean blinkTable() {
        return false;
    }

    /**
     * The table definition to use for the result. This is required only when a {@link #retainingLastTableSupplier()}
     * does not produce a table at construction time; if no definition is provided and the initial supplier invocation
     * yields no table, construction fails. When the supplier does produce an initial table, its definition is used.
     *
     * @return the optional table definition
     */
    public abstract Optional<TableDefinition> tableDefinition();

    @Check
    final void checkExactlyOneSupplier() {
        if (tableSupplier().isPresent() == retainingLastTableSupplier().isPresent()) {
            throw new IllegalArgumentException(
                    "Exactly one of tableSupplier or retainingLastTableSupplier must be specified");
        }
    }

    @Check
    final void checkRefreshIntervalAndDependencies() {
        if (refreshInterval().isPresent() && !dependencies().isEmpty()) {
            throw new IllegalArgumentException("refreshInterval and dependencies are mutually exclusive");
        }
        refreshInterval().ifPresent(interval -> {
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("refreshInterval must be positive");
            }
        });
    }

    @Check
    final void checkBlink() {
        if (blinkTable() && refreshInterval().isEmpty() && dependencies().isEmpty()) {
            throw new IllegalArgumentException("blinkTable requires a refreshInterval or dependencies");
        }
    }

    public interface Builder {
        Builder tableSupplier(Supplier<Table> tableSupplier);

        Builder retainingLastTableSupplier(Supplier<Optional<Table>> retainingLastTableSupplier);

        Builder refreshInterval(Duration refreshInterval);

        Builder addDependencies(Table dependency);

        Builder addDependencies(Table... dependencies);

        Builder addAllDependencies(Iterable<? extends Table> dependencies);

        Builder copyData(boolean copyData);

        Builder blinkTable(boolean blinkTable);

        Builder tableDefinition(TableDefinition tableDefinition);

        FunctionGeneratedTableSpec build();
    }
}
