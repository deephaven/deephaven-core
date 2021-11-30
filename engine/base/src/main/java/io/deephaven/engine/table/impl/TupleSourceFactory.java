package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;

/**
 * Factory for producing Deephaven engine TupleSource instances.
 */
public class TupleSourceFactory {

    @FunctionalInterface
    public interface TupleSourceCreatorProvider {
        TupleSourceCreator get();
    }

    private static final class TupleSourceCreatorHolder {
        private static final TupleSourceCreator tupleSourceCreator =
                ServiceLoader.load(TupleSourceCreatorProvider.class).iterator().next().get();
    }

    private static TupleSourceCreator tupleSourceCreator() {
        return TupleSourceCreatorHolder.tupleSourceCreator;
    }

    @FunctionalInterface
    public interface TupleSourceCreator {
        /**
         * See {@link TupleSourceFactory#makeTupleSource(ColumnSource[])}.
         */
        TupleSource makeTupleSource(@NotNull ColumnSource... columnSources);
    }

    /**
     * Create a {@link TupleSource tuple source} for the supplied array of {@link ColumnSource column sources}.
     *
     * @param columnSources The column sources
     * @return The tuple factory
     */
    public static TupleSource makeTupleSource(@NotNull final ColumnSource... columnSources) {
        return tupleSourceCreator().makeTupleSource(columnSources);
    }
}
