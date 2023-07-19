package io.deephaven.engine.table;

import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.ServiceLoader;

public class MultiJoinFactory {

    /**
     * Creator interface for runtime-supplied implementation.
     */
    public interface Creator {
        MultiJoinTable of(@NotNull final MultiJoinInput... joinDescriptors);
    }

    /**
     * Creator provider to supply the implementation at runtime.
     */
    @FunctionalInterface
    public interface CreatorProvider {
        Creator get();
    }

    private static final class MultiJoinTableCreatorHolder {
        private static final MultiJoinFactory.Creator creator =
                ServiceLoader.load(MultiJoinFactory.CreatorProvider.class).iterator().next().get();
    }

    private static MultiJoinFactory.Creator multiJoinTableCreator() {
        return MultiJoinTableCreatorHolder.creator;
    }

    /**
     * Join tables that have common key column names.
     * <p>
     *
     * @param keys the key column pairs in the format "Result=Source" or "ColumnInBoth"
     * @param inputTables the tables to join together
     * @return a MultiJoinTable with one row for each key and the corresponding row in each input table
     */
    public static MultiJoinTable of(@NotNull final String[] keys, @NotNull final Table... inputTables) {
        return multiJoinTableCreator().of(createSimpleJoinInput(keys, inputTables));
    }

    /**
     * Perform a multiJoin for one or more tables.
     *
     * @param joinDescriptors the description of each table that contributes to the result
     * @return a MultiJoinTable with one row for each key and the corresponding row in each input table
     */
    public static MultiJoinTable of(@NotNull final MultiJoinInput... joinDescriptors) {
        return multiJoinTableCreator().of(joinDescriptors);
    }

    @TestUseOnly
    @NotNull
    public static MultiJoinInput[] createSimpleJoinInput(@NotNull final String[] keys,
            @NotNull final Table[] inputTables) {
        return Arrays.stream(inputTables)
                .map(t -> MultiJoinInput.of(t, keys))
                .toArray(MultiJoinInput[]::new);
    }
}
