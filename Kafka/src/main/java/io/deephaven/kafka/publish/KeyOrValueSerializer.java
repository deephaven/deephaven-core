package io.deephaven.kafka.publish;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Chunk-oriented serializer that supplies keys or values for stream publishing.
 */
public interface KeyOrValueSerializer<SERIALIZED_TYPE> {
    /**
     * Create a chunk of output keys or values that correspond to {@code orderedKeys}. The output {@link ObjectChunk
     * chunks} should be cached in the {@code context} for re-use, but the data returned in them should be functionally
     * immutable and not rely on pooled or re-usable objects.
     *
     * @param context A {@link Context} created by {@link #makeContext(int)}
     * @param orderedKeys The row keys to serialize
     * @param previous If previous row values should be used, as with row key removals
     *
     * @return A chunk of serialized data keys or values, with {@code ObjectChunk.size() == orderedKeys.size()}
     */
    ObjectChunk<SERIALIZED_TYPE, Attributes.Values> handleChunk(Context context, OrderedKeys orderedKeys,
            boolean previous);

    /**
     * Create a context for calling {@link #handleChunk(Context, OrderedKeys, boolean)}.
     *
     * @param size The maximum number of rows that will be serialized for each chunk
     *
     * @return A Context for the KeyOrValueSerializer
     */
    Context makeContext(int size);

    /**
     * Context interface.
     */
    interface Context extends SafeCloseable {
    }

    /**
     * Factory interface.
     */
    interface Factory<SERIALIZED_TYPE> {
        /**
         * Validate that this factory is compatible with {@code tableDefinition}.
         *
         * @param tableDefinition A {@link TableDefinition} specifying all available source columns
         */
        void validateColumns(@NotNull TableDefinition tableDefinition);

        /**
         * Get a list of the source column names that will be used by the serializers returns by {@link #create(Table)}.
         *
         * @param tableDefinition The {@link TableDefinition} of a {@link Table} that will be subsequently supplied to
         *        {@link #create(Table)}
         * @return The list of input column names
         */
        List<String> sourceColumnNames(@NotNull TableDefinition tableDefinition);

        /**
         * Create a serializer using columns of {@code source} as input.
         *
         * @param source The source {@link Table}
         * @return The resulting serializer
         */
        KeyOrValueSerializer<SERIALIZED_TYPE> create(@NotNull Table source);
    }
}
