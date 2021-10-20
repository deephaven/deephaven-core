package io.deephaven.kafka.publish;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.utils.ChunkBoxer;
import io.deephaven.db.v2.utils.OrderedKeys;

public class SimpleKeyOrValueSerializer<SERIALIZED_TYPE> implements KeyOrValueSerializer<SERIALIZED_TYPE> {

    private final ColumnSource<SERIALIZED_TYPE> source;
    private final ChunkBoxer.BoxerKernel boxer;

    public SimpleKeyOrValueSerializer(Table table, String columnName) {
        source = table.getColumnSource(columnName);
        boxer = ChunkBoxer.getBoxer(source.getChunkType(), PublishToKafka.CHUNK_SIZE);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public ObjectChunk<SERIALIZED_TYPE, Attributes.Values> handleChunk(Context context, OrderedKeys orderedKeys,
            boolean previous) {
        final SimpleContext simpleContext = (SimpleContext) context;
        final Chunk chunk = source.getChunk(simpleContext.sourceGetContext, orderedKeys);
        return boxer.box(chunk);
    }

    @Override
    public Context makeContext(int size) {
        return new SimpleContext(size);
    }

    private class SimpleContext implements Context {

        private final ChunkSource.GetContext sourceGetContext;

        private SimpleContext(final int size) {
            sourceGetContext = source.makeGetContext(size);
        }

        @Override
        public void close() {
            sourceGetContext.close();
        }
    }
}
