//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.util.unboxer.ChunkUnboxer;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class UnionChunkWriter<T> extends BaseChunkWriter<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "UnionChunkWriter";

    private final UnionChunkReader.Mode mode;
    private final List<Class<?>> classMatchers;
    private final List<ChunkWriter<Chunk<Values>>> writers;
    private final List<ChunkType> writerChunkTypes;
    private final int[] columnOfInterestMapping;

    public UnionChunkWriter(
            final UnionChunkReader.Mode mode,
            final List<Class<?>> classMatchers,
            final List<ChunkWriter<Chunk<Values>>> writers,
            final List<ChunkType> writerChunkTypes,
            final int[] columnOfInterestMapping) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, false);
        this.mode = mode;
        this.classMatchers = classMatchers;
        this.writers = writers;
        this.writerChunkTypes = writerChunkTypes;
        this.columnOfInterestMapping = columnOfInterestMapping;
        // the specification doesn't allow the union column to have more than signed byte number of types
        Assert.leq(classMatchers.size(), "classMatchers.size()", Byte.MAX_VALUE, "Byte.MAX_VALUE");
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<T, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    @Override
    protected int computeNullCount(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> {
            if (objectChunk.isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(objectChunk.isNull((int) row)));
    }

    public final class Context extends ChunkWriter.Context {
        public Context(
                @NotNull final ObjectChunk<T, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);
        }
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        // noinspection unchecked
        return new UnionChunkInputStream((Context) context, subset, options);
    }

    private class UnionChunkInputStream extends BaseChunkInputStream<Context> {

        private int cachedSize = -1;
        private final WritableByteChunk<Values> columnOfInterest;
        private final WritableIntChunk<Values> columnOffset;
        private final DrainableColumn[] innerColumns;

        private UnionChunkInputStream(
                @NotNull final Context context,
                @Nullable final RowSet mySubset,
                @NotNull final BarrageOptions options) throws IOException {
            super(context, mySubset, options);
            final int numColumns = classMatchers.size();
            final ObjectChunk<T, Values> chunk = context.getChunk().asObjectChunk();
            final int resultChunkSize = subset != null ? subset.intSize() : chunk.size();
            if (mode == UnionChunkReader.Mode.Sparse) {
                columnOffset = null;
            } else {
                columnOffset = WritableIntChunk.makeWritableChunk(resultChunkSize);
            }

            columnOfInterest = WritableByteChunk.makeWritableChunk(resultChunkSize);
            // noinspection unchecked
            final SizedChunk<Values>[] innerSizedChunks = new SizedChunk[numColumns];
            // noinspection unchecked
            final WritableObjectChunk<Object, Values>[] innerChunks = new WritableObjectChunk[numColumns];
            for (int ii = 0; ii < numColumns; ++ii) {
                // noinspection resource
                innerSizedChunks[ii] = new SizedChunk<>(ChunkType.Object);

                if (mode == UnionChunkReader.Mode.Sparse) {
                    innerSizedChunks[ii].ensureCapacity(resultChunkSize);
                    innerSizedChunks[ii].get().fillWithNullValue(0, resultChunkSize);
                } else {
                    innerSizedChunks[ii].ensureCapacity(0);
                }
                innerChunks[ii] = innerSizedChunks[ii].get().asWritableObjectChunk();
            }

            // Create (and close) a flat row set if subset is not provided.
            try (final RowSet toClose = subset == null ? RowSetFactory.flat(resultChunkSize) : null) {
                final RowSet toUse = subset == null ? toClose : subset;
                final MutableInt index = new MutableInt(0);
                toUse.forAllRowKeys(rowKey -> {
                    final Object value = chunk.get((int) rowKey);
                    final int idx = index.getAndIncrement();
                    int jj;
                    for (jj = 0; jj < classMatchers.size(); ++jj) {
                        if (value == null || classMatchers.get(jj).isAssignableFrom(value.getClass())) {
                            columnOfInterest.set(idx, (byte) columnOfInterestMapping[jj]);
                            if (mode == UnionChunkReader.Mode.Sparse) {
                                innerChunks[jj].set(idx, value);
                            } else {
                                int size = innerChunks[jj].size();
                                columnOffset.set(idx, size);
                                if (innerChunks[jj].capacity() <= size) {
                                    int newSize = Math.max(16, size * 2);
                                    innerSizedChunks[jj].ensureCapacityPreserve(newSize);
                                    innerChunks[jj] = innerSizedChunks[jj].get().asWritableObjectChunk();
                                }
                                innerChunks[jj].add(value);
                            }
                            break;
                        }
                    }

                    if (jj == classMatchers.size()) {
                        if (value == null) {
                            throw new UnsupportedOperationException(
                                    "UnionChunkWriter found null-value without null child");
                        }
                        throw new UnsupportedOperationException("UnionChunkWriter found unexpected class: "
                                + value.getClass() + " allowed classes: " +
                                classMatchers.stream().map(Class::getSimpleName)
                                        .collect(Collectors.joining(", ")));
                    }
                });
            }
            innerColumns = new DrainableColumn[numColumns];
            for (int ii = 0; ii < numColumns; ++ii) {
                final ChunkType chunkType = writerChunkTypes.get(ii);
                final ChunkWriter<Chunk<Values>> writer = writers.get(ii);
                final WritableObjectChunk<Object, Values> innerChunk = innerChunks[ii];

                if (classMatchers.get(ii) == Boolean.class) {
                    // do a quick conversion to byte since the boolean unboxer expects bytes
                    for (int jj = 0; jj < innerChunk.size(); ++jj) {
                        innerChunk.set(jj, BooleanUtils.booleanAsByte((Boolean) innerChunk.get(jj)));
                    }
                }

                // Notes on ownership: the context assumes ownership of the chunk and will close it
                // when the context is closed. When we supply a boxed innerChunk, we can rely on the context to
                // close it. However, when unboxing, the UnboxerKernel creates and owns a new chunk so we should
                // not close the kernel but we do need to close the inner chunk.
                final ChunkUnboxer.UnboxerKernel kernel = chunkType == ChunkType.Object
                        ? null
                        : ChunkUnboxer.getUnboxer(chunkType, innerChunk.size());

                final Chunk<Values> chunkToWrite;
                if (kernel != null) {
                    // noinspection unchecked
                    chunkToWrite = (Chunk<Values>) kernel.unbox(innerChunk);
                    innerChunk.close();
                } else {
                    chunkToWrite = innerChunk;
                }
                try (ChunkWriter.Context innerContext = writer.makeContext(chunkToWrite, 0)) {
                    innerColumns[ii] = writer.getInputStream(innerContext, null, options);
                }
            }
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(), nullCount());
            for (DrainableColumn innerColumn : innerColumns) {
                innerColumn.visitFieldNodes(listener);
            }
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // one buffer for the column of interest
            listener.noteLogicalBuffer(padBufferSize(subset.intSize(DEBUG_NAME)));
            // one buffer for the column offset
            if (columnOffset != null) {
                listener.noteLogicalBuffer(padBufferSize((long) Integer.BYTES * subset.intSize(DEBUG_NAME)));
            }

            for (DrainableColumn innerColumn : innerColumns) {
                innerColumn.visitBuffers(listener);
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            columnOfInterest.close();
            if (columnOffset != null) {
                columnOffset.close();
            }
            for (DrainableColumn innerColumn : innerColumns) {
                innerColumn.close();
            }
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                long size = 0;
                size += padBufferSize(subset.intSize(DEBUG_NAME));
                if (columnOffset != null) {
                    size += padBufferSize(Integer.BYTES * subset.size());
                }
                for (DrainableColumn innerColumn : innerColumns) {
                    size += innerColumn.available();
                }
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, size);
            }

            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead) {
                return 0;
            }

            hasBeenRead = true;
            long bytesWritten = 0;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // must write out the column of interest
            for (int ii = 0; ii < columnOfInterest.size(); ++ii) {
                dos.writeByte(columnOfInterest.get(ii));
            }
            bytesWritten += columnOfInterest.size();
            bytesWritten += writePadBuffer(dos, bytesWritten);

            // must write out the column offset
            if (columnOffset != null) {
                for (int ii = 0; ii < columnOffset.size(); ++ii) {
                    dos.writeInt(columnOffset.get(ii));
                }
                bytesWritten += LongSizedDataStructure.intSize(DEBUG_NAME, (long) Integer.BYTES * columnOffset.size());
            }
            bytesWritten += writePadBuffer(dos, bytesWritten);

            for (DrainableColumn innerColumn : innerColumns) {
                bytesWritten += innerColumn.drainTo(outputStream);
            }
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
