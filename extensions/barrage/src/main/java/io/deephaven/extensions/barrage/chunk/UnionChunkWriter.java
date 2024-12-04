//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.unboxer.ChunkUnboxer;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class UnionChunkWriter<T> extends BaseChunkWriter<ObjectChunk<T, Values>> {
    public enum Mode {
        Dense, Sparse
    }

    private static final String DEBUG_NAME = "UnionChunkWriter";

    private final Mode mode;
    private final List<Class<?>> classMatchers;
    private final List<ChunkWriter<Chunk<Values>>> writers;
    private final List<ChunkType> writerChunkTypes;

    public UnionChunkWriter(
            final Mode mode,
            final List<Class<?>> classMatchers,
            final List<ChunkWriter<Chunk<Values>>> writers,
            final List<ChunkType> writerChunkTypes) {
        super(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, 0, false, false);
        this.mode = mode;
        this.classMatchers = classMatchers;
        this.writers = writers;
        this.writerChunkTypes = writerChunkTypes;
        // the specification doesn't allow the union column to have more than signed byte number of types
        Assert.leq(classMatchers.size(), "classMatchers.size()", 127);
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<T, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    public final class Context extends ChunkWriter.Context<ObjectChunk<T, Values>> {
        public Context(
                @NotNull final ObjectChunk<T, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);
        }
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context<ObjectChunk<T, Values>> context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
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
            final ObjectChunk<T, Values> chunk = context.getChunk();
            if (mode == Mode.Sparse) {
                columnOffset = null;
            } else {
                // noinspection resource
                columnOffset = WritableIntChunk.makeWritableChunk(chunk.size());
            }


            // noinspection resource
            columnOfInterest = WritableByteChunk.makeWritableChunk(chunk.size());
            // noinspection unchecked
            final WritableObjectChunk<Object, Values>[] innerChunks = new WritableObjectChunk[numColumns];
            for (int ii = 0; ii < numColumns; ++ii) {
                // noinspection resource
                innerChunks[ii] = WritableObjectChunk.makeWritableChunk(chunk.size());

                if (mode == Mode.Sparse) {
                    innerChunks[ii].fillWithNullValue(0, chunk.size());
                } else {
                    innerChunks[ii].setSize(0);
                }
            }
            for (int ii = 0; ii < chunk.size(); ++ii) {
                final Object value = chunk.get(ii);
                int jj;
                for (jj = 0; jj < classMatchers.size(); ++jj) {
                    if (value.getClass().isAssignableFrom(classMatchers.get(jj))) {
                        if (mode == Mode.Sparse) {
                            columnOfInterest.set(ii, (byte) jj);
                            innerChunks[jj].set(ii, value);
                        } else {
                            columnOfInterest.set(ii, (byte) jj);
                            columnOffset.set(ii, innerChunks[jj].size());
                            innerChunks[jj].add(value);
                        }
                        break;
                    }
                }

                if (jj == classMatchers.size()) {
                    throw new UnsupportedOperationException("UnionChunkWriter found unexpected class: "
                            + value.getClass() + " allowed classes: " +
                            classMatchers.stream().map(Class::getSimpleName)
                                    .collect(Collectors.joining(", ")));
                }
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

                // note that we do not close the kernel since we steal the inner chunk into the context
                final ChunkUnboxer.UnboxerKernel kernel = chunkType == ChunkType.Object
                        ? null
                        : ChunkUnboxer.getUnboxer(chunkType, innerChunk.size());

                // noinspection unchecked
                try (ChunkWriter.Context<Chunk<Values>> innerContext = writer.makeContext(kernel != null
                        ? (Chunk<Values>) kernel.unbox(innerChunk)
                        : innerChunk, 0)) {

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
            columnOffset.close();
            for (DrainableColumn innerColumn : innerColumns) {
                innerColumn.close();
            }
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                long size = 0;
                size += padBufferSize(subset.intSize(DEBUG_NAME));
                size += padBufferSize(Integer.BYTES * subset.size());
                for (DrainableColumn innerColumn : innerColumns) {
                    size += innerColumn.available();
                }
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, size);
            }

            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            read = true;
            long bytesWritten = 0;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // must write out the column of interest
            for (int ii = 0; ii < columnOfInterest.size(); ++ii) {
                dos.writeByte(columnOfInterest.get(ii));
            }
            bytesWritten += columnOfInterest.size();
            bytesWritten += writePadBuffer(dos, bytesWritten);

            // must write out the column offset
            for (int ii = 0; ii < columnOffset.size(); ++ii) {
                dos.writeInt(columnOffset.get(ii));
            }
            bytesWritten += LongSizedDataStructure.intSize(DEBUG_NAME, (long) Integer.BYTES * columnOffset.size());
            bytesWritten += writePadBuffer(dos, bytesWritten);

            for (DrainableColumn innerColumn : innerColumns) {
                bytesWritten += innerColumn.drainTo(outputStream);
            }
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
