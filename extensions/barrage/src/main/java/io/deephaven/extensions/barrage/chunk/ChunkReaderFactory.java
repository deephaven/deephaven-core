//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Type;

/**
 * Supports creation of {@link ChunkReader} instances to use when processing a flight stream. JVM implementations for
 * client and server should probably use {@link DefaultChunkReadingFactory#INSTANCE}.
 */
public interface ChunkReaderFactory {
    /**
     * Describes type info used by factory implementations when creating a ChunkReader.
     */
    class TypeInfo {
        private final ChunkType chunkType;
        private final Class<?> type;
        private final Class<?> componentType;
        private final Field arrowField;

        public TypeInfo(ChunkType chunkType, Class<?> type, Class<?> componentType, Field arrowField) {
            this.chunkType = chunkType;
            this.type = type;
            this.componentType = componentType;
            this.arrowField = arrowField;
        }

        public ChunkType chunkType() {
            return chunkType;
        }

        public Class<?> type() {
            return type;
        }

        public Class<?> componentType() {
            return componentType;
        }

        public Field arrowField() {
            return arrowField;
        }

        public Field componentArrowField() {
            if (arrowField.typeType() != Type.List) {
                throw new IllegalStateException("Not a flight List");
            }
            if (arrowField.childrenLength() != 1) {
                throw new IllegalStateException("Incorrect number of child Fields");
            }
            return arrowField.children(0);
        }
    }

    /**
     * Factory method to create a TypeInfo instance.
     * 
     * @param chunkType the output chunk type
     * @param type the Java type to be read into the chunk
     * @param componentType the Java type of nested components
     * @param arrowField the Arrow type to be read into the chunk
     * @return a TypeInfo instance
     */
    static TypeInfo typeInfo(ChunkType chunkType, Class<?> type, Class<?> componentType, Field arrowField) {
        return new TypeInfo(chunkType, type, componentType, arrowField);
    }

    /**
     * Returns a {@link ChunkReader} for the specified arguments.
     *
     * @param options options for reading the stream
     * @param factor a multiplicative factor to apply when reading integers
     * @param typeInfo the type of data to read into a chunk
     * @return a ChunkReader based on the given options, factory, and type to read
     */
    ChunkReader getReader(final StreamReaderOptions options, final int factor, final TypeInfo typeInfo);

    /**
     * Returns a {@link ChunkReader} for the specified arguments.
     *
     * @param options options for reading the stream
     * @param typeInfo the type of data to read into a chunk
     * @return a ChunkReader based on the given options, factory, and type to read
     */
    default ChunkReader getReader(final StreamReaderOptions options, final TypeInfo typeInfo) {
        return getReader(options, 1, typeInfo);
    }

}
