//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Type;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 *
 */
public interface ChunkReadingFactory {
    /**
     *
     */
    class ChunkTypeInfo {
        private final ChunkType chunkType;
        private final Class<?> type;
        private final Class<?> componentType;
        private final Field arrowField;

        public ChunkTypeInfo(ChunkType chunkType, Class<?> type, Class<?> componentType, Field arrowField) {
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
     *
     * @param options
     * @param factor
     * @param typeInfo
     * @return
     * @throws IOException
     */
    ChunkReader extractChunkFromInputStream(
            final StreamReaderOptions options,
            final int factor,
            final ChunkTypeInfo typeInfo) throws IOException;

    /**
     *
     * @param options
     * @param typeInfo
     * @return
     * @throws IOException
     */
    default ChunkReader extractChunkFromInputStream(final StreamReaderOptions options, final ChunkTypeInfo typeInfo)
            throws IOException {
        return extractChunkFromInputStream(options, 1, typeInfo);
    }

}
