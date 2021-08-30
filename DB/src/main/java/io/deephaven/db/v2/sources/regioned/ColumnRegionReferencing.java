package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface ColumnRegionReferencing<ATTR extends Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
    extends ColumnRegion<ATTR> {

    @NotNull
    REFERENCED_COLUMN_REGION getReferencedRegion();

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        throw new UnsupportedOperationException(getClass() + " does not know its chunk type");
    }

    interface Converter<ATTR extends Any> {

        /**
         * Converts all the native source values represented by {@code orderedKeys} <em>from a
         * single region</em> into the {@code destination} chunk by appending.
         */
        void convertRegion(WritableChunk<? super ATTR> destination, Chunk<? extends ATTR> source,
            OrderedKeys orderedKeys);
    }

    class Null<ATTR extends Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
        extends ColumnRegion.Null<ATTR>
        implements ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION> {

        private final REFERENCED_COLUMN_REGION nullReferencedColumnRegion;

        public Null(REFERENCED_COLUMN_REGION nullReferencedColumnRegion) {
            super((nullReferencedColumnRegion.mask()));
            this.nullReferencedColumnRegion = nullReferencedColumnRegion;
        }

        @Override
        @NotNull
        public REFERENCED_COLUMN_REGION getReferencedRegion() {
            return nullReferencedColumnRegion;
        }
    }
}
