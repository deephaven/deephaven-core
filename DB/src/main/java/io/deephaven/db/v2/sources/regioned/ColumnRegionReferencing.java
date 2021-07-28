package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

public interface ColumnRegionReferencing<ATTR extends Attributes.Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
        extends ColumnRegion<ATTR> {

    @NotNull
    REFERENCED_COLUMN_REGION getReferencedRegion();

    interface Converter<ATTR extends Attributes.Any> {

        /**
         * Converts all the native source values represented by {@code orderedKeys} <em>from a single region</em> into
         * the {@code destination} chunk by appending.
         */
        void convertRegion(WritableChunk<? super ATTR> destination, Chunk<? extends ATTR> source, OrderedKeys orderedKeys);
    }

    class Null<ATTR extends Attributes.Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>> extends ColumnRegion.Null<ATTR>
            implements ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION> {

        private final REFERENCED_COLUMN_REGION nullReferencedColumnRegion;

        public Null(REFERENCED_COLUMN_REGION nullReferencedColumnRegion) {
            this.nullReferencedColumnRegion = nullReferencedColumnRegion;
        }

        @NotNull @Override
        public REFERENCED_COLUMN_REGION getReferencedRegion() {
            return nullReferencedColumnRegion;
        }
    }
}
