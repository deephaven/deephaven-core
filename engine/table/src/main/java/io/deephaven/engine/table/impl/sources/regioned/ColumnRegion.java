package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.Releasable;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface ColumnRegion<ATTR extends Any> extends Page<ATTR>, Releasable {

    @Override
    @FinalDefault
    default long firstRowOffset() {
        return 0;
    }

    abstract class Null<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegion<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();

            destination.fillWithNullValue(offset, length);
            destination.setSize(offset + length);
        }
    }
}
