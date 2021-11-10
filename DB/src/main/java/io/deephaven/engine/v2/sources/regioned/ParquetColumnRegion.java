package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.page.ChunkPage;

interface ParquetColumnRegion<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
