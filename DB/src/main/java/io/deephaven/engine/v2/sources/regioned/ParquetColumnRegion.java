package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.page.ChunkPage;

interface ParquetColumnRegion<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
