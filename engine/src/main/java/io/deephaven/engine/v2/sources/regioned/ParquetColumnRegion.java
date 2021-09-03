package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.page.ChunkPage;

interface ParquetColumnRegion<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
