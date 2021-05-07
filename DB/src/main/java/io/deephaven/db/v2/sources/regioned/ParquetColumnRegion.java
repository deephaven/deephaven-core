package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;

interface ParquetColumnRegion<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
