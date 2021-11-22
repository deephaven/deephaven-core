package io.deephaven.parquet.table.region;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegion;

interface ParquetColumnRegion<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
