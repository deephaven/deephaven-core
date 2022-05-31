package io.deephaven.parquet.table.region;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegion;

interface ParquetColumnRegion<ATTR extends Any> extends ColumnRegion<ATTR> {
    ChunkPage<ATTR> getChunkPageContaining(long elementIndex);
}
