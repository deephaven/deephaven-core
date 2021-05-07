package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class RegionedColumnSourceDictionaryKey<T> extends RegionedColumnSourceInt.NativeType<T, Attributes.DictionaryKeys> {

    RegionedColumnSourceDictionaryKey(RegionedColumnSourceBase<T, Attributes.DictionaryKeys, ColumnRegionReferencing<Attributes.DictionaryKeys, ColumnRegionInt<Attributes.DictionaryKeys>>> outerColumnSource) {
        super(outerColumnSource);
    }

    @Override
    public ColumnRegionInt<Attributes.DictionaryKeys> makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
                                                                 @NotNull ColumnLocation<?> columnLocation,
                                                                 int regionIndex) {
        if (columnLocation.exists()) {
            if (columnLocation.getFormat() == TableLocation.Format.PARQUET) {
                ColumnChunkPageStore<Attributes.DictionaryKeys> columnChunkPageStore =
                        columnLocation.asParquetFormat().getDictionaryKeysPageStore(columnDefinition);
                return columnChunkPageStore == null ? null :
                        new ParquetColumnRegionInt<>(Objects.requireNonNull(columnChunkPageStore));
            }
            throw new IllegalArgumentException("Unsupported column location format " + columnLocation.getFormat() + " in " + columnLocation);
        }

        return null;
    }
}
