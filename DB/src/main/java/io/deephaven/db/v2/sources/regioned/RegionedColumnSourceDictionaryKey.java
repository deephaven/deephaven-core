package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import org.jetbrains.annotations.NotNull;

class RegionedColumnSourceDictionaryKey<T> extends RegionedColumnSourceInt.NativeType<T, DictionaryKeys> {

    RegionedColumnSourceDictionaryKey(RegionedColumnSourceBase<T, DictionaryKeys, ColumnRegionReferencing<DictionaryKeys, ColumnRegionInt<DictionaryKeys>>> outerColumnSource) {
        super(outerColumnSource);
    }

    @Override
    public ColumnRegionInt<DictionaryKeys> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                      @NotNull final ColumnLocation<?> columnLocation,
                                                      final int regionIndex) {
        if (columnLocation.exists()) {
            return columnLocation.makeDictionaryKeysRegion(columnDefinition);
        }

        return null;
    }
}
