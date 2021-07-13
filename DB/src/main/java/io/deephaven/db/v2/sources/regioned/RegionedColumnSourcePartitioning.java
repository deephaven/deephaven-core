package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.page.Page;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

class RegionedColumnSourcePartitioning
        extends RegionedColumnSourceArray<String, Attributes.Values, ColumnRegionObject<String, Attributes.Values>>
        implements ColumnSourceGetDefaults.ForObject<String> {

    RegionedColumnSourcePartitioning() {
        // There's no reason to have deferred columns, Supplier::get will just use the column that's made immediately.
        super(ColumnRegionObject.createNull(), String.class, Supplier::get);
    }

    @Override
    public String get(long elementIndex) {
        return lookupRegion(elementIndex).getObject(elementIndex);
    }

    @Override
    public ColumnRegionObject<String, Attributes.Values> makeRegion(@NotNull ColumnDefinition<?> columnDefinition, @NotNull ColumnLocation columnLocation, int regionIndex) {
        return new Constant(columnLocation.getTableLocation().getColumnPartition().toString());
    }

    static final class Constant implements ColumnRegionObject<String, Attributes.Values>, Page.WithDefaultsForRepeatingValues<Attributes.Values> {

        private final String string;

        Constant(String string) {
            this.string = string;
        }

        @Override
        public String getObject(long elementIndex) {
            return string;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, int length) {
            int size = destination.size();

            destination.asWritableObjectChunk().fillWithValue(size, length, string);
            destination.setSize(size + length);
        }

        @Override
        public Class<String> getNativeType() {
            return String.class;
        }
    }
}
