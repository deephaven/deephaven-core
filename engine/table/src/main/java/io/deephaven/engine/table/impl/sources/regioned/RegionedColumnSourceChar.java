package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Regioned column source implementation for columns of chars.
 */
abstract class RegionedColumnSourceChar<ATTR extends Values>
        extends RegionedColumnSourceArray<Character, ATTR, ColumnRegionChar<ATTR>>
        implements ColumnSourceGetDefaults.ForChar {

    RegionedColumnSourceChar(@NotNull final ColumnRegionChar<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionChar<ATTR>> makeDeferred) {
        super(nullRegion, char.class, makeDeferred);
    }

    @Override
    public char getChar(final long elementIndex) {
        return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex)).getChar(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionChar<Values>> {
        @Override
        default ColumnRegionChar<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionChar(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceChar<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionChar.createNull(PARAMETERS.regionMask), DeferredColumnRegionChar::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native char type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Character, ColumnRegionChar<ATTR>>
            implements ColumnSourceGetDefaults.ForChar {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionChar<ATTR>>> outerColumnSource) {
            super(Character.class, outerColumnSource);
        }

        @Override
        public char getChar(final long elementIndex) {
            return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex)).getChar(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionChar<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceChar<Values> {

        Partitioning() {
            super(ColumnRegionChar.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionChar<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !Character.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a Character at location " + locationKey);
            }
            return new ColumnRegionChar.Constant<>(regionMask(), unbox((Character) partitioningColumnValue));
        }
    }
}
