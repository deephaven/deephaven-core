package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.StringCache;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Interface for {@link io.deephaven.db.v2.sources.ColumnSource}s that can provide a {@link Table} view of their
 * symbol tables, providing a many:1 or 1:1 mapping of unique {@code long} identifiers to the symbol values in this
 * source.
 * <p>Such sources are also expected to be reinterpretable ({@link ColumnSource#allowsReinterpret(Class)}) as
 * {@code long} {@link ColumnSource}s of the same identifiers.
 */
public interface SymbolTableSource<SYMBOL_TYPE> extends ColumnSource<SYMBOL_TYPE> {

    /**
     * The name for the column of {@code long} identifiers.
     */
    String ID_COLUMN_NAME = "ID";

    /**
     * The name for the column of symbol values, which will have the same data type as this column source.
     */
    String SYMBOL_COLUMN_NAME = "Symbol";

    /**
     * <p>Get a static {@link Table} view of this SymbolTableSource's symbol table, providing a many:1 or 1:1 mapping of
     * unique {@code long} identifiers to the symbol values in this source.
     *
     * @param sourceIndex      The {@link Index} whose keys must be mappable via the result {@link Table}'s identifier
     *                         column
     * @param useLookupCaching Whether symbol lookups performed to generate the symbol table should apply caching
     * @return The symbol table
     */
    Table getStaticSymbolTable(@NotNull Index sourceIndex, boolean useLookupCaching);

    /**
     * <p>Get a {@link Table} view of this SymbolTableSource's symbol table, providing a many:1 or 1:1 mapping of unique
     * {@code long} identifiers to the symbol values in this source.
     *
     * <p>The result will be refreshing if {@code table} is a refreshing {@link io.deephaven.db.v2.DynamicTable}.
     *
     * @param sourceTable      The {@link QueryTable} whose {@link Index} keys must be mappable via the result
     *                         {@link Table}'s identifier column
     * @param useLookupCaching Whether symbol lookups performed to generate the symbol table should apply caching
     * @return The symbol table
     */
    Table getSymbolTable(@NotNull QueryTable sourceTable, boolean useLookupCaching);

    /**
     * Construct a symbol column source for the supplied location, with no built-in offset caching.
     *
     * @param columnDefinition The column definition
     * @param columnLocation   The column location
     * @param cache            The string cache
     * @return A symbol column source for the supplied location
     */
    static <STRING_LIKE_TYPE extends CharSequence> ColumnSource<STRING_LIKE_TYPE> makeSymbolColumnSource(@NotNull final ColumnDefinition<STRING_LIKE_TYPE> columnDefinition,
                                                                                                         @NotNull final ColumnLocation columnLocation,
                                                                                                         final StringCache<STRING_LIKE_TYPE> cache) {
        final RegionedColumnSource<STRING_LIKE_TYPE> columnSource = makeSymbolColumnSourceInternal(columnDefinition, cache);
        columnSource.addRegion(columnDefinition, columnLocation);
        return columnSource;
    }

    @VisibleForTesting
    static <STRING_LIKE_TYPE extends CharSequence> RegionedColumnSourceSymbol<STRING_LIKE_TYPE, ?> makeSymbolColumnSourceInternal(@NotNull final ColumnDefinition<STRING_LIKE_TYPE> columnDefinition,
                                                                                                                                  final StringCache<STRING_LIKE_TYPE> cache) {
        return RegionedColumnSourceSymbol.createWithoutCache(RegionedTableComponentFactory.getStringDecoder(cache, columnDefinition), cache.getType());
    }
}
