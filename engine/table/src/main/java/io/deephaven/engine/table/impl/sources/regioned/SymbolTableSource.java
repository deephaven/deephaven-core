package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * <p>
 * Interface for {@link ColumnSource}s that can provide a {@link Table} view of their symbol tables, providing a many:1
 * or 1:1 mapping of unique {@code long} identifiers to the symbol values in this source.
 * <p>
 * Such sources are also expected to be reinterpretable ({@link ColumnSource#allowsReinterpret(Class)}) as {@code long}
 * {@link ColumnSource}s of the same identifiers.
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
     * @param sourceIndex The {@link RowSet} whose keys must be mappable
     * @return Whether this SymbolTableSource can provide a symbol table that covers all keys in {@code sourceIndex}.
     */
    boolean hasSymbolTable(@NotNull final RowSet sourceIndex);

    /**
     * <p>
     * Get a static {@link Table} view of this SymbolTableSource's symbol table, providing a many:1 or 1:1 mapping of
     * unique {@code long} identifiers to the symbol values in this source.
     *
     * @param sourceIndex The {@link RowSet} whose keys must be mappable via the result {@link Table}'s identifier
     *        column
     * @param useLookupCaching Hint whether symbol lookups performed to generate the symbol table should apply caching.
     *        Implementations may ignore this hint.
     * @return The symbol table
     */
    Table getStaticSymbolTable(@NotNull RowSet sourceIndex, boolean useLookupCaching);

    /**
     * <p>
     * Get a {@link Table} view of this SymbolTableSource's symbol table, providing a many:1 or 1:1 mapping of unique
     * {@code long} identifiers to the symbol values in this source.
     *
     * <p>
     * The result will be refreshing if {@code table} {@link Table#isRefreshing() is refreshing}.
     *
     * @param sourceTable The {@link QueryTable} whose row keys must be mappable via the result {@link Table}'s
     *        identifier column
     * @param useLookupCaching Hint whether symbol lookups performed to generate the symbol table should apply caching.
     *        Implementations may ignore this hint.
     * @return The symbol table
     */
    Table getSymbolTable(@NotNull QueryTable sourceTable, boolean useLookupCaching);
}
