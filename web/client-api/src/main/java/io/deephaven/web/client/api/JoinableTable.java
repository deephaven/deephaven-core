//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

/**
 * Represents a table which can be joined to another table. Current implementations are {@link JsTable} and
 * {@link JsTotalsTable}.
 */
@JsType(namespace = "dh")
public interface JoinableTable {
    @JsIgnore
    ClientTableState state();

    @JsMethod
    Promise<JsTable> freeze();

    @JsMethod
    Promise<JsTable> snapshot(JsTable baseTable, @JsOptional Boolean doInitialSnapshot,
            @JsOptional String[] stampColumns);

    /**
     * Joins this table to the provided table, using one of the specified join types:
     * <ul>
     * <li><code>AJ</code>, <code>ReverseAJ</code> (or <code>RAJ</code>) - inexact timeseries joins, based on the
     * provided matching rule.</li>
     * <li><code>CROSS_JOIN</code> (or <code>Join</code>) - cross join of all rows that have matching values in both
     * tables.</li>
     * <li><code>EXACT_JOIN</code> (or <code>ExactJoin</code> - matches values in exactly one row in the right table,
     * with errors if there is not exactly one.</li>
     * <li><code>NATURAL_JOIN</code> (or <code>Natural</code> - matches values in at most one row in the right table,
     * with nulls if there is no match or errors if there are multiple matches.</li>
     * </ul>
     *
     * Note that <code>Left</code> join is not supported here, unlike DHE.
     * <p>
     * See the <a href="https://deephaven.io/core/docs/conceptual/choose-joins/">Choose a join method</a> document for
     * more guidance on picking a join operation.
     *
     * @deprecated Instead, call the specific method for the join type.
     * @param joinType The type of join to perform, see the list above.
     * @param rightTable The table to match to values in this table
     * @param columnsToMatch Columns that should match
     * @param columnsToAdd Columns from the right table to add to the result - empty/null/absent to add all columns
     * @param asOfMatchRule If joinType is AJ/RAJ/ReverseAJ, the match rule to use
     * @return a promise that will resolve to the joined table
     */
    @JsMethod
    @Deprecated
    Promise<JsTable> join(String joinType, JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable String asOfMatchRule);

    /**
     * Performs an inexact timeseries join, where rows in this table will have columns added from the closest matching
     * row from the right table.
     * <p>
     * The {@code asOfMatchRule} value can be one of:
     * <ul>
     * <li>LESS_THAN_EQUAL</li>
     * <li>LESS_THAN</li>
     * <li>GREATER_THAN_EQUAL</li>
     * <li>GREATER_THAN</li>
     * </ul>
     *
     * @param rightTable the table to match to values in this table
     * @param columnsToMatch the columns that should match, according to the asOfMatchRole
     * @param columnsToAdd columns from the right table to add to the resulting table, empty/null/absent to add all
     *        columns
     * @param asOfMatchRule the match rule to use, see above
     * @return a promise that will resolve to the joined table
     */
    @JsMethod
    Promise<JsTable> asOfJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable String asOfMatchRule);

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified cross join
     * operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
     * right table being added to the output. The <b>reserveBits</b> optional parameter lets the client control how the
     * key space is distributed between the rows in the two tables, see the Java <b>Table</b> class for details.
     *
     * @param rightTable the table to match to values in this table
     * @param columnsToMatch the columns that should match exactly
     * @param columnsToAdd columns from the right table to add to the resulting table, empty/null/absent to add all
     *        columns
     * @param reserveBits the number of bits of key-space to initially reserve per group, null/absent will let the
     *        server select a value
     * @return a promise that will resolve to the joined table
     */
    @JsMethod
    Promise<JsTable> crossJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable Double reserveBits);

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified exact join
     * operation. The `columnsToAdd` parameter is optional, not specifying it will result in all columns from the right
     * table being added to the output.
     *
     * @param rightTable the table to match to values in this table
     * @param columnsToMatch the columns that should match exactly
     * @param columnsToAdd columns from the right table to add to the resulting table, empty/null/absent to add all
     *        columns
     * @return a promise that will resolve to the joined table
     */
    @JsMethod
    Promise<JsTable> exactJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd);

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified natural join
     * operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
     * right table being added to the output.
     *
     * @param rightTable the table to match to values in this table
     * @param columnsToMatch the columns that should match exactly
     * @param columnsToAdd columns from the right table to add to the resulting table, empty/null/absent to add all
     *        columns
     * @return a promise that will resolve to the joined table
     */
    @JsMethod
    Promise<JsTable> naturalJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd);
}
