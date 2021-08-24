/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Value;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The interface for a query table to perform retrieve values from a column for select like
 * operations.
 */
public interface SelectColumn {

    static SelectColumn of(Selectable selectable) {
        return selectable.expression().walk(new ExpressionAdapter(selectable.newColumn())).getOut();
    }

    static SelectColumn[] from(Collection<? extends Selectable> selectables) {
        return selectables.stream().map(SelectColumn::of).toArray(SelectColumn[]::new);
    }

    /**
     * Convenient static final instance of a zero length Array of SelectColumns for use in toArray
     * calls.
     */
    SelectColumn[] ZERO_LENGTH_SELECT_COLUMN_ARRAY = new SelectColumn[0];

    /**
     * Initialize the SelectColumn using the input table and return a list of underlying columns
     * that this SelectColumn is dependent upon.
     *
     * @param table the table to initialize internals from
     * @return a list containing all columns from 'table' that the result depends on
     */
    List<String> initInputs(Table table);

    /**
     * Initialize the column from the provided set of underlying columns and index.
     *
     * @param index the base index
     * @param columnsOfInterest the input columns
     *
     * @return a list of columns on which the result of this is dependent
     */
    List<String> initInputs(Index index, Map<String, ? extends ColumnSource> columnsOfInterest);

    /**
     * Initialize any internal column definitions from the provided initial.
     *
     * @param columnDefinitionMap the starting set of column definitions
     *
     * @return a list of columns on which the result of this is dependent
     */
    List<String> initDef(Map<String, ColumnDefinition> columnDefinitionMap);

    /**
     * Get the data type stored in the resultant column.
     *
     * @return the type
     */
    Class getReturnedType();

    /**
     * Get a list of the names of columns used in this SelectColumn. Behavior is undefined if none
     * of the init* methods have been called yet.
     * 
     * @return the columns used in this SelectColumn
     */
    List<String> getColumns();

    /**
     * Get a list of the names of column arrays used in this SelectColumn. Behavior is undefined if
     * none of the init* methods have been called yet.
     * 
     * @return the list of column arrays used
     */
    List<String> getColumnArrays();

    /**
     * Get a {@link ColumnSource} that can be used to access the data on demand.
     *
     * @return a {@link ColumnSource}
     */
    @NotNull
    ColumnSource getDataView();

    /**
     * Returns a lazily computed view of this column.
     *
     * @return a lazily computed column source
     */
    @NotNull
    ColumnSource getLazyView();

    /**
     * Get the name of the resultant column.
     *
     * @return the name of the column
     */
    String getName();

    /**
     * Get a MatchPair for this column, if applicable.
     * 
     * @return
     */
    MatchPair getMatchPair();

    /**
     * Create a new {@link WritableSource} with sufficient capacity for the rows in the index.
     *
     * @param size The number of rows to allocate
     *
     * @return a new {@link WritableSource} with sufficient capacity for 'dataSubset'
     */
    WritableSource newDestInstance(long size);

    /**
     *
     * @return
     */
    boolean isRetain();

    /**
     * Should we disallow use of this column for refreshing tables?
     *
     * Some formulas can not be reliably computed with a refreshing table, therefore we will refuse
     * to compute those values.
     */
    boolean disallowRefresh();

    /**
     * Create a copy of this SelectColumn.
     *
     * @return an independent copy of this SelectColumn.
     */
    SelectColumn copy();

    class ExpressionAdapter implements Expression.Visitor, Value.Visitor {
        private final ColumnName lhs;
        private SelectColumn out;

        ExpressionAdapter(ColumnName lhs) {
            this.lhs = Objects.requireNonNull(lhs);
        }

        public SelectColumn getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(Value rhs) {
            rhs.walk((Value.Visitor) this);
        }

        @Override
        public void visit(ColumnName rhs) {
            out = new SourceColumn(rhs.name(), lhs.name());
        }

        @Override
        public void visit(RawString rhs) {
            out =
                SelectColumnFactory.getExpression(String.format("%s=%s", lhs.name(), rhs.value()));
        }

        @Override
        public void visit(long rhs) {
            out = SelectColumnFactory.getExpression(String.format("%s=%dL", lhs.name(), rhs));
        }
    }
}
