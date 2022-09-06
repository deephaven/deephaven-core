/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Value;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * The interface for a query table to perform retrieve values from a column for select like operations.
 */
public interface SelectColumn extends Selectable {

    static SelectColumn of(Selectable selectable) {
        return (selectable instanceof SelectColumn)
                ? (SelectColumn) selectable
                : selectable.expression().walk(new ExpressionAdapter(selectable.newColumn())).getOut();
    }

    static SelectColumn[] from(Selectable... selectables) {
        return from(Arrays.asList(selectables));
    }

    static SelectColumn[] from(Collection<? extends Selectable> selectables) {
        return selectables.stream().map(SelectColumn::of).toArray(SelectColumn[]::new);
    }

    static SelectColumn[] copyFrom(SelectColumn[] selectColumns) {
        return Arrays.stream(selectColumns).map(SelectColumn::copy).toArray(SelectColumn[]::new);
    }

    /**
     * Convenient static final instance of a zero length Array of SelectColumns for use in toArray calls.
     */
    SelectColumn[] ZERO_LENGTH_SELECT_COLUMN_ARRAY = new SelectColumn[0];

    /**
     * Initialize the SelectColumn using the input table and return a list of underlying columns that this SelectColumn
     * is dependent upon.
     *
     * @param table the table to initialize internals from
     * @return a list containing all columns from 'table' that the result depends on
     */
    List<String> initInputs(Table table);

    /**
     * Initialize the column from the provided set of underlying columns and row set.
     *
     * @param rowSet the base row set
     * @param columnsOfInterest the input columns
     *
     * @return a list of columns on which the result of this is dependent
     */
    List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest);

    /**
     * Initialize any internal column definitions from the provided initial.
     *
     * @param columnDefinitionMap the starting set of column definitions
     *
     * @return a list of columns on which the result of this is dependent
     * @apiNote Any {@link io.deephaven.engine.context.QueryLibrary}, {@link io.deephaven.engine.context.QueryScope}, or
     *          {@link QueryCompiler} usage needs to be resolved within initDef. Implementations must be idempotent.
     */
    List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap);

    /**
     * Get the data type stored in the resultant column.
     *
     * @return the type
     */
    Class<?> getReturnedType();

    /**
     * Get a list of the names of columns used in this SelectColumn. Behavior is undefined if none of the init* methods
     * have been called yet.
     *
     * @return the columns used in this SelectColumn
     */
    List<String> getColumns();

    /**
     * Get a list of the names of column arrays used in this SelectColumn. Behavior is undefined if none of the init*
     * methods have been called yet.
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
    ColumnSource<?> getDataView();

    /**
     * Returns a lazily computed view of this column.
     *
     * @return a lazily computed column source
     */
    @NotNull
    ColumnSource<?> getLazyView();

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
     * Create a new {@link WritableColumnSource}.
     *
     * The returned column source must be capable of handling updates.
     *
     * @param size A hint as to the number of rows that will be used
     *
     * @return a new {@link WritableColumnSource}
     */
    WritableColumnSource<?> newDestInstance(long size);

    /**
     * Create a new {@link io.deephaven.engine.table.ColumnSource#isImmutable immutable} {@link WritableColumnSource}.
     *
     * The returned column source should be flat, and need not handle updates.
     *
     * @param size A hint as to the number of rows that will be used
     *
     * @return a new {@link WritableColumnSource}
     */
    WritableColumnSource<?> newFlatDestInstance(long size);

    /**
     *
     * @return
     */
    boolean isRetain();

    /**
     * Should we disallow use of this column for refreshing tables?
     *
     * Some formulas can not be reliably computed with a refreshing table, therefore we will refuse to compute those
     * values.
     */
    boolean disallowRefresh();

    /**
     * Returns true if this column is stateless (i.e. one row does not depend on the order of evaluation for another
     * row).
     */
    boolean isStateless();

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
            out = SelectColumnFactory.getExpression(String.format("%s=%s", lhs.name(), rhs.value()));
        }

        @Override
        public void visit(long rhs) {
            out = SelectColumnFactory.getExpression(String.format("%s=%dL", lhs.name(), rhs));
        }
    }

    // region Selectable impl

    @Override
    default ColumnName newColumn() {
        return ColumnName.of(getName());
    }

    @Override
    default Expression expression() {
        throw new UnsupportedOperationException("SelectColumns do not implement expression");
    }

    // endregion Selectable impl
}
