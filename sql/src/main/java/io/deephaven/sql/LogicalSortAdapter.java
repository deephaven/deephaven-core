//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.literal.Literal;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;

import java.util.Objects;

final class LogicalSortAdapter {

    public static TableSpec namedTable(LogicalSort sort, NamedAdapter namedAdapter) {
        return of(sort, namedAdapter, namedAdapter);
    }

    public static TableSpec indexTable(LogicalSort sort, IndexRef indexRef) {
        return of(sort, indexRef, indexRef);
    }

    private static TableSpec of(LogicalSort sort, RelNodeAdapter relNodeAdapter, FieldAdapter fieldAdapter) {
        // SQLTODO(sort-offset)
        if (sort.offset != null) {
            throw new UnsupportedOperationException("SQLTODO(sort-offset): QST does not support slice...");
        }
        final TableSpec parent = relNodeAdapter.table(sort.getInput());
        final TableSpec sortTable;
        if (sort.collation.getFieldCollations().isEmpty()) {
            // Calcite represents `SELECT * FROM my_table LIMIT 7` as a LogicalSort with an empty field collation.
            // We can skip all of the SortTable.builder() wrapping in this case.
            sortTable = parent;
        } else {
            final SortTable.Builder builder = SortTable.builder().parent(parent);
            for (RelFieldCollation fieldCollation : sort.collation.getFieldCollations()) {
                final int inputFieldIndex = fieldCollation.getFieldIndex();
                final RelDataTypeField inputField = sort.getInput().getRowType().getFieldList().get(inputFieldIndex);
                final RexInputRef inputRef = new RexInputRef(inputFieldIndex, inputField.getType());
                final ColumnName inputColumnName = fieldAdapter.input(inputRef, inputField);
                switch (fieldCollation.direction) {
                    case ASCENDING:
                        if (fieldCollation.nullDirection == NullDirection.LAST) {
                            throw new UnsupportedOperationException(
                                    "Deephaven does not currently support ascending sort with nulls last");
                        }
                        builder.addColumns(SortColumn.asc(inputColumnName));
                        break;
                    case DESCENDING:
                        if (fieldCollation.nullDirection == NullDirection.FIRST) {
                            throw new UnsupportedOperationException(
                                    "Deephaven does not currently support descending sort with nulls first");
                        }
                        builder.addColumns(SortColumn.desc(inputColumnName));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported sort direction " + fieldCollation.direction);
                }
            }
            sortTable = builder.build();
        }
        if (sort.fetch != null) {
            final Expression expression = fieldAdapter.expression(sort, sort.fetch);
            if (!(expression instanceof Literal)) {
                throw new UnsupportedOperationException("Deephaven only supports literals for FETCH");
            }
            return ((Literal) expression).walk(new ApplyHead(sortTable));
        }
        return sortTable;
    }

    private static class ApplyHead implements Literal.Visitor<TableSpec> {
        private final TableSpec input;

        public ApplyHead(TableSpec input) {
            this.input = Objects.requireNonNull(input);
        }

        @Override
        public TableSpec visit(int literal) {
            return input.head(literal);
        }

        @Override
        public TableSpec visit(long literal) {
            return input.head(literal);
        }

        @Override
        public TableSpec visit(byte literal) {
            return input.head(literal);
        }

        @Override
        public TableSpec visit(short literal) {
            return input.head(literal);
        }

        @Override
        public TableSpec visit(boolean literal) {
            throw new UnsupportedOperationException("Unable to LIMIT number of rows from boolean literal");
        }

        @Override
        public TableSpec visit(float literal) {
            throw new UnsupportedOperationException("Unable to LIMIT number of rows from float literal");
        }

        @Override
        public TableSpec visit(double literal) {
            throw new UnsupportedOperationException("Unable to LIMIT number of rows from double literal");
        }

        @Override
        public TableSpec visit(String literal) {
            throw new UnsupportedOperationException("Unable to LIMIT number of rows from String literal");
        }

        @Override
        public TableSpec visit(char literal) {
            throw new UnsupportedOperationException("Unable to LIMIT number of rows from char literal");
        }
    }
}
