//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.UpdatableTable;
import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.util.input.InputTableValidationException;
import io.deephaven.engine.util.input.StructuredErrorImpl;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangeValidatingInputTable implements InputTableUpdater {
    private final InputTableUpdater wrapped;
    private final String column;
    private final int min;
    private final int max;

    public static Table make(Table input, final String column,
            final int min,
            final int max) {
        final InputTableUpdater updater = (InputTableUpdater) input.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        final RangeValidatingInputTable validatedUpdater = new RangeValidatingInputTable(updater, column, min, max);
        return input.withAttributes(Map.of(Table.INPUT_TABLE_ATTRIBUTE, validatedUpdater));
    }


    private RangeValidatingInputTable(InputTableUpdater wrapped,
            final String column,
            final int min,
            final int max) {
        this.wrapped = wrapped;
        this.column = column;
        final Class<?> dataType = getTableDefinition().getColumn(column).getDataType();
        if (dataType != int.class) {
            throw new IllegalArgumentException("Range column must be an integer, but " + column + " is " + dataType);
        }
        this.min = min;
        this.max = max;
    }

    @Override
    public List<String> getKeyNames() {
        return wrapped.getKeyNames();
    }

    @Override
    public List<String> getValueNames() {
        return wrapped.getValueNames();
    }

    @Override
    public @Nullable String getColumnRestrictions(String columnName) {
        return wrapped.getColumnRestrictions(columnName);
    }

    @Override
    public TableDefinition getTableDefinition() {
        return wrapped.getTableDefinition();
    }

    @Override
    public void validateAddOrModify(Table tableToApply) {
        final List<InputTableValidationException.StructuredError> errors = new ArrayList<>();
        final MutableInt position = new MutableInt(0);
        try (final CloseablePrimitiveIteratorOfInt vals = tableToApply.integerColumnIterator(column)) {
            vals.forEachRemaining((int val) -> {
                if (val < min || val > max) {
                    errors.add(new StructuredErrorImpl(
                            "Value out of range: " + val + " must be between " + min + " and " + max + " inclusive",
                            column, position.get()));
                }
                position.increment();
            });
        }
        if (!errors.isEmpty()) {
            throw new InputTableValidationException(errors);
        }

        wrapped.validateAddOrModify(tableToApply);
    }

    @Override
    public void validateDelete(Table tableToDelete) {
        wrapped.validateDelete(tableToDelete);
    }

    @Override
    public void add(Table newData) throws IOException {
        wrapped.add(newData);
    }

    @Override
    public void addAsync(Table newData, InputTableStatusListener listener) {
        wrapped.addAsync(newData, listener);
    }

    @Override
    public void delete(Table table) throws IOException {
        wrapped.delete(table);
    }

    @Override
    public void deleteAsync(Table table, InputTableStatusListener listener) {
        wrapped.deleteAsync(table, listener);
    }

    @Override
    public boolean isKey(String columnName) {
        return wrapped.isKey(columnName);
    }

    @Override
    public boolean hasColumn(String columnName) {
        return wrapped.hasColumn(columnName);
    }
}
