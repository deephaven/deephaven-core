//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import com.google.protobuf.Any;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * An abstract base class for {@link InputTableUpdater} implementations that wrap an existing input table.
 *
 * <p>
 * This class provides a default implementation for most methods by delegating to the wrapped input table. Subclasses
 * should override {@link #getColumnRestrictions(String)} and {@link #validateAddOrModify(Table)} to provide custom
 * validation logic.
 * </p>
 */
@TestUseOnly
public abstract class AbstractBaseValidatingInputTable implements InputTableUpdater {
    protected final InputTableUpdater wrapped;

    /**
     * Wraps an existing input table updater with a new validating updater created by the provided {@code createUpdater}
     * function.
     *
     * @param input the input table, must have an {@link InputTableUpdater} as its {@link Table#INPUT_TABLE_ATTRIBUTE}
     * @param createUpdater a function that takes the existing input table's updater and returns a new validating
     *        updater
     * @return a new input table that validates according to the provided {@code createUpdater} function
     */
    protected static Table wrapUpdater(final Table input, final UnaryOperator<InputTableUpdater> createUpdater) {
        final InputTableUpdater updater = (InputTableUpdater) input.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        return input.withAttributes(Map.of(Table.INPUT_TABLE_ATTRIBUTE, createUpdater.apply(updater)));
    }

    /**
     * Construct a new validating input table that wraps the given input table.
     *
     * @param wrapped the input table to wrap
     */
    protected AbstractBaseValidatingInputTable(InputTableUpdater wrapped) {
        this.wrapped = wrapped;
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
    public abstract @Nullable List<Any> getColumnRestrictions(String columnName);

    @Override
    public TableDefinition getTableDefinition() {
        return wrapped.getTableDefinition();
    }

    @Override
    public abstract void validateAddOrModify(Table tableToApply);

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

