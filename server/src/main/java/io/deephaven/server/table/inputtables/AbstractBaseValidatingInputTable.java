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

/**
 * An abstract base class for {@link InputTableUpdater} implementations that wrap an existing input table.
 *
 * <p>
 * This class provides a default implementation for most methods by delegating to the wrapped input table. Subclasses
 * should override {@link #getColumnRestrictions(String)} and {@link #validateAddOrModify(Table)} to provide custom
 * validation logic.
 * </p>
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
public abstract class AbstractBaseValidatingInputTable implements InputTableUpdater {
    protected final InputTableUpdater wrapped;

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

