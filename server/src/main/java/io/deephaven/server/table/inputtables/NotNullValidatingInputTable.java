//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import com.google.protobuf.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.util.input.InputTableValidationException;
import io.deephaven.engine.util.input.StructuredErrorImpl;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * This is an example of an {@link InputTableUpdater} that validates that the values in a column are not null.
 *
 * <p>
 * This class wraps an existing input table, and before performing the underlying validation performs its own validation
 * that the column does not contain null values.
 * </p>
 */
public class NotNullValidatingInputTable extends AbstractBaseValidatingInputTable {
    private final String column;

    /**
     * Wrap {@code input}, which must be an input table into a new input table that validates that the values in
     * {@code column} are not null.
     *
     * @param input the table to wrap
     * @param column the column to validate
     * @return a new input table that validates {@code column} is not null
     */
    public static Table make(Table input, final String column) {
        return wrapUpdater(input, updater -> new NotNullValidatingInputTable(updater, column));
    }


    private NotNullValidatingInputTable(InputTableUpdater wrapped, final String column) {
        super(wrapped);
        this.column = column;
    }


    @Override
    public @Nullable List<Any> getColumnRestrictions(String columnName) {
        final List<Any> columnRestrictions = wrapped.getColumnRestrictions(columnName);
        if (!columnName.equals(column)) {
            return columnRestrictions;
        }

        final List<Any> result = new ArrayList<>();
        if (columnRestrictions != null) {
            result.addAll(columnRestrictions);
        }
        final NotNullRestriction notNullRestriction = NotNullRestriction.newBuilder().build();
        result.add(Any.pack(notNullRestriction, "docs.deephaven.io"));
        return result;
    }


    @Override
    public void validateAddOrModify(Table tableToApply) {
        final List<InputTableValidationException.StructuredError> errors = new ArrayList<>();
        final MutableInt position = new MutableInt(0);

        try (final CloseableIterator<Object> it = tableToApply.columnIterator(column)) {
            it.forEachRemaining(value -> {
                if (value == null) {
                    errors.add(new StructuredErrorImpl(
                            "Value must not be null",
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
}

