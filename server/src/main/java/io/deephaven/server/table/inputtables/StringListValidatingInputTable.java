//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import com.google.protobuf.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.util.input.InputTableValidationException;
import io.deephaven.engine.util.input.StructuredErrorImpl;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an example of an {@link InputTableUpdater} that validates that the values in a String column belong to a
 * given set of allowed values.
 *
 * <p>
 * This class wraps an existing input table, and before performing the underlying validation performs its own validation
 * that the column values are in the allowed set (or null).
 * </p>
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
public class StringListValidatingInputTable extends AbstractBaseValidatingInputTable {
    private final String column;
    private final Set<String> allowedValues;
    private final List<String> allowedValuesList;

    /**
     * Wrap {@code input}, which must be an input table into a new input table that validates that the values in
     * {@code column} belong to the given set of allowed values.
     *
     * @param input the table to wrap
     * @param column the column to validate, must be a String type
     * @param allowedValues the array of allowed values
     * @return a new input table that validates {@code column} values are in the allowed set
     */
    public static Table make(Table input, final String column, final String... allowedValues) {
        final InputTableUpdater updater = (InputTableUpdater) input.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        final StringListValidatingInputTable validatedUpdater =
                new StringListValidatingInputTable(updater, column, allowedValues);
        return input.withAttributes(Map.of(Table.INPUT_TABLE_ATTRIBUTE, validatedUpdater));
    }


    private StringListValidatingInputTable(InputTableUpdater wrapped, final String column,
            final String... allowedValues) {
        super(wrapped);
        this.column = column;
        this.allowedValuesList = List.of(allowedValues);
        this.allowedValues = Set.of(allowedValues);
        final Class<?> dataType = getTableDefinition().getColumn(column).getDataType();
        if (dataType != String.class) {
            throw new IllegalArgumentException(
                    "String list validation only applies to String columns, but " + column + " is " + dataType);
        }
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
        final StringListRestriction stringListRestriction =
                StringListRestriction.newBuilder().addAllAllowedValues(allowedValuesList).build();
        result.add(Any.pack(stringListRestriction, "docs.deephaven.io"));
        return result;
    }


    @Override
    public void validateAddOrModify(Table tableToApply) {
        final List<InputTableValidationException.StructuredError> errors = new ArrayList<>();
        final MutableInt position = new MutableInt(0);
        final ColumnSource<String> columnSource = tableToApply.getColumnSource(column, String.class);

        try (final CloseableIterator<String> it = tableToApply.columnIterator(column)) {
            it.forEachRemaining(value -> {
                if (!allowedValues.contains(value)) {
                    errors.add(new StructuredErrorImpl(
                            "Value '" + value + "' is not in the allowed list: " + allowedValuesList,
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

