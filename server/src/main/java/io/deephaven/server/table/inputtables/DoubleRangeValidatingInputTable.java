//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import com.google.protobuf.Any;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.util.input.InputTableValidationException;
import io.deephaven.engine.util.input.StructuredErrorImpl;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is an example of an {@link InputTableUpdater} that validates that the values in a Double column are within a
 * given range.
 *
 * <p>
 * This class wraps an existing input table, and before performing the underlying validation performs its own validation
 * on the range of the column.
 * </p>
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
public class DoubleRangeValidatingInputTable extends AbstractBaseValidatingInputTable {
    private final String column;
    private final double min;
    private final double max;

    /**
     * Wrap {@code input}, which must be an input table into a new input table that validates that the values in
     * {@code column} are within the range {@code ([min, max]}.
     *
     * @param input the table to wrap
     * @param column the column to validate, must be a double type
     * @param min the minimum value allowed, inclusive
     * @param max the maximum value allowed, inclusive
     * @return a new input table that validates the range of {@code column}
     */
    public static Table make(Table input, final String column,
            final double min,
            final double max) {
        final InputTableUpdater updater = (InputTableUpdater) input.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        final DoubleRangeValidatingInputTable validatedUpdater =
                new DoubleRangeValidatingInputTable(updater, column, min, max);
        return input.withAttributes(Map.of(Table.INPUT_TABLE_ATTRIBUTE, validatedUpdater));
    }


    private DoubleRangeValidatingInputTable(InputTableUpdater wrapped,
            final String column,
            final double min,
            final double max) {
        super(wrapped);
        this.column = column;
        final Class<?> dataType = getTableDefinition().getColumn(column).getDataType();
        if (dataType != double.class) {
            throw new IllegalArgumentException("Range column must be a double, but " + column + " is " + dataType);
        }
        this.min = min;
        this.max = max;
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
        final DoubleRangeRestriction rangeRestriction =
                DoubleRangeRestriction.newBuilder().setMinInclusive(min).setMaxInclusive(max).build();
        result.add(Any.pack(rangeRestriction, "docs.deephaven.io"));
        return result;
    }


    @Override
    public void validateAddOrModify(Table tableToApply) {
        final List<InputTableValidationException.StructuredError> errors = new ArrayList<>();
        final MutableInt position = new MutableInt(0);
        try (final CloseablePrimitiveIteratorOfDouble vals = tableToApply.doubleColumnIterator(column)) {
            vals.forEachRemaining((double val) -> {
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
}

