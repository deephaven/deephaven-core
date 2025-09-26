//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.lang.FormulaMethodInvocations;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.engine.validation.MethodInvocationValidator;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Validates a column expression coming from gRPC, to ensure that the included code will use the customer defined list
 * of allowed methods.
 *
 * <p>
 * The formula is compiled as part of this validation, and the
 * {@link io.deephaven.engine.table.impl.lang.QueryLanguageParser} produces a list of the methods that the formula would
 * invoke if executed. The set of used methods is compared against our allowlist.
 * </p>
 *
 * <p>
 * This validator is stricter than the {@link MethodNameColumnExpressionValidator}, which does not take the class
 * instances into account.
 * </p>
 *
 * <p>
 * The {@link ExpressionValidatorModule#getParsingColumnExpressionValidatorFromConfiguration(Configuration)} method is
 * used to create a validator based on configuration properties.
 * </p>
 */
public class ParsingColumnExpressionValidator implements ColumnExpressionValidator {
    private final Collection<MethodInvocationValidator> methodInvocationValidators;

    /**
     * The collection of method validators to check each invocation against. At least one validator must return true for
     * the method to be permitted. If any validator returns false, then the
     *
     * @param methodInvocationValidators the collection of method validators to use
     */
    public ParsingColumnExpressionValidator(final Collection<MethodInvocationValidator> methodInvocationValidators) {
        this.methodInvocationValidators = methodInvocationValidators;
    }

    @Override
    public WhereFilter[] validateSelectFilters(final String[] conditionalExpressions, final Table table) {
        final WhereFilter[] whereFilters = WhereFilterFactory.getExpressions(conditionalExpressions);
        return doValidateWhereFilters(conditionalExpressions, table, whereFilters);
    }

    @Override
    public void validateConditionFilters(final List<ConditionFilter> conditionFilters, final Table table) {
        for (final ConditionFilter conditionFilter : conditionFilters) {
            doValidateWhereFilters(new String[] {conditionFilter.toString()}, table,
                    new WhereFilter[] {conditionFilter});
        }
    }

    private WhereFilter @NotNull [] doValidateWhereFilters(final String[] conditionalExpressions,
            final Table table,
            final WhereFilter[] whereFilters) {
        final List<String> dummyAssignments = new ArrayList<>();
        for (int ii = 0; ii < whereFilters.length; ++ii) {
            final WhereFilter sf = whereFilters[ii];
            if (sf instanceof ConditionFilter) {
                dummyAssignments
                        .add(String.format("__boolean_placeholder_%d__ = (%s)", ii, conditionalExpressions[ii]));
            }
        }
        if (!dummyAssignments.isEmpty()) {
            final String[] daArray = dummyAssignments.toArray(String[]::new);
            final SelectColumn[] selectColumns = SelectColumnFactory.getExpressions(daArray);
            validateColumnExpressions(selectColumns, daArray, table);
        }
        return whereFilters;
    }

    @Override
    public void validateColumnExpressions(final SelectColumn[] selectColumns, final String[] originalExpressions,
            final Table table) {
        // It's unfortunate that we have to validateSelect which does a bunch of analysis, just to get throw-away cloned
        // columns back, so we can check here for disallowed methods. (We need to make sure SwitchColumns get
        // initialized.)
        final QueryTable prototype = (QueryTable) TableTools.newTable(table.getDefinition());
        final SelectColumn[] clonedColumns = prototype.validateSelect(selectColumns).getClonedColumns();
        validateColumnExpressions(clonedColumns, originalExpressions);
    }


    private void validateColumnExpressions(final SelectColumn[] selectColumns,
            final String[] originalExpressions) {
        assert (selectColumns.length == originalExpressions.length);
        for (int ii = 0; ii < selectColumns.length; ++ii) {
            validateSelectColumn(selectColumns[ii], originalExpressions[ii]);
        }
    }

    private void validateSelectColumn(SelectColumn selectColumn, final String originalExpression) {
        while (selectColumn instanceof SwitchColumn) {
            selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
        }

        if (!(selectColumn instanceof FormulaColumn)) {
            // other variants should be safe, only test DhFormulaColumn
            return;
        }

        if (selectColumn instanceof DhFormulaColumn) {
            // we can properly validate this; anything else cannot be properly validated
            final DhFormulaColumn dhFormulaColumn = (DhFormulaColumn) selectColumn;
            validateInvocations(dhFormulaColumn.getFormulaMethodInvocations());
        } else {
            throw new IllegalStateException("Cannot validate user expression: " + originalExpression);
        }
    }

    private void validateInvocations(@NotNull final FormulaMethodInvocations formulaMethodInvocations) {
        if (formulaMethodInvocations.hasImplicitCalls()) {
            throw new IllegalStateException("User expression may not use implicit method calls.");
        }
        for (final Constructor<?> constructor : formulaMethodInvocations.getUsedConstructors()) {
            if (!isPermitted(constructor)) {
                throw new IllegalStateException(
                        "User expressions are not permitted to instantiate " + constructor.getDeclaringClass() + "("
                                + formatArguments(constructor.getParameterTypes()) + ")");
            }
        }
        for (final Method m : formulaMethodInvocations.getUsedMethods()) {
            if (!isPermitted(m)) {
                final String isStatic = Modifier.isStatic(m.getModifiers()) ? " static" : "";
                throw new IllegalStateException(
                        "User expressions are not permitted to use" + isStatic + " method " + m.getName() + "("
                                + formatArguments(m.getParameterTypes())
                                + ") on " + m.getDeclaringClass());
            }
        }
    }

    private static String formatArguments(final Class<?>[] parameterTypes) {
        return Arrays
                .stream(parameterTypes).map(Class::getCanonicalName)
                .collect(Collectors.joining(", "));
    }

    private boolean isPermitted(final Constructor<?> constructor) {
        boolean permitted = false;
        for (final MethodInvocationValidator validator : methodInvocationValidators) {
            final Boolean result = validator.permitConstructor(constructor);
            if (result != null) {
                if (!result) {
                    return false;
                }
                permitted = true;
            }
        }
        return permitted;
    }

    private boolean isPermitted(final Method method) {
        boolean permitted = false;
        for (final MethodInvocationValidator validator : methodInvocationValidators) {
            final Boolean result = validator.permitMethod(method);
            if (result != null) {
                if (!result) {
                    return false;
                }
                permitted = true;
            }
        }
        return permitted;
    }
}
