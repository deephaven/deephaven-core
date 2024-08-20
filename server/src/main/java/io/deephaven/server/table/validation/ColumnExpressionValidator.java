//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.SwitchColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.util.ColorUtilImpl;
import io.deephaven.libs.GroovyStaticImports;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeLiteralReplacedExpression;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.time.calendar.StaticCalendarMethods;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates a column expression coming from the web api, to ensure that the included code will use the limited
 * supported API, and no use of `new`.
 *
 * This must be an early pass at the AST on the server, as the server's stricter validation will not function without
 * it.
 */
public class ColumnExpressionValidator extends VoidVisitorAdapter<Object> {
    private static final Set<String> whitelistedStaticMethods;
    private static final Set<String> whitelistedInstanceMethods;
    static {
        // list all static methods in supported util classes:
        whitelistedStaticMethods = Stream
                .of(
                        QueryLanguageFunctionUtils.class,
                        GroovyStaticImports.class,
                        DateTimeUtils.class,
                        ColorUtilImpl.class,
                        Calendars.class,
                        StaticCalendarMethods.class)
                .map(Class::getDeclaredMethods)
                .flatMap(Arrays::stream)
                .filter(m -> Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

        // list all non-inherited instance methods in supported data classes:
        // DateTime
        // String
        whitelistedInstanceMethods = Stream
                .of(Instant.class, String.class)
                .map(Class::getDeclaredMethods)
                .flatMap(Arrays::stream)
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    public static WhereFilter[] validateSelectFilters(final String[] conditionalExpressions, final Table table) {
        final WhereFilter[] whereFilters = WhereFilterFactory.getExpressions(conditionalExpressions);
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

    public static void validateColumnExpressions(final SelectColumn[] selectColumns, final String[] originalExpressions,
            final Table table) {
        // It's unfortunate that we have to validateSelect which does a bunch of analysis, just to get throw-away cloned
        // columns back, so we can check here for disallowed methods. (We need to make sure SwitchColumns get
        // initialized.)
        final SelectColumn[] clonedColumns =
                ((QueryTable) table.coalesce()).validateSelect(selectColumns).getClonedColumns();
        validateColumnExpressions(clonedColumns, originalExpressions);
    }

    private static void validateColumnExpressions(final SelectColumn[] selectColumns,
            final String[] originalExpressions) {
        assert (selectColumns.length == originalExpressions.length);
        for (int ii = 0; ii < selectColumns.length; ++ii) {
            validateSelectColumnHelper(selectColumns[ii], originalExpressions[ii]);
        }
    }

    private static void validateSelectColumnHelper(SelectColumn selectColumn, final String originalExpression) {
        while (selectColumn instanceof SwitchColumn) {
            selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
        }

        if (!(selectColumn instanceof FormulaColumn)) {
            // other variants should be safe, only test DhFormulaColumn
            return;
        }
        // Explicitly only supporting Column=Formula formats here
        final int indexOfEquals = originalExpression.indexOf('=');
        Assert.assertion(indexOfEquals != -1, "Expected formula expression");
        final String formulaString = originalExpression.substring(indexOfEquals + 1);
        final TimeLiteralReplacedExpression timeConversionResult;
        try {
            timeConversionResult = TimeLiteralReplacedExpression.convertExpression(formulaString);
        } catch (final Exception e) {
            // in theory not possible, since we already parsed it once
            throw new IllegalStateException("Error occurred while re-compiling formula for whitelist", e);
        }
        // we pass the formula itself, since this has undergone the time conversion
        validateInvocations(timeConversionResult.getConvertedFormula());
    }

    private static final JavaParser staticJavaParser = new JavaParser();

    private static void validateInvocations(String expression) {
        // copied, modified from QueryLanguageParser.java
        // before parsing, finish Deephaven-specific language features:
        expression = QueryLanguageParser.convertBackticks(expression);
        expression = QueryLanguageParser.convertSingleEquals(expression);

        // then, parse into an AST
        final ParseResult<Expression> result;
        try {
            synchronized (staticJavaParser) {
                result = staticJavaParser.parseExpression(expression);
            }
        } catch (final ParseProblemException e) {
            // in theory not possible, since we already parsed once
            throw new IllegalStateException("Error occurred while re-parsing formula for whitelist", e);
        }

        // now that we finally have the AST...
        // check method and constructor calls that weren't already checked
        if (!result.isSuccessful()) {
            throw new IllegalArgumentException(
                    "Invalid expression " + expression + ": " + result.getProblems().toString());
        }
        result.getResult().ifPresent(expr -> expr.accept(new ColumnExpressionValidator(), null));
    }

    @Override
    public void visit(final MethodCallExpr n, final Object arg) {
        // verify that this is a call on a supported instance, or is one of the supported static methods
        if (n.getScope().isEmpty()) {
            if (!whitelistedStaticMethods.contains(n.getNameAsString())) {
                throw new IllegalStateException(
                        "User expressions are not permitted to use method " + n.getNameAsString());
            }
        } else {
            // note that it is possible that there is a scoped static method in this block, and that the
            // user unnecessarily specified the classname TODO handle this if it becomes an issue
            if (!whitelistedInstanceMethods.contains(n.getNameAsString())) {
                throw new IllegalStateException(
                        "User expressions are not permitted to use method " + n.getNameAsString());
            }
        }
        super.visit(n, arg);
    }

    @Override
    public void visit(final ObjectCreationExpr n, final Object arg) {
        throw new IllegalStateException("Can't instantiate " + n.getType());
    }
}
