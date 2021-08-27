package io.deephaven.grpc_api.table.validation;

import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ParseResult;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.SelectValidationResult;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.lang.DBLanguageFunctionUtil;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.lang.DBLanguageParser.Result;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.DBColorUtilImpl;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.select.*;
import io.deephaven.db.v2.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.db.v2.select.codegen.FormulaAnalyzer;
import io.deephaven.libs.GroovyStaticImports;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.visitor.GenericVisitorAdapter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates a column expression coming from the web api, to ensure that the included code will use the limited
 * supported API, and no use of `new`.
 *
 * This must be an early pass at the AST on the server, as the server's stricter validation will not function without
 * it.
 */
public class ColumnExpressionValidator extends GenericVisitorAdapter<Void, Void> {
    private static final Set<String> whitelistedStaticMethods;
    private static final Set<String> whitelistedInstanceMethods;
    static {
        // list all static methods in supported util classes:
        whitelistedStaticMethods = Stream
                .of(
                        DBLanguageFunctionUtil.class,
                        GroovyStaticImports.class,
                        DBTimeUtils.class,
                        DBColorUtilImpl.class)
                .map(Class::getDeclaredMethods)
                .flatMap(Arrays::stream)
                .filter(m -> Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

        // list all non-inherited instance methods in supported data classes:
        // DBDateTime
        // String
        whitelistedInstanceMethods = Stream
                .of(
                        DBDateTime.class,
                        String.class)
                .map(Class::getDeclaredMethods)
                .flatMap(Arrays::stream)
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    public static SelectFilter[] validateSelectFilters(final String[] conditionalExpressions, final Table table) {
        final SelectFilter[] selectFilters = SelectFilterFactory.getExpressions(conditionalExpressions);
        final List<String> dummyAssignments = new ArrayList<>();
        for (int ii = 0; ii < selectFilters.length; ++ii) {
            final SelectFilter sf = selectFilters[ii];
            if (sf instanceof ConditionFilter) {
                dummyAssignments
                        .add(String.format("__boolean_placeholder_%d__ = (%s)", ii, conditionalExpressions[ii]));
            }
        }
        if (!dummyAssignments.isEmpty()) {
            final String[] daArray = dummyAssignments.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            final SelectColumn[] selectColumns = SelectColumnFactory.getExpressions(daArray);
            validateColumnExpressions(selectColumns, daArray, table);
        }
        return selectFilters;
    }

    public static void validateColumnExpressions(final SelectColumn[] selectColumns,
            final String[] originalExpressions,
            final Table table) {
        assert (selectColumns.length == originalExpressions.length);

        final SelectValidationResult validationResult = table.validateSelect(selectColumns);
        SelectAndViewAnalyzer top = validationResult.getAnalyzer();
        // We need the cloned columns because the SelectAndViewAnalyzer has left state behind in them
        // (namely the "realColumn" of the SwitchColumn) that we want to look at in validateSelectColumnHelper.
        final SelectColumn[] clonedColumns = validationResult.getClonedColumns();
        // Flatten and reverse the analyzer stack
        final List<SelectAndViewAnalyzer> analyzers = new ArrayList<>();
        while (top != null) {
            analyzers.add(top);
            top = top.getInner();
        }
        Collections.reverse(analyzers);
        assert (analyzers.size() == clonedColumns.length + 1);

        final Map<String, ColumnDefinition<?>> availableColumns = new LinkedHashMap<>();
        for (int ii = 0; ii < clonedColumns.length; ++ii) {
            analyzers.get(ii).updateColumnDefinitionsFromTopLayer(availableColumns);
            validateSelectColumnHelper(clonedColumns[ii], originalExpressions[ii], availableColumns, table);
        }
    }

    private static void validateSelectColumnHelper(SelectColumn selectColumn,
            final String originalExpression,
            final Map<String, ColumnDefinition<?>> availableColumns,
            final Table table) {
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

        final Result compiledFormula;
        final DBTimeUtils.Result timeConversionResult;
        try {
            timeConversionResult = DBTimeUtils.convertExpression(formulaString);
            compiledFormula = FormulaAnalyzer.getCompiledFormula(availableColumns, timeConversionResult, null);
        } catch (final Exception e) {
            // in theory not possible, since we already parsed it once
            throw new IllegalStateException("Error occurred while re-compiling formula for whitelist", e);
        }
        final boolean isAddOnly = table instanceof BaseTable && ((BaseTable) table).isAddOnly();
        if (table.isLive() && !(isAddOnly && table.isFlat())) {
            final Set<String> disallowedVariables = new HashSet<>();
            disallowedVariables.add("i");
            disallowedVariables.add("ii");
            // TODO walk QueryScope.getInstance() and remove them too?

            if (compiledFormula.getVariablesUsed().stream().anyMatch(disallowedVariables::contains)) {
                throw new IllegalStateException("Formulas involving live tables are not permitted to use i or ii");
            }
        }

        // we pass the formula itself, since this has undergone the time conversion
        validateInvocations(timeConversionResult.getConvertedFormula());
    }

    private static final JavaParser staticJavaParser = new JavaParser();

    private static void validateInvocations(String expression) {
        // copied, modified from DBLanguageParser.java
        // before parsing, finish Deephaven-specific language features:
        expression = DBLanguageParser.convertBackticks(expression);
        expression = DBLanguageParser.convertSingleEquals(expression);

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
    public Void visit(final MethodCallExpr n, final Void arg) {
        // verify that this is a call on a supported instance, or is one of the supported static methods
        if (!n.getScope().isPresent()) {
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
        return super.visit(n, arg);
    }

    @Override
    public Void visit(final ObjectCreationExpr n, final Void arg) {
        throw new IllegalStateException("Can't instantiate " + n.getType());
    }
}
