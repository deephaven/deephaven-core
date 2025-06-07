//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.client.impl.FilterAdapter;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.ops.FilterTableGrpcImpl;
import io.deephaven.time.DateTimeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class TestColumnExpressionValidator {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @SuppressWarnings("unused")
    public static class NotAString1 {
        public int length() {
            return 7;
        }

        public String frog() {
            return "ribbit";
        }

        public String frog(final String arg1) {
            return "bud light";
        }

        public String frog(final Integer arg1) {
            return "bud light";
        }
    }

    @SuppressWarnings("unused")
    public static class NotAString2 {
        public int length() {
            return 7;
        }

        public String frog() {
            return "ribbit";
        }

        public String frog(final String arg1) {
            return "bud light";
        }

        public String frog(final Integer arg1) {
            return "bud light";
        }
    }

    public static boolean methodCall() {
        return false;
    }

    public static String today() {
        return DateTimeUtils.today();
    }

    @Test
    public void testStringMethods() {
        testStringMethodsNaming(new MethodNameColumnExpressionValidator());
        testStringMethodsParsing(ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()));
    }

    @Test
    public void testMethodMatching() {
        final Set<Method> instanceMethods = ParsingColumnExpressionValidator
                .buildInstanceMethodList(List.of(getClass().getCanonicalName() + "$NotAString1#frog()",
                        getClass().getCanonicalName() + "$NotAString1#frog(java.lang.Integer)"));

        final ColumnExpressionValidator validator = ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance())
                .withInstanceMethods(instanceMethods);

        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()",
                        "E=new " + NotAString2.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"D.frog() = ``"}, input);
        validator.validateSelectFilters(new String[] {"D.frog(7) = ``"}, input);

        // The method "frog" is not in our method allow-list
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {"D.frog(`Asdf`) = ``"}, input));
        Assert.assertEquals(
                "User expressions are not permitted to use method frog(java.lang.String) on class io.deephaven.server.table.validation.TestColumnExpressionValidator$NotAString1",
                ise.getMessage());

        // The E is not the same class
        final IllegalStateException ise2 = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {"E.frog(8) = ``"}, input));
        Assert.assertEquals(
                "User expressions are not permitted to use method frog(java.lang.Integer) on class io.deephaven.server.table.validation.TestColumnExpressionValidator$NotAString2",
                ise2.getMessage());
    }

    @Test
    public void testVectorAnnotations() {
        final Table input = TableTools.emptyTable(10).update("A=ii").groupBy();
        final ColumnExpressionValidator validator = ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance());
        final String[] expressions = new String[] {"A0=A.get(0)", "AS=A.size()", "SV=A.subVector(0, 10)"};

        validator.validateColumnExpressions(SelectColumnFactory.getExpressions(expressions), expressions, input);
    }

    @Test
    public void testStructuredFiltersNameValidator() {
        testStructuredFiltersValidator(new MethodNameColumnExpressionValidator(),
                new MethodNameColumnExpressionValidator(Set.of(), Set.of()));
    }

    @Test
    public void testStructuredFiltersParsingValidator() {
        testStructuredFiltersValidator(
                ExpressionValidatorModule
                        .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()),
                new ParsingColumnExpressionValidator(Set.of(), Set.of(), Set.of(), Set.of(), Set.of()));
    }

    public void testStructuredFiltersValidator(final ColumnExpressionValidator goodValidator,
            final ColumnExpressionValidator badValidator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        final List<ConditionFilter> validatedFilters = new ArrayList<>();

        final MutableObject<ColumnExpressionValidator> wrapped = new MutableObject<>();
        final ColumnExpressionValidator wrappingValidator = new ColumnExpressionValidator() {

            @Override
            public WhereFilter[] validateSelectFilters(String[] conditionalExpressions, Table table) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateColumnExpressions(SelectColumn[] selectColumns, String[] originalExpressions,
                    Table table) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConditionFilters(List<ConditionFilter> conditionFilters, Table sourceTable) {
                validatedFilters.addAll(conditionFilters);
                wrapped.getValue().validateConditionFilters(conditionFilters, sourceTable);
            }
        };

        wrapped.setValue(goodValidator);
        final FilterTableGrpcImpl filterTableGrpc =
                new FilterTableGrpcImpl(new TableServiceContextualAuthWiring.AllowAll(),
                        wrappingValidator);

        // at this point it is very difficult to create a generically useful Condition to pass into the gRPC builder;
        // but we can at least construct the Filter, and then pass it through our validator
        final FilterTableRequest filterIsNull = FilterTableRequest.newBuilder()
                .addFilters(FilterAdapter.of(Filter.and(Filter.not(Filter.isNull(ColumnName.of("A")))))).build();
        filterTableGrpc.create(filterIsNull,
                List.of(SessionState.wrapAsExport(input)));
        Assert.assertEquals(1, validatedFilters.size());
        Assert.assertEquals("!(isNull(A))", validatedFilters.get(0).toString());

        validatedFilters.clear();
        wrapped.setValue(badValidator);

        final IllegalStateException ise =
                Assert.assertThrows(IllegalStateException.class, () -> filterTableGrpc.create(filterIsNull,
                        List.of(SessionState.wrapAsExport(input))));
        Assert.assertEquals(1, validatedFilters.size());
        Assert.assertEquals("!(isNull(A))", validatedFilters.get(0).toString());
        System.out.println(ise.getMessage());
        Assert.assertTrue(Pattern.matches(
                "User expressions are not permitted to use.*method isNull(\\(java.lang.Object\\) on class io.deephaven.function.Basic)?",
                ise.getMessage()));
    }

    private void testStringMethodsNaming(final ColumnExpressionValidator validator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"A.length() = 1"}, input);
        // We are permitting all methods on string, which is catching the length and toString method of D; even though
        // it was not explicitly permitted.
        validator.validateSelectFilters(new String[] {"D.length() = 1"}, input);
        validator.validateSelectFilters(new String[] {"D.toString() = ``"}, input);

        // The method "frog" is not in our method allow-list
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {"D.frog() = ``"}, input));
        Assert.assertEquals("User expressions are not permitted to use method frog", ise.getMessage());
    }

    private void testStringMethodsParsing(final ColumnExpressionValidator validator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"A.length() = 1"}, input);
        // We are permitting all methods on string, which is catching the length and toString method of D; even though
        // it was not explicitly permitted.
        final IllegalStateException ise1 = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {"D.length() = 1"}, input));
        Assert.assertEquals(
                "User expressions are not permitted to use method length() on class io.deephaven.server.table.validation.TestColumnExpressionValidator$NotAString1",
                ise1.getMessage());

        // The method "frog" is also not in our method allow-list
        final IllegalStateException ise3 = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {"D.frog() = ``"}, input));
        Assert.assertEquals(
                "User expressions are not permitted to use method frog() on class io.deephaven.server.table.validation.TestColumnExpressionValidator$NotAString1",
                ise3.getMessage());

        // We let you toString all the things
        validator.validateSelectFilters(new String[] {"D.toString() = ``"}, input);
    }

    @Test
    public void testObjectCreation() {
        testObjectCreation(new MethodNameColumnExpressionValidator());
        testObjectCreation(ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()));
    }

    private void testObjectCreation(final ColumnExpressionValidator validator) {
        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String expr = "X=A.substring(1)";
        final SelectColumn[] sc = SelectColumnFactory.getExpressions(expr);
        validator.validateColumnExpressions(sc, new String[] {expr}, input);

        final String newObject = "X=new String()";
        final SelectColumn[] sc2 = SelectColumnFactory.getExpressions(newObject);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc2, new String[] {newObject}, input));
        Assert.assertTrue(ise.getMessage().startsWith("User expressions are not permitted to instantiate "));
        Assert.assertTrue(ise.getMessage().endsWith("String"));
    }
}
