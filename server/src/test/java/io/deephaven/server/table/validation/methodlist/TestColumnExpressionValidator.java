//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.client.impl.FilterAdapter;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.engine.validation.MethodInvocationValidator;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.ops.FilterTableGrpcImpl;
import io.deephaven.server.table.validation.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.UserInvocationPermitted;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class TestColumnExpressionValidator {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private final MethodInvocationValidator ALLOW_ALL_VALIDATOR = new MethodInvocationValidator() {
        @Override
        public Boolean permitConstructor(final Constructor<?> constructor) {
            return true;
        }

        @Override
        public Boolean permitMethod(final Method method) {
            return true;
        }
    };

    final MethodInvocationValidator DENY_CONSTRUCTORS_VALIDATOR = new MethodInvocationValidator() {
        @Override
        public Boolean permitConstructor(final Constructor<?> constructor) {
            return false;
        }

        @Override
        public Boolean permitMethod(final Method method) {
            return null;
        }
    };

    final MethodInvocationValidator PERMIT_CONSTRUCTORS_VALIDATOR = new MethodInvocationValidator() {
        @Override
        public Boolean permitConstructor(final Constructor<?> constructor) {
            return true;
        }

        @Override
        public Boolean permitMethod(final Method method) {
            return null;
        }
    };

    final MethodInvocationValidator DENY_METHODS_VALIDATOR = new MethodInvocationValidator() {
        @Override
        public Boolean permitConstructor(final Constructor<?> constructor) {
            return null;
        }

        @Override
        public Boolean permitMethod(final Method method) {
            return false;
        }
    };

    private final MethodInvocationValidator NO_OPINION_VALIDATOR = new MethodInvocationValidator() {
        @Override
        public Boolean permitConstructor(final Constructor<?> constructor) {
            return null;
        }

        @Override
        public Boolean permitMethod(final Method method) {
            return null;
        }
    };

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

        public class DoubleInner {
            public String toad() {
                return "budweiser";
            };

            public class TripleInner {
                public String amphibian(int a) {
                    return "kermit";
                }
            }
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

    public static class AnnotatedConstructor {
        final int a;

        public AnnotatedConstructor() {
            a = 1;
        }

        @UserInvocationPermitted(value = "test_annotation1")
        public AnnotatedConstructor(final int a) {
            this.a = a;
        }

        @UserInvocationPermitted(value = "test_annotation2")
        public AnnotatedConstructor(final String a) {
            this.a = Integer.parseInt(a);
        }
    }

    @SuppressWarnings("unused")
    public static class UnannotatedClass {
        public String im() {
            return "im";
        }

        public static String stm() {
            return "sm";
        }
    }

    @SuppressWarnings("unused")
    @UserInvocationPermitted(value = "test_annotation3", classScope = UserInvocationPermitted.ScopeType.Static)
    public static class AnnotatedStaticClass {
        public String instance() {
            return "im";
        }

        public static String stm() {
            return "sm";
        }
    }

    @SuppressWarnings("unused")
    @UserInvocationPermitted(value = "test_annotation4", classScope = UserInvocationPermitted.ScopeType.Instance)
    public static class AnnotatedInstanceClass {
        public String im() {
            return "im";
        }

        public static String stm() {
            return "sm";
        }
    }

    @SuppressWarnings("unused")
    @UserInvocationPermitted(value = "test_annotation5")
    public static class AnnotatedMethods {
        @UserInvocationPermitted(value = "test_annotation6")
        public String im() {
            return "im";
        }

        @UserInvocationPermitted(value = "test_annotation7")
        public static String stm() {
            return "sm";
        }

        @UserInvocationPermitted(value = "test_annotation8")
        public String im8() {
            return "im";
        }

        @UserInvocationPermitted(value = "test_annotation9")
        public static String stm9() {
            return "sm";
        }
    }

    @UserInvocationPermitted(value = "test_annotation11")
    public interface InterfaceAnnotated {
        @UserInvocationPermitted(value = "test_annotation10")
        Number m1(Integer a);

        @UserInvocationPermitted(value = "test_annotation10")
        boolean m2(int a);
    }

    @UserInvocationPermitted(value = "test_annotation11")
    public interface InterfaceAnnotatedGeneric<T extends Number> {
        // TODO: add a test for this inheritance
        @UserInvocationPermitted(value = "test_annotation10")
        boolean m1(T a);

        @UserInvocationPermitted(value = "test_annotation10")
        boolean m2(int a);
    }

    public static class InheritedAnnotation implements InterfaceAnnotated {
        @Override
        public Integer m1(final Integer a) {
            return 7;
        }

        @Override
        public boolean m2(final int a) {
            return false;
        }

        public boolean m2(final double b) {
            return false;
        }

        public boolean m1(final Double b) {
            return true;
        }
    }

    public interface ExtendedGeneric extends InterfaceAnnotatedGeneric<Long> {
        @UserInvocationPermitted(value = "test_annotation11")
        int m3();
    }

    public static class InheritedAnnotationGeneric implements InterfaceAnnotatedGeneric<Integer> {
        @Override
        public boolean m1(final Integer a) {
            return false;
        }

        @Override
        public boolean m2(final int a) {
            return false;
        }

        public boolean m2(final double b) {
            return false;
        }

        public boolean m1(final Double b) {
            return true;
        }
    }

    public static class InheritedAnnotationGenericNoParam implements InterfaceAnnotatedGeneric {
        @Override
        public boolean m1(final Number a) {
            return false;
        }

        @Override
        public boolean m2(final int a) {
            return false;
        }

        public boolean m2(final double b) {
            return false;
        }

        public boolean m1(final Double b) {
            return true;
        }
    }

    @UserInvocationPermitted(value = "test_annotation11")
    public interface AnotherInterface {
        long m4();
    }

    public interface YetAnotherInterface {
        long m5();
    }

    public static class DoubleInheritance implements ExtendedGeneric, AnotherInterface, YetAnotherInterface {
        @Override
        public int m3() {
            return 42;
        }

        @Override
        public boolean m1(final Long a) {
            return false;
        }

        @Override
        public boolean m2(final int a) {
            return false;
        }

        @Override
        public long m4() {
            return 0;
        }

        @Override
        public long m5() {
            return 0;
        }

        public int m3(final int a) {
            return 1 + a;
        }

        public boolean m2(final double a) {
            return false;
        }

        public boolean m2(final Short a) {
            return false;
        }
    }


    private static void allowedSelectMethod(final String expression, final ColumnExpressionValidator validator,
            final Table input) {
        final SelectColumn[] sc = SelectColumnFactory.getExpressions(expression);
        validator.validateColumnExpressions(sc, new String[] {expression}, input.getDefinition());
        // We like to do this call twice so caching is exercised
        validator.validateColumnExpressions(sc, new String[] {expression}, input.getDefinition());
    }

    private static void disallowedSelectMethod(final String expression, final ColumnExpressionValidator validator,
            final Table input, final String expected) {
        final SelectColumn[] sc = SelectColumnFactory.getExpressions(expression);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc, new String[] {expression}, input.getDefinition()));

        Assert.assertEquals(expected, ise.getMessage());
    }

    @Test
    public void testStringMethods() {
        testStringMethodsNaming(new MethodNameColumnExpressionValidator());
        testStringMethodsParsing(ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()));
        testStringMethodsParsing(
                new ParsingColumnExpressionValidator(List.of(new MethodListInvocationValidator(
                        List.of("java.lang.Object toString()", "java.lang.String length()",
                                "io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils *(..)")))));
    }

    @Test
    public void testMethodMatching() {
        final List<String> methodList = new ArrayList<>();

        methodList.add(getClass().getCanonicalName() + ".NotAString1 frog()");
        methodList.add(getClass().getCanonicalName() + ".NotAString1 frog(java.lang.Integer)");

        methodList.addAll(ExpressionValidatorModule.getMethodListFromConfiguration(Configuration.getInstance()));

        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(new AnnotationMethodInvocationValidator(Set.of("function_library")),
                        new MethodListInvocationValidator(methodList)));

        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()",
                        "E=new " + NotAString2.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"D.frog() = ``"}, input.getDefinition());
        validator.validateSelectFilters(new String[] {"D.frog(7) = ``"}, input.getDefinition());

        // The method "frog" is not in our method allow-list
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method frog(java.lang.String) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString1",
                "D.frog(`Asdf`) = ``");

        // The E is not the same class
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method frog(java.lang.Integer) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString2",
                "E.frog(8) = ``");
    }

    @Test
    public void testOpenRewrite() {
        final MethodListInvocationValidator iv =
                new MethodListInvocationValidator(List.of(NotAString1.class.getCanonicalName() + " frog()",
                        NotAString1.class.getCanonicalName() + " frog(java.lang.Integer)"));

        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()",
                        "E=new " + NotAString2.class.getCanonicalName() + "()");

        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(new AnnotationMethodInvocationValidator(Set.of("function_library")), iv));

        validator.validateSelectFilters(new String[] {"D.frog() = ``"}, input.getDefinition());
        validator.validateSelectFilters(new String[] {"D.frog(7) = ``"}, input.getDefinition());

        // The method "frog" is not in our method allow-list
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method frog(java.lang.String) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString1",
                "D.frog(`Asdf`) = ``");

        // The E is not the same class
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method frog(java.lang.Integer) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString2",
                "E.frog(8) = ``");
    }

    private static void disallowedFilterMethod(final ColumnExpressionValidator validator,
            final Table input,
            final String errorMessage,
            final String expression) {

        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateSelectFilters(new String[] {expression}, input.getDefinition()));
        Assert.assertEquals(errorMessage, ise.getMessage());
    }


    @Test
    public void testVectorAnnotations() {
        final Table input = TableTools.emptyTable(10).update("A=ii").groupBy();
        final ColumnExpressionValidator validator = ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance());
        final String[] expressions = new String[] {"A0=A.get(0)", "AS=A.size()", "SV=A.subVector(0, 10)"};

        validator.validateColumnExpressions(SelectColumnFactory.getExpressions(expressions), expressions,
                input.getDefinition());
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
                new ParsingColumnExpressionValidator(Set.of()));
    }

    public void testStructuredFiltersValidator(final ColumnExpressionValidator goodValidator,
            final ColumnExpressionValidator badValidator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        final List<ConditionFilter> validatedFilters = new ArrayList<>();

        final MutableObject<ColumnExpressionValidator> wrapped = new MutableObject<>();
        final ColumnExpressionValidator wrappingValidator = new ColumnExpressionValidator() {

            @Override
            public WhereFilter[] validateSelectFilters(final String[] conditionalExpressions,
                    final TableDefinition definition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateColumnExpressions(final SelectColumn[] selectColumns,
                    final String[] originalExpressions,
                    final TableDefinition definition) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void validateConditionFilters(final List<ConditionFilter> conditionFilters,
                    final TableDefinition definition) {
                validatedFilters.addAll(conditionFilters);
                wrapped.getValue().validateConditionFilters(conditionFilters, definition);
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

        // and a match filter should be fine with no actual validation necessary
        validatedFilters.clear();

        final FilterTableRequest matchFilter = FilterTableRequest.newBuilder()
                .addFilters(FilterAdapter.of(Filter.and(FilterComparison.eq(ColumnName.of("A"), Literal.of("X")))))
                .build();
        filterTableGrpc.create(matchFilter,
                List.of(SessionState.wrapAsExport(input)));
        Assert.assertEquals(0, validatedFilters.size());
    }

    private void testStringMethodsNaming(final ColumnExpressionValidator validator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"A.length() = 1"}, input.getDefinition());
        // We are permitting all methods on string, which is catching the length and toString method of D; even though
        // it was not explicitly permitted.
        validator.validateSelectFilters(new String[] {"D.length() = 1"}, input.getDefinition());
        validator.validateSelectFilters(new String[] {"D.toString() = ``"}, input.getDefinition());

        // The method "frog" is not in our method allow-list
        disallowedFilterMethod(validator, input, "User expressions are not permitted to use method frog",
                "D.frog() = ``");
    }

    private void testStringMethodsParsing(final ColumnExpressionValidator validator) {
        final Table input =
                TableTools.emptyTable(1).update("A=`A`", "D=new " + NotAString1.class.getCanonicalName() + "()");

        validator.validateSelectFilters(new String[] {"A.length() = 1"}, input.getDefinition());
        // We are permitting all methods on string, which is catching the length and toString method of D; even though
        // it was not explicitly permitted.
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method length() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString1",
                "D.length() = 1");

        // The method "frog" is also not in our method allow-list
        disallowedFilterMethod(validator, input,
                "User expressions are not permitted to use method frog() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$NotAString1",
                "D.frog() = ``");

        // We let you toString all the things
        validator.validateSelectFilters(new String[] {"D.toString() = ``"}, input.getDefinition());
    }

    @Test
    public void testObjectCreationBackwardsCompatibility() {
        final ColumnExpressionValidator validator = new MethodNameColumnExpressionValidator();
        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String expr = "X=A.substring(1)";
        allowedSelectMethod(expr, validator, input);

        final String newObject = "X=new String()";
        final SelectColumn[] sc2 = SelectColumnFactory.getExpressions(newObject);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc2, new String[] {newObject}, input.getDefinition()));
        Assert.assertTrue("Actual: " + ise.getMessage(),
                ise.getMessage().startsWith("User expressions are not permitted to instantiate "));
        Assert.assertTrue("Actual: " + ise.getMessage(),
                ise.getMessage().endsWith("String"));
    }

    @Test
    public void testObjectCreationParsed() {
        final ColumnExpressionValidator validator = ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance());
        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String expr = "X=A.substring(1)";
        allowedSelectMethod(expr, validator, input);

        // Object is not allowed, because we do not use a wildcard on object
        final String newObject = "X=new Object()";
        final SelectColumn[] sc2 = SelectColumnFactory.getExpressions(newObject);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc2, new String[] {newObject}, input.getDefinition()));
        Assert.assertTrue("Actual: " + ise.getMessage(),
                ise.getMessage().startsWith("User expressions are not permitted to instantiate "));
        Assert.assertTrue("Actual: " + ise.getMessage(),
                ise.getMessage().endsWith("Object()"));

        // But string is permitted, because we have a rule for all string methods (which includes the constructors)
        allowedSelectMethod("X = new String()", validator, input);
    }

    @Test
    public void testConstructorAnnotations() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation1"));
        final ParsingColumnExpressionValidator validator =
                new ParsingColumnExpressionValidator(List.of(annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String noAnnotation = "X=new " + AnnotatedConstructor.class.getCanonicalName() + "()";
        disallowedSelectMethod(noAnnotation, validator, input,
                "User expressions are not permitted to instantiate class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedConstructor()");

        final String wrongAnnotation = "X=new " + AnnotatedConstructor.class.getCanonicalName() + "(`Asdf`)";
        disallowedSelectMethod(wrongAnnotation, validator, input,
                "User expressions are not permitted to instantiate class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedConstructor(java.lang.String)");

        final String goodAnnotation = "X=new " + AnnotatedConstructor.class.getCanonicalName() + "(7)";
        allowedSelectMethod(goodAnnotation, validator, input);
    }

    @Test
    public void testStaticTargetAnnotations() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation3", "test_annotation4"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR, annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String callInstance = "X= new " + AnnotatedStaticClass.class.getCanonicalName() + "().instance()";
        disallowedSelectMethod(callInstance, validator, input,
                "User expressions are not permitted to use method instance() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedStaticClass");

        final String noAnnotations = "X=" + UnannotatedClass.class.getCanonicalName() + ".stm()";
        disallowedSelectMethod(noAnnotations, validator, input,
                "User expressions are not permitted to use static method stm() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$UnannotatedClass");

        final String callStatic = "X=" + AnnotatedStaticClass.class.getCanonicalName() + ".stm()";
        allowedSelectMethod(callStatic, validator, input);
    }

    @Test
    public void testMethodAnnotations() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation6", "test_annotation7"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR, annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String callInstance = "X= new " + AnnotatedMethods.class.getCanonicalName() + "().im()";
        allowedSelectMethod(callInstance, validator, input);
        allowedSelectMethod(callInstance, validator, input);

        final String callStatic = "X=" + AnnotatedMethods.class.getCanonicalName() + ".stm()";
        allowedSelectMethod(callStatic, validator, input);
        allowedSelectMethod(callStatic, validator, input);

        final String noAnnotationInstance = "X= new " + AnnotatedMethods.class.getCanonicalName() + "().im8()";
        disallowedSelectMethod(noAnnotationInstance, validator, input,
                "User expressions are not permitted to use method im8() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedMethods");

        final String noAnnotationStatic = "X= new " + AnnotatedMethods.class.getCanonicalName() + "().stm9()";
        disallowedSelectMethod(noAnnotationStatic, validator, input,
                "User expressions are not permitted to use static method stm9() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedMethods");
    }

    @Test
    public void testInstanceTargetAnnotations() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation3", "test_annotation4"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR, annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String callStatic = "X=" + AnnotatedInstanceClass.class.getCanonicalName() + ".stm()";

        disallowedSelectMethod(callStatic, validator, input,
                "User expressions are not permitted to use static method stm() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedInstanceClass");

        final String noAnnotations = "X= new " + UnannotatedClass.class.getCanonicalName() + "().im()";
        disallowedSelectMethod(noAnnotations, validator, input,
                "User expressions are not permitted to use method im() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$UnannotatedClass");

        final String callInstance = "X= new " + AnnotatedInstanceClass.class.getCanonicalName() + "().im()";
        allowedSelectMethod(callInstance, validator, input);
    }


    @Test
    public void testValidatorConstructorCombination() {
        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String callConstructor = "X=new " + AnnotatedConstructor.class.getCanonicalName() + "()";

        final ParsingColumnExpressionValidator validator =
                new ParsingColumnExpressionValidator(
                        List.of(ALLOW_ALL_VALIDATOR, NO_OPINION_VALIDATOR, DENY_CONSTRUCTORS_VALIDATOR));
        final SelectColumn[] sc1 = SelectColumnFactory.getExpressions(callConstructor);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc1, new String[] {callConstructor}, input.getDefinition()));

        Assert.assertEquals(
                "User expressions are not permitted to instantiate class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedConstructor()",
                ise.getMessage());

        final ParsingColumnExpressionValidator validator2 =
                new ParsingColumnExpressionValidator(List.of(NO_OPINION_VALIDATOR));

        final IllegalStateException ise2 = Assert.assertThrows(IllegalStateException.class,
                () -> validator2.validateColumnExpressions(sc1, new String[] {callConstructor}, input.getDefinition()));
        Assert.assertEquals(
                "User expressions are not permitted to instantiate class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedConstructor()",
                ise2.getMessage());

        final ParsingColumnExpressionValidator validator3 =
                new ParsingColumnExpressionValidator(List.of(ALLOW_ALL_VALIDATOR));
        validator3.validateColumnExpressions(sc1, new String[] {callConstructor}, input.getDefinition());

        final ParsingColumnExpressionValidator validator4 =
                new ParsingColumnExpressionValidator(List.of(NO_OPINION_VALIDATOR, ALLOW_ALL_VALIDATOR));
        validator4.validateColumnExpressions(sc1, new String[] {callConstructor}, input.getDefinition());

        final ParsingColumnExpressionValidator validator5 =
                new ParsingColumnExpressionValidator(List.of(DENY_CONSTRUCTORS_VALIDATOR));
        final IllegalStateException ise3 = Assert.assertThrows(IllegalStateException.class,
                () -> validator5.validateColumnExpressions(sc1, new String[] {callConstructor}, input.getDefinition()));
        Assert.assertEquals(
                "User expressions are not permitted to instantiate class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$AnnotatedConstructor()",
                ise3.getMessage());
    }

    @Test
    public void testValidatorMethodCombination() {
        final ParsingColumnExpressionValidator validator =
                new ParsingColumnExpressionValidator(
                        List.of(ALLOW_ALL_VALIDATOR, NO_OPINION_VALIDATOR, DENY_METHODS_VALIDATOR));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        final String methodCall = "X=Integer.valueOf(7)";

        final SelectColumn[] sc1 = SelectColumnFactory.getExpressions(methodCall);
        final IllegalStateException ise = Assert.assertThrows(IllegalStateException.class,
                () -> validator.validateColumnExpressions(sc1, new String[] {methodCall}, input.getDefinition()));

        Assert.assertEquals(
                "User expressions are not permitted to use static method valueOf(int) on class java.lang.Integer",
                ise.getMessage());

        final ParsingColumnExpressionValidator validator2 =
                new ParsingColumnExpressionValidator(List.of(NO_OPINION_VALIDATOR));

        final IllegalStateException ise2 = Assert.assertThrows(IllegalStateException.class,
                () -> validator2.validateColumnExpressions(sc1, new String[] {methodCall}, input.getDefinition()));
        Assert.assertEquals(
                "User expressions are not permitted to use static method valueOf(int) on class java.lang.Integer",
                ise2.getMessage());

        final ParsingColumnExpressionValidator validator3 =
                new ParsingColumnExpressionValidator(List.of(ALLOW_ALL_VALIDATOR));
        validator3.validateColumnExpressions(sc1, new String[] {methodCall}, input.getDefinition());

        final ParsingColumnExpressionValidator validator4 =
                new ParsingColumnExpressionValidator(List.of(NO_OPINION_VALIDATOR, ALLOW_ALL_VALIDATOR));
        validator4.validateColumnExpressions(sc1, new String[] {methodCall}, input.getDefinition());

        final ParsingColumnExpressionValidator validator5 =
                new ParsingColumnExpressionValidator(List.of(DENY_METHODS_VALIDATOR));
        final IllegalStateException ise3 = Assert.assertThrows(IllegalStateException.class,
                () -> validator5.validateColumnExpressions(sc1, new String[] {methodCall}, input.getDefinition()));
        Assert.assertEquals(
                "User expressions are not permitted to use static method valueOf(int) on class java.lang.Integer",
                ise3.getMessage());
    }

    @Test
    public void testInheritedMethodAnnotation() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation10"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR, annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        disallowedSelectMethod("X= new " + InheritedAnnotation.class.getCanonicalName() + "().m2(7.0)", validator,
                input,
                "User expressions are not permitted to use method m2(double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotation");

        allowedSelectMethod("X= new " + InheritedAnnotation.class.getCanonicalName() + "().m2(8)", validator, input);
    }

    @Test
    public void testInheritedMethodAnnotationGeneric() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation10"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR,
                        new MethodListInvocationValidator(
                                ExpressionValidatorModule.getMethodListFromConfiguration(Configuration.getInstance())),
                        annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");


        // bad
        // boolean m2(final double b);
        // boolean m1(final Double b)
        disallowedSelectMethod("X= new " + InheritedAnnotationGeneric.class.getCanonicalName() + "().m2(7.0)",
                validator,
                input,
                "User expressions are not permitted to use method m2(double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotationGeneric");

        disallowedSelectMethod("X= new " + InheritedAnnotationGeneric.class.getCanonicalName() + "().m1(7.0)",
                validator,
                input,
                "User expressions are not permitted to use method m1(java.lang.Double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotationGeneric");

        // good
        // Integer m1(final Integer a);
        // boolean m2(final int a);
        allowedSelectMethod(
                "X= new " + InheritedAnnotationGeneric.class.getCanonicalName() + "().m2(Integer.valueOf(8))",
                validator, input);
        allowedSelectMethod("X= new " + InheritedAnnotationGeneric.class.getCanonicalName() + "().m1(8)", validator,
                input);


        // bad
        // boolean m2(final double b);
        // boolean m1(final Double b)
        disallowedSelectMethod("X= new " + InheritedAnnotationGenericNoParam.class.getCanonicalName() + "().m2(7.0)",
                validator,
                input,
                "User expressions are not permitted to use method m2(double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotationGenericNoParam");

        disallowedSelectMethod("X= new " + InheritedAnnotationGenericNoParam.class.getCanonicalName() + "().m1(7.0)",
                validator,
                input,
                "User expressions are not permitted to use method m1(java.lang.Double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotationGenericNoParam");

        // good
        // Integer m1(final Number a);
        // boolean m2(final int a);
        allowedSelectMethod(
                "X= new " + InheritedAnnotationGenericNoParam.class.getCanonicalName() + "().m2(Integer.valueOf(8))",
                validator, input);
        allowedSelectMethod("X= new " + InheritedAnnotationGenericNoParam.class.getCanonicalName() + "().m1(8)",
                validator, input);
    }

    @Test
    public void testDoubleInheritedMethodAnnotation() {

        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(
                        Set.of("test_annotation10", "test_annotation11", "function_library"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR,
                        new MethodListInvocationValidator(
                                ExpressionValidatorModule.getMethodListFromConfiguration(Configuration.getInstance())),
                        annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "()", validator, input);

        System.out.println("M3:" + new DoubleInheritance().m3(7));

        // bad
        // public boolean m3(final int a);
        // public boolean m2(final double a);
        // public boolean m2(final Short a);
        disallowedSelectMethod("X= new " + DoubleInheritance.class.getCanonicalName() + "().m3(7)",
                validator,
                input,
                "User expressions are not permitted to use method m3(int) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$DoubleInheritance");
        disallowedSelectMethod("X= new " + DoubleInheritance.class.getCanonicalName() + "().m2(7.0)",
                validator,
                input,
                "User expressions are not permitted to use method m2(double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$DoubleInheritance");
        disallowedSelectMethod("X= new " + DoubleInheritance.class.getCanonicalName() + "().m2(new Short((short)5))",
                validator,
                input,
                "User expressions are not permitted to use method m2(java.lang.Short) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$DoubleInheritance");

        disallowedSelectMethod("X= new " + DoubleInheritance.class.getCanonicalName() + "().m5()",
                validator,
                input,
                "User expressions are not permitted to use method m5() on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$DoubleInheritance");

        // good
        // public int m3(); - just part of the directly inherited interface
        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "().m3()", validator, input);
        // public boolean m1(final Long a); - inherited through a generic
        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "().m1(new Long(Long.MAX_VALUE))",
                validator, input);
        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "().m1(Long.MAX_VALUE)", validator,
                input);
        // public boolean m2(final int a);
        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "().m2(8)", validator, input);
        // public long m4(final long a);
        allowedSelectMethod("X=new " + DoubleInheritance.class.getCanonicalName() + "().m4()", validator, input);
    }

    @Test
    public void testInheritedInterfaceAnnotation() {
        final AnnotationMethodInvocationValidator annotationMethodInvocationValidator =
                new AnnotationMethodInvocationValidator(Set.of("test_annotation11"));
        final ColumnExpressionValidator validator = new ParsingColumnExpressionValidator(
                List.of(PERMIT_CONSTRUCTORS_VALIDATOR, annotationMethodInvocationValidator));

        final Table input = TableTools.emptyTable(1).update("A=`Abc`");

        disallowedSelectMethod("X= new " + InheritedAnnotation.class.getCanonicalName() + "().m2(7.0)", validator,
                input,
                "User expressions are not permitted to use method m2(double) on class io.deephaven.server.table.validation.methodlist.TestColumnExpressionValidator$InheritedAnnotation");

        allowedSelectMethod("X= new " + InheritedAnnotation.class.getCanonicalName() + "().m2(8)", validator, input);
    }


    @Test
    public void testImplicitMethods() throws IOException {
        testImplicitMethods(ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()));
    }

    private void testImplicitMethods(final ColumnExpressionValidator validator) throws IOException {
        final GroovyDeephavenSession session = GroovyDeephavenSession.of(
                ExecutionContext.getContext().getUpdateGraph(),
                ExecutionContext.getContext().getOperationInitializer(),
                ObjectTypeLookup.NoOp.INSTANCE,
                GroovyDeephavenSession.RunScripts.none());
        try (final SafeCloseable ignored = session.getExecutionContext().open()) {
            session.evaluateScript("function = { a -> return a }");

            final Table input = TableTools.emptyTable(1).update("A=`Abc`");

            final String expr = "X=function(A)";
            disallowedSelectMethod(expr, validator, input, "User expression may not use implicit method calls.");
        }
    }

    @Test
    public void testNoFormulas() {
        testNoFormulas(new MethodNameColumnExpressionValidator());
        testNoFormulas(ExpressionValidatorModule
                .getParsingColumnExpressionValidatorFromConfiguration(Configuration.getInstance()));
    }

    private void testNoFormulas(final ColumnExpressionValidator validator) {
        final Table input = TableTools.emptyTable(1).update("A=`Abc`", "D=7");

        final String expr = "X=A";
        allowedSelectMethod(expr, validator, input);

        final String[] matchFilters = new String[] {"A in `def`, `qdz`", "D=8"};
        validator.validateSelectFilters(matchFilters, input.getDefinition());
    }

    @Test
    public void testConstructorMatching() throws NoSuchMethodException {
        final List<String> allowedConstructor = List.of("java.lang.String <constructor>(char[], int, int)");
        final MethodListInvocationValidator validator = new MethodListInvocationValidator(allowedConstructor);
        Assert.assertTrue(validator.permitConstructor(String.class.getConstructor(char[].class, int.class, int.class)));
        Assert.assertNull(validator.permitConstructor(String.class.getConstructor(char[].class)));

        final List<String> allowedConstructor2 = List.of("java.math.BigInteger <constructor>(String)");
        final MethodListInvocationValidator validator2 = new MethodListInvocationValidator(allowedConstructor2);
        Assert.assertTrue(validator2.permitConstructor(BigInteger.class.getConstructor(String.class)));
        Assert.assertNull(validator2.permitConstructor(BigInteger.class.getConstructor(byte[].class)));
        Assert.assertNull(validator2.permitConstructor(BigDecimal.class.getConstructor(String.class)));
    }

    @Test
    public void testCachingConstructorBehavior() throws NoSuchMethodException {
        final MutableInt allowedMethodCount = new MutableInt(0);
        final MutableInt disallowedMethodCount = new MutableInt(0);
        final MutableInt nullMethodCount = new MutableInt(0);

        final Constructor<Integer> intConstructor = Integer.class.getConstructor(int.class);
        final Constructor<Double> doubleConstructor = Double.class.getConstructor(double.class);
        final Constructor<Long> longConstructor = Long.class.getConstructor(long.class);

        final MethodInvocationValidator validator = new MethodInvocationValidator() {
            @Override
            public Boolean permitConstructor(Constructor<?> constructor) {
                if (constructor.equals(intConstructor)) {
                    allowedMethodCount.increment();
                    return true;
                }
                if (constructor.equals(doubleConstructor)) {
                    disallowedMethodCount.increment();
                    return false;
                }
                nullMethodCount.increment();
                return null;
            }

            @Override
            public Boolean permitMethod(Method method) {
                return null;
            }
        };
        final CachingMethodInvocationValidator cachingValidator = new CachingMethodInvocationValidator(validator);

        Assert.assertTrue(cachingValidator.permitConstructor(intConstructor));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(0, disallowedMethodCount.get());
        Assert.assertEquals(0, nullMethodCount.get());
        Assert.assertFalse(cachingValidator.permitConstructor(doubleConstructor));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(0, nullMethodCount.get());
        Assert.assertNull(cachingValidator.permitConstructor(longConstructor));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(1, nullMethodCount.get());

        // verify we did not need to ask the validator a second time
        Assert.assertTrue(cachingValidator.permitConstructor(intConstructor));
        Assert.assertFalse(cachingValidator.permitConstructor(doubleConstructor));
        Assert.assertNull(cachingValidator.permitConstructor(longConstructor));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(1, nullMethodCount.get());
    }
}
