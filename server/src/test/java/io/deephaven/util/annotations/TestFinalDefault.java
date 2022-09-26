package io.deephaven.util.annotations;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.MethodInfo;
import io.github.classgraph.ScanResult;
import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class TestFinalDefault {
    private static final Logger logger = LoggerFactory.getLogger(TestFinalDefault.class);
    private static final String FINAL_DEFAULT = FinalDefault.class.getName();
    private static final String EXPECT_EXCEPTION = ExpectException.class.getName();

    @Test
    public void testForOverriddenFinalDefault() {
        checkForOverridesInPackage("io.deephaven", FINAL_DEFAULT);
    }

    @Test
    public void testClassA() {
        expectFailure(() -> checkForOverridesInClass(TestClassA.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testClassB() {
        expectFailure(() -> checkForOverridesInClass(TestClassB.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testClassC() {
        expectFailure(() -> checkForOverridesInClass(TestClassC.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSubInterfaceA() {
        expectFailure(() -> checkForOverridesInClass(TestSubInterfaceA.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSubInterfaceB() {
        expectFailure(() -> checkForOverridesInClass(TestSubInterfaceB.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSubInterfaceC() {
        expectFailure(() -> checkForOverridesInClass(TestSubInterfaceC.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSuperClassA() {
        expectFailure(() -> checkForOverridesInClass(TestSuperClassA.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSuperClassB() {
        expectFailure(() -> checkForOverridesInClass(TestSuperClassB.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testSuperClassC() {
        expectFailure(() -> checkForOverridesInClass(TestSuperClassC.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testGenericClassA() {
        expectFailure(() -> checkForOverridesInClass(TestGenericClassA.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testGenericClassB() {
        expectFailure(() -> checkForOverridesInClass(TestGenericClassB.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testGenericClassC() {
        expectFailure(() -> checkForOverridesInClass(TestGenericClassC.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testFixedClassA() {
        expectFailure(() -> checkForOverridesInClass(TestFixedClassA.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testFixedClassB() {
        expectFailure(() -> checkForOverridesInClass(TestFixedClassB.class, EXPECT_EXCEPTION));
    }

    @Test
    public void testFixedClassC() {
        expectFailure(() -> checkForOverridesInClass(TestFixedClassC.class, EXPECT_EXCEPTION));
    }

    @Target({ElementType.METHOD})
    @Inherited
    public @interface ExpectException {
    }
    public interface TestInterface {
        @ExpectException
        default void test() {}
    }
    public static class TestClassA implements TestInterface {
        public void test() {}
    }
    public static class TestClassB implements TestInterface {
        @Override
        public void test() {}
    }
    public static class TestClassC implements TestInterface {
        @ExpectException
        public void test() {}
    }
    public interface TestSubInterfaceA extends TestInterface {
        default void test() {}
    }
    public interface TestSubInterfaceB extends TestInterface {
        @Override
        default void test() {}
    }
    public interface TestSubInterfaceC extends TestInterface {
        @ExpectException
        default void test() {}
    }
    public static class TestSuperClass {
        @ExpectException
        public void test() {}
    }
    public static class TestSuperClassA extends TestSuperClass {
        public void test() {}
    }
    public static class TestSuperClassB extends TestSuperClass {
        @Override
        public void test() {}
    }
    public static class TestSuperClassC extends TestSuperClass {
        @ExpectException
        public void test() {}
    }
    public interface TestGenericInterface<T> {
        @ExpectException
        default T test(T t) {
            return null;
        }
    }
    public static class TestGenericClassA<T> implements TestGenericInterface<T> {
        public T test(T t) {
            return null;
        }
    }
    public static class TestGenericClassB<T> implements TestGenericInterface<T> {
        @Override
        public T test(T t) {
            return null;
        }
    }
    public static class TestGenericClassC<T> implements TestGenericInterface<T> {
        @ExpectException
        public T test(T t) {
            return null;
        }
    }
    public static class TestFixedClassA implements TestGenericInterface<String> {
        public String test(String s) {
            return null;
        }
    }
    public static class TestFixedClassB implements TestGenericInterface<String> {
        @Override
        public String test(String s) {
            return null;
        }
    }
    public static class TestFixedClassC implements TestGenericInterface<String> {
        @ExpectException
        public String test(String s) {
            return null;
        }
    }

    private static void expectFailure(final Runnable runner) {
        try {
            runner.run();
        } catch (final AssertionError e) {
            return; // assertion is expected
        }
        Assert.fail("Expected assertion failure, but none was thrown");
    }

    @SuppressWarnings("SameParameterValue")
    private static void checkForOverridesInPackage(final String packageName, final String annotationName) {
        checkForOverrides(new ClassGraph().enableAllInfo().acceptPackages(packageName), annotationName);
    }

    @SuppressWarnings("SameParameterValue")
    private static void checkForOverridesInClass(final Class<?> cls, final String annotationName) {
        checkForOverrides(new ClassGraph().enableAllInfo().acceptClasses(cls.getName()), annotationName);
    }

    private static void checkForOverrides(final ClassGraph classGraph, final String annotationName) {
        final List<String> errors = new ArrayList<>();
        int numClassesChecked = 0;
        int numAnnotatedMethodsChecked = 0;
        try (ScanResult scanResult = classGraph.scan()) {
            for (final ClassInfo classInfo : scanResult.getClassesWithMethodAnnotation(annotationName)) {
                numClassesChecked++;
                for (final MethodInfo methodInfo : classInfo.getDeclaredMethodInfo()) {
                    if (!methodInfo.hasAnnotation(annotationName)) {
                        continue;
                    }
                    numAnnotatedMethodsChecked++;
                    findOverrides(methodInfo).forEach(method -> {
                        errors.add("Method " + methodInfo.getName() + " in "
                                + method.getDeclaringClass() + " overrides " + classInfo.getName() + " but is "
                                + "annotated with " + annotationName + ".");
                    });
                }
            }
        }

        if (!errors.isEmpty()) {
            errors.forEach(error -> logger.error().append(error).endl());
            Assert.fail("Found " + errors.size() + " methods annotated with " + annotationName
                    + " that are overridden.");
        }
        logger.info().append("Verified ").append(annotationName).append(" on ").append(numClassesChecked)
                .append(" classes and ").append(numAnnotatedMethodsChecked).append(" methods.").endl();
    }

    private static Collection<Method> findOverrides(final MethodInfo target) {
        final Collection<Method> overrides = new ArrayList<>();

        final Method targetMethod = target.loadClassAndGetMethod();
        final Consumer<ClassInfo> visitClass = (ci) -> {
            Arrays.stream(ci.loadClass().getDeclaredMethods())
                    .filter(m -> overrides(m, targetMethod))
                    .forEach(overrides::add);
        };

        final ClassInfo clsInfo = target.getClassInfo();
        clsInfo.getSubclasses().forEach(visitClass);
        if (clsInfo.isInterface()) {
            clsInfo.getClassesImplementing().forEach(visitClass);
        }

        return overrides;
    }

    private static boolean overrides(final Method override, final Method target) {
        if (!target.getName().equals(override.getName())) {
            return false;
        }
        if (target.getParameterCount() != override.getParameterCount()) {
            return false;
        }
        for (int ii = 0; ii < target.getParameterCount(); ++ii) {
            if (!target.getParameterTypes()[ii].isAssignableFrom(override.getParameterTypes()[ii])) {
                return false;
            }
        }
        return target.getReturnType().isAssignableFrom(override.getReturnType());
    }
}
