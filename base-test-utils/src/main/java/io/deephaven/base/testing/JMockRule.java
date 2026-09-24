//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.testing;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.jmock.Mockery;
import org.jmock.api.Imposteriser;
import org.jmock.api.Invocation;
import org.jmock.api.Invokable;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.lib.action.CustomAction;
import org.jmock.Sequence;
import org.jmock.States;
import org.jmock.internal.ExpectationBuilder;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * JMock fixture as a {@link TestRule}: owns a {@link Mockery} with Deephaven's threading policy and caching
 * imposteriser, and asserts that it is satisfied once the test has finished.
 *
 * <p>
 * Use it as {@code @Rule public final JMockRule jmock = new JMockRule();}. Composing it as a rule rather than
 * inheriting a base class leaves the single superclass slot free for whatever else a test needs.
 */
public class JMockRule implements TestRule {

    private final Mockery context;

    public JMockRule() {
        context = new Mockery();
        context.setThreadingPolicy(new Synchroniser());
        context.setImposteriser(CachingImposteriser.INSTANCE);
    }

    public Mockery context() {
        return context;
    }

    public <T> T mock(Class<T> tClass) {
        return context.mock(tClass);
    }

    public <T> T mock(Class<T> tClass, String s) {
        return context.mock(tClass, s);
    }

    public States states(String s) {
        return context.states(s);
    }

    public Sequence sequence(String s) {
        return context.sequence(s);
    }

    public void checking(ExpectationBuilder expectationBuilder) {
        context.checking(expectationBuilder);
    }

    public void assertIsSatisfied() {
        context.assertIsSatisfied();
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                statement.evaluate();
                context.assertIsSatisfied();
            }
        };
    }

    public static class Expectations extends org.jmock.Expectations {

        public static <T> Matcher<T> some(Class<T> type) {
            return CoreMatchers.instanceOf(type);
        }

        public static <T> Matcher<T> any(Class<T> type) {
            return CoreMatchers.anyOf(CoreMatchers.instanceOf(type), CoreMatchers.nullValue(type));
        }

        private static AtomicInteger willDoCounter = new AtomicInteger(0);

        public void willDo(final Supplier<Object> proc) {
            this.currentBuilder().setAction(run(proc));
        }

        public static CustomAction run(final Supplier<Object> proc) {
            return new CustomAction("willDo_" + willDoCounter.incrementAndGet()) {
                @Override
                public Object invoke(Invocation invocation) throws Throwable {
                    return proc.get();
                }
            };
        }

        public void willDo(final Function<org.jmock.api.Invocation, Object> proc) {
            this.currentBuilder().setAction(run(proc));
        }

        public static CustomAction run(final Function<org.jmock.api.Invocation, Object> proc) {
            return new CustomAction("willDo_" + willDoCounter.incrementAndGet()) {
                @Override
                public Object invoke(Invocation invocation) throws Throwable {
                    return proc.apply(invocation);
                }
            };
        }
    }

    // ----------------------------------------------------------------
    public static class CachingImposteriser implements Imposteriser {

        public static final CachingImposteriser INSTANCE = new CachingImposteriser();

        private final static Class[] CONSTRUCTOR_PARAMS = {InvocationHandler.class};

        private static Map<ProxyInfo, Function<Invokable, ?>> proxyInfoToConstructorMap =
                new HashMap<>();

        // ----------------------------------------------------------------
        @Override // from Imposteriser
        public boolean canImposterise(Class<?> type) {
            return ByteBuddyClassImposteriser.INSTANCE.canImposterise(type);
        }

        // ----------------------------------------------------------------
        @Override // from Imposteriser
        public <T> T imposterise(final Invokable mockObject, Class<T> mockedType, Class<?>... ancillaryTypes) {
            ProxyInfo proxyInfo = new ProxyInfo(mockedType, ancillaryTypes);
            Function<Invokable, ?> constructor =
                    proxyInfoToConstructorMap.get(proxyInfo);
            if (null == constructor) {
                constructor = createConstructor(proxyInfo);
                proxyInfoToConstructorMap.put(proxyInfo, constructor);
            }
            // noinspection unchecked
            return (T) constructor.apply(mockObject);
        }

        // ----------------------------------------------------------------
        private Function<Invokable, ?> createConstructor(ProxyInfo proxyInfo) {
            if (proxyInfo.mockedType.isInterface()) {
                return createInterfaceConstructor(proxyInfo);
            } else {
                return createClassConstructor(proxyInfo);
            }
        }

        // ----------------------------------------------------------------
        /** Based on {@link org.jmock.lib.JavaReflectionImposteriser}. */
        private Function<Invokable, ?> createInterfaceConstructor(
                ProxyInfo proxyInfo) {
            ClassLoader proxyClassLoader = JMockRule.class.getClassLoader();
            Class proxyClass = Proxy.getProxyClass(proxyClassLoader, proxyInfo.proxiedClasses);

            final Constructor constructor;
            try {
                constructor = proxyClass.getConstructor(CONSTRUCTOR_PARAMS);
            } catch (NoSuchMethodException e) {
                throw io.deephaven.base.verify.Assert.exceptionNeverCaught(e);
            }
            return invokable -> {
                try {
                    return constructor.newInstance((InvocationHandler) (proxy, method, args) -> invokable
                            .invoke(new Invocation(proxy, method, args)));
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw io.deephaven.base.verify.Assert.exceptionNeverCaught(e);
                }
            };
        }

        // ----------------------------------------------------------------
        /** Based on {@link ByteBuddyClassImposteriser}. */
        private Function<Invokable, ?> createClassConstructor(
                final ProxyInfo proxyInfo) {
            return imposter -> ByteBuddyClassImposteriser.INSTANCE.imposterise(imposter, proxyInfo.mockedType,
                    proxyInfo.ancillaryTypes);
        }
    }

    // ----------------------------------------------------------------
    private static class ProxyInfo {

        public Class[] proxiedClasses;
        public Class mockedType;
        public Class[] ancillaryTypes;


        // ----------------------------------------------------------------
        public ProxyInfo(Class<?> mockedType, Class<?>... ancillaryTypes) {
            this.mockedType = mockedType;
            this.ancillaryTypes = ancillaryTypes;
            proxiedClasses = new Class<?>[ancillaryTypes.length + 1];
            proxiedClasses[0] = mockedType;
            System.arraycopy(ancillaryTypes, 0, proxiedClasses, 1, ancillaryTypes.length);
        }

        // ------------------------------------------------------------
        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that == null || getClass() != that.getClass()) {
                return false;
            }
            ProxyInfo proxyInfo = (ProxyInfo) that;
            if (!Arrays.equals(proxiedClasses, proxyInfo.proxiedClasses)) {
                return false;
            }
            return true;
        }

        // ------------------------------------------------------------
        @Override
        public int hashCode() {
            return Arrays.hashCode(proxiedClasses);
        }

        // ------------------------------------------------------------
        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            for (Class proxiedClass : proxiedClasses) {
                stringBuilder.append(0 == stringBuilder.length() ? "[" : ", ").append(proxiedClass.getSimpleName());
            }
            return stringBuilder.append("]").toString();
        }
    }
}
