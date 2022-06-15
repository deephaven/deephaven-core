/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.testing;

import io.deephaven.base.Function;
import io.deephaven.base.verify.Assert;
import junit.framework.TestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.States;
import org.jmock.api.Imposteriser;
import org.jmock.api.Invocation;
import org.jmock.api.Invokable;
import org.jmock.auto.internal.Mockomatic;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.internal.ExpectationBuilder;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.concurrent.Synchroniser;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

abstract public class BaseCachedJMockTestCase extends TestCase {
    protected final Mockery context;

    {
        // use an initializer rather than setUp so forgetting to
        // call super.setUp won't use the wrong imposteriser
        context = new Mockery();
        context.setThreadingPolicy(new Synchroniser());
        context.setImposteriser(CachingImposteriser.INSTANCE);
        new Mockomatic(context).fillIn(this);
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
    protected void tearDown() throws Exception {
        context.assertIsSatisfied();
        super.tearDown();
    }

    public static class Expectations extends org.jmock.Expectations {

        public static <T> Matcher<T> some(Class<T> type) {
            return CoreMatchers.instanceOf(type);
        }

        public static <T> Matcher<T> any(Class<T> type) {
            return CoreMatchers.anyOf(CoreMatchers.instanceOf(type), CoreMatchers.nullValue(type));
        }

        private static AtomicInteger willDoCounter = new AtomicInteger(0);

        public void willDo(final Function.Nullary<Object> proc) {
            this.currentBuilder().setAction(run(proc));
        }

        public static CustomAction run(final Function.Nullary<Object> proc) {
            return new CustomAction("willDo_" + willDoCounter.incrementAndGet()) {
                @Override
                public Object invoke(Invocation invocation) throws Throwable {
                    return proc.call();
                }
            };
        }

        public void willDo(final Function.Unary<Object, Invocation> proc) {
            this.currentBuilder().setAction(run(proc));
        }

        public static CustomAction run(final Function.Unary<Object, Invocation> proc) {
            return new CustomAction("willDo_" + willDoCounter.incrementAndGet()) {
                @Override
                public Object invoke(Invocation invocation) throws Throwable {
                    return proc.call(invocation);
                }
            };
        }
    }

    // ----------------------------------------------------------------
    public static class CachingImposteriser implements Imposteriser {

        public static final BaseCachedJMockTestCase.CachingImposteriser INSTANCE = new CachingImposteriser();

        private final static Class[] CONSTRUCTOR_PARAMS = {InvocationHandler.class};

        private static Map<ProxyInfo, Function.Unary<?, Invokable>> proxyInfoToConstructorMap = new HashMap<>();

        // ----------------------------------------------------------------
        @Override // from Imposteriser
        public boolean canImposterise(Class<?> type) {
            return ByteBuddyClassImposteriser.INSTANCE.canImposterise(type);
        }

        // ----------------------------------------------------------------
        @Override // from Imposteriser
        public <T> T imposterise(final Invokable mockObject, Class<T> mockedType, Class<?>... ancillaryTypes) {
            ProxyInfo proxyInfo = new ProxyInfo(mockedType, ancillaryTypes);
            Function.Unary<?, Invokable> constructor = proxyInfoToConstructorMap.get(proxyInfo);
            if (null == constructor) {
                constructor = createConstructor(proxyInfo);
                proxyInfoToConstructorMap.put(proxyInfo, constructor);
            }
            // noinspection unchecked
            return (T) constructor.call(mockObject);
        }

        // ----------------------------------------------------------------
        private Function.Unary<?, Invokable> createConstructor(ProxyInfo proxyInfo) {
            if (proxyInfo.mockedType.isInterface()) {
                return createInterfaceConstructor(proxyInfo);
            } else {
                return createClassConstructor(proxyInfo);
            }
        }

        // ----------------------------------------------------------------
        /** Based on {@link org.jmock.lib.JavaReflectionImposteriser}. */
        private Function.Unary<?, Invokable> createInterfaceConstructor(ProxyInfo proxyInfo) {
            ClassLoader proxyClassLoader = BaseCachedJMockTestCase.class.getClassLoader();
            Class proxyClass = Proxy.getProxyClass(proxyClassLoader, proxyInfo.proxiedClasses);

            final Constructor constructor;
            try {
                constructor = proxyClass.getConstructor(CONSTRUCTOR_PARAMS);
            } catch (NoSuchMethodException e) {
                throw Assert.exceptionNeverCaught(e);
            }
            return new Function.Unary<Object, Invokable>() {
                @Override
                public Object call(final Invokable invokable) {
                    try {
                        return constructor.newInstance(new InvocationHandler() {
                            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                                return invokable.invoke(new Invocation(proxy, method, args));
                            }
                        });
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw Assert.exceptionNeverCaught(e);
                    }
                }
            };
        }

        // ----------------------------------------------------------------
        /** Based on {@link ByteBuddyClassImposteriser}. */
        private Function.Unary<?, Invokable> createClassConstructor(final ProxyInfo proxyInfo) {
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
