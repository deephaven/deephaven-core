//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import org.jpy.PyObject;

import javax.annotation.Nullable;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tool to wrap Python Callables into Java functional interfaces. Useful for Java-aware Python code to implement Java
 * functional interfaces to pass callbacks/handlers/etc to Python-oblivious Java APIs.
 */
public class JavaLambdaFactory {
    /**
     * Wraps a python callable in a java functional interface, with an optional extra step to coerce a returned value to
     * handle Deephaven nulls.
     * 
     * @param samInterface the Java interface to wrap the callable in. If this has more than one abstract method, an
     *        exception will be thrown.
     * @param callable The Python Callable to wrap
     * @param coerceToType An optional, expected return type for the Callable to return.
     * @param <T> the type of the lambda to return
     * @return a Java functional interface implementation
     */
    public static <T> T create(Class<T> samInterface, PyObject callable, @Nullable Class<?> coerceToType) {
        // examine the given interface type, find single abstract method
        Set<Method> abstractMethods = Arrays.stream(samInterface.getMethods())
                .filter(m -> Modifier.isAbstract(m.getModifiers()))
                .collect(Collectors.toSet());
        if (abstractMethods.isEmpty()) {
            throw new IllegalArgumentException("Provided interface " + samInterface + " has zero abstract methods");
        }
        if (abstractMethods.size() > 1) {
            throw new IllegalArgumentException(
                    "Provided interface " + samInterface + " has too many abstract methods: " + abstractMethods);
        }
        if (coerceToType == null) {
            coerceToType = Object.class;
        }

        MethodHandles.Lookup caller = MethodHandles.lookup();
        // Look up our helper method below. The best option would be tu use the LambdaMetafactory here and directly
        // instantiate our lambda around this method reference, but we don't know the expected arity/types. We could
        // generate new methods/etc on the fly at which point we could just implement the class on the fly, and skip the
        // LambdaMetafactory entirely. Instead, we'll do a little more work on the MethodHandle, and then wrap in a
        // proxy, using provided JVM tools.
        MethodHandle helper;
        try {
            helper = caller.findStatic(JavaLambdaFactory.class, "invoke",
                    MethodType.methodType(Object.class, PyObject.class, Class.class, Object[].class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Internal error, failed to find JavaLambdaFactory.invoke() method", e);
        }

        // "close" over the callable we received from python, using it as the first argument to the helper
        MethodHandle closeOnCallable = MethodHandles.insertArguments(helper, 0, callable, coerceToType);

        // Take the rest of the arguments in the lambda interface, and map to an object array, to be passed as varargs
        MethodHandle nonVarArgs = closeOnCallable.asVarargsCollector(Object[].class);

        // Wrap in a proxy which will implement the requested interface
        return MethodHandleProxies.asInterfaceInstance(samInterface, nonVarArgs);
    }

    private static Object invoke(PyObject callable, Class<?> coerceToType, Object... args) {
        PyObject out = callable.call("__call__", args);
        if (coerceToType != null) {
            return PythonValueGetter.getValue(out, coerceToType);
        }
        return out;
    }
}
