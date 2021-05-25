/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.jpy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.jpy.PyLib.CallableKind;

import static org.jpy.PyLib.assertPythonRuns;

/**
 * The {@code InvocationHandler} for used by the proxy instances created by the
 * {@link PyObject#createProxy(Class)} and {@link PyModule#createProxy(Class)}
 * methods.
 *
 * @author Norman Fomferra
 * @since 0.7
 */
class PyProxyHandler implements InvocationHandler {
    // preloaded Method objects for the methods in java.lang.Object
    private static Method hashCodeMethod;
    
    private static Method equalsMethod;
    
    private static Method toStringMethod;

    static {
        try {
            hashCodeMethod = Object.class.getMethod("hashCode");
            equalsMethod = Object.class.getMethod("equals", new Class[] { Object.class });
            toStringMethod = Object.class.getMethod("toString");
        } catch (NoSuchMethodException e) {
            throw new NoSuchMethodError(e.getMessage());
        }
    }
    
    private final PyObject pyObject;
    
    private final PyLib.CallableKind callableKind;
    
    public PyProxyHandler(PyObject pyObject, PyLib.CallableKind callableKind) {
        if (pyObject == null) {
            throw new NullPointerException("pyObject");
        }
        this.pyObject = pyObject;
        this.callableKind = callableKind;
    }

    @Override
    public Object invoke(Object proxyObject, Method method, Object[] args) throws Throwable {
        //assertPythonRuns(); // todo: get rid of this check to remove a call down into JNI?

        if (method.isDefault()) {
            // This allows our proxy-able interfaces to define default methods.
            // Note: in this current implementation, defaults methods will always take precedence.
            final Class<?> declaringClass = method.getDeclaringClass();

            // https://blog.jooq.org/2018/03/28/correct-reflective-access-to-interface-default-methods-in-java-8-9-10/

            // note: the following throws an IllegalAccessException of the form no private access for invokespecial
            //return MethodHandles.lookup()
            //    .in(declaringClass)
            //    .unreflectSpecial(method, declaringClass)
            //    .bindTo(proxyObject)
            //    .invokeWithArguments(args);

            // Unfortunately, the following doesn't work w/ Java 9+. There should be some new api
            // methods that work w/ Java 9+ though. (MethodHandles#privateLookupIn)
            final Constructor<Lookup> constructor = Lookup.class
                .getDeclaredConstructor(Class.class);
            constructor.setAccessible(true);
            return constructor
                .newInstance(declaringClass)
                .in(declaringClass)
                .unreflectSpecial(method, declaringClass)
                .bindTo(proxyObject)
                .invokeWithArguments(args);
        }

        final long pointer = this.pyObject.getPointer();

        if ((PyLib.Diag.getFlags() & PyLib.Diag.F_METH) != 0) {
            System.out.printf("org.jpy.PyProxyHandler: invoke: %s(%s) on pyObject=%s in thread %s\n", method.getName(),
                    Arrays.toString(args), Long.toHexString(pointer), Thread.currentThread());
        }
        final String methodName = method.getName();
        final Class<?> returnType = method.getReturnType();
        if (method.equals(hashCodeMethod)) {
            return callPythonHash();
        } else if (method.equals(equalsMethod)) {
            return this.pyObject.eq(args[0]);
        } else if (method.equals(toStringMethod)) {
            return this.pyObject.str();
        } else if ("close".equals(method.getName())
            && method.getParameterCount() == 0
            && void.class.equals(method.getReturnType())
            && AutoCloseable.class.isAssignableFrom(method.getDeclaringClass())) {
            this.pyObject.close();
            return null;
        }

        return PyLib.callAndReturnValue(
            pointer,
            callableKind == CallableKind.METHOD,
            methodName,
            args != null ? args.length : 0, args,
            method.getParameterTypes(),
            returnType);
    }

    PyObject getPyObject() {
        return pyObject;
    }

    /**
     * Calls the Python hash() function on the Python object, and downsamples to
     * 32 bits of it, since Python hash codes are 64 bits on 64 bit
     * machines.
     */
    private int callPythonHash() {
        return Long.hashCode(this.pyObject.hash());
    }
}
