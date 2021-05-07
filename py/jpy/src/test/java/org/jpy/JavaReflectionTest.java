/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jpy;

import org.jpy.annotations.Mutable;
import org.jpy.annotations.Return;
import org.jpy.fixtures.MethodReturnValueTestFixture;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

/**
 * The tests in here are actually not tests; they are only used to clarify the reflection API and JVM properties.
 *
 * @author Norman Fomferra
 */
public class JavaReflectionTest {

    @Test
    public void testPrimitiveAndVoidNames() throws Exception {
        assertEquals("boolean", Boolean.TYPE.getName());
        assertEquals("byte", Byte.TYPE.getName());
        assertEquals("char", Character.TYPE.getName());
        assertEquals("short", Short.TYPE.getName());
        assertEquals("int", Integer.TYPE.getName());
        assertEquals("long", Long.TYPE.getName());
        assertEquals("float", Float.TYPE.getName());
        assertEquals("double", Double.TYPE.getName());
        assertEquals("void", Void.TYPE.getName());
    }

    @Test
    public void testObjectArrays() throws Exception {
        Class<?> aClass = String[][][].class;
        assertEquals("[[[Ljava.lang.String;", aClass.getName());
        assertEquals(String[][].class, aClass.getComponentType());
        assertEquals(String[].class, aClass.getComponentType().getComponentType());
        assertEquals(String.class, aClass.getComponentType().getComponentType().getComponentType());
        assertEquals(null, aClass.getComponentType().getComponentType().getComponentType().getComponentType());
    }

    @Test
    public void testPrimitiveArrays() throws Exception {
        Class<?> aClass = double[][][].class;
        assertEquals("[[[D", aClass.getName());
        assertEquals(double[][].class, aClass.getComponentType());
        assertEquals(double[].class, aClass.getComponentType().getComponentType());
        assertEquals(Double.TYPE, aClass.getComponentType().getComponentType().getComponentType());
        assertEquals(null, aClass.getComponentType().getComponentType().getComponentType().getComponentType());
    }

    @Test
    public void testAnnotationNames() throws Exception {
        Method method = getClass().getMethod("iHaveAnAnnotationParam", Object.class);
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        assertEquals(1, parameterAnnotations.length);
        assertEquals(2, parameterAnnotations[0].length);
        assertEquals("@org.jpy.annotations.Return()", parameterAnnotations[0][0].toString());
        assertEquals("@org.jpy.annotations.Mutable()", parameterAnnotations[0][1].toString());
    }

    @SuppressWarnings("UnusedDeclaration")
    public Object iHaveAnAnnotationParam(@Return @Mutable Object x) {
        return x;
    }

    public static void main(String[] args) {
        dumpTypeInfo(double[].class);
        dumpTypeInfo(String.class);
        dumpTypeInfo(MethodReturnValueTestFixture.class);
    }

    private static void dumpTypeInfo(Class<?> type) {
        dumpBasicInfo(type, "type", "");

        Constructor<?>[] constructors = type.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            System.out.println("  constructor = " + constructor.getName());
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            dumpParameterTypes(parameterTypes);
        }

        Method[] methods = type.getDeclaredMethods();
        for (Method method : methods) {
            System.out.println("  method '" + method.getName() + "'");

            Class<?> returnType = method.getReturnType();
            dumpBasicInfo(returnType, "returnType", "    ");

            Class<?>[] parameterTypes = method.getParameterTypes();
            dumpParameterTypes(parameterTypes);
        }
    }

    private static void dumpParameterTypes(Class<?>[] parameterTypes) {
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            dumpBasicInfo(parameterType, "parameterTypes[" + i + "]", "    ");
        }
    }

    private static void dumpBasicInfo(Class<?> type, String property, String indent) {
        System.out.println(indent + property + ".name: '" + type.getName() + "'");
        Class<?> componentType = type.getComponentType();
        System.out.println(indent + property + ".componentType: '" + (componentType != null ? componentType.getName() : "null") + "'");
    }
}
