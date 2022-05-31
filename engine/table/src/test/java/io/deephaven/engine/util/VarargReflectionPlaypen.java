package io.deephaven.engine.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Just create a couple of methods to figure out what they look like to reflection.
 */
public class VarargReflectionPlaypen {
    public static class Foo {
    }

    public static void m1(String... args) {
        System.out.println("m1: " + args);
    }

    public static void m2(String[] args) {
        System.out.println("m2");
    }

    public static void m1(Foo... args) {
        System.out.println("m3");
    }

    public static void m3(String... args) {
        System.out.println("m3: " + args);
    }

    public static void main(String[] args) {
        Class<VarargReflectionPlaypen> clazz = VarargReflectionPlaypen.class;
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            System.out.println(method);
            System.out.println("Modifiers: " + method.getModifiers());
            System.out.println(Modifier.toString(method.getModifiers()));
        }

        // m3(null);
    }
}
