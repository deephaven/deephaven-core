/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.lang;

import io.deephaven.db.tables.dbarrays.DbIntArray;

import java.io.Serializable;

@SuppressWarnings({"WeakerAccess", "unused"})
public class DBLanguageParserDummyClass {
    public final int value;

    public DBLanguageParserDummyClass() {
        this(0);
    }

    public DBLanguageParserDummyClass(int value) {
        this.value = value;
    }

    public static long arrayAndDbArrayFunction(int[] arr) {
        return arr.length;
    }

    public static long arrayAndDbArrayFunction(DbIntArray arr) {
        return arr.size();
    }

    public static long dbArrayOnlyFunction(DbIntArray arr) {
        return arrayAndDbArrayFunction(arr);
    }

    public static long arrayOnlyFunction(int[] arr) {
        return arrayAndDbArrayFunction(arr);
    }

    public static int functionWithInterfacesAsArgTypes(CharSequence cs, Serializable s) {
        return 0;
    }

    public static int functionWithEnumAsArgs(DBLanguageParserDummyEnum enumVal) {
        return 0;
    }

    public static int functionWithEnumAsArgs(DBLanguageParserDummyInterface.AnEnum enumVal) {
        return 0;
    }

    public static int functionWithEnumVarArgs(DBLanguageParserDummyEnum... enumVals) {
        return 0;
    }

    public static int overloadedStaticMethod() {
        return 0;
    }

    public static int overloadedStaticMethod(String arg) {
        return 0;
    }

    public static int overloadedStaticMethod(String... args) {
        return 0;
    }

    public static int overloadedMethod(String... args) {
        return 0;
    }

    public static int overloadedMethod() {
        return 0;
    }

    public static int overloadedMethod(String arg) {
        return 0;
    }

    public final InnerClass innerClassInstance = new InnerClass();
    public final InnerClass2 innerClass2Instance = new InnerClass2();

    public static double[] interpolate(double[] x, double[] y, double[] xi, NestedEnum anEnumArg,
        boolean extrapolate) {
        return new double[] {0};
    }

    public enum NestedEnum {
        ONE, TWO
    }

    public interface SubclassOfDBLanguageParserDummyClass {
        enum EnumInInterface {
            THING_ONE, THING_TWO
        }
    }

    public class InnerClass implements SubclassOfDBLanguageParserDummyClass {
        public static final String staticVar = "InnerClass_staticVar";
        public final String instanceVar;
        public final InnerInnerClass innerInnerClassInstance = new InnerInnerClass();

        public InnerClass() {
            this("anInnerClass.instanceVar");
        }

        public InnerClass(String instanceVar) {
            this.instanceVar = instanceVar;
        }

        public class InnerInnerClass {
            public final String innerInnerInstanceVar = "anInnerInnerClass.instanceVar";
        }
    }

    public class InnerClass2 implements SubclassOfDBLanguageParserDummyClass {
        public final InnerClass innerClassAsInstanceOfAnotherInnerClass =
            new InnerClass("InnerClass2.innerClassAsInstanceOfAnotherInnerClass.instancevar");
    }

    public static class StaticNestedClass implements SubclassOfDBLanguageParserDummyClass {
        public static final String staticVar = "StaticNestedClass.staticVar";
        public static final StaticNestedClass staticInstanceOfStaticClass = new StaticNestedClass();
        public final String instanceVar = "aStaticNestedClass.instanceVar";
    }
}
