package io.deephaven.base;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

@RunWith(Parameterized.class)
public class ClassUtilTest {
    @Parameterized.Parameters
    public static List<Function<Class<?>, String>[]> naming() {
        return Arrays.asList(
                new Function[] {(Function<Class<?>, String>) Class::getName},
                new Function[] {(Function<Class<?>, String>) Class::getCanonicalName});
    }

    private final Function<Class<?>, String> namer;

    public ClassUtilTest(Function<Class<?>, String> namer) {
        this.namer = namer;
    }

    private Class<?> lookup(Class<?> clazz) throws ClassNotFoundException {
        String string = namer.apply(clazz);
        System.out.println(string);
        return ClassUtil.lookupClass(string);
    }

    private void assertRoundTrip(Class<?> clazz) throws ClassNotFoundException {
        assertSame(clazz, lookup(clazz));
    }

    @Test
    public void lookupClassSimple() throws ClassNotFoundException {
        assertRoundTrip(String.class);
        assertRoundTrip(ClassUtilTest.class);
    }

    @Test
    public void lookupClassPrimitive() throws ClassNotFoundException {
        assertRoundTrip(int.class);
        assertRoundTrip(Integer.class);
        assertRoundTrip(char.class);

        // assertRoundTrip(void.class);
        assertRoundTrip(Void.class);
    }

    @Test
    public void lookupClassArray() throws ClassNotFoundException {
        assertRoundTrip(String[].class);
        assertRoundTrip(String[][][][][][].class);
        assertRoundTrip(ClassUtilTest[][][][][][].class);

        assertRoundTrip(int[].class);
    }

    @Test
    public void lookupClassInner() throws ClassNotFoundException {
        assertRoundTrip(StaticInnerClass.class);
        assertRoundTrip(InnerClass.class);
        assertRoundTrip(StaticInnerClass.StaticInnerStaticInnerClass.class);
        assertRoundTrip(StaticInnerClass.InnerStaticInnerClass.class);
        assertRoundTrip(InnerClass.InnerInnerClass.class);
        assertRoundTrip(Outer.class);
    }

    @Test
    public void lookupClassWithClinit() throws ClassNotFoundException {
        assertRoundTrip(VerifyNotInitialized.class);
        assertFalse(verifyNotInitializedWasNotInitialized);
    }

    @Test
    public void testGenericStrings() throws ClassNotFoundException {
        assertSame(Map.Entry.class, ClassUtil.lookupClass(namer.apply(Map.Entry.class) + "<String, String>"));
        // note that this name isn't quite legal for getName(), will try a few iterations to make sure we DWIM
        assertSame(Map.Entry[].class, ClassUtil.lookupClass(namer.apply(Map.Entry.class) + "<String, String>[]"));
        assertSame(Map.Entry[].class, ClassUtil.lookupClass(namer.apply(Map.Entry[].class) + "<String, String>"));
        assertSame(Map.Entry[].class, ClassUtil.lookupClass("[L" + namer.apply(Map.Entry.class) + "<String, String>;"));
    }

    public static class StaticInnerClass {

        public class StaticInnerStaticInnerClass {
        }
        public static class InnerStaticInnerClass {
        }
    }

    public class InnerClass {
        public class InnerInnerClass {
        }
    }

    // Do not reset this by hand, that won't cause the class to be re-initialized
    public static boolean verifyNotInitializedWasNotInitialized = false;
}


class Outer {
}


class VerifyNotInitialized {
    static {
        ClassUtilTest.verifyNotInitializedWasNotInitialized = true;
    }
}
