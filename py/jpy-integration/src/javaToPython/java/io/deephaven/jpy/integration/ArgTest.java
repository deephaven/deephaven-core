package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import io.deephaven.jpy.integration.SomeJavaClassOutTest.SomeJavaClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArgTest extends PythonTest {

    // we need to choose a value that the python runtime does *not* have a reference to
    private static final int UNIQ_INT = 0xbadc0fee;
    private static final String UNIQ_STR = "bad coffee";
    private static final SomeJavaClass SJC = new SomeJavaClass();
    private static final int[] INTS = new int[] {31337, 42, UNIQ_INT};
    private static final Integer[] INTEGERS = new Integer[] {31337, 42, UNIQ_INT};
    private static final String[] STRINGS = new String[] {UNIQ_STR, "good coffee"};
    private static final SomeJavaClass[] SJCS = new SomeJavaClass[] {SJC, new SomeJavaClass()};

    private NoopModule noop;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        noop = NoopModule.create(getCreateModule());
        ref = ReferenceCounting.create();
        jpy = JpyModule.create();
        // jpy.setFlags(EnumSet.of(Flag.ALL));
    }

    @After
    public void tearDown() {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        jpy.close();
        ref.close();
        noop.close();
    }

    @Test
    public void explicitInt() {
        noop.noop(UNIQ_INT);
    }

    @Test
    public void explicitInteger() {
        noop.noop(Integer.valueOf(UNIQ_INT));
    }

    @Test
    public void implicitInteger() {
        noop.noop((Object) UNIQ_INT);
    }

    @Test
    public void explicitString() {
        noop.noop(UNIQ_STR);
    }

    @Test
    public void implicitString() {
        noop.noop((Object) UNIQ_STR);
    }

    @Test
    public void explicitSJC() {
        noop.noop(SJC);
    }

    @Test
    public void implicitSJC() {
        noop.noop((Object) SJC);
    }

    @Test
    public void explicitInts() {
        noop.noop(INTS);
    }

    @Test
    public void implicitInts() {
        noop.noop((Object) INTS);
    }

    @Test
    public void explicitIntegers() {
        noop.noop(INTEGERS);
    }

    @Test
    public void implicitIntegers() {
        noop.noop((Object) INTEGERS);
    }

    @Test
    public void explicitStrings() {
        noop.noop(STRINGS);
    }

    @Test
    public void implicitStrings() {
        noop.noop((Object) STRINGS);
    }

    @Test
    public void explicitSJCS() {
        noop.noop(SJCS);
    }

    @Test
    public void implicitSJCS() {
        noop.noop((Object) SJCS);
    }

    // todo: all these tests as PyObject inputs

}
