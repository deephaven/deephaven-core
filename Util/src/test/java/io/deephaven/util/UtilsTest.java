package io.deephaven.util;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    interface IFoo {
        int foo();
    }
    abstract static class AFoo implements IFoo {
        public int foo() {
            return 0;
        };
    }
    static class Foo extends AFoo {
        int anything() {
            return 1;
        }
    }

    @Test
    public void testSimpleName() {
        Foo f1 = new Foo();
        Foo f2 = new Foo() {};

        Assert.assertTrue(f2.getClass().getSimpleName().isEmpty());
        Assert.assertTrue(!Utils.getSimpleNameFor(f1).isEmpty());
        // simple name of anonymous extension same as parent // note: we could want to change this someday
        Assert.assertEquals(Utils.getSimpleNameFor(f1), Utils.getSimpleNameFor(f2));

        // class and object behave the same
        Assert.assertEquals(Utils.getSimpleNameFor(f1), Utils.getSimpleNameFor(Foo.class));
        Assert.assertEquals(Utils.getSimpleNameFor(new Foo() {}), Utils.getSimpleNameFor(Foo.class));
        Assert.assertEquals(Utils.getSimpleNameFor(new AFoo() {}), Utils.getSimpleNameFor(AFoo.class));

        // simple anonymous class returns "Object"
        Assert.assertEquals(Utils.getSimpleNameFor(new IFoo() {
            @Override
            public int foo() {
                return 0;
            }
        }), Utils.getSimpleNameFor(Object.class));

        // check the lambda version
        // Assert.assertEquals(Utils.getSimpleNameFor((IFoo) () -> 0), Utils.getSimpleNameFor(Object.class));
        // this getClass().getSimpleName() on a lambda returns something like "UtilsTest$$Lambda$2/385242642"
    }
}
