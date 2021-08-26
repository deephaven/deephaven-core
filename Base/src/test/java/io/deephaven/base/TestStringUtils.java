package io.deephaven.base;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.TreeSet;

/**
 * Unit tests for StringUtils.
 */
public class TestStringUtils extends BaseArrayTestCase {
    public void testJoinStrings() {
        Collection<String> strings = new LinkedHashSet<>();
        strings.add("Foo");
        strings.add("Bar");
        strings.add("Baz");

        assertEquals("Foo,Bar,Baz", StringUtils.joinStrings(strings, ","));
        assertEquals("Foo/Bar/Baz", StringUtils.joinStrings(strings.stream(), "/"));
        assertEquals("Foo\nBar\nBaz", StringUtils.joinStrings(strings.iterator(), "\n"));
        assertEquals("", StringUtils.joinStrings(Collections.emptyList(), "\n"));

        Collection<Integer> objects = new TreeSet<>();
        objects.add(3);
        objects.add(1);
        objects.add(7);
        objects.add(2);

        assertEquals("1\n2\n3\n7", StringUtils.joinStrings(objects, "\n"));
        assertEquals("1\\3\\7",
            StringUtils.joinStrings(objects.stream().filter(i -> i % 2 != 0), "\\"));
        assertEquals("1,2,3,7", StringUtils.joinStrings(objects.iterator(), ","));

    }
}
