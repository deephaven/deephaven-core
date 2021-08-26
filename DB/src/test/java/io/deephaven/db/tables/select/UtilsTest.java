/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import junit.framework.TestCase;

public class UtilsTest extends TestCase {
    public void testReplace() {
        assertEquals("a a bc ab a", Utils.replaceFormulaTokens("b b bc ab b", "b", "a"));
        assertEquals("bb bb bc ab bb", Utils.replaceFormulaTokens("b b bc ab b", "b", "bb"));
        assertEquals("ba ba bc ab ba", Utils.replaceFormulaTokens("b b bc ab b", "b", "ba"));
        assertEquals("aaaa a bc ab a", Utils.replaceFormulaTokens("aaaa b bc ab b", "b", "a"));
        assertEquals("abc abc bc ab abc", Utils.replaceFormulaTokens("b b bc ab b", "b", "abc"));
    }
}
