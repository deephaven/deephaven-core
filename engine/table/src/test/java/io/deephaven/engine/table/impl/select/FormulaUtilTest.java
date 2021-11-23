/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.select.FormulaUtil;
import junit.framework.TestCase;

public class FormulaUtilTest extends TestCase {
    public void testReplace() {
        assertEquals("a a bc ab a", FormulaUtil.replaceFormulaTokens("b b bc ab b", "b", "a"));
        assertEquals("bb bb bc ab bb", FormulaUtil.replaceFormulaTokens("b b bc ab b", "b", "bb"));
        assertEquals("ba ba bc ab ba", FormulaUtil.replaceFormulaTokens("b b bc ab b", "b", "ba"));
        assertEquals("aaaa a bc ab a", FormulaUtil.replaceFormulaTokens("aaaa b bc ab b", "b", "a"));
        assertEquals("abc abc bc ab abc", FormulaUtil.replaceFormulaTokens("b b bc ab b", "b", "abc"));
    }
}
