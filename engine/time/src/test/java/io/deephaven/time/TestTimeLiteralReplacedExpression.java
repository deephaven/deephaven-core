package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

public class TestTimeLiteralReplacedExpression extends BaseArrayTestCase {
    
    public void testConvertExpression() throws Exception {
        TestCase.assertEquals("_date0", TimeLiteralReplacedExpression.convertExpression("'2010-01-01 NY'").getConvertedFormula());
        TestCase.assertEquals("_time0", TimeLiteralReplacedExpression.convertExpression("'12:00'").getConvertedFormula());
        TestCase.assertEquals("_period0", TimeLiteralReplacedExpression.convertExpression("'T1S'").getConvertedFormula());
        TestCase.assertEquals("'g'", TimeLiteralReplacedExpression.convertExpression("'g'").getConvertedFormula());
    }

}
