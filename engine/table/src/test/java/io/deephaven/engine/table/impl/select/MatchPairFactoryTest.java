package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.exceptions.ExpressionException;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import junit.framework.TestCase;

public class MatchPairFactoryTest extends TestCase {
    public void testMatchPair() {
        MatchPair[] results;

        final String[] matches = {"$A", "B", "Cz1_$", "d$_1"};
        results = MatchPairFactory.getExpressions(matches);
        assertEquals(matches.length, results.length);
        for (final MatchPair matchPair : results) {
            assertEquals(matchPair.leftColumn, matchPair.rightColumn);
        }

        results = MatchPairFactory.getExpressions("Bah = Humbug", "\tfIdDl3 ==sT1ck5 ");
        assertEquals(results[0].leftColumn, "Bah");
        assertEquals(results[0].rightColumn, "Humbug");
        assertEquals(results[1].leftColumn, "fIdDl3");
        assertEquals(results[1].rightColumn, "sT1ck5");

        try {
            MatchPairFactory.getExpressions("0a==a0");
            fail("MatchPairFactory accepted invalid expression \"0a==a0\"");
        } catch (final ExpressionException e) {
            assertEquals(e.getMessage(), "Unable to parse expression: \"0a==a0\"");
        }
    }
}
