//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AsOfJoinMatchTest {

    @Test
    void aj() {
        checkAj("Foo", "Foo");
        checkAj("Foo>=Foo", "Foo");
    }

    @Test
    void raj() {
        checkRaj("Foo", "Foo");
        checkRaj("Foo<=Foo", "Foo");
    }

    @Test
    void gt() {
        check("Foo>Bar", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check("Foo> Bar", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check("Foo >Bar", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check("Foo > Bar", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check(" Foo>Bar", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check("Foo>Bar ", "Foo", AsOfJoinRule.GREATER_THAN, "Bar");
        check("Foo>Foo", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
        check("Foo> Foo", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
        check("Foo >Foo", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
        check("Foo > Foo", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
        check(" Foo>Foo", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
        check("Foo>Foo ", "Foo", AsOfJoinRule.GREATER_THAN, "Foo");
    }

    @Test
    void geq() {
        check("Foo>=Bar", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check("Foo>= Bar", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check("Foo >=Bar", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check("Foo >= Bar", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check(" Foo>=Bar", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check("Foo>=Bar ", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Bar");
        check("Foo>=Foo", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
        check("Foo>= Foo", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
        check("Foo >=Foo", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
        check("Foo >= Foo", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
        check(" Foo>=Foo", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
        check("Foo>=Foo ", "Foo", AsOfJoinRule.GREATER_THAN_EQUAL, "Foo");
    }

    @Test
    void lt() {
        check("Foo<Bar", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check("Foo< Bar", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check("Foo <Bar", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check("Foo < Bar", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check(" Foo<Bar", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check("Foo<Bar ", "Foo", AsOfJoinRule.LESS_THAN, "Bar");
        check("Foo<Foo", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
        check("Foo< Foo", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
        check("Foo <Foo", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
        check("Foo < Foo", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
        check(" Foo<Foo", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
        check("Foo<Foo ", "Foo", AsOfJoinRule.LESS_THAN, "Foo");
    }

    @Test
    void leq() {
        check("Foo<=Bar", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check("Foo<= Bar", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check("Foo <=Bar", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check("Foo <= Bar", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check(" Foo<=Bar", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check("Foo<=Bar ", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Bar");
        check("Foo<=Foo", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
        check("Foo<= Foo", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
        check("Foo <=Foo", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
        check("Foo <= Foo", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
        check(" Foo<=Foo", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
        check("Foo<=Foo ", "Foo", AsOfJoinRule.LESS_THAN_EQUAL, "Foo");
    }

    @Test
    void bads() {
        checkBad("Foo");
        checkBad("1>Foo");
        checkBad("Foo > Bar > Baz");
        checkBad("Foo=Bar");
        checkBad("Foo==Bar");
        checkBad("Foo=");
        checkBad("=Bar");
        checkBad("Foo= ");
        checkBad(" =Bar");
        checkBad(">");
        checkBad(">=");
        checkBad("<");
        checkBad("<=");
        checkBad("Foo>");
        checkBad("Foo>=");
        checkBad("Foo<");
        checkBad("Foo<=");
        checkBad("Foo> ");
        checkBad("Foo>= ");
        checkBad("Foo< ");
        checkBad("Foo<= ");
        checkBad("Foo>1");
        checkBad("Foo>=1");
        checkBad("Foo<1");
        checkBad("Foo<=1");
    }

    public static void check(String x, String expectedLhs, AsOfJoinRule expectedRule, String expectedRhs) {
        final AsOfJoinMatch expected = AsOfJoinMatch.of(
                ColumnName.of(expectedLhs),
                expectedRule,
                ColumnName.of(expectedRhs));
        assertThat(AsOfJoinMatch.parse(x)).isEqualTo(expected);
        switch (expectedRule) {
            case GREATER_THAN_EQUAL:
            case GREATER_THAN:
                assertThat(AsOfJoinMatch.parseForAj(x)).isEqualTo(expected);
                break;
            case LESS_THAN_EQUAL:
            case LESS_THAN:
                assertThat(AsOfJoinMatch.parseForRaj(x)).isEqualTo(expected);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    public static void checkAj(String x, String expectedColumn) {
        final AsOfJoinMatch expected = AsOfJoinMatch.of(
                ColumnName.of(expectedColumn),
                AsOfJoinRule.GREATER_THAN_EQUAL,
                ColumnName.of(expectedColumn));
        assertThat(AsOfJoinMatch.parseForAj(x)).isEqualTo(expected);
    }

    public static void checkRaj(String x, String expectedColumn) {
        final AsOfJoinMatch expected = AsOfJoinMatch.of(
                ColumnName.of(expectedColumn),
                AsOfJoinRule.LESS_THAN_EQUAL,
                ColumnName.of(expectedColumn));
        assertThat(AsOfJoinMatch.parseForRaj(x)).isEqualTo(expected);
    }

    public static void checkBad(String x) {
        try {
            AsOfJoinMatch.parse(x);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
