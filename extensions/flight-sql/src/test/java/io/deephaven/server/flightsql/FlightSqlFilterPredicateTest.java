//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import org.assertj.core.api.PredicateAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlightSqlFilterPredicateTest {

    private static final List<String> EMPTY_STRING = List.of("");

    private static final List<String> ONE_CHARS = List.of(" ", "X", "%", "_", ".", "*", "\uD83D\uDCA9");

    private static final List<String> TWO_CHARS =
            List.of("ab", "Cd", "F ", " f", "_-", "  ", "\uD83D\uDCA9\uD83D\uDCA9");

    private static final List<String> THREE_CHARS =
            List.of("abc", "Cde", "F  ", "  f", " v ", "_-_", "   ", "\uD83D\uDCA9\uD83D\uDCA9\uD83D\uDCA9");

    @Test
    void rejectAll() {
        predicate("")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void acceptAll() {
        for (String flightSqlPattern : new String[] {"%", "%%", "%%%", "%%%%"}) {
            predicate(flightSqlPattern)
                    .acceptsAll(EMPTY_STRING)
                    .acceptsAll(ONE_CHARS)
                    .acceptsAll(TWO_CHARS)
                    .acceptsAll(THREE_CHARS);
        }
    }

    @Test
    void acceptsAnyOneChar() {
        predicate("_")
                .rejectsAll(EMPTY_STRING)
                .acceptsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void acceptsOnePlusChar() {
        for (String flightSqlPattern : new String[] {"_%", "%_"}) {
            predicate(flightSqlPattern)
                    .rejectsAll(EMPTY_STRING)
                    .acceptsAll(ONE_CHARS)
                    .acceptsAll(TWO_CHARS)
                    .acceptsAll(THREE_CHARS);
        }
    }

    @Test
    void acceptsTwoPlusChar() {
        for (String flightSqlPattern : new String[] {"__%", "%__", "_%_"}) {
            predicate(flightSqlPattern)
                    .rejectsAll(EMPTY_STRING)
                    .rejectsAll(ONE_CHARS)
                    .acceptsAll(TWO_CHARS)
                    .acceptsAll(THREE_CHARS);
        }
    }

    @Test
    void acceptLiteralString() {
        predicate("Foo")
                .accepts("Foo")
                .rejects("Bar")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);

    }

    @Test
    void acceptUndescoreAsAnyOne() {
        predicate("foo_ball")
                .accepts("foo_ball", "foosball", "foodball", "foo\uD83D\uDCA9ball")
                .rejects("foo__ball", "Foo_ball", "foo_all", "foo\uD83D\uDCA9\uD83D\uDCA9ball")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void acceptUndescoreAsOnePlus() {
        predicate("foo%ball")
                .accepts("foo_ball", "foosball", "foodball", "foo\uD83D\uDCA9ball", "foo__ball",
                        "foo\uD83D\uDCA9\uD83D\uDCA9ball")
                .rejects("Foo_ball", "foo_all")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Disabled("No way to match literal underscore")
    @Test
    void matchLiteralUnderscore() {

    }

    @Disabled("No way to match literal percentage")
    @Test
    void matchLiteralPercentage() {

    }

    @Test
    void plusIsNotSpecial() {
        predicate("A+")
                .accepts("A+")
                .rejects("A", "AA", "A ")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void starIsNotSpecial() {
        predicate("A*")
                .accepts("A*")
                .rejects("A", "AA", "A ", "AAA")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void dotstarIsNotSpecial() {
        predicate(".*")
                .accepts(".*")
                .rejects("A", "AA", "A ", "AAA")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    @Test
    void predicateContainsUnicode() {
        // A better test would be to include a Unicode character that contains '_' or '%' encoded as part of the low
        // surrogate (if this is possible), that way we could ensure that it is not treated as a special character
        predicate("𞸷")
                .accepts("𞸷")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
        predicate("_𞸷_")
                .accepts("𞸷𞸷𞸷", "x𞸷X", "T𞸷a", " 𞸷 ")
                .rejects("𞸷", "𞸷𞸷")
                .rejectsAll(EMPTY_STRING)
                .rejectsAll(ONE_CHARS)
                .rejectsAll(TWO_CHARS)
                .rejectsAll(THREE_CHARS);
    }

    private static PredicateAssert<String> predicate(String flightSqlPattern) {
        return assertThat(FlightSqlResolver.flightSqlFilterPredicate(flightSqlPattern));
    }
}
