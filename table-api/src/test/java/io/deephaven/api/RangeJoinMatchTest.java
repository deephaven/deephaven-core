//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

public class RangeJoinMatchTest {

    @Test
    public void testAllValidCombinationsAndParseSymmetry() {
        for (final String leftStartColumnName : new String[] {"LeftStartA", "LeftStartB"}) {
            for (final RangeStartRule rangeStartRule : RangeStartRule.values()) {
                for (final String rightRangeColumnName : new String[] {"LeftStartA", "LeftEndA", "RightRangeC"}) {
                    for (final RangeEndRule rangeEndRule : RangeEndRule.values()) {
                        for (final String leftEndColumnName : new String[] {"LeftEndA", "LeftEndB"}) {
                            final RangeJoinMatch fromBuilder = RangeJoinMatch.builder()
                                    .leftStartColumn(ColumnName.of(leftStartColumnName))
                                    .rangeStartRule(rangeStartRule)
                                    .rightRangeColumn(ColumnName.of(rightRangeColumnName))
                                    .rangeEndRule(rangeEndRule)
                                    .leftEndColumn(ColumnName.of(leftEndColumnName))
                                    .build();
                            final RangeJoinMatch fromOf = RangeJoinMatch.of(
                                    ColumnName.of(leftStartColumnName),
                                    rangeStartRule,
                                    ColumnName.of(rightRangeColumnName),
                                    rangeEndRule,
                                    ColumnName.of(leftEndColumnName));
                            assertThat(fromBuilder).isEqualTo(fromOf);

                            final String fromBuilderAsString = Strings.of(fromBuilder);
                            final String fromOfAsString = Strings.of(fromOf);
                            assertThat(fromBuilderAsString).isEqualTo(fromOfAsString);

                            final RangeJoinMatch fromBuilderParsed = RangeJoinMatch.parse(fromBuilderAsString);
                            final RangeJoinMatch fromOfParsed = RangeJoinMatch.parse(fromOfAsString);

                            assertThat(fromBuilderParsed).isEqualTo(fromBuilder);
                            assertThat(fromOfParsed).isEqualTo(fromOf);
                            assertThat(fromBuilderParsed).isEqualTo(fromOfParsed);
                        }
                    }
                }
            }
        }
    }

    @Test
    void testInvalidParseInputs() {
        for (final String input : new String[] {
                "this is not a range match at all",
                "<- ABC < DEF <= GHI", // Left arrow with less than
                "ABC <= DEF < GHI ->", // Right arrow with greater than
                "ABC = DEF", // Only two columns
                "ABC = DEF < GHI", // Equality for start rule
                "ABC == DEF < GHI", // Equality for start rule
                "ABC < DEF == GHI", // Equality for end rule
                "ABC < DEF = GHI" // Equality for end rule
        }) {
            expectIllegalArgumentException(() -> RangeJoinMatch.parse(input));
        }
    }

    @Test
    void testLeftColumnsDifferentCheck() {
        final RangeJoinMatch.Builder builder = RangeJoinMatch.builder();
        builder.leftStartColumn(ColumnName.of("L"))
                .rangeStartRule(RangeStartRule.LESS_THAN)
                .rightRangeColumn(ColumnName.of("R"))
                .rangeEndRule(RangeEndRule.GREATER_THAN)
                .leftEndColumn(ColumnName.of("L"));
        expectIllegalArgumentException(builder::build);
    }

    private static void expectIllegalArgumentException(final Runnable test) {
        try {
            test.run();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException expected) {
        }
    }
}
