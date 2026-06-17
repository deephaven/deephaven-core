//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.NanComparison;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.server.table.ops.filter.FilterTestUtils.*;

public class FlipNonReferenceMatchExpressionTest extends AbstractNormalizingFilterTest {

    @Test
    public void testFlipNonReferenceMatchExpression() {
        assertUnchanged("correct form",
                in("ColumnA", 1, 2));
        assertUnchanged("correct form",
                in("ColumnA", 1));

        assertFilterEquals("all literals",
                NormalizeFilterUtil.doIn(literal(1), Arrays.asList(literal(2), literal(3)), CaseSensitivity.MATCH_CASE,
                        MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN),
                or(
                        NormalizeFilterUtil.doIn(literal(2), Collections.singletonList(literal(1)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN),
                        NormalizeFilterUtil.doIn(literal(3), Collections.singletonList(literal(1)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN)));

        assertFilterEquals("all literals, NaN will match",
                NormalizeFilterUtil.doIn(literal(1.0), Arrays.asList(literal(2.0), literal(Double.NaN)),
                        CaseSensitivity.MATCH_CASE,
                        MatchType.REGULAR, NanComparison.NAN_EQUALS_NAN),
                or(
                        NormalizeFilterUtil.doIn(literal(2.0), Collections.singletonList(literal(1.0)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_EQUALS_NAN),
                        NormalizeFilterUtil.doIn(literal(Double.NaN), Collections.singletonList(literal(1.0)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_EQUALS_NAN)));

        assertFilterEquals("reference on right",
                NormalizeFilterUtil.doIn(literal(1), Arrays.asList(reference("ColumnA"), literal(4), literal(5)),
                        CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN),
                or(
                        in("ColumnA", 1),
                        NormalizeFilterUtil.doIn(literal(4), Collections.singletonList(literal(1)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN),
                        NormalizeFilterUtil.doIn(literal(5), Collections.singletonList(literal(1)),
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN)));

        assertFilterEquals("reference on right, no OR required",
                NormalizeFilterUtil.doIn(literal(1), Collections.singletonList(reference("ColumnA")),
                        CaseSensitivity.MATCH_CASE, MatchType.REGULAR, NanComparison.NAN_NOT_EQUALS_NAN),
                in("ColumnA", 1));
    }

    @Override
    protected Condition execute(Condition f) {
        return FlipNonReferenceMatchExpression.exec(f);
    }
}
