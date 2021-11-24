package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.Reference;
import org.junit.Test;

import static io.deephaven.server.table.ops.filter.FilterTestUtils.*;

public class MakeExpressionsNullSafeTest extends AbstractNormalizingFilterTest {
    @Test
    public void testMakeExpressionsNullSafe() {
        assertUnchanged("doesnt affect EQ",
                NormalizeFilterUtil.doComparison(CompareCondition.CompareOperation.EQUALS, CaseSensitivity.MATCH_CASE,
                        reference("ColumnA"), literal("A")));

        assertFilterEquals("add null checks when implicit invoke is needed",
                NormalizeFilterUtil.doComparison(CompareCondition.CompareOperation.EQUALS, CaseSensitivity.IGNORE_CASE,
                        reference("ColumnA"), reference("ColumnB")),
                or(
                        and(
                                NormalizeFilterUtil.doIsNull(Reference.newBuilder().setColumnName("ColumnA").build()),
                                NormalizeFilterUtil.doIsNull(Reference.newBuilder().setColumnName("ColumnB").build())),
                        and(
                                not(
                                        NormalizeFilterUtil
                                                .doIsNull(Reference.newBuilder().setColumnName("ColumnA").build())),
                                invoke("equalsIgnoreCase", reference("ColumnA"), reference("ColumnB")))));
    }

    @Override
    protected Condition execute(Condition f) {
        return MakeExpressionsNullSafe.exec(f);
    }
}
