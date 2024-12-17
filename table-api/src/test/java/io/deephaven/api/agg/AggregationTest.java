//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.agg.Aggregation.Visitor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationTest {
    private static int aggregationCount() {
        int expected = 0;
        for (Method method : Visitor.class.getMethods()) {
            if ("visit".equals(method.getName()) && method.getParameterCount() == 1
                    && Aggregation.class.isAssignableFrom(method.getParameterTypes()[0])) {
                ++expected;
            }
        }
        return expected;
    }

    @Test
    void visitAll() {
        int expected = aggregationCount();
        final CountingVisitor countingVisitor = new CountingVisitor();
        Aggregation.visitAll(countingVisitor);
        assertThat(countingVisitor.count)
                .withFailMessage(
                        "io.deephaven.api.agg.Aggregation.visitAll is probably missing a case - was a new Aggregation created?")
                .isEqualTo(expected);
    }

    private static class CountingVisitor implements Visitor {
        private int count = 0;

        @Override
        public void visit(Aggregations aggregations) {
            ++count;
        }

        @Override
        public void visit(ColumnAggregation columnAgg) {
            ++count;
        }

        @Override
        public void visit(ColumnAggregations columnAggs) {
            ++count;
        }

        @Override
        public void visit(Count c) {
            ++count;
        }

        @Override
        public void visit(CountWhere countWhere) {
            ++count;
        }

        @Override
        public void visit(FirstRowKey firstRowKey) {
            ++count;
        }

        @Override
        public void visit(LastRowKey lastRowKey) {
            ++count;
        }

        @Override
        public void visit(Partition partition) {
            ++count;
        }

        @Override
        public void visit(Formula formula) {
            ++count;
        }
    }
}
