package io.deephaven.api.agg.spec;

import io.deephaven.api.agg.spec.AggSpec.Visitor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

public class AggSpecTest {
    private static int aggSpecCount() {
        int expected = 0;
        for (Method method : Visitor.class.getMethods()) {
            if ("visit".equals(method.getName()) && method.getParameterCount() == 1
                    && AggSpec.class.isAssignableFrom(method.getParameterTypes()[0])) {
                ++expected;
            }
        }
        return expected;
    }

    @Test
    void visitAll() {
        int expected = aggSpecCount();
        final CountingVisitor countingVisitor = new CountingVisitor();
        AggSpec.visitAll(countingVisitor);
        assertThat(countingVisitor.count)
                .withFailMessage(
                        "io.deephaven.api.agg.spec.AggSpec.visitAll is probably missing a case - was a new AggSpec created?")
                .isEqualTo(expected);
    }

    private static class CountingVisitor implements AggSpec.Visitor {
        private int count = 0;

        @Override
        public void visit(AggSpecAbsSum absSum) {
            ++count;
        }

        @Override
        public void visit(AggSpecApproximatePercentile approxPct) {
            ++count;
        }

        @Override
        public void visit(AggSpecAvg avg) {
            ++count;
        }

        @Override
        public void visit(AggSpecCountDistinct countDistinct) {
            ++count;
        }

        @Override
        public void visit(AggSpecDistinct distinct) {
            ++count;
        }

        @Override
        public void visit(AggSpecFirst first) {
            ++count;
        }

        @Override
        public void visit(AggSpecFormula formula) {
            ++count;
        }

        @Override
        public void visit(AggSpecFreeze freeze) {
            ++count;
        }

        @Override
        public void visit(AggSpecGroup group) {
            ++count;
        }

        @Override
        public void visit(AggSpecLast last) {
            ++count;
        }

        @Override
        public void visit(AggSpecMax max) {
            ++count;
        }

        @Override
        public void visit(AggSpecMedian median) {
            ++count;
        }

        @Override
        public void visit(AggSpecMin min) {
            ++count;
        }

        @Override
        public void visit(AggSpecPercentile pct) {
            ++count;
        }

        @Override
        public void visit(AggSpecSortedFirst sortedFirst) {
            ++count;
        }

        @Override
        public void visit(AggSpecSortedLast sortedLast) {
            ++count;
        }

        @Override
        public void visit(AggSpecStd std) {
            ++count;
        }

        @Override
        public void visit(AggSpecSum sum) {
            ++count;
        }

        @Override
        public void visit(AggSpecTDigest tDigest) {
            ++count;
        }

        @Override
        public void visit(AggSpecUnique unique) {
            ++count;
        }

        @Override
        public void visit(AggSpecWAvg wAvg) {
            ++count;
        }

        @Override
        public void visit(AggSpecWSum wSum) {
            ++count;
        }

        @Override
        public void visit(AggSpecVar var) {
            ++count;
        }
    }
}
