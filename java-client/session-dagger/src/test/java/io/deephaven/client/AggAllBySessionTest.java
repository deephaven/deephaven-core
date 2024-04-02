//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecAbsSum;
import io.deephaven.api.agg.spec.AggSpecApproximatePercentile;
import io.deephaven.api.agg.spec.AggSpecAvg;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import io.deephaven.api.agg.spec.AggSpecDistinct;
import io.deephaven.api.agg.spec.AggSpecFirst;
import io.deephaven.api.agg.spec.AggSpecFormula;
import io.deephaven.api.agg.spec.AggSpecFreeze;
import io.deephaven.api.agg.spec.AggSpecGroup;
import io.deephaven.api.agg.spec.AggSpecLast;
import io.deephaven.api.agg.spec.AggSpecMax;
import io.deephaven.api.agg.spec.AggSpecMedian;
import io.deephaven.api.agg.spec.AggSpecMin;
import io.deephaven.api.agg.spec.AggSpecPercentile;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecStd;
import io.deephaven.api.agg.spec.AggSpecSum;
import io.deephaven.api.agg.spec.AggSpecTDigest;
import io.deephaven.api.agg.spec.AggSpecUnique;
import io.deephaven.api.agg.spec.AggSpecVar;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.api.object.UnionObject;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class AggAllBySessionTest extends TableSpecTestBase {
    private static final TableSpec STATIC_BASE = TableSpec.empty(1000).view(
            "Key=ii%3",
            "Key2=ii%17",
            "B=(byte)ii",
            "S=(short)ii",
            "I=(int)ii",
            "L=(long)ii",
            "F=(float)ii",
            "D=(double)ii");

    private static final TableSpec TICKING_BASE = TimeTable.of(Duration.ofMillis(10)).view(
            "Key=ii%3",
            "Key2=ii%17",
            "B=(byte)ii",
            "S=(short)ii",
            "I=(int)ii",
            "L=(long)ii",
            "F=(float)ii",
            "D=(double)ii");

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        // This is a pattern to encourage new tests any time a new AggSpec is defined.
        // The alternative is a less-extensible list that needs to be manually updated:
        // List.of(AggSpec.sum(), AggSpec.avg(), ...)
        final CreateAggSpecs examples = new CreateAggSpecs();
        AggSpec.visitAll(examples);
        return () -> Stream.of(
                examples.out.stream().map(AggAllBySessionTest::aggAllByStatic),
                examples.out.stream().map(AggAllBySessionTest::aggAllByKeyStatic),
                examples.out.stream().map(AggAllBySessionTest::aggAllByKeysStatic),
                examples.out.stream().map(AggAllBySessionTest::aggAllByTicking),
                examples.out.stream().map(AggAllBySessionTest::aggAllByKeyTicking),
                examples.out.stream().map(AggAllBySessionTest::aggAllByKeysTicking))
                .flatMap(Function.identity())
                .map(e -> new Object[] {e})
                .iterator();
    }

    public AggAllBySessionTest(TableSpec table) {
        super(table);
    }

    private static TableSpec aggAllByStatic(AggSpec a) {
        return STATIC_BASE.aggAllBy(a);
    }

    private static TableSpec aggAllByKeyStatic(AggSpec a) {
        return STATIC_BASE.aggAllBy(a, "Key");
    }

    private static TableSpec aggAllByKeysStatic(AggSpec a) {
        return STATIC_BASE.aggAllBy(a, "Key", "Key2");
    }

    private static TableSpec aggAllByTicking(AggSpec a) {
        return TICKING_BASE.aggAllBy(a);
    }

    private static TableSpec aggAllByKeyTicking(AggSpec a) {
        return TICKING_BASE.aggAllBy(a, "Key");
    }

    private static TableSpec aggAllByKeysTicking(AggSpec a) {
        return TICKING_BASE.aggAllBy(a, "Key", "Key2");
    }

    private static class CreateAggSpecs implements AggSpec.Visitor {

        private final List<AggSpec> out = new ArrayList<>();

        @Override
        public void visit(AggSpecAbsSum absSum) {
            out.add(AggSpecAbsSum.of());
        }

        @Override
        public void visit(AggSpecApproximatePercentile approxPct) {
            out.add(AggSpecApproximatePercentile.of(0.25));
            out.add(AggSpecApproximatePercentile.of(0.25, 50));
        }

        @Override
        public void visit(AggSpecAvg avg) {
            out.add(AggSpecAvg.of());
        }

        @Override
        public void visit(AggSpecCountDistinct countDistinct) {
            out.add(AggSpecCountDistinct.of(false));
            out.add(AggSpecCountDistinct.of(true));
        }

        @Override
        public void visit(AggSpecDistinct distinct) {
            out.add(AggSpecDistinct.of(false));
            out.add(AggSpecDistinct.of(true));
        }

        @Override
        public void visit(AggSpecFirst first) {
            out.add(AggSpecFirst.of());
        }

        @Override
        public void visit(AggSpecFormula formula) {
            out.add(AggSpecFormula.of("each"));
        }

        @Override
        public void visit(AggSpecFreeze freeze) {
            // freeze needs a different construction for testing
            // java.lang.IllegalStateException: FreezeBy only allows one row per state!
            // out.add(AggSpecFreeze.of());
        }

        @Override
        public void visit(AggSpecGroup group) {
            out.add(AggSpecGroup.of());
        }

        @Override
        public void visit(AggSpecLast last) {
            out.add(AggSpecLast.of());
        }

        @Override
        public void visit(AggSpecMax max) {
            out.add(AggSpecMax.of());
        }

        @Override
        public void visit(AggSpecMedian median) {
            out.add(AggSpecMedian.of(false));
            out.add(AggSpecMedian.of(true));
        }

        @Override
        public void visit(AggSpecMin min) {
            out.add(AggSpecMin.of());
        }

        @Override
        public void visit(AggSpecPercentile pct) {
            out.add(AggSpecPercentile.of(0.25, false));
            out.add(AggSpecPercentile.of(0.25, true));
        }

        @Override
        public void visit(AggSpecSortedFirst sortedFirst) {
            out.add(AggSpecSortedFirst.builder().addColumns(SortColumn.asc(ColumnName.of("B"))).build());
        }

        @Override
        public void visit(AggSpecSortedLast sortedLast) {
            out.add(AggSpecSortedLast.builder().addColumns(SortColumn.asc(ColumnName.of("B"))).build());
        }

        @Override
        public void visit(AggSpecStd std) {
            out.add(AggSpecStd.of());
        }

        @Override
        public void visit(AggSpecSum sum) {
            out.add(AggSpecSum.of());
        }

        @Override
        public void visit(AggSpecTDigest tDigest) {
            out.add(AggSpec.tDigest());
            out.add(AggSpec.tDigest(50));
        }

        @Override
        public void visit(AggSpecUnique unique) {
            out.add(AggSpecUnique.of(false, null));
            out.add(AggSpecUnique.of(true, null));

            // all columns are numeric, can be casted
            out.add(AggSpecUnique.of(false, UnionObject.of((byte) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((byte) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((short) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((short) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of(-1)));
            out.add(AggSpecUnique.of(true, UnionObject.of(-1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((long) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((long) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((float) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((float) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((double) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((double) -1)));
        }

        @Override
        public void visit(AggSpecWAvg wAvg) {
            out.add(AggSpecWAvg.of(ColumnName.of("B")));
        }

        @Override
        public void visit(AggSpecWSum wSum) {
            out.add(AggSpecWSum.of(ColumnName.of("B")));
        }

        @Override
        public void visit(AggSpecVar var) {
            out.add(AggSpecVar.of());
        }
    }
}
