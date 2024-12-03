//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Aggregation.Visitor;
import io.deephaven.api.agg.Aggregations;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.ColumnAggregations;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.FirstRowKey;
import io.deephaven.api.agg.Formula;
import io.deephaven.api.agg.LastRowKey;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.object.UnionObject;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class AggBySessionTest extends TableSpecTestBase {

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
        // This is a pattern to encourage new tests any time a new Aggregation is defined.
        // The alternative is a less-extensible list that needs to be manually updated:
        // List.of(...)
        final CreateAggregations examples = new CreateAggregations();
        Aggregation.visitAll(examples);
        return () -> Stream.of(
                examples.out.stream().map(AggBySessionTest::aggByStatic),
                examples.out.stream().map(AggBySessionTest::aggByKeyStatic),
                examples.out.stream().map(AggBySessionTest::aggByKeysStatic),
                examples.out.stream().map(AggBySessionTest::aggByPreserveEmptyStatic),
                examples.out.stream().map(AggBySessionTest::aggByKeyPreserveEmptyStatic),
                examples.out.stream().map(AggBySessionTest::aggByKeysPreserveEmptyStatic),
                examples.out.stream().map(AggBySessionTest::aggByTicking),
                examples.out.stream().map(AggBySessionTest::aggByKeyTicking),
                examples.out.stream().map(AggBySessionTest::aggByKeysTicking),
                examples.out.stream().map(AggBySessionTest::aggByPreserveEmptyTicking),
                examples.out.stream().map(AggBySessionTest::aggByKeyPreserveEmptyTicking),
                examples.out.stream().map(AggBySessionTest::aggByKeysPreserveEmptyTicking))
                .flatMap(Function.identity())
                .map(e -> new Object[] {e})
                .iterator();
    }

    private static TableSpec aggByStatic(Aggregation a) {
        return STATIC_BASE.aggBy(a);
    }

    private static TableSpec aggByKeyStatic(Aggregation a) {
        return STATIC_BASE.aggBy(a, "Key");
    }

    private static TableSpec aggByKeysStatic(Aggregation a) {
        return STATIC_BASE.aggBy(a, "Key", "Key2");
    }

    private static TableSpec aggByPreserveEmptyStatic(Aggregation a) {
        return STATIC_BASE.aggBy(Collections.singleton(a), true);
    }

    private static TableSpec aggByKeyPreserveEmptyStatic(Aggregation a) {
        return STATIC_BASE.aggBy(Collections.singleton(a), true, null, Collections.singleton(ColumnName.of("Key")));
    }

    private static TableSpec aggByKeysPreserveEmptyStatic(Aggregation a) {
        return STATIC_BASE.aggBy(Collections.singleton(a), true, null,
                Arrays.asList(ColumnName.of("Key"), ColumnName.of("Key2")));
    }

    private static TableSpec aggByTicking(Aggregation a) {
        return TICKING_BASE.aggBy(a);
    }

    private static TableSpec aggByKeyTicking(Aggregation a) {
        return TICKING_BASE.aggBy(a, "Key");
    }

    private static TableSpec aggByKeysTicking(Aggregation a) {
        return TICKING_BASE.aggBy(a, "Key", "Key2");
    }

    private static TableSpec aggByPreserveEmptyTicking(Aggregation a) {
        return TICKING_BASE.aggBy(Collections.singleton(a), true);
    }

    private static TableSpec aggByKeyPreserveEmptyTicking(Aggregation a) {
        return TICKING_BASE.aggBy(Collections.singleton(a), true, null, Collections.singleton(ColumnName.of("Key")));
    }

    private static TableSpec aggByKeysPreserveEmptyTicking(Aggregation a) {
        return TICKING_BASE.aggBy(Collections.singleton(a), true, null,
                Arrays.asList(ColumnName.of("Key"), ColumnName.of("Key2")));
    }

    // TODO: add initial group testings

    public AggBySessionTest(TableSpec table) {
        super(table);
    }

    private static class CreateAggregations implements Visitor {
        private final List<Aggregation> out = new ArrayList<>();

        @Override
        public void visit(Aggregations aggregations) {
            out.add(Aggregations.builder()
                    .addAggregations(Count.of("MyCount"))
                    .addAggregations(Aggregation.AggSum("B", "S", "I", "L", "F", "D"))
                    .build());
            out.add(Aggregations.builder()
                    .addAggregations(
                            Aggregation.AggUnique(true, UnionObject.of((byte) -1), "B"),
                            Aggregation.AggUnique(true, UnionObject.of((short) -1), "S"),
                            Aggregation.AggUnique(true, UnionObject.of((int) -1), "I"),
                            Aggregation.AggUnique(true, UnionObject.of((long) -1), "L"),
                            Aggregation.AggUnique(true, UnionObject.of((float) -1), "F"),
                            Aggregation.AggUnique(true, UnionObject.of((double) -1), "D"))
                    .build());
        }

        @Override
        public void visit(ColumnAggregation columnAgg) {
            out.add(Aggregation.AggSum("B"));
        }

        @Override
        public void visit(ColumnAggregations columnAggs) {
            out.add(Aggregation.AggSum("B", "S", "I", "L", "F", "D"));
        }

        @Override
        public void visit(Count count) {
            out.add(Count.of("MyCount"));
        }

        @Override
        public void visit(FirstRowKey firstRowKey) {
            out.add(FirstRowKey.of("First"));
        }

        @Override
        public void visit(LastRowKey lastRowKey) {
            out.add(LastRowKey.of("Last"));
        }

        @Override
        public void visit(Partition partition) {
            out.add(Partition.of("Output", false));
            out.add(Partition.of("Output", true));
        }

        @Override
        public void visit(Formula formula) {
            out.add(Formula.of("Formula", "3.0 + 1"));
            out.add(Formula.of("Formula", "sum(S)"));
            out.add(Formula.of("Formula", "sum(S + I)"));
            out.add(Formula.of("Formula", "sum(S + I + D)"));
            out.add(Formula.of("Formula", "sum(S + I + D + L)"));
        }
    }
}
