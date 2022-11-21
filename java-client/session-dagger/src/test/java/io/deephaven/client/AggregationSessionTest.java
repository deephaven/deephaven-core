package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Aggregation.Visitor;
import io.deephaven.api.agg.Aggregations;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.ColumnAggregations;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.FirstRowKey;
import io.deephaven.api.agg.LastRowKey;
import io.deephaven.api.agg.Partition;
import io.deephaven.qst.table.AggregationTable;
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
public class AggregationSessionTest extends TableSpecTestBase {

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
                examples.out.stream().map(AggregationSessionTest::aggByStatic),
                examples.out.stream().map(AggregationSessionTest::aggByKeyStatic),
                examples.out.stream().map(AggregationSessionTest::aggByKeysStatic),
                examples.out.stream().map(AggregationSessionTest::aggByPreserveEmptyStatic),
                examples.out.stream().map(AggregationSessionTest::aggByKeyPreserveEmptyStatic),
                examples.out.stream().map(AggregationSessionTest::aggByKeysPreserveEmptyStatic),
                examples.out.stream().map(AggregationSessionTest::aggByTicking),
                examples.out.stream().map(AggregationSessionTest::aggByKeyTicking),
                examples.out.stream().map(AggregationSessionTest::aggByKeysTicking),
                examples.out.stream().map(AggregationSessionTest::aggByPreserveEmptyTicking),
                examples.out.stream().map(AggregationSessionTest::aggByKeyPreserveEmptyTicking),
                examples.out.stream().map(AggregationSessionTest::aggByKeysPreserveEmptyTicking))
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

    public AggregationSessionTest(TableSpec table) {
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
    }
}
