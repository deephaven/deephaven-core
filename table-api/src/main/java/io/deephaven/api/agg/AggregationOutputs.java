package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered output {@link ColumnName column names} for {@link Aggregation aggregations}.
 */
public class AggregationOutputs implements Aggregation.Visitor {

    public static Stream<ColumnName> of(Aggregation aggregation) {
        return aggregation.walk(new AggregationOutputs()).getOut();
    }

    public static Stream<ColumnName> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(AggregationOutputs::of);
    }

    private Stream<ColumnName> out;

    Stream<ColumnName> getOut() {
        return Objects.requireNonNull(out);
    }

    private void visitPair(Pair pair) {
        out = Stream.of(pair.output());
    }

    @Override
    public void visit(AbsSum absSum) {
        visitPair(absSum.pair());
    }

    @Override
    public void visit(Array array) {
        visitPair(array.pair());
    }

    @Override
    public void visit(Avg avg) {
        visitPair(avg.pair());
    }

    @Override
    public void visit(Count count) {
        out = Stream.of(count.column());
    }

    @Override
    public void visit(CountDistinct countDistinct) {
        visitPair(countDistinct.pair());
    }

    @Override
    public void visit(Distinct distinct) {
        visitPair(distinct.pair());
    }

    @Override
    public void visit(First first) {
        visitPair(first.pair());
    }

    @Override
    public void visit(Last last) {
        visitPair(last.pair());
    }

    @Override
    public void visit(Max max) {
        visitPair(max.pair());
    }

    @Override
    public void visit(Med med) {
        visitPair(med.pair());
    }

    @Override
    public void visit(Min min) {
        visitPair(min.pair());
    }

    @Override
    public void visit(Multi<?> multi) {
        out = of(multi.aggregations());
    }

    @Override
    public void visit(Pct pct) {
        visitPair(pct.pair());
    }

    @Override
    public void visit(SortedFirst sortedFirst) {
        visitPair(sortedFirst.pair());
    }

    @Override
    public void visit(SortedLast sortedLast) {
        visitPair(sortedLast.pair());
    }

    @Override
    public void visit(Std std) {
        visitPair(std.pair());
    }

    @Override
    public void visit(Sum sum) {
        visitPair(sum.pair());
    }

    @Override
    public void visit(Unique unique) {
        visitPair(unique.pair());
    }

    @Override
    public void visit(Var var) {
        visitPair(var.pair());
    }

    @Override
    public void visit(WAvg wAvg) {
        visitPair(wAvg.pair());
    }

    @Override
    public void visit(WSum wSum) {
        visitPair(wSum.pair());
    }
}
