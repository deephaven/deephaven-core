//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ReadonlyArray;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.JsTableOperations;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Union type representing all aggregation variants accepted by
 * {@link io.deephaven.web.client.api.JsTableOperations#aggBy(AggByOptions)},
 * {@link io.deephaven.web.client.api.JsTableOperations#rangeJoin(JsTableOperations, ReadonlyArray, JsTableOperations.RangeJoinMatch, ReadonlyArray)},
 * etc. This includes both column-based aggregations (subtypes of {@link ColumnAggregation}) that operate on
 * input/output column pairs, and non-column aggregations ({@link Count}, {@link CountWhere}, {@link Partition},
 * {@link FirstRowKey}, {@link LastRowKey}) that produce a single named output column.
 *
 * <p>
 * Each variant carries a {@code type} field (accessible via {@link #getType()}) that acts as a discriminant for
 * TypeScript narrowing.
 *
 * @see AggAllByUnion for the subset accepted by {@code aggAllBy}
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion(anonymous = false)
@TsName(name = "AggregationUnion", namespace = "dh.agg")
public interface AggregationUnion {
    @JsOverlay
    @TsUnionMember
    default AbsSum asAbsSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default ApproxPercentile asApproxPercentile() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Avg asAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default CountDistinct asCountDistinct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Distinct asDistinct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default First asFirst() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Formula asFormula() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Freeze asFreeze() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Group asGroup() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Last asLast() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Max asMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Median asMedian() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Min asMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Percentile asPercentile() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default SortedFirst asSortedFirst() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default SortedLast asSortedLast() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Std asStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Sum asSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default TDigest asTDigest() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Unique asUnique() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Var asVar() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default WAvg asWAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default WSum asWSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Count asCount() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default CountWhere asCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Partition asPartition() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default FirstRowKey asFirstRowKey() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default LastRowKey asLastRowKey() {
        return Js.uncheckedCast(this);
    }

    /**
     * Helper to read the type discriminant from any aggregation variant. Not visible from JS/TS as part of this union,
     * but allows access to the {@code type} field that each aggregation type has.
     */
    @JsProperty
    String getType();

    /**
     * Builds a proto {@link Aggregation} message from this union variant. Non-spec types (Count, CountWhere, Partition,
     * FirstRowKey, LastRowKey) use their own proto oneof arms; spec-based types delegate to
     * {@link AggAllByUnion#makeAggSpec()} and wrap the result in {@code AggregationColumns}.
     */
    @JsOverlay
    default Aggregation.Builder makeAggregation() {
        Aggregation.Builder result = Aggregation.newBuilder();

        switch (getType()) {
            case "Count": {
                Count count = asCount();
                result.setCount(Aggregation.AggregationCount.newBuilder()
                        .setColumnName(count.col));
                return result;
            }
            case "CountWhere": {
                CountWhere countWhere = asCountWhere();
                result.setCountWhere(Aggregation.AggregationCountWhere.newBuilder()
                        .setColumnName(countWhere.col)
                        .addAllFilters(countWhere.filters.asList()));
                return result;
            }
            case "Partition": {
                Partition partition = asPartition();
                result.setPartition(Aggregation.AggregationPartition.newBuilder()
                        .setColumnName(partition.col)
                        .setIncludeGroupByColumns(
                                partition.includeGroupByColumns == null || partition.includeGroupByColumns));
                return result;
            }
            case "FirstRowKey": {
                FirstRowKey firstRowKey = asFirstRowKey();
                result.setFirstRowKey(Aggregation.AggregationRowKey.newBuilder()
                        .setColumnName(firstRowKey.col));
                return result;
            }
            case "LastRowKey": {
                LastRowKey lastRowKey = asLastRowKey();
                result.setLastRowKey(Aggregation.AggregationRowKey.newBuilder()
                        .setColumnName(lastRowKey.col));
                return result;
            }
            default:
                break;
        }

        // Spec-based aggregation types
        AggSpec.Builder spec = ((AggAllByUnion) this).makeAggSpec();

        Aggregation.AggregationColumns.Builder colsBuilder =
                Aggregation.AggregationColumns.newBuilder()
                        .setSpec(spec);
        ColumnAggregation colAgg = Js.cast(this);
        if (colAgg.columns != null) {
            colsBuilder.addAllMatchPairs(JsTable.MatchPairUnion.toStringArray(colAgg.columns));
        }

        return result.setColumns(colsBuilder);
    }
}
