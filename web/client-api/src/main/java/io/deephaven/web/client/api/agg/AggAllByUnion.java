//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Union type for the aggregations that {@code aggAllBy} accepts, which are the column-based aggregation specs. It does
 * not support {@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey} or {@link LastRowKey}.
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion(anonymous = false, namespace = "dh.agg", name = "AggAllByUnion")
public interface AggAllByUnion {
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

    /**
     * Helper to read the type discriminant from any aggregation variant.
     */
    @JsProperty
    String getType();

    /**
     * Builds an {@link AggSpec.Builder} from this union value based on its type discriminant.
     */
    @JsOverlay
    default AggSpec.Builder makeAggSpec() {
        AggSpec.Builder spec = AggSpec.newBuilder();
        switch (getType()) {
            case "AbsSum":
                spec.setAbsSum(AggSpec.AggSpecAbsSum.getDefaultInstance());
                break;
            case "ApproxPercentile": {
                ApproxPercentile approxPct = asApproxPercentile();
                AggSpec.AggSpecApproximatePercentile.Builder builder =
                        AggSpec.AggSpecApproximatePercentile.newBuilder()
                                .setPercentile(approxPct.percentile);
                if (approxPct.compression != null) {
                    builder.setCompression(approxPct.compression);
                }
                spec.setApproximatePercentile(builder);
                break;
            }
            case "Avg":
                spec.setAvg(AggSpec.AggSpecAvg.getDefaultInstance());
                break;
            case "CountDistinct": {
                CountDistinct countDistinct = asCountDistinct();
                spec.setCountDistinct(AggSpec.AggSpecCountDistinct.newBuilder()
                        .setCountNulls(countDistinct.countNulls != null && countDistinct.countNulls));
                break;
            }
            case "Distinct": {
                Distinct distinct = asDistinct();
                spec.setDistinct(AggSpec.AggSpecDistinct.newBuilder()
                        .setIncludeNulls(distinct.includeNulls != null && distinct.includeNulls));
                break;
            }
            case "First":
                spec.setFirst(AggSpec.AggSpecFirst.getDefaultInstance());
                break;
            case "Freeze":
                spec.setFreeze(AggSpec.AggSpecFreeze.getDefaultInstance());
                break;
            case "Group":
                spec.setGroup(AggSpec.AggSpecGroup.getDefaultInstance());
                break;
            case "Last":
                spec.setLast(AggSpec.AggSpecLast.getDefaultInstance());
                break;
            case "Max":
                spec.setMax(AggSpec.AggSpecMax.getDefaultInstance());
                break;
            case "Median": {
                Median median = asMedian();
                spec.setMedian(AggSpec.AggSpecMedian.newBuilder()
                        .setAverageEvenlyDivided(median.averageEvenlyDivided == null || median.averageEvenlyDivided));
                break;
            }
            case "Min":
                spec.setMin(AggSpec.AggSpecMin.getDefaultInstance());
                break;
            case "Percentile": {
                Percentile pct = asPercentile();
                spec.setPercentile(AggSpec.AggSpecPercentile.newBuilder()
                        .setPercentile(pct.percentile)
                        .setAverageEvenlyDivided(pct.averageEvenlyDivided != null && pct.averageEvenlyDivided));
                break;
            }
            case "SortedFirst": {
                SortedFirst sortedFirst = asSortedFirst();
                AggSpec.AggSpecSorted.Builder sortedBuilder = AggSpec.AggSpecSorted.newBuilder();
                for (Column.ColumnOrName sort : sortedFirst.sortedColumns.asList()) {
                    sortedBuilder.addColumns(AggSpec.AggSpecSortedColumn.newBuilder()
                            .setColumnName(sort.columnName()));
                }
                spec.setSortedFirst(sortedBuilder);
                break;
            }
            case "SortedLast": {
                SortedLast sortedLast = asSortedLast();
                AggSpec.AggSpecSorted.Builder sortedBuilder = AggSpec.AggSpecSorted.newBuilder();
                for (Column.ColumnOrName sort : sortedLast.sortedColumns.asList()) {
                    sortedBuilder.addColumns(AggSpec.AggSpecSortedColumn.newBuilder()
                            .setColumnName(sort.columnName()));
                }
                spec.setSortedLast(sortedBuilder);
                break;
            }
            case "Std":
                spec.setStd(AggSpec.AggSpecStd.getDefaultInstance());
                break;
            case "Sum":
                spec.setSum(AggSpec.AggSpecSum.getDefaultInstance());
                break;
            case "TDigest": {
                TDigest tDigest = asTDigest();
                AggSpec.AggSpecTDigest.Builder tDigestBuilder = AggSpec.AggSpecTDigest.newBuilder();
                if (tDigest.compression != null) {
                    tDigestBuilder.setCompression(tDigest.compression);
                }
                spec.setTDigest(tDigestBuilder);
                break;
            }
            case "Unique": {
                Unique unique = asUnique();
                spec.setUnique(AggSpec.AggSpecUnique.newBuilder()
                        .setIncludeNulls(unique.includeNulls != null && unique.includeNulls)
                        .setNonUniqueSentinel(Unique.toProto(unique.nonUniqueSentinel)));
                break;
            }
            case "Var":
                spec.setVar(AggSpec.AggSpecVar.getDefaultInstance());
                break;
            case "WAvg": {
                WAvg wAvg = asWAvg();
                spec.setWeightedAvg(AggSpec.AggSpecWeighted.newBuilder()
                        .setWeightColumn(wAvg.weightColumn.columnName()));
                break;
            }
            case "WSum": {
                WSum wSum = asWSum();
                spec.setWeightedSum(AggSpec.AggSpecWeighted.newBuilder()
                        .setWeightColumn(wSum.weightColumn.columnName()));
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported aggregation: " + getType());
        }

        return spec;
    }
}
