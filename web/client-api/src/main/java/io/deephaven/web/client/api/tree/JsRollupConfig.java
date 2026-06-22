//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.tree;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.proto.backplane.grpc.RollupRequest;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Describes a grouping and aggregations for a roll-up table. Pass to the {@code Table.rollup} function to create a
 * roll-up table.
 */
@JsType(name = "RollupConfig", namespace = "dh")
public class JsRollupConfig {

    /**
     * Ordered list of columns to group by to form the hierarchy of the resulting roll-up table.
     */
    public JsArray<String> groupingColumns = null;
    /**
     * Mapping from each aggregation name to the ordered list of columns it should be applied to in the resulting
     * roll-up table.
     */
    public JsPropertyMap<JsArray<String>> aggregations =
            Js.cast(JsObject.create(null));
    /**
     * Optional parameter indicating if an extra leaf node should be added at the bottom of the hierarchy, showing the
     * rows in the underlying table which make up that grouping. Since these values might be a different type from the
     * rest of the column, any client code must check if {@code TreeRow.hasChildren} = {@code false}, and if so,
     * interpret those values as if they were {@code Column.constituentType} instead of {@code Column.type}. Defaults to
     * {@code false}.
     */
    public boolean includeConstituents = false;
    @JsNullable
    public boolean includeOriginalColumns = false;
    /**
     * Optional parameter indicating if original column descriptions should be included. Defaults to {@code true}.
     */
    public boolean includeDescriptions = true;

    @JsConstructor
    public JsRollupConfig() {}

    @JsIgnore
    public JsRollupConfig(JsPropertyMap<Object> source) {
        this();

        if (source.has("aggregations")) {
            aggregations = source.getAsAny("aggregations").cast();
        }
        if (source.has("groupingColumns")) {
            groupingColumns = source.getAsAny("groupingColumns").cast();
        }
        if (source.has("includeConstituents")) {
            includeConstituents = source.getAsAny("includeConstituents").asBoolean();
        }
        if (source.has("includeOriginalColumns")) {
            includeOriginalColumns = source.getAsAny("includeOriginalColumns").asBoolean();
            if (!includeOriginalColumns) {
                JsLog.warn("includeOriginalColumns=false will be ignored");
            }
        }
        if (source.has("includeDescriptions")) {
            includeDescriptions = source.getAsAny("includeDescriptions").asBoolean();
            if (!includeDescriptions) {
                JsLog.warn("includeDescriptions=false will be ignored");
            }
        }
    }

    @JsIgnore
    public RollupRequest buildRequest(JsArray<Column> tableColumns) {
        RollupRequest.Builder request = RollupRequest.newBuilder();

        request.addAllGroupByColumns(groupingColumns.asList());
        request.setIncludeConstituents(includeConstituents);
        List<Aggregation> aggregations = new ArrayList<>();
        Map<String, LinkedHashSet<String>> aggs = new HashMap<>();
        List<String> colsNeedingCompoundNames = new ArrayList<>();
        Set<String> seenColNames = new HashSet<>();
        groupingColumns.forEach((col, p1) -> seenColNames.add(Js.cast(col)));
        this.aggregations.forEach(key -> {
            LinkedHashSet<String> cols = new LinkedHashSet<>();
            this.aggregations.get(key).forEach((col, index) -> {
                String colName = Js.cast(col);
                cols.add(colName);
                if (seenColNames.contains(colName)) {
                    colsNeedingCompoundNames.add(colName);
                } else {
                    seenColNames.add(colName);
                }
                return null;
            });
            aggs.put(key, cols);
        });

        aggs.forEach((aggregationType, cols) -> {
            Aggregation.Builder agg = Aggregation.newBuilder();

            List<String> aggColumns = dedup(cols, colsNeedingCompoundNames, aggregationType);
            Aggregation.AggregationColumns columns = null;

            switch (aggregationType) {
                case JsAggregationOperation.COUNT: {
                    agg.setCount(Aggregation.AggregationCount.newBuilder()
                            .setColumnName(unusedColumnName(tableColumns, "Count", "count", "RollupCount"))
                            .build());
                    break;
                }
                case JsAggregationOperation.COUNT_DISTINCT: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setCountDistinct(AggSpec.AggSpecCountDistinct.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.DISTINCT: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setDistinct(AggSpec.AggSpecDistinct.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder().setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.MIN: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setMin(AggSpec.AggSpecMin.newBuilder());
                    columns = Aggregation.AggregationColumns.newBuilder().setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.MAX: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setMax(AggSpec.AggSpecMax.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.SUM: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setSum(AggSpec.AggSpecSum.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.ABS_SUM: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setAbsSum(AggSpec.AggSpecAbsSum.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.VAR: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setVar(AggSpec.AggSpecVar.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.AVG: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setAvg(AggSpec.AggSpecAvg.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.STD: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setStd(AggSpec.AggSpecStd.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.FIRST: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setFirst(AggSpec.AggSpecFirst.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.LAST: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setLast(AggSpec.AggSpecLast.getDefaultInstance());
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.UNIQUE: {
                    AggSpec.Builder spec = AggSpec.newBuilder();
                    spec.setUnique(AggSpec.AggSpecUnique.newBuilder()
                            .setNonUniqueSentinel(
                                    AggSpec.AggSpecNonUniqueSentinel.newBuilder().setNullValue(NullValue.NULL_VALUE)));
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                // case JsAggregationOperation.SORTED_FIRST: {
                // // TODO #3302 support this
                // }
                // case JsAggregationOperation.SORTED_LAST: {
                // // TODO #3302 support this
                // }
                // case JsAggregationOperation.WSUM: {
                // // TODO #3302 support this
                // }
                case JsAggregationOperation.SKIP: {
                    // cancel entirely, start the loop again
                    return;
                }
                default:
                    JsLog.warn("Aggregation " + aggregationType + " not supported, ignoring");
            }

            if (columns == null || !columns.getMatchPairsList().isEmpty()) {
                aggregations.add(agg.build());
            }
        });

        if (!aggregations.isEmpty()) {
            request.addAllAggregations(aggregations);
        }

        return request.build();
    }

    private List<String> dedup(LinkedHashSet<String> cols, List<String> colsNeedingCompoundNames,
            String aggregationType) {
        return cols.stream().map(col -> {
            if (colsNeedingCompoundNames.contains(col)) {
                return col + "_" + aggregationType + " = " + col;
            }
            return col;
        }).collect(Collectors.toList());
    }

    private String unusedColumnName(JsArray<Column> existingColumns, String... suggestedNames) {
        // Try to use the default column names
        for (String suggestedName : suggestedNames) {
            if (!existingColumns.some((p0, p1) -> p0.getName().equals(suggestedName))) {
                return suggestedName;
            }
        }

        // Next add a suffix and use that if possible
        for (String suggestedName : suggestedNames) {
            if (!existingColumns.some((p0, p1) -> p0.getName().equals(suggestedName + "_"))) {
                return suggestedName + "_";
            }
        }

        // Give up and add a timestamp suffix
        for (String suggestedName : suggestedNames) {
            if (!existingColumns
                    .some((p0, p1) -> p0.getName().equals(suggestedName + "_" + System.currentTimeMillis()))) {
                return suggestedName + "_" + System.currentTimeMillis();
            }
        }

        // Really give up so Java is happy
        throw new IllegalStateException("Failed to generate a name");
    }
}
