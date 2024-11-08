//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.tree;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsString;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Table_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.RollupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AggSpec;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Aggregation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggregation.AggregationColumns;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggregation.AggregationCount;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecAbsSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecAvg;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecCountDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecFirst;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecLast;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecNonUniqueSentinel;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecUnique;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecVar;
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
import java.util.stream.Collector;

/**
 * Describes a grouping and aggregations for a roll-up table. Pass to the <b>Table.rollup</b> function to create a
 * roll-up table.
 */
@JsType(name = "RollupConfig", namespace = "dh")
public class JsRollupConfig {

    /**
     * Ordered list of columns to group by to form the hierarchy of the resulting roll-up table.
     */
    public JsArray<JsString> groupingColumns = null;
    /**
     * Mapping from each aggregation name to the ordered list of columns it should be applied to in the resulting
     * roll-up table.
     */
    public JsPropertyMap<JsArray<String>> aggregations =
            Js.cast(JsObject.create(null));
    /**
     * Optional parameter indicating if an extra leaf node should be added at the bottom of the hierarchy, showing the
     * rows in the underlying table which make up that grouping. Since these values might be a different type from the
     * rest of the column, any client code must check if TreeRow.hasChildren = false, and if so, interpret those values
     * as if they were Column.constituentType instead of Column.type. Defaults to false.
     */
    public boolean includeConstituents = false;
    @JsNullable
    public boolean includeOriginalColumns = false;
    /**
     * Optional parameter indicating if original column descriptions should be included. Defaults to true.
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
        RollupRequest request = new RollupRequest();

        request.setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupingColumns));
        request.setIncludeConstituents(includeConstituents);
        JsArray<Aggregation> aggregations = new JsArray<>();
        request.setAggregationsList(aggregations);
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
            Aggregation agg = new Aggregation();

            JsArray<String> aggColumns = dedup(cols, colsNeedingCompoundNames, aggregationType);
            AggregationColumns columns = null;

            switch (aggregationType) {
                case JsAggregationOperation.COUNT: {
                    AggregationCount count = new AggregationCount();
                    count.setColumnName(unusedColumnName(tableColumns, "Count", "count", "RollupCount"));
                    agg.setCount(count);
                    break;
                }
                case JsAggregationOperation.COUNT_DISTINCT: {
                    AggSpec spec = new AggSpec();
                    spec.setCountDistinct(new AggSpecCountDistinct());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.DISTINCT: {
                    AggSpec spec = new AggSpec();
                    spec.setDistinct(new AggSpecDistinct());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.MIN: {
                    AggSpec spec = new AggSpec();
                    spec.setMin(new AggSpecMin());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.MAX: {
                    AggSpec spec = new AggSpec();
                    spec.setMax(new AggSpecMax());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.SUM: {
                    AggSpec spec = new AggSpec();
                    spec.setSum(new AggSpecSum());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.ABS_SUM: {
                    AggSpec spec = new AggSpec();
                    spec.setAbsSum(new AggSpecAbsSum());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.VAR: {
                    AggSpec spec = new AggSpec();
                    spec.setVar(new AggSpecVar());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.AVG: {
                    AggSpec spec = new AggSpec();
                    spec.setAvg(new AggSpecAvg());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.STD: {
                    AggSpec spec = new AggSpec();
                    spec.setStd(new AggSpecStd());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.FIRST: {
                    AggSpec spec = new AggSpec();
                    spec.setFirst(new AggSpecFirst());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.LAST: {
                    AggSpec spec = new AggSpec();
                    spec.setLast(new AggSpecLast());
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.UNIQUE: {
                    AggSpec spec = new AggSpec();
                    AggSpecUnique unique = new AggSpecUnique();
                    AggSpecNonUniqueSentinel sentinel = new AggSpecNonUniqueSentinel();
                    sentinel.setNullValue(Table_pb.NullValue.getNULL_VALUE());
                    unique.setNonUniqueSentinel(sentinel);
                    spec.setUnique(unique);
                    columns = new AggregationColumns();
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
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

            if (columns == null || columns.getMatchPairsList().length > 0) {
                aggregations.push(agg);
            }
        });

        if (aggregations.length != 0) {
            request.setAggregationsList(aggregations);
        }

        return request;
    }

    private JsArray<String> dedup(LinkedHashSet<String> cols, List<String> colsNeedingCompoundNames,
            String aggregationType) {
        return cols.stream().map(col -> {
            if (colsNeedingCompoundNames.contains(col)) {
                return col + "_" + aggregationType + " = " + col;
            }
            return col;
        }).collect(Collector.of(
                JsArray<String>::new,
                JsArray::push,
                (arr1, arr2) -> arr1.concat(arr2.asArray(new String[0]))));
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
