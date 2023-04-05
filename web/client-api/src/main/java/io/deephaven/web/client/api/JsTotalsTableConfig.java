/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.Global;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsString;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AggSpec;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AggregateRequest;
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
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecUnique;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecVar;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;

@JsType(name = "TotalsTableConfig", namespace = "dh")
public class JsTotalsTableConfig {
    @Deprecated // Use JsAggregationOperation instead
    public static final String COUNT = "Count",
            MIN = "Min",
            MAX = "Max",
            SUM = "Sum",
            ABS_SUM = "AbsSum",
            VAR = "Var",
            AVG = "Avg",
            STD = "Std",
            FIRST = "First",
            LAST = "Last",
            // ARRAY = "Array",
            SKIP = "Skip";
    private static final List<String> knownAggTypes = Arrays.asList(
            JsAggregationOperation.COUNT,
            JsAggregationOperation.MIN,
            JsAggregationOperation.MAX,
            JsAggregationOperation.SUM,
            JsAggregationOperation.ABS_SUM,
            JsAggregationOperation.VAR,
            JsAggregationOperation.AVG,
            JsAggregationOperation.STD,
            JsAggregationOperation.FIRST,
            JsAggregationOperation.LAST,
            JsAggregationOperation.COUNT_DISTINCT,
            JsAggregationOperation.DISTINCT,
            JsAggregationOperation.UNIQUE);

    public boolean showTotalsByDefault = false;
    public boolean showGrandTotalsByDefault = false;
    @TsTypeRef(JsAggregationOperation.class)
    public String defaultOperation = JsAggregationOperation.SUM;
    public JsPropertyMap<JsArray<@TsTypeRef(JsAggregationOperation.class) String>> operationMap =
            Js.cast(JsObject.create(null));

    public JsArray<String> groupBy = new JsArray<>();

    public JsTotalsTableConfig() {}

    @JsIgnore
    public JsTotalsTableConfig(JsPropertyMap<Object> source) {
        this();
        if (source.has("showTotalsByTable")) {
            showTotalsByDefault = Js.isTruthy(source.getAsAny("showTotalsByDefault"));
        }
        if (source.has("showGrandTotalsByDefault")) {
            showGrandTotalsByDefault = Js.isTruthy(source.getAsAny("showGrandTotalsByDefault"));
        }
        if (source.has("defaultOperation")) {
            defaultOperation = source.getAsAny("defaultOperation").asString();
            checkOperation(defaultOperation);
        }
        if (source.has("operationMap")) {
            operationMap = source.getAsAny("operationMap").cast();
            operationMap.forEach(key -> {
                operationMap.get(key).forEach((value, index, array) -> {
                    checkOperation(Js.cast(value));
                    return null;
                });
            });
        }
        if (source.has("groupBy")) {
            groupBy = source.getAsAny("groupBy").cast();
        }
    }

    /**
     * Implementation from TotalsTableBuilder.fromDirective, plus changes required to make this able to act on plan JS
     * objects/arrays.
     *
     * Note that this omits groupBy for now, until the server directive format supports it!
     */
    @JsIgnore
    public static JsTotalsTableConfig parse(String configString) {
        JsTotalsTableConfig builder = new JsTotalsTableConfig();
        if (configString == null || configString.isEmpty()) {
            return builder;
        }

        final String[] splitSemi = configString.split(";");
        final String[] frontMatter = splitSemi[0].split(",");

        if (frontMatter.length < 3) {
            throw new IllegalArgumentException("Invalid Totals Table: " + configString);
        }
        builder.showTotalsByDefault = Boolean.parseBoolean(frontMatter[0]);
        builder.showGrandTotalsByDefault = Boolean.parseBoolean(frontMatter[1]);
        builder.defaultOperation = frontMatter[2];
        checkOperation(builder.defaultOperation);


        if (splitSemi.length > 1) {
            final String[] columnDirectives = splitSemi[1].split(",");
            for (final String columnDirective : columnDirectives) {
                if (columnDirective.trim().isEmpty())
                    continue;
                final String[] kv = columnDirective.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(
                            "Invalid Totals Table: " + configString + ", bad column " + columnDirective);
                }
                final String[] operations = kv[1].split(":");
                builder.operationMap.set(kv[0], new JsArray<>());
                for (String op : operations) {
                    checkOperation(op);
                    builder.operationMap.get(kv[0]).push(op);
                }
            }
        }

        return builder;
    }

    private static void checkOperation(String op) {
        if (!knownAggTypes.contains(op)) {
            throw new IllegalArgumentException("Operation " + op + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "JsTotalsTableConfig{" +
                "showTotalsByDefault=" + showTotalsByDefault +
                ", showGrandTotalsByDefault=" + showGrandTotalsByDefault +
                ", defaultOperation='" + defaultOperation + '\'' +
                ", operationMap=" + Global.JSON.stringify(operationMap) + // Object.create(null) has no valueOf
                ", groupBy=" + groupBy +
                '}';
    }

    /**
     * Implementation from TotalsTableBuilder.buildDirective(), plus a minor change to iterate JS arrays/objects
     * correctly.
     *
     * Note that this omits groupBy until the server directive format supports it!
     */
    @JsIgnore
    public String serialize() {
        final StringBuilder builder = new StringBuilder();
        builder.append(Boolean.toString(showTotalsByDefault)).append(",")
                .append(Boolean.toString(showGrandTotalsByDefault)).append(",").append(defaultOperation).append(";");
        operationMap
                .forEach(key -> builder.append(key).append("=").append(operationMap.get(key).join(":")).append(","));
        return builder.toString();
    }

    /**
     * Expose groupBy as a plain Java array, so it can be serialized correctly to the server.
     */
    @JsIgnore
    public String[] groupByArray() {
        String[] strings = new String[groupBy.length];
        groupBy.forEach((str, index, array) -> strings[index] = Js.cast(str));
        return strings;
    }


    @JsIgnore
    public AggregateRequest buildRequest() {
        AggregateRequest request = new AggregateRequest();

        request.setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupBy));
        JsArray<Aggregation> aggregations = new JsArray<>();
        request.setAggregationsList(aggregations);
        Map<String, LinkedHashSet<String>> aggs = new HashMap<>();
        List<String> colsNeedingCompoundNames = new ArrayList<>();
        Set<String> seenColNames = new HashSet<>();
        groupBy.forEach((col, p1, p2) -> seenColNames.add(Js.cast(col)));
        this.operationMap.forEach(col -> {
            this.operationMap.get(col).forEach((agg, index, arr) -> {
                String colName = Js.cast(col);
                aggs.computeIfAbsent(Js.cast(agg), ignore -> new LinkedHashSet<>()).add(colName);
                if (seenColNames.contains(colName)) {
                    colsNeedingCompoundNames.add(colName);
                } else {
                    seenColNames.add(colName);
                }
                return null;
            });
        });

        aggs.forEach((aggregationType, cols) -> {
            Aggregation agg = new Aggregation();

            JsArray<String> aggColumns = dedup(cols, colsNeedingCompoundNames, aggregationType);
            AggregationColumns columns = null;

            switch (aggregationType) {
                case JsAggregationOperation.COUNT: {
                    AggregationCount count = new AggregationCount();
                    count.setColumnName("Count");// TODO likely need to dedup
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
                    spec.setUnique(new AggSpecUnique());
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
                return col + "__" + aggregationType + " = " + col;
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
            if (!existingColumns.some((p0, p1, p2) -> p0.getName().equals(suggestedName))) {
                return suggestedName;
            }
        }

        // Next add a suffix and use that if possible
        for (String suggestedName : suggestedNames) {
            if (!existingColumns.some((p0, p1, p2) -> p0.getName().equals(suggestedName + "_"))) {
                return suggestedName + "_";
            }
        }

        // Give up and add a timestamp suffix
        for (String suggestedName : suggestedNames) {
            if (!existingColumns
                    .some((p0, p1, p2) -> p0.getName().equals(suggestedName + "_" + System.currentTimeMillis()))) {
                return suggestedName + "_" + System.currentTimeMillis();
            }
        }

        // Really give up so Java is happy
        throw new IllegalStateException("Failed to generate a name");
    }
}
