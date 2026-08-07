//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.Global;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.fu.JsLog;
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
import java.util.stream.Collectors;

/**
 * Describes how a Totals Table will be generated from its parent table. Each table has a default (which may be @code
 * null}) indicating how that table was configured when it was declared, and each Totals Table has a similar property
 * describing how it was created. Both the {@code Table.getTotalsTable} and {@code Table.getGrandTotalsTable} methods
 * take this config as an optional parameter - without it, the table's default will be used, or if null, a default
 * instance of {@code TotalsTableConfig} will be supplied.
 *
 * This class has a no-arg constructor, allowing an instance to be made with the default values provided. However, any
 * JS object can be passed in to the methods which accept instances of this type, provided their values adhere to the
 * expected formats.
 */
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
    protected static final List<String> knownAggTypes = Arrays.asList(
            JsAggregationOperation.COUNT,
            JsAggregationOperation.MIN,
            JsAggregationOperation.MAX,
            JsAggregationOperation.SUM,
            JsAggregationOperation.ABS_SUM,
            JsAggregationOperation.VAR,
            JsAggregationOperation.AVG,
            JsAggregationOperation.MEDIAN,
            JsAggregationOperation.STD,
            JsAggregationOperation.FIRST,
            JsAggregationOperation.LAST,
            JsAggregationOperation.COUNT_DISTINCT,
            JsAggregationOperation.DISTINCT,
            JsAggregationOperation.UNIQUE,
            JsAggregationOperation.SKIP);

    /**
     * Specifies if a Totals Table should be expanded by default in the UI. Defaults to {@code false}.
     */
    public boolean showTotalsByDefault = false;
    /**
     * Specifies if a Grand Totals Table should be expanded by default in the UI. Defaults to {@code false}.
     */
    public boolean showGrandTotalsByDefault = false;
    /**
     * Specifies the default operation for columns that do not have a specific operation applied; defaults to "Sum".
     */
    @TsTypeRef(JsAggregationOperation.class)
    public String defaultOperation = JsAggregationOperation.SUM;
    /**
     * Mapping from each column name to the aggregation(s) that should be applied to that column in the resulting Totals
     * Table. If a column is omitted, the defaultOperation is used.
     */
    public JsPropertyMap<JsArray<@TsTypeRef(JsAggregationOperation.class) String>> operationMap =
            Js.cast(JsObject.create(null));

    /**
     * Groupings to use when generating the Totals Table. One row will exist for each unique set of values observed in
     * these columns. See also {@code Table.selectDistinct}.
     */
    public JsArray<String> groupBy = new JsArray<>();

    private JsArray<String> customColumns;
    private JsArray<String> dropColumns;

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
                operationMap.get(key).forEach((value, index) -> {
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
     * Implementation from {@code TotalsTableBuilder.fromDirective}, plus changes required to make this able to act on
     * plan JS objects/arrays.
     *
     * Note that this omits {@code groupBy} for now, until the server directive format supports it!
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
        return "TotalsTableConfig{" +
                "showTotalsByDefault=" + showTotalsByDefault +
                ", showGrandTotalsByDefault=" + showGrandTotalsByDefault +
                ", defaultOperation='" + defaultOperation + '\'' +
                ", operationMap=" + Global.JSON.stringify(operationMap) + // Object.create(null) has no valueOf
                ", groupBy=" + groupBy +
                '}';
    }

    /**
     * Implementation from {@code TotalsTableBuilder.buildDirective}, plus a minor change to iterate JS arrays/objects
     * correctly.
     *
     * Note that this omits {@code groupBy} until the server directive format supports it!
     */
    @JsIgnore
    public String serialize() {
        final StringBuilder builder = new StringBuilder();
        builder.append(showTotalsByDefault).append(",")
                .append(showGrandTotalsByDefault).append(",").append(defaultOperation).append(";");
        operationMap
                .forEach(key -> builder.append(key).append("=").append(operationMap.get(key).join(":")).append(","));
        return builder.toString();
    }

    @JsIgnore
    public AggregateRequest buildRequest(JsArray<Column> allColumns) {
        AggregateRequest.Builder request = AggregateRequest.newBuilder();
        customColumns = new JsArray<>();
        dropColumns = new JsArray<>();

        request.addAllGroupByColumns(groupBy.asList());
        List<Aggregation> aggregations = new ArrayList<>();
        Map<String, String> columnTypes = Arrays.stream(Js.<Column[]>uncheckedCast(allColumns))
                .collect(Collectors.toMap(Column::getName, Column::getType));
        Map<String, LinkedHashSet<String>> aggs = new HashMap<>();
        List<String> colsNeedingCompoundNames = new ArrayList<>();
        Set<String> seenColNames = new HashSet<>();
        groupBy.forEach((col, p1) -> seenColNames.add(Js.cast(col)));
        this.operationMap.forEach(colName -> {
            this.operationMap.get(colName).forEach((agg, index) -> {
                if (!JsAggregationOperation.canAggregateType(agg, columnTypes.get(colName))) {
                    // skip this column. to follow DHE's behavior
                    return null;
                }
                aggs.computeIfAbsent(agg, ignore -> new LinkedHashSet<>()).add(colName);
                if (seenColNames.contains(colName)) {
                    colsNeedingCompoundNames.add(colName);
                } else {
                    seenColNames.add(colName);
                }
                return null;
            });
        });
        Set<String> unusedColumns = new HashSet<>(columnTypes.keySet());
        unusedColumns.removeAll(seenColNames);
        // no unused column can collide, add to the default operation list
        aggs.computeIfAbsent(defaultOperation, ignore -> new LinkedHashSet<>())
                .addAll(unusedColumns.stream().filter(
                        colName -> JsAggregationOperation.canAggregateType(defaultOperation, columnTypes.get(colName)))
                        .collect(Collectors.toList()));

        aggs.forEach((aggregationType, cols) -> {
            Aggregation.Builder agg = Aggregation.newBuilder();

            List<String> aggColumns = dedup(cols, colsNeedingCompoundNames, aggregationType);
            Aggregation.AggregationColumns columns = null;

            switch (aggregationType) {
                case JsAggregationOperation.COUNT: {
                    Aggregation.AggregationCount count = Aggregation.AggregationCount.newBuilder()
                            .setColumnName("Count")
                            .build();
                    agg.setCount(count);
                    boolean dropCount = true;
                    for (String pair : aggColumns) {
                        String colName = pair.split("=")[0].trim();
                        if (colName.equals("Count")) {
                            dropCount = false;
                        }
                        customColumns.push(colName + " = Count");
                    }
                    if (dropCount) {
                        dropColumns.push("Count");
                    }
                    break;
                }
                case JsAggregationOperation.COUNT_DISTINCT: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setCountDistinct(AggSpec.AggSpecCountDistinct.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.DISTINCT: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setDistinct(AggSpec.AggSpecDistinct.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    aggColumns.forEach((p0) -> {
                        String colName = p0.split("=")[0].trim();
                        customColumns.push(colName + "= `` + " + colName);
                    });
                    break;
                }
                case JsAggregationOperation.MIN: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setMin(AggSpec.AggSpecMin.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.MAX: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setMax(AggSpec.AggSpecMax.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.SUM: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setSum(AggSpec.AggSpecSum.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.ABS_SUM: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setAbsSum(AggSpec.AggSpecAbsSum.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();

                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.VAR: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setVar(AggSpec.AggSpecVar.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();

                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.AVG: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setAvg(AggSpec.AggSpecAvg.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.MEDIAN: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setMedian(AggSpec.AggSpecMedian.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.STD: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setStd(AggSpec.AggSpecStd.getDefaultInstance())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.FIRST: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setFirst(AggSpec.AggSpecFirst.newBuilder())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.LAST: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setLast(AggSpec.AggSpecLast.newBuilder())
                            .build();
                    columns = Aggregation.AggregationColumns.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(aggColumns)
                            .build();
                    agg.setColumns(columns);
                    break;
                }
                case JsAggregationOperation.UNIQUE: {
                    AggSpec spec = AggSpec.newBuilder()
                            .setUnique(AggSpec.AggSpecUnique.newBuilder()
                                    .setNonUniqueSentinel(AggSpec.AggSpecNonUniqueSentinel.newBuilder()
                                            .setNullValue(NullValue.NULL_VALUE)))
                            .build();
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

            if (columns == null || columns.getMatchPairsCount() > 0) {
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
                return col + "__" + aggregationType + " = " + col;
            }
            return col;
        }).collect(Collectors.toList());
    }

    @JsIgnore
    public JsArray<String> getCustomColumns() {
        return customColumns;
    }

    @JsIgnore
    public JsArray<String> getDropColumns() {
        return dropColumns;
    }
}
