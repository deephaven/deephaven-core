package io.deephaven.web.client.api;

import elemental2.core.Global;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsString;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;
import java.util.List;

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
            JsAggregationOperation.SKIP,
            JsAggregationOperation.COUNT_DISTINCT,
            JsAggregationOperation.DISTINCT,
            JsAggregationOperation.UNIQUE);

    public boolean showTotalsByDefault = false;
    public boolean showGrandTotalsByDefault = false;
    public String defaultOperation = SUM;
    public JsPropertyMap<JsArray<JsString>> operationMap = Js.cast(JsObject.create(null));

    public JsArray<JsString> groupBy = new JsArray<>();

    @JsConstructor
    public JsTotalsTableConfig() {}

    @JsIgnore
    public JsTotalsTableConfig(JsPropertyMap<Object> source) {
        this();
        if (source.has("showTotalsByTable")) {
            showTotalsByDefault = Js.isTruthy(source.getAny("showTotalsByDefault"));
        }
        if (source.has("showGrandTotalsByDefault")) {
            showGrandTotalsByDefault = Js.isTruthy(source.getAny("showGrandTotalsByDefault"));
        }
        if (source.has("defaultOperation")) {
            defaultOperation = source.getAny("defaultOperation").asString();
            checkOperation(defaultOperation);
        }
        if (source.has("operationMap")) {
            operationMap = source.getAny("operationMap").cast();
            operationMap.forEach(key -> {
                operationMap.get(key).forEach((value, index, array) -> {
                    checkOperation(Js.cast(value));
                    return null;
                });
            });
        }
        if (source.has("groupBy")) {
            groupBy = source.getAny("groupBy").cast();
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
                    builder.operationMap.get(kv[0]).push(Js.<JsString>cast(op));
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
}
