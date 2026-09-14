//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Column.ColumnOrName;
import io.deephaven.web.client.api.JsTable.MatchPairUnion;
import io.deephaven.web.client.api.Sort;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

/**
 * Base sealed type for all aggregation definitions.
 *
 * <p>
 * Column-based aggregations (those backed by an {@code AggSpec} and optional input/output column pairs) extend
 * {@link ColumnAggregation}. Non-column aggregations that produce a single output column or have special semantics
 * ({@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey}, {@link LastRowKey}) extend this class
 * directly.
 *
 * <p>
 * Each concrete subtype carries a {@code type} field whose compile-time constant value acts as a discriminant, enabling
 * TypeScript consumers to narrow the union via {@code switch (agg.type)}.
 *
 * @see ColumnAggregation
 * @see AggregationUnion
 */
@JsType(namespace = "dh.agg")
public abstract sealed class Aggregation
        permits ColumnAggregation, Count, CountWhere, Partition, FirstRowKey, LastRowKey {

    /**
     * Creates a Sum aggregation.
     *
     * @param columns the column(s) to aggregate; can be renaming expressions, i.e. {@code "new_col = col"}. Default is
     *        null, only valid when used in {@code aggAllBy}.
     * @return an aggregation
     */
    @JsMethod
    public static Sum AggSum(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Sum agg = new Sum();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates an absolute-sum aggregation.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static AbsSum AggAbsSum(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        AbsSum agg = new AbsSum();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates an Average (arithmetic mean) aggregation.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Avg AggAvg(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Avg agg = new Avg();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a First aggregation, returning the first value in each group.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static First AggFirst(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        First agg = new First();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Freeze aggregation, which keeps the first value seen and ignores subsequent changes.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Freeze AggFreeze(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Freeze agg = new Freeze();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Group aggregation, collecting values into arrays.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Group AggGroup(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Group agg = new Group();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Last aggregation, returning the last value in each group.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Last AggLast(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Last agg = new Last();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Max aggregation.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Max AggMax(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Max agg = new Max();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Min aggregation.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Min AggMin(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Min agg = new Min();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Std (sample standard deviation) aggregation, computed using Bessel's correction.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Std AggStd(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Std agg = new Std();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Var (sample variance) aggregation, computed using Bessel's correction.
     *
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Var AggVar(@JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Var agg = new Var();
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Median aggregation.
     *
     * @param averageEvenlyDivided when the group size is even, whether to average the two middle values. When
     *        {@code true}, averages the two middle values. When {@code false}, uses the smaller value. Only applies to
     *        numeric types. Defaults to {@code true}.
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Median AggMed(@JsOptional @JsNullable Boolean averageEvenlyDivided,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Median agg = new Median();
        agg.averageEvenlyDivided = averageEvenlyDivided;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Percentile aggregation.
     *
     * @param percentile the percentile to calculate, must be in the range [0.0, 1.0]
     * @param averageEvenlyDivided whether to average the two boundary values when the percentile falls evenly between
     *        them. Only applies to numeric types. Defaults to false if not specified
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Percentile AggPct(double percentile, @JsOptional @JsNullable Boolean averageEvenlyDivided,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Percentile agg = new Percentile();
        agg.percentile = percentile;
        agg.averageEvenlyDivided = averageEvenlyDivided;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates an Approximate Percentile aggregation using T-Digest.
     *
     * @param percentile the percentile to calculate, must be in the range [0.0, 1.0]
     * @param compression T-Digest compression factor; must be &ge; 1, values above 1000 are not recommended. If null,
     *        the server will choose a default.
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static ApproxPercentile AggApproxPct(double percentile, @JsOptional @JsNullable Double compression,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        ApproxPercentile agg = new ApproxPercentile();
        agg.percentile = percentile;
        agg.compression = compression;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Count Distinct aggregation, counting the number of distinct values in each group.
     *
     * @param countNulls whether null values should be counted as distinct values
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static CountDistinct AggCountDistinct(@JsOptional @JsNullable Boolean countNulls,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        CountDistinct agg = new CountDistinct();
        agg.countNulls = countNulls;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Distinct aggregation, collecting the distinct values within each group as arrays.
     *
     * @param includeNulls whether null values should be included in the distinct output values. Defaults to
     *        {@code false}.
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Distinct AggDistinct(@JsOptional @JsNullable Boolean includeNulls,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Distinct agg = new Distinct();
        agg.includeNulls = includeNulls;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Unique aggregation, returning the single unique value within each group. If there is more than one
     * distinct value, the result is null (or a specified sentinel).
     *
     * @param includeNulls whether null is treated as a value for determining uniqueness. Defaults to {@code false}.
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Unique AggUnique(@JsOptional @JsNullable Boolean includeNulls,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Unique agg = new Unique();
        agg.includeNulls = includeNulls;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Weighted Average aggregation.
     *
     * @param weightColumn the column containing the weights
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static WAvg AggWAvg(ColumnOrName weightColumn,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        WAvg agg = new WAvg();
        agg.weightColumn = weightColumn;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Weighted Sum aggregation.
     *
     * @param weightColumn the column containing the weights
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static WSum AggWSum(ColumnOrName weightColumn,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        WSum agg = new WSum();
        agg.weightColumn = weightColumn;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a SortedFirst aggregation, returning the first value when each group is sorted by the given columns.
     *
     * @param sortedColumns the columns to sort by when determining the "first" row
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static SortedFirst AggSortedFirst(ReadonlyArray<Sort.SortUnion> sortedColumns,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        SortedFirst agg = new SortedFirst();
        agg.sortedColumns = sortedColumns;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a SortedLast aggregation, returning the last value when each group is sorted by the given columns.
     *
     * @param sortedColumns the columns to sort by when determining the "last" row
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static SortedLast AggSortedLast(ReadonlyArray<Sort.SortUnion> sortedColumns,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        SortedLast agg = new SortedLast();
        agg.sortedColumns = sortedColumns;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a T-Digest aggregation, storing the T-Digest data structure for later percentile calculations.
     *
     * @param compression T-Digest compression factor; must be &ge; 1. If null, the server will choose a default.
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static TDigest AggTDigest(@JsNullable Double compression,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        TDigest agg = new TDigest();
        agg.compression = compression;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Formula aggregation, applying a user-defined formula to each group.
     *
     * @param formula the formula to evaluate
     * @param paramToken the parameter token in the formula that will be replaced with the input column name
     * @param columns the column(s) to aggregate
     * @return an aggregation
     */
    @JsMethod
    public static Formula AggFormula(String formula, String paramToken,
            @JsOptional @JsNullable ReadonlyArray<MatchPairUnion> columns) {
        Formula agg = new Formula();
        agg.formula = formula;
        agg.paramToken = paramToken;
        agg.columns = columns;
        return agg;
    }

    /**
     * Creates a Count aggregation, counting the number of rows in each group. Not supported in {@code aggAllBy}.
     *
     * @param col the output column name to hold the counts
     * @return an aggregation
     */
    @JsMethod
    public static Count AggCount(String col) {
        Count agg = new Count();
        agg.col = col;
        return agg;
    }

    /**
     * Creates a CountWhere aggregation, counting rows that match the given filter(s) in each group. Not supported in
     * {@code aggAllBy}.
     *
     * @param col the output column name to hold the counts
     * @param filters the filter expression(s) to apply before counting
     * @return an aggregation
     */
    @JsMethod
    public static CountWhere AggCountWhere(String col, ReadonlyArray<String> filters) {
        CountWhere agg = new CountWhere();
        agg.col = col;
        agg.filters = filters;
        return agg;
    }

    /**
     * Creates a Partition aggregation, splitting the table into sub-tables, one per group. Not supported in
     * {@code aggAllBy}.
     *
     * @param col the output column name to hold the sub-tables
     * @param includeGroupByColumns whether to include the group-by columns in each sub-table; defaults to {@code true}
     * @return an aggregation
     */
    @JsMethod
    public static Partition AggPartition(String col, @JsOptional @JsNullable Boolean includeGroupByColumns) {
        Partition agg = new Partition();
        agg.col = col;
        agg.includeGroupByColumns = includeGroupByColumns;
        return agg;
    }

    /**
     * Creates a FirstRowKey aggregation, returning the row key of the first row in each group. Not supported in
     * {@code aggAllBy}.
     *
     * @param col the output column name to hold the first row key
     * @return an aggregation
     */
    @JsMethod
    public static FirstRowKey AggFirstRowKey(String col) {
        FirstRowKey agg = new FirstRowKey();
        agg.col = col;
        return agg;
    }

    /**
     * Creates a LastRowKey aggregation, returning the row key of the last row in each group. Not supported in
     * {@code aggAllBy}.
     *
     * @param col the output column name to hold the last row key
     * @return an aggregation
     */
    @JsMethod
    public static LastRowKey AggLastRowKey(String col) {
        LastRowKey agg = new LastRowKey();
        agg.col = col;
        return agg;
    }
}
