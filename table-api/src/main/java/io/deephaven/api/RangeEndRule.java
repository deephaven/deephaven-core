package io.deephaven.api;

/**
 * Each output row of a range join corresponds to exactly one left table row and one or more aggregations over a range
 * of responsive right table rows. Responsive right table rows must have identical values to the left table row for the
 * exact match columns, and the relationship between the left end column value and right range column value must respect
 * one of the enumerated RangeEndRule options.
 */
public enum RangeEndRule {

    /**
     * The left end column value must be strictly greater than right range column values.
     */
    GREATER_THAN,

    /**
     * The left end column value must be greater than or equal to right range column values.
     */
    GREATER_THAN_OR_EQUAL,

    /**
     * The left end column value must be greater than or equal to right range column values. If no matching right range
     * column value is equal to the left end column value, the immediately following matching right row should be
     * included in the aggregation if such a row exists.
     */
    GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING
}
