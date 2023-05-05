package io.deephaven.api;

/**
 * Each output row of a range join corresponds to exactly one left table row and one or more aggregations over a range
 * of responsive right table rows. Responsive right table rows must have identical values to the left table row for the
 * exact match columns, and the relationship between the left start column value and right range column value must
 * respect one of the enumerated RangeStartRule options.
 */
public enum RangeStartRule {

    /**
     * The left start column value must be strictly less than right range column values.
     */
    LESS_THAN,

    /**
     * The left start column value must be less than or equal to right range column values.
     */
    LESS_THAN_OR_EQUAL,

    /**
     * The left start column value must be less than or equal to right range column values. If no matching right range
     * column value is equal to the left start column value, the immediately preceding matching right row should be
     * included in the aggregation if such a row exists.
     */
    LESS_THAN_OR_EQUAL_ALLOW_PRECEDING
}
