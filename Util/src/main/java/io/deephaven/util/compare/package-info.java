/**
 * The Deephaven compare package serves as a low-level building block for Deephaven's sort order and equality testing.
 * Deephaven sort order is "nulls first". For floating-point primitives, the sort order is also "NaNs last" and
 * {@code +0.0} equals {@code -0.0}. The "eq" method for each type is consistent with the "compare" method; that is,
 * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}. The "hashCode"
 * method for each type is consistent with the "eq" method; that is, {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
 */
package io.deephaven.util.compare;
