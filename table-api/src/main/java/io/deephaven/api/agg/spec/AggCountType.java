//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

/**
 * The types of counts that can be performed.
 */
public enum AggCountType {
    NON_NULL, NULL, NEGATIVE, POSITIVE, ZERO, NAN, INFINITE, FINITE
}
