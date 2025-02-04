//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

/**
 * Specifies the type of natural join to perform, specifically how to handle duplicate and missing right hand table
 * rows.
 */
public enum NaturalJoinType {

    /**
     * Throw an error if a duplicate right hand table row is found. This is the default behavior if not specified.
     */
    ERROR_ON_DUPLICATE,

    /**
     * Match the first right hand table row and ignore later duplicates.
     */
    FIRST_MATCH,

    /**
     * Match the last right hand table row and ignore earlier duplicates.
     */
    LAST_MATCH,

    /**
     * Match exactly one right hand table row; throw an error if there are zero or more than one matches.
     */
    EXACTLY_ONE_MATCH,
}
