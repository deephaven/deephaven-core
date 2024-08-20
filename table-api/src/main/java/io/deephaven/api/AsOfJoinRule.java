//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import java.util.Collection;

/**
 * The match condition rule for the final match column of as-of-join.
 *
 * @see TableOperations#asOfJoin(Object, Collection, AsOfJoinMatch, Collection)
 * @see JoinMatch
 */
public enum AsOfJoinRule {
    // @formatter:off
    LESS_THAN_EQUAL("<="),
    LESS_THAN("<"),
    GREATER_THAN_EQUAL(">="),
    GREATER_THAN(">");
    // @formatter:on

    public static AsOfJoinRule parse(String x) {
        switch (x) {
            case "<=":
                return LESS_THAN_EQUAL;
            case "<":
                return LESS_THAN;
            case ">=":
                return GREATER_THAN_EQUAL;
            case ">":
                return GREATER_THAN;
            default:
                throw new IllegalArgumentException(String.format("Unable to parse AsOfJoinRule '%s'", x));
        }
    }

    private final String operatorString;

    AsOfJoinRule(String operatorString) {
        this.operatorString = operatorString;
    }

    public String operatorString() {
        return operatorString;
    }
}
