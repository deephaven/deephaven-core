/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import java.util.Collection;

/**
 * The match condition rule for the final match column of reverse-as-of-join.
 *
 * @see TableOperations#raj(Object, Collection, Collection, ReverseAsOfJoinRule)
 * @see JoinMatch
 */
public enum ReverseAsOfJoinRule {
    GREATER_THAN_EQUAL, GREATER_THAN
}
