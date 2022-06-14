/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import java.util.Collection;

/**
 * The match condition rule for the final match column of as-of-join.
 *
 * @see TableOperations#aj(Object, Collection, Collection, AsOfJoinRule)
 * @see JoinMatch
 */
public enum AsOfJoinRule {
    LESS_THAN_EQUAL, LESS_THAN
}
