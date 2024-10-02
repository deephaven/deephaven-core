//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;

/**
 * Workaround for <a href="https://github.com/apache/arrow/issues/44290">[Java][Flight] Add ActionType description
 * getter</a>
 */
public class ActionTypeExposer {

    public static Flight.ActionType toProtocol(ActionType actionType) {
        return actionType.toProtocol();
    }
}
