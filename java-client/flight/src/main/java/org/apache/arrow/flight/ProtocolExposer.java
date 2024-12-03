//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;


public final class ProtocolExposer {

    /**
     * Workaround for <a href="https://github.com/apache/arrow/issues/44290">[Java][Flight] Add ActionType description
     * getter</a>
     */
    public static Flight.ActionType toProtocol(ActionType actionType) {
        return actionType.toProtocol();
    }

    public static ActionType fromProtocol(Flight.ActionType actionType) {
        return new ActionType(actionType);
    }

    public static Flight.Action toProtocol(Action action) {
        return action.toProtocol();
    }

    public static Action fromProtocol(Flight.Action action) {
        return new Action(action);
    }

    public static Flight.Result toProtocol(Result action) {
        return action.toProtocol();
    }

    public static Result fromProtocol(Flight.Result result) {
        return new Result(result);
    }

    public static Flight.FlightDescriptor toProtocol(FlightDescriptor descriptor) {
        return descriptor.toProtocol();
    }

    public static FlightDescriptor fromProtocol(Flight.FlightDescriptor descriptor) {
        return new FlightDescriptor(descriptor);
    }

    public static Flight.Ticket toProtocol(Ticket ticket) {
        return ticket.toProtocol();
    }

    public static Ticket fromProtocol(Flight.Ticket ticket) {
        return new Ticket(ticket);
    }
}
