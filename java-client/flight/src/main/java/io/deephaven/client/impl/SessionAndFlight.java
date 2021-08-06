package io.deephaven.client.impl;

public interface SessionAndFlight {

    Session session();

    FlightClientImpl flight();
}
