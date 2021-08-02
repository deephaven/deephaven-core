package io.deephaven.client.impl;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.types.pojo.Schema;

public interface Flight extends AutoCloseable {

    Schema getSchema(Export export);

    FlightStream get(Export export);

    Iterable<FlightInfo> list();

    void close() throws InterruptedException;
}
