package io.deephaven.stream;

import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.LivenessReferent;

/**
 * Stream connector interface, for components which act as a {@link StreamConsumer} for an upstream source and a
 * {@link StreamPublisher} for a downstream sink.
 */
public interface StreamConnector extends StreamConsumer, StreamPublisher {
}
