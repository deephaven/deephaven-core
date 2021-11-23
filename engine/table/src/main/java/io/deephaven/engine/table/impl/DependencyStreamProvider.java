package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.NotificationQueue;

import java.util.stream.Stream;

/**
 * An interface for things that provide a stream of dependencies.
 */
public interface DependencyStreamProvider {
    /**
     * Return a stream of dependencies for this object.
     */
    Stream<NotificationQueue.Dependency> getDependencyStream();
}
