//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.server.runner.TicketResolversFromServiceLoader;
import io.deephaven.server.session.TicketResolver;

/**
 * Factory service for creating RemoteFileSourceCommandResolver instances.
 * This service is registered via @AutoService and provides ticket resolver functionality
 * for handling remote file source plugin commands through the Deephaven server infrastructure.
 */
@AutoService(TicketResolversFromServiceLoader.Factory.class)
public class RemoteFileSourceTicketResolverFactoryService implements TicketResolversFromServiceLoader.Factory {
    @Override
    public TicketResolver create(final TicketResolversFromServiceLoader.TicketResolverOptions options) {
        return new RemoteFileSourceCommandResolver();
    }
}
