package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.server.runner.TicketResolversFromServiceLoader;
import io.deephaven.server.session.TicketResolver;

@AutoService(TicketResolversFromServiceLoader.Factory.class)
public class RemoteFileSourceTicketResolverFactoryService implements TicketResolversFromServiceLoader.Factory {
    @Override
    public TicketResolver create(final TicketResolversFromServiceLoader.TicketResolverOptions options) {
        return new RemoteFileSourceCommandResolver();
    }
}
