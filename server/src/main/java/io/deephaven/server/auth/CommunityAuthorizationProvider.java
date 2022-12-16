/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.*;
import io.deephaven.server.session.TicketResolverBase;

public class CommunityAuthorizationProvider implements AuthorizationProvider {
    @Override
    public ApplicationServiceAuthWiring getApplicationServiceAuthWiring() {
        return new ApplicationServiceAuthWiring.AllowAll();
    }

    @Override
    public ConfigServiceAuthWiring getConfigServiceAuthWiring() {
        return new ConfigServiceAuthWiring.AllowAll();
    }

    @Override
    public ConsoleServiceAuthWiring getConsoleServiceAuthWiring() {
        return new ConsoleServiceAuthWiring.AllowAll();
    }

    @Override
    public ObjectServiceAuthWiring getObjectServiceAuthWiring() {
        return new ObjectServiceAuthWiring.AllowAll();
    }

    @Override
    public SessionServiceAuthWiring getSessionServiceAuthWiring() {
        return new SessionServiceAuthWiring.AllowAll();
    }

    @Override
    public StorageServiceAuthWiring getStorageServiceAuthWiring() {
        return new StorageServiceAuthWiring.AllowAll();
    }

    @Override
    public HealthAuthWiring getHealthAuthWiring() {
        return new HealthAuthWiring.AllowAll();
    }

    @Override
    public TableServiceContextualAuthWiring getTableServiceContextualAuthWiring() {
        return new TableServiceContextualAuthWiring.AllowAll();
    }

    @Override
    public InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring() {
        return new InputTableServiceContextualAuthWiring.AllowAll();
    }

    @Override
    public PartitionedTableServiceContextualAuthWiring getPartitionedTableServiceContextualAuthWiring() {
        return new PartitionedTableServiceContextualAuthWiring.AllowAll();
    }

    @Override
    public HierarchicalTableServiceContextualAuthWiring getHierarchicalTableServiceContextualAuthWiring() {
        return new HierarchicalTableServiceContextualAuthWiring.AllowAll();
    }

    @Override
    public TicketResolverBase.AuthTransformation getTicketTransformation() {
        return TicketResolverBase.identityTransformation();
    }

    @Override
    public AuthContext getInstanceAuthContext() {
        return new AuthContext.SuperUser();
    }
}
