//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.ApplicationServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConfigServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConsoleServiceAuthWiring;
import io.deephaven.auth.codegen.impl.HealthAuthWiring;
import io.deephaven.auth.codegen.impl.HierarchicalTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.ObjectServiceAuthWiring;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.SessionServiceAuthWiring;
import io.deephaven.auth.codegen.impl.StorageServiceAuthWiring;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.server.session.NoopTicketResolverAuthorization;
import io.deephaven.server.session.TicketResolver;

import javax.inject.Inject;

/**
 * An "allow all" authorization provider.
 */
public class AllowAllAuthorizationProvider implements AuthorizationProvider {

    @Inject
    public AllowAllAuthorizationProvider() {}

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
    public TicketResolver.Authorization getTicketResolverAuthorization() {
        return new NoopTicketResolverAuthorization();
    }

    @Override
    public AuthContext getInstanceAuthContext() {
        return new AuthContext.SuperUser();
    }
}
