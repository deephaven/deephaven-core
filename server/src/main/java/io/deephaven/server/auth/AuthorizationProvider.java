//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.*;
import io.deephaven.server.session.TicketResolver;

public interface AuthorizationProvider {
    /**
     * @return the authorization provider for ApplicationService
     */
    ApplicationServiceAuthWiring getApplicationServiceAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    ConfigServiceAuthWiring getConfigServiceAuthWiring();

    /**
     * @return the authorization provider for ConsoleService
     */
    ConsoleServiceAuthWiring getConsoleServiceAuthWiring();

    /**
     * @return the authorization provider for ObjectService
     */
    ObjectServiceAuthWiring getObjectServiceAuthWiring();

    /**
     * @return the authorization provider for SessionService
     */
    SessionServiceAuthWiring getSessionServiceAuthWiring();

    /**
     * @return the authorization provider for StorageService
     */
    StorageServiceAuthWiring getStorageServiceAuthWiring();

    /**
     * @return the authorization provider for HealthService
     */
    HealthAuthWiring getHealthAuthWiring();

    /**
     * @return the authorization provider for TableService
     */
    TableServiceContextualAuthWiring getTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for InputTableService
     */
    InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for PartitionTableService
     */
    PartitionedTableServiceContextualAuthWiring getPartitionedTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for HierarchicalTableService
     */
    HierarchicalTableServiceContextualAuthWiring getHierarchicalTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for TicketResolvers; used when resolving from, and publishing to, tickets
     */
    TicketResolver.Authorization getTicketResolverAuthorization();

    /**
     * @return the default auth context to use during start-up and in other non-interactive contexts
     */
    AuthContext getInstanceAuthContext();
}
