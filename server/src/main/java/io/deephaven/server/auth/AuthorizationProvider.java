package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.ApplicationServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConfigServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConsoleServiceAuthWiring;
import io.deephaven.auth.codegen.impl.HealthAuthWiring;
import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.ObjectServiceAuthWiring;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.SessionServiceAuthWiring;
import io.deephaven.auth.codegen.impl.StorageServiceAuthWiring;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.server.session.TicketResolverBase;

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
     * @return the authorization transformation used when resolving tickets
     */
    TicketResolverBase.AuthTransformation getTicketTransformation();

    /**
     * @return the default auth context to use during start-up and in other non-interactive contexts
     */
    AuthContext getInstanceAuthContext();
}
