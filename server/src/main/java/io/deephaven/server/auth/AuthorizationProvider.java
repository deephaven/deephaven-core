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
     * @return the authorization provider for ConfigService
     */
    ConsoleServiceAuthWiring getConsoleServiceAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    ObjectServiceAuthWiring getObjectServiceAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    SessionServiceAuthWiring getSessionServiceAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    StorageServiceAuthWiring getStorageServiceAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    HealthAuthWiring getHealthAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    TableServiceContextualAuthWiring getTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    PartitionedTableServiceContextualAuthWiring getPartitionedTableServiceContextualAuthWiring();

    /**
     * @return the authorization provider for ConfigService
     */
    TicketResolverBase.AuthTransformation getTicketTransformation();

    /**
     * @return the default auth context to use during start-up and in other non-interactive contexts
     */
    AuthContext getInstanceAuthContext();
}
