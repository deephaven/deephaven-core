package io.deephaven.server.test;

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
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.TicketResolverBase;

public class TestAuthorizationProvider implements AuthorizationProvider {
    private final ApplicationServiceAuthWiring.TestUseOnly applicationServiceAuthWiring =
            new ApplicationServiceAuthWiring.TestUseOnly();
    private final ConfigServiceAuthWiring.TestUseOnly configServiceAuthWiring =
            new ConfigServiceAuthWiring.TestUseOnly();
    private final ConsoleServiceAuthWiring.TestUseOnly consoleServiceAuthWiring =
            new ConsoleServiceAuthWiring.TestUseOnly();
    private final ObjectServiceAuthWiring.TestUseOnly objectServiceAuthWiring =
            new ObjectServiceAuthWiring.TestUseOnly();
    private final SessionServiceAuthWiring.TestUseOnly sessionServiceAuthWiring =
            new SessionServiceAuthWiring.TestUseOnly();
    private final StorageServiceAuthWiring.TestUseOnly storageServiceAuthWiring =
            new StorageServiceAuthWiring.TestUseOnly();
    private final HealthAuthWiring.TestUseOnly healthAuthWiring =
            new HealthAuthWiring.TestUseOnly();
    private final TableServiceContextualAuthWiring.TestUseOnly tableServiceContextualAuthWiring =
            new TableServiceContextualAuthWiring.TestUseOnly();
    private final InputTableServiceContextualAuthWiring.TestUseOnly inputTableServiceContextualAuthWiring =
            new InputTableServiceContextualAuthWiring.TestUseOnly();
    private final PartitionedTableServiceContextualAuthWiring.TestUseOnly partitionedTableServiceContextualAuthWiring =
            new PartitionedTableServiceContextualAuthWiring.TestUseOnly();

    public TicketResolverBase.AuthTransformation delegateTicketTransformation;

    @Override
    public ApplicationServiceAuthWiring.TestUseOnly getApplicationServiceAuthWiring() {
        return applicationServiceAuthWiring;
    }

    @Override
    public ConfigServiceAuthWiring.TestUseOnly getConfigServiceAuthWiring() {
        return configServiceAuthWiring;
    }

    @Override
    public ConsoleServiceAuthWiring.TestUseOnly getConsoleServiceAuthWiring() {
        return consoleServiceAuthWiring;
    }

    @Override
    public ObjectServiceAuthWiring.TestUseOnly getObjectServiceAuthWiring() {
        return objectServiceAuthWiring;
    }

    @Override
    public SessionServiceAuthWiring.TestUseOnly getSessionServiceAuthWiring() {
        return sessionServiceAuthWiring;
    }

    @Override
    public StorageServiceAuthWiring.TestUseOnly getStorageServiceAuthWiring() {
        return storageServiceAuthWiring;
    }

    @Override
    public HealthAuthWiring.TestUseOnly getHealthAuthWiring() {
        return healthAuthWiring;
    }

    @Override
    public TableServiceContextualAuthWiring.TestUseOnly getTableServiceContextualAuthWiring() {
        return tableServiceContextualAuthWiring;
    }

    @Override
    public InputTableServiceContextualAuthWiring.TestUseOnly getInputTableServiceContextualAuthWiring() {
        return inputTableServiceContextualAuthWiring;
    }

    @Override
    public PartitionedTableServiceContextualAuthWiring.TestUseOnly getPartitionedTableServiceContextualAuthWiring() {
        return partitionedTableServiceContextualAuthWiring;
    }

    @Override
    public TicketResolverBase.AuthTransformation getTicketTransformation() {
        return new TicketResolverBase.AuthTransformation() {
            @Override
            public <T> T transform(T source) {
                if (delegateTicketTransformation != null) {
                    return delegateTicketTransformation.transform(source);
                }
                return source;
            }
        };
    }

    @Override
    public AuthContext getInstanceAuthContext() {
        return new AuthContext.SuperUser();
    }
}
