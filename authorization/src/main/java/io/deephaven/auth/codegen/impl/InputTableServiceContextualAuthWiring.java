//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import java.lang.Override;
import java.util.List;

/**
 * This interface provides type-safe authorization hooks for InputTableServiceGrpc.
 */
public interface InputTableServiceContextualAuthWiring {
    /**
     * Authorize a request to AddTableToInputTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke AddTableToInputTable
     */
    void checkPermissionAddTableToInputTable(AuthContext authContext, AddTableRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to DeleteTableFromInputTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke DeleteTableFromInputTable
     */
    void checkPermissionDeleteTableFromInputTable(AuthContext authContext, DeleteTableRequest request,
            List<Table> sourceTables);

    /**
     * A default implementation that funnels all requests to invoke {@code checkPermission}.
     */
    abstract class DelegateAll implements InputTableServiceContextualAuthWiring {
        protected abstract void checkPermission(AuthContext authContext, List<Table> sourceTables);

        public void checkPermissionAddTableToInputTable(AuthContext authContext,
                AddTableRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionDeleteTableFromInputTable(AuthContext authContext,
                DeleteTableRequest request, List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }
    }

    /**
     * A default implementation that allows all requests.
     */
    class AllowAll extends DelegateAll {
        @Override
        protected void checkPermission(AuthContext authContext, List<Table> sourceTables) {}
    }

    /**
     * A default implementation that denies all requests.
     */
    class DenyAll extends DelegateAll {
        @Override
        protected void checkPermission(AuthContext authContext, List<Table> sourceTables) {
            ServiceAuthWiring.operationNotAllowed();
        }
    }

    class TestUseOnly implements InputTableServiceContextualAuthWiring {
        public InputTableServiceContextualAuthWiring delegate;

        public void checkPermissionAddTableToInputTable(AuthContext authContext,
                AddTableRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionAddTableToInputTable(authContext, request, sourceTables);
            }
        }

        public void checkPermissionDeleteTableFromInputTable(AuthContext authContext,
                DeleteTableRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionDeleteTableFromInputTable(authContext, request, sourceTables);
            }
        }
    }
}
