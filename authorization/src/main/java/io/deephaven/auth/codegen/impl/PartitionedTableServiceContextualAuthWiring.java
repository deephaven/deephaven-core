//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.GetTableRequest;
import io.deephaven.proto.backplane.grpc.MergeRequest;
import io.deephaven.proto.backplane.grpc.PartitionByRequest;
import java.lang.Override;
import java.util.List;

/**
 * This interface provides type-safe authorization hooks for PartitionedTableServiceGrpc.
 */
public interface PartitionedTableServiceContextualAuthWiring {
    /**
     * Authorize a request to PartitionBy.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke PartitionBy
     */
    void checkPermissionPartitionBy(AuthContext authContext, PartitionByRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Merge.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Merge
     */
    void checkPermissionMerge(AuthContext authContext, MergeRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to GetTable.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke GetTable
     */
    void checkPermissionGetTable(AuthContext authContext, GetTableRequest request,
            List<Table> sourceTables);

    /**
     * A default implementation that funnels all requests to invoke {@code checkPermission}.
     */
    abstract class DelegateAll implements PartitionedTableServiceContextualAuthWiring {
        protected abstract void checkPermission(AuthContext authContext, List<Table> sourceTables);

        public void checkPermissionPartitionBy(AuthContext authContext, PartitionByRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionMerge(AuthContext authContext, MergeRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionGetTable(AuthContext authContext, GetTableRequest request,
                List<Table> sourceTables) {
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

    class TestUseOnly implements PartitionedTableServiceContextualAuthWiring {
        public PartitionedTableServiceContextualAuthWiring delegate;

        public void checkPermissionPartitionBy(AuthContext authContext, PartitionByRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionPartitionBy(authContext, request, sourceTables);
            }
        }

        public void checkPermissionMerge(AuthContext authContext, MergeRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionMerge(authContext, request, sourceTables);
            }
        }

        public void checkPermissionGetTable(AuthContext authContext, GetTableRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionGetTable(authContext, request, sourceTables);
            }
        }
    }
}
