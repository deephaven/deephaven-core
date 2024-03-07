//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.HierarchicalTableApplyRequest;
import io.deephaven.proto.backplane.grpc.HierarchicalTableSourceExportRequest;
import io.deephaven.proto.backplane.grpc.HierarchicalTableViewRequest;
import io.deephaven.proto.backplane.grpc.RollupRequest;
import io.deephaven.proto.backplane.grpc.TreeRequest;
import java.lang.Override;
import java.util.List;

/**
 * This interface provides type-safe authorization hooks for HierarchicalTableServiceGrpc.
 */
public interface HierarchicalTableServiceContextualAuthWiring {
    /**
     * Authorize a request to Rollup.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Rollup
     */
    void checkPermissionRollup(AuthContext authContext, RollupRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to Tree.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Tree
     */
    void checkPermissionTree(AuthContext authContext, TreeRequest request, List<Table> sourceTables);

    /**
     * Authorize a request to Apply.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke Apply
     */
    void checkPermissionApply(AuthContext authContext, HierarchicalTableApplyRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to View.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke View
     */
    void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
            List<Table> sourceTables);

    /**
     * Authorize a request to ExportSource.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ExportSource
     */
    void checkPermissionExportSource(AuthContext authContext,
            HierarchicalTableSourceExportRequest request, List<Table> sourceTables);

    /**
     * A default implementation that funnels all requests to invoke {@code checkPermission}.
     */
    abstract class DelegateAll implements HierarchicalTableServiceContextualAuthWiring {
        protected abstract void checkPermission(AuthContext authContext, List<Table> sourceTables);

        public void checkPermissionRollup(AuthContext authContext, RollupRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionTree(AuthContext authContext, TreeRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionApply(AuthContext authContext, HierarchicalTableApplyRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
                List<Table> sourceTables) {
            checkPermission(authContext, sourceTables);
        }

        public void checkPermissionExportSource(AuthContext authContext,
                HierarchicalTableSourceExportRequest request, List<Table> sourceTables) {
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

    class TestUseOnly implements HierarchicalTableServiceContextualAuthWiring {
        public HierarchicalTableServiceContextualAuthWiring delegate;

        public void checkPermissionRollup(AuthContext authContext, RollupRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionRollup(authContext, request, sourceTables);
            }
        }

        public void checkPermissionTree(AuthContext authContext, TreeRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionTree(authContext, request, sourceTables);
            }
        }

        public void checkPermissionApply(AuthContext authContext, HierarchicalTableApplyRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionApply(authContext, request, sourceTables);
            }
        }

        public void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionView(authContext, request, sourceTables);
            }
        }

        public void checkPermissionExportSource(AuthContext authContext,
                HierarchicalTableSourceExportRequest request, List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionExportSource(authContext, request, sourceTables);
            }
        }
    }
}
