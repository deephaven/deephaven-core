//
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
// ---------------------------------------------------------------------
// This class is generated by GenerateContextualAuthWiring. DO NOT EDIT!
// ---------------------------------------------------------------------
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.HierarchicalTableViewRequest;
import io.deephaven.proto.backplane.grpc.RollupRequest;
import io.deephaven.proto.backplane.grpc.TreeRequest;
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
     * Authorize a request to View.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @param sourceTables the operation's source tables
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke View
     */
    void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
            List<Table> sourceTables);

    class AllowAll implements HierarchicalTableServiceContextualAuthWiring {
        public void checkPermissionRollup(AuthContext authContext, RollupRequest request,
                List<Table> sourceTables) {}

        public void checkPermissionTree(AuthContext authContext, TreeRequest request,
                List<Table> sourceTables) {}

        public void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
                List<Table> sourceTables) {}
    }

    class DenyAll implements HierarchicalTableServiceContextualAuthWiring {
        public void checkPermissionRollup(AuthContext authContext, RollupRequest request,
                List<Table> sourceTables) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void checkPermissionTree(AuthContext authContext, TreeRequest request,
                List<Table> sourceTables) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
                List<Table> sourceTables) {
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

        public void checkPermissionView(AuthContext authContext, HierarchicalTableViewRequest request,
                List<Table> sourceTables) {
            if (delegate != null) {
                delegate.checkPermissionView(authContext, request, sourceTables);
            }
        }
    }
}
