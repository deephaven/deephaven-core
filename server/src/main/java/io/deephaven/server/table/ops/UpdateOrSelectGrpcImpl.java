//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.Selectable;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public abstract class UpdateOrSelectGrpcImpl extends GrpcTableOperation<SelectOrUpdateRequest> {
    @FunctionalInterface
    protected interface RealTableOperation {
        Table apply(Table source, Collection<? extends Selectable> columns);
    }

    private final RealTableOperation realTableOperation;
    private final boolean requiresSharedLock;

    protected UpdateOrSelectGrpcImpl(
            final PermissionFunction<SelectOrUpdateRequest> permission,
            final Function<BatchTableRequest.Operation, SelectOrUpdateRequest> getRequest,
            final RealTableOperation realTableOperation,
            final boolean requiresSharedLock) {
        super(permission, getRequest, SelectOrUpdateRequest::getResultId, SelectOrUpdateRequest::getSourceId);
        this.realTableOperation = realTableOperation;
        this.requiresSharedLock = requiresSharedLock;
    }

    @Override
    public Table create(final SelectOrUpdateRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] columnSpecs = request.getColumnSpecsList().toArray(String[]::new);
        final SelectColumn[] expressions = SelectColumnFactory.getExpressions(columnSpecs);
        ColumnExpressionValidator.validateColumnExpressions(expressions, columnSpecs, parent);

        if (parent.isRefreshing() && requiresSharedLock) {
            final UpdateGraph updateGraph = parent.getUpdateGraph();
            return updateGraph.sharedLock().computeLocked(
                    () -> realTableOperation.apply(parent, Arrays.asList(expressions)));
        }

        return realTableOperation.apply(parent, Arrays.asList(expressions));
    }

    @Singleton
    public static class UpdateGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public UpdateGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionUpdate, BatchTableRequest.Operation::getUpdate,
                    Table::update, true);
        }
    }

    @Singleton
    public static class LazyUpdateGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public LazyUpdateGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionLazyUpdate, BatchTableRequest.Operation::getLazyUpdate,
                    Table::lazyUpdate, true);
        }
    }

    @Singleton
    public static class ViewGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public ViewGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionView, BatchTableRequest.Operation::getView,
                    Table::view, false);
        }
    }

    @Singleton
    public static class UpdateViewGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public UpdateViewGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionUpdateView, BatchTableRequest.Operation::getUpdateView,
                    Table::updateView, false);
        }
    }

    @Singleton
    public static class SelectGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public SelectGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionSelect, BatchTableRequest.Operation::getSelect,
                    Table::select, true);
        }
    }
}
