package io.deephaven.server.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.function.Function;

public abstract class UpdateOrSelectGrpcImpl extends GrpcTableOperation<SelectOrUpdateRequest> {
    @FunctionalInterface
    protected interface RealTableOperation {
        Table apply(Table source, SelectColumn[] selectColumns);
    }

    private final RealTableOperation realTableOperation;
    private final boolean requiresSharedLock;

    protected UpdateOrSelectGrpcImpl(
            final Function<BatchTableRequest.Operation, SelectOrUpdateRequest> getRequest,
            final RealTableOperation realTableOperation, final boolean requiresSharedLock) {
        super(getRequest, SelectOrUpdateRequest::getResultId, SelectOrUpdateRequest::getSourceId);
        this.realTableOperation = realTableOperation;
        this.requiresSharedLock = requiresSharedLock;
    }

    @Override
    public Table create(final SelectOrUpdateRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] columnSpecs = request.getColumnSpecsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectColumn[] expressions = SelectColumnFactory.getExpressions(columnSpecs);
        ColumnExpressionValidator.validateColumnExpressions(expressions, columnSpecs, parent);

        if (parent.isRefreshing() && requiresSharedLock) {
            return UpdateGraphProcessor.DEFAULT.sharedLock()
                    .computeLocked(() -> realTableOperation.apply(parent, expressions));
        }

        return realTableOperation.apply(parent, expressions);
    }

    @Singleton
    public static class UpdateGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public UpdateGrpcImpl() {
            super(BatchTableRequest.Operation::getUpdate, Table::update, true);
        }
    }

    @Singleton
    public static class LazyUpdateGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public LazyUpdateGrpcImpl() {
            super(BatchTableRequest.Operation::getLazyUpdate, Table::lazyUpdate, true);
        }
    }

    @Singleton
    public static class ViewGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public ViewGrpcImpl() {
            super(BatchTableRequest.Operation::getView, Table::view, false);
        }
    }

    @Singleton
    public static class UpdateViewGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public UpdateViewGrpcImpl() {
            super(BatchTableRequest.Operation::getUpdateView, Table::updateView, false);
        }
    }

    @Singleton
    public static class SelectGrpcImpl extends UpdateOrSelectGrpcImpl {
        @Inject
        public SelectGrpcImpl() {
            super(BatchTableRequest.Operation::getSelect, Table::select, true);
        }
    }
}
