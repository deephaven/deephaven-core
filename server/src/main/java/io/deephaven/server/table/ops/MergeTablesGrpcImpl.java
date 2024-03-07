//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class MergeTablesGrpcImpl extends GrpcTableOperation<MergeTablesRequest> {

    @Inject
    public MergeTablesGrpcImpl(
            final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionMergeTables, BatchTableRequest.Operation::getMerge,
                MergeTablesRequest::getResultId, MergeTablesRequest::getSourceIdsList);
    }

    @Override
    public void validateRequest(final MergeTablesRequest request) throws StatusRuntimeException {
        if (request.getSourceIdsList().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Cannot merge zero source tables.");
        }
    }

    @Override
    public Table create(final MergeTablesRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.gt(sourceTables.size(), "sourceTables.size()", 0);

        final String keyColumn = request.getKeyColumn();
        final List<Table> tables = sourceTables.stream()
                .map(SessionState.ExportObject::get)
                .collect(Collectors.toList());

        if (tables.stream().noneMatch(Table::isRefreshing)) {
            return keyColumn.isEmpty() ? TableTools.merge(tables) : TableTools.mergeSorted(keyColumn, tables);
        } else {
            final UpdateGraph updateGraph = tables.get(0).getUpdateGraph(tables.toArray(Table[]::new));
            Table result;
            try (final SafeCloseable ignored = updateGraph.sharedLock().lockCloseable()) {
                result = TableTools.merge(tables);
            }
            if (!keyColumn.isEmpty()) {
                result = result.sort(keyColumn);
            }
            return result;
        }
    }
}
