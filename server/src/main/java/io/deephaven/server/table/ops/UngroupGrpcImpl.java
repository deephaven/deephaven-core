//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class UngroupGrpcImpl extends GrpcTableOperation<UngroupRequest> {

    @Inject
    public UngroupGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionUngroup, BatchTableRequest.Operation::getUngroup,
                UngroupRequest::getResultId, UngroupRequest::getSourceId);
    }

    @Override
    public Table create(final UngroupRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table parent = sourceTables.get(0).get();
        final List<ColumnName> columnsToUngroup = request.getColumnsToUngroupList()
                .stream()
                .map(ColumnName::of)
                .collect(Collectors.toList());
        try (final SafeCloseable ignored = lock(parent)) {
            return parent.ungroup(request.getNullFill(), columnsToUngroup);
        }
    }

    private static SafeCloseable lock(Table base) {
        if (base.isRefreshing()) {
            return base.getUpdateGraph().sharedLock().lockCloseable();
        } else {
            return null;
        }
    }
}
