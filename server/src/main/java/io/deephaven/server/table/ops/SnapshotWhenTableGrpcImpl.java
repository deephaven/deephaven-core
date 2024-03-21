//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Builder;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;

@Singleton
public final class SnapshotWhenTableGrpcImpl extends GrpcTableOperation<SnapshotWhenTableRequest> {

    private static List<TableReference> refs(SnapshotWhenTableRequest request) {
        return Arrays.asList(request.getBaseId(), request.getTriggerId());
    }

    private static SnapshotWhenOptions options(SnapshotWhenTableRequest request) {
        final Builder builder = SnapshotWhenOptions.builder();
        if (request.getInitial()) {
            builder.addFlags(Flag.INITIAL);
        }
        if (request.getIncremental()) {
            builder.addFlags(Flag.INCREMENTAL);
        }
        if (request.getHistory()) {
            builder.addFlags(Flag.HISTORY);
        }
        for (String stampColumn : request.getStampColumnsList()) {
            builder.addStampColumns(JoinAddition.parse(stampColumn));
        }
        return builder.build();
    }

    @Inject
    public SnapshotWhenTableGrpcImpl(final TableServiceContextualAuthWiring auth) {
        super(
                auth::checkPermissionSnapshotWhen,
                Operation::getSnapshotWhen,
                SnapshotWhenTableRequest::getResultId,
                SnapshotWhenTableGrpcImpl::refs);
    }

    @Override
    public void validateRequest(SnapshotWhenTableRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, SnapshotWhenTableRequest.BASE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, SnapshotWhenTableRequest.TRIGGER_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getBaseId());
        Common.validate(request.getTriggerId());
        try {
            options(request);
        } catch (UnsupportedOperationException e) {
            throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED, e.getMessage());
        }
    }

    @Override
    public Table create(
            final SnapshotWhenTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        final Table base = sourceTables.get(0).get();
        final Table trigger = sourceTables.get(1).get();
        final SnapshotWhenOptions options = options(request);
        try (final SafeCloseable ignored = lock(base, trigger)) {
            return base.snapshotWhen(trigger, options);
        }
    }

    private SafeCloseable lock(Table base, Table trigger) {
        if (base.isRefreshing()) {
            UpdateGraph updateGraph = base.getUpdateGraph();
            return updateGraph.sharedLock().lockCloseable();
        } else if (trigger.isRefreshing()) {
            UpdateGraph updateGraph = trigger.getUpdateGraph();
            return updateGraph.sharedLock().lockCloseable();
        }
        return null;
    }
}
