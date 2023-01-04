/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Builder;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Feature;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Singleton
public final class SnapshotTableGrpcImpl extends GrpcTableOperation<SnapshotWhenTableRequest> {

    private static List<TableReference> refs(SnapshotWhenTableRequest request) {
        return Arrays.asList(request.getBaseId(), request.getTriggerId());
    }

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public SnapshotTableGrpcImpl(
            final TableServiceContextualAuthWiring auth,
            final UpdateGraphProcessor updateGraphProcessor) {
        super(
                auth::checkPermissionSnapshot,
                Operation::getSnapshotWhen,
                SnapshotWhenTableRequest::getResultId,
                SnapshotTableGrpcImpl::refs);
        this.updateGraphProcessor = Objects.requireNonNull(updateGraphProcessor);
    }

    @Override
    public void validateRequest(SnapshotWhenTableRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, SnapshotWhenTableRequest.BASE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, SnapshotWhenTableRequest.TRIGGER_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getBaseId());
        Common.validate(request.getTriggerId());
        if (request.getHistory()) {
            if (request.getInitial() || request.getIncremental() || !request.getStampColumnsList().isEmpty()) {
                final Descriptor descriptor = SnapshotWhenTableRequest.getDescriptor();
                throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED, String.format(
                        "%s with %s (%d) == true does not support %s (%d), %s (%d), nor non-empty %s (%d)",
                        descriptor.getFullName(),
                        descriptor.findFieldByNumber(SnapshotWhenTableRequest.HISTORY_FIELD_NUMBER).getName(),
                        SnapshotWhenTableRequest.HISTORY_FIELD_NUMBER,
                        descriptor.findFieldByNumber(SnapshotWhenTableRequest.INITIAL_FIELD_NUMBER).getName(),
                        SnapshotWhenTableRequest.INITIAL_FIELD_NUMBER,
                        descriptor.findFieldByNumber(SnapshotWhenTableRequest.INCREMENTAL_FIELD_NUMBER).getName(),
                        SnapshotWhenTableRequest.INCREMENTAL_FIELD_NUMBER,
                        descriptor.findFieldByNumber(SnapshotWhenTableRequest.STAMP_COLUMNS_FIELD_NUMBER).getName(),
                        SnapshotWhenTableRequest.STAMP_COLUMNS_FIELD_NUMBER));
            }
        }
    }

    @Override
    public Table create(
            final SnapshotWhenTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        final Table base = sourceTables.get(0).get();
        final Table trigger = sourceTables.get(1).get();
        final Builder builder = SnapshotWhenOptions.builder();
        if (request.getInitial()) {
            builder.addFlags(Feature.INITIAL);
        }
        if (request.getIncremental()) {
            builder.addFlags(Feature.INCREMENTAL);
        }
        if (request.getHistory()) {
            builder.addFlags(Feature.HISTORY);
        }
        for (String stampColumn : request.getStampColumnsList()) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        try (final SafeCloseable _lock = lock(base, trigger)) {
            return base.snapshotWhen(trigger, builder.build());
        }
    }

    private SafeCloseable lock(Table base, Table trigger) {
        return base.isRefreshing() || trigger.isRefreshing() ? updateGraphProcessor.sharedLock().lockCloseable() : null;
    }
}
