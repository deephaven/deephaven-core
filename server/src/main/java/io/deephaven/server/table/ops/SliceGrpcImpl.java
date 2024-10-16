//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class SliceGrpcImpl extends GrpcTableOperation<SliceRequest> {

    @Inject
    public SliceGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionSlice, BatchTableRequest.Operation::getSlice,
                SliceRequest::getResultId, SliceRequest::getSourceId);
    }

    @Override
    public void validateRequest(SliceRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, SliceRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getSourceId());

        if ((request.getFirstPositionInclusive() >= 0 && request.getLastPositionExclusive() >= 0
                && request.getLastPositionExclusive() < request.getFirstPositionInclusive())) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Cannot slice with a non-negative start position that is after a non-negative end position.");
        }
        if ((request.getFirstPositionInclusive() < 0 && request.getLastPositionExclusive() < 0
                && request.getLastPositionExclusive() < request.getFirstPositionInclusive())) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Cannot slice with a negative start position that is after a negative end position.");
        }
    }

    @Override
    public final Table create(final SliceRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table sourceTable = sourceTables.get(0).get();
        return sourceTable.slice(request.getFirstPositionInclusive(), request.getLastPositionExclusive());
    }
}
