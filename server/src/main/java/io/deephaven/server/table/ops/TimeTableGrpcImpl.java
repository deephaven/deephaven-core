/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTimeUtils;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class TimeTableGrpcImpl extends GrpcTableOperation<TimeTableRequest> {

    private final Scheduler scheduler;

    @Inject()
    public TimeTableGrpcImpl(
            final TableServiceContextualAuthWiring authWiring,
            final Scheduler scheduler) {
        super(authWiring::checkPermissionTimeTable, BatchTableRequest.Operation::getTimeTable,
                TimeTableRequest::getResultId);
        this.scheduler = scheduler;
    }

    @Override
    public void validateRequest(final TimeTableRequest request) throws StatusRuntimeException {
        final long periodNanos = request.getPeriodNanos();
        if (periodNanos <= 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "periodNanos must be >= 0 (found: " + periodNanos + ")");
        }
    }

    @Override
    public Table create(final TimeTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);
        final long startTime = request.getStartTimeNanos();
        final long periodValue = request.getPeriodNanos();
        return new TimeTable(ExecutionContext.getContext().getUpdateGraph(), scheduler,
                startTime <= 0 ? null : DateTimeUtils.epochNanosToInstant(startTime), periodValue, false);
    }
}
