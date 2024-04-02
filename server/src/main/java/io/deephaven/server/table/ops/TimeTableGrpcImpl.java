//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.updateby.spec.CumMinMaxSpec;
import io.deephaven.api.updateby.spec.CumSumSpec;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTimeUtils;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
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
        final long periodNanos = adaptPeriod(request);
        if (periodNanos <= 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "periodNanos must be >= 0 (found: " + periodNanos + ")");
        }
    }

    @Override
    public Table create(final TimeTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);

        return new TimeTable(ExecutionContext.getContext().getUpdateGraph(), scheduler,
                adaptStartTime(request),
                adaptPeriod(request),
                request.getBlinkTable());
    }

    private static Instant adaptStartTime(@SuppressWarnings("unused") final TimeTableRequest request) {
        if (request.hasStartTimeString()) {
            return DateTimeUtils.parseInstant(request.getStartTimeString());
        }
        // Return null if start time nanos is zero or negative.
        return request.getStartTimeNanos() <= 0 ? null : DateTimeUtils.epochNanosToInstant(request.getStartTimeNanos());
    }

    private static long adaptPeriod(@SuppressWarnings("unused") final TimeTableRequest request) {
        if (request.hasPeriodString()) {
            return DateTimeUtils.parseDurationNanos(request.getPeriodString());
        }
        return request.getPeriodNanos();
    }
}
