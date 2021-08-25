package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.TimeTable;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class TimeTableGrpcImpl extends GrpcTableOperation<TimeTableRequest> {

    private final Scheduler scheduler;
    private final LiveTableMonitor liveTableMonitor;

    @Inject()
    public TimeTableGrpcImpl(final Scheduler scheduler, final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getTimeTable, TimeTableRequest::getResultId);
        this.scheduler = scheduler;
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public void validateRequest(final TimeTableRequest request) throws StatusRuntimeException {
        final long periodNanos = request.getPeriodNanos();
        if (periodNanos <= 0) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "periodNanos must be >= 0 (found: " + periodNanos + ")");
        }
    }

    @Override
    public Table create(final TimeTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);
        final long startTime = request.getStartTimeNanos();
        final long periodValue = request.getPeriodNanos();
        final TimeTable timeTable =
                new TimeTable(scheduler, startTime <= 0 ? null : DBTimeUtils.nanosToTime(startTime), periodValue);
        liveTableMonitor.addTable(timeTable);
        return timeTable;
    }
}
