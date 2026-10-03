//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.TestControlledScheduler;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlightSqlResolverExpiredSessionTest {

    /**
     * A session can expire between the handler's session check and the prepared statement registering its close
     * callback; the registration then throws, and nothing the statement published before it may be left behind.
     */
    @Test
    void createPreparedStatementOnExpiredSessionLeavesNothingBehind() {
        final TestControlledScheduler scheduler = new TestControlledScheduler();
        final FlightSqlResolver resolver = new FlightSqlResolver(new TestAuthorizationProvider(), scheduler);
        // a session whose expiration has been cleared, as onExpired() leaves it
        final SessionState session = new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                TestExecutionContext::createForUnitTests, new AuthContext.SuperUser());
        assertThat(session.isExpired()).isTrue();

        final Action action = new Action(FlightSqlActionHelper.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                Any.pack(ActionCreatePreparedStatementRequest.newBuilder().setQuery("SELECT 1").build())
                        .toByteArray());
        assertThatThrownBy(() -> resolver.doAction(session, action, new NoopObserver()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Code.UNAUTHENTICATED));
        assertThat(resolver.numPreparedStatements()).isZero();
    }

    private static final class NoopObserver implements StreamObserver<Result> {
        @Override
        public void onNext(final Result value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}
    }
}
