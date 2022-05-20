package io.deephaven.server.console;

import com.google.rpc.Code;
import io.deephaven.configuration.Configuration;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.CancelCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.auth.AuthContext.SuperUser;

import java.util.Objects;

public class ConsoleAccessDefaultImpl implements ConsoleAccess {
    public static final String CONSOLE_DISABLED_PROP = "deephaven.console.disable";
    public static final boolean REMOTE_CONSOLE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault(CONSOLE_DISABLED_PROP, false);

    private final SessionService sessionService;

    public ConsoleAccessDefaultImpl(SessionService sessionService) {
        this.sessionService = Objects.requireNonNull(sessionService);
    }

    private SessionState check() {
        if (REMOTE_CONSOLE_DISABLED) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Remote console disabled");
        }
        final SessionState currentSession = sessionService.getCurrentSession();
        if (currentSession.getAuthContext() instanceof SuperUser) {
            return currentSession;
        }
        throw GrpcUtil.statusRuntimeException(Code.PERMISSION_DENIED, "Only super user is allowed to use the console");
    }

    @Override
    public SessionState getConsoleTypes(GetConsoleTypesRequest request) {
        return sessionService.getCurrentSession();
    }

    @Override
    public SessionState startConsole(StartConsoleRequest request) {
        return check();
    }

    @Override
    public SessionState subscribeToLogs(LogSubscriptionRequest request) {
        return check();
    }

    @Override
    public SessionState executeCommand(ExecuteCommandRequest request) {
        return check();
    }

    @Override
    public SessionState cancelCommand(CancelCommandRequest request) {
        return check();
    }

    @Override
    public SessionState bindTableToVariable(BindTableToVariableRequest request) {
        return check();
    }

    @Override
    public SessionState autoCompleteStream() {
        return check();
    }
}
