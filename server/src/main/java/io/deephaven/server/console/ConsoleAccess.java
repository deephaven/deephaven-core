package io.deephaven.server.console;

import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.CancelCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.server.session.SessionState;

public interface ConsoleAccess {

    SessionState getConsoleTypes(GetConsoleTypesRequest request);

    SessionState startConsole(StartConsoleRequest request);

    SessionState subscribeToLogs(LogSubscriptionRequest request);

    SessionState executeCommand(ExecuteCommandRequest request);

    SessionState cancelCommand(CancelCommandRequest request);

    SessionState bindTableToVariable(BindTableToVariableRequest request);

    SessionState autoCompleteStream();

}
