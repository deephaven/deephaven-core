package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;

import java.util.Optional;

public interface ApplicationStates {

    Optional<ApplicationState> getApplicationState(String applicationId);
}
