package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;

import java.util.Optional;

public interface ApplicationStates {

    Optional<ApplicationState> getApplicationState(String applicationId);
}
