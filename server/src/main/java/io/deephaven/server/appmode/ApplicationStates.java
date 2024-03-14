//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;

import java.util.Optional;

public interface ApplicationStates {

    Optional<ApplicationState> getApplicationState(String applicationId);
}
