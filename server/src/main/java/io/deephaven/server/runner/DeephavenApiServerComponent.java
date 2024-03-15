//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.BindsInstance;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.PrintStream;

public interface DeephavenApiServerComponent {

    DeephavenApiServer getServer();

    interface Builder<Self extends Builder<Self, Component>, Component extends DeephavenApiServerComponent> {
        @BindsInstance
        Self withOut(@Nullable @Named("out") PrintStream out);

        @BindsInstance
        Self withErr(@Nullable @Named("err") PrintStream err);

        Component build();
    }
}
