/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.BindsInstance;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.PrintStream;

public interface DeephavenApiServerComponent {

    DeephavenApiServer getServer();

    interface Builder<B extends Builder<B>> {
        @BindsInstance
        B withOut(@Nullable @Named("out") PrintStream out);

        @BindsInstance
        B withErr(@Nullable @Named("err") PrintStream err);
    }
}
