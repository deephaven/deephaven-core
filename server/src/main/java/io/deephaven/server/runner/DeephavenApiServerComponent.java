package io.deephaven.server.runner;

import dagger.BindsInstance;

import javax.inject.Named;
import java.io.PrintStream;

public interface DeephavenApiServerComponent {

    DeephavenApiServer getServer();

    interface Builder<B extends Builder<B>> {

        @BindsInstance
        B withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

        @BindsInstance
        B withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

        @BindsInstance
        B withOut(@Named("out") PrintStream out);

        @BindsInstance
        B withErr(@Named("err") PrintStream err);
    }
}
