package io.deephaven.server.appmode;

import dagger.Provides;

import java.io.IOException;

public interface ApplicationInjector {
    void run() throws IOException, ClassNotFoundException;

    /**
     * An no-op implementation.
     */
    enum Noop implements ApplicationInjector {
        INSTANCE;

        /**
         * Provides {@link #INSTANCE} as {@link ApplicationInjector}.
         */
        @dagger.Module
        public interface Module {

            @Provides
            static ApplicationInjector providesImpl() {
                return INSTANCE;
            }
        }

        @Override
        public void run() {

        }
    }
}
