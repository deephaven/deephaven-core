//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.python.server;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.base.system.StandardStreamState;
import io.deephaven.internal.log.InitSink;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.server.log.LogModule;

import javax.inject.Singleton;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Module
public interface EmbeddedPyLogModule {

    @Provides
    static LogBuffer providesLogBuffer() {
        return LogBufferGlobal.getInstance().orElseThrow(() -> new RuntimeException("No global LogBuffer found"));
    }

    @Provides
    static LogSink providesLogSink() {
        // In contract, this should be a singleton - see LogInit#checkLogSinkIsSingleton()
        return LoggerFactory.getLogger(LogModule.class).getSink();
    }

    @Provides
    @ElementsIntoSet
    static Set<InitSink> providesLoggerSinkSetups() {
        return StreamSupport
                .stream(ServiceLoader.load(InitSink.class).spliterator(), false)
                .collect(Collectors.toSet());
    }

    @Provides
    @Singleton
    static StandardStreamState providesStandardStreamState() {
        return new StandardStreamState(Collections.emptySet());
    }

}
