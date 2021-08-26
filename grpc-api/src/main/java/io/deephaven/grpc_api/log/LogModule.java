package io.deephaven.grpc_api.log;

import io.deephaven.base.system.StandardStreamReceiver;
import io.deephaven.base.system.StandardStreamState;
import io.deephaven.base.system.StreamToPrintStreams;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.StreamToLogBuffer;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.internal.log.InitSink;
import io.deephaven.internal.log.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Module
public interface LogModule {

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
    @Singleton // StreamToLogBuffer maintains state
    static StreamToLogBuffer providesStreamToLogBuffer(LogBuffer logBuffer) {
        final boolean toStdout = Boolean.getBoolean("stdout.toLogBuffer");
        final boolean toStderr = Boolean.getBoolean("stderr.toLogBuffer");
        // TODO (core#90): Add configuration and documentation for LogBufferOutputStream
        return new StreamToLogBuffer(logBuffer, toStdout, toStderr, 256, 1 << 19); // 512 KiB
    }

    @Provides
    static StreamToPrintStreams providesStreamToReal(@Named("out") PrintStream out, @Named("err") PrintStream err) {
        final boolean skipStdout = Boolean.getBoolean("stdout.skipReal");
        final boolean skipStderr = Boolean.getBoolean("stderr.skipReal");
        return new StreamToPrintStreams(skipStdout ? null : out, skipStderr ? null : err);
    }

    @Provides
    @Singleton
    static StandardStreamState providesStandardStreamState(Set<StandardStreamReceiver> receivers) {
        return new StandardStreamState(receivers);
    }

    @Binds
    @IntoSet
    StandardStreamReceiver bindsStreamToReal(StreamToPrintStreams streamToReal);

    @Binds
    @IntoSet
    StandardStreamReceiver bindsStreamToLogBufferInstance(StreamToLogBuffer streamToLogBuffer);
}
