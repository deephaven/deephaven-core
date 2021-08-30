package io.deephaven.util;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.*;

public final class GrpcLogging {
    public static void setupFromBooleanProperty(
        final String property, final boolean defaultValue, final String shadowPath) {
        final boolean logAllDefault = defaultValue;
        final String logAllStr = System.getProperty(property);
        final boolean logAll =
            (logAllStr == null) ? logAllDefault : Boolean.parseBoolean(logAllStr);
        if (!logAll) {
            return;
        }
        setAllLogging(shadowPath + ".");
    }

    private static void setAllLogging(final String shadowPfx) {
        final SimpleFormatter formatter = new SimpleFormatter();
        // Set ALL for top levels.
        final Logger grpcLogger = Logger.getLogger(shadowPfx + "io.grpc");
        grpcLogger.setLevel(Level.ALL);
        final BridgingLogHandler handler = new BridgingLogHandler();
        handler.setFormatter(formatter);
        // Loggers only filter records; the records are actually published, or not, by the handlers.
        // Thus if the handler is not also setLevel, none of this works.
        handler.setLevel(Level.ALL);

        // Explicitly set INFO, instead of ALL logging at specific lower levels that generate
        // frequent, useless, annoying FINE level messages otherwise.
        // This avoids polluting our console logs with distracting stuff that makes it harder
        // to debug actual issues.
        final Set<String> filteredSet = new HashSet<>();
        for (String annoyingLoggingPath : new String[] {
                "io.grpc.netty.Utils",
                "io.grpc.netty.GrpcSslContexts",
                "io.grpc.InternalChannelz",
                "io.grpc.Context",
                "io.grpc.NameResolverRegistry",
                "io.grpc.LoadBalancerRegistry",
                "io.grpc.ChannelLogger",
                "io.grpc.netty.NettyClientHandler",
        }) {
            filteredSet.add(shadowPfx + annoyingLoggingPath);
        }
        handler.setFilter((final LogRecord record) -> {
            if (record.getLevel().intValue() >= Level.INFO.intValue()) {
                return true;
            }
            final String loggerName = record.getLoggerName();
            if (loggerName == null) {
                return false;
            }
            return !filteredSet.contains(loggerName);
        });
        grpcLogger.addHandler(handler);
    }
}
