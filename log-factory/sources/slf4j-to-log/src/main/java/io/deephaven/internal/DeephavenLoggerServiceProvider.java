package io.deephaven.internal;

import com.google.auto.service.AutoService;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

@AutoService(SLF4JServiceProvider.class)
public class DeephavenLoggerServiceProvider implements SLF4JServiceProvider {
    private static final BasicMarkerFactory INSTANCE = new BasicMarkerFactory();

    // to avoid constant folding by the compiler, this field must *not* be final
    public static String REQUESTED_API_VERSION = "2.0.0-alpha4"; // !final

    @Override
    public ILoggerFactory getLoggerFactory() {
        return DeephavenLoggerFactory.INSTANCE;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return INSTANCE;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return new NOPMDCAdapter();
    }

    @Override
    public String getRequestedApiVersion() {
        return REQUESTED_API_VERSION;
    }

    @Override
    public void initialize() {

    }
}
