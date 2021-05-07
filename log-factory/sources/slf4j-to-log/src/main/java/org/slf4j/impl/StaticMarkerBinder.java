package org.slf4j.impl;

import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.spi.MarkerFactoryBinder;

public final class StaticMarkerBinder implements MarkerFactoryBinder {

    private static final BasicMarkerFactory INSTANCE = new BasicMarkerFactory();

    @Override
    public final IMarkerFactory getMarkerFactory() {
        return INSTANCE;
    }

    @Override
    public final String getMarkerFactoryClassStr() {
        return BasicMarkerFactory.class.getName();
    }
}
