package io.deephaven.dbtypes;

import io.deephaven.configuration.Configuration;

import java.lang.reflect.Constructor;

class FactoryInstances {
    private FactoryInstances() {}

    private static final DbFileFactory FILE_FACTORY;

    static {
        try {
            final String implClassName = Configuration.getInstance().getProperty("DbTypes.DbFile.impl");
            final Class c = Class.forName(implClassName);
            final Constructor constructor = c.getConstructor();
            FILE_FACTORY = (DbFileFactory) constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create file factory.", e);
        }
    }

    static DbFileFactory getFileFactory() {
        return FILE_FACTORY;
    }

    private static final DbImageFactory IMAGE_FACTORY;

    static {
        try {
            final String implClassName = Configuration.getInstance().getProperty("DbTypes.DbImage.impl");
            final Class c = Class.forName(implClassName);
            final Constructor constructor = c.getConstructor();
            IMAGE_FACTORY = (DbImageFactory) constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create image factory.", e);
        }
    }

    static DbImageFactory getImageFactory() {
        return IMAGE_FACTORY;
    }


}
