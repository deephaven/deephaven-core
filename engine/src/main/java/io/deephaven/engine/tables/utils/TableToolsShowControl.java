package io.deephaven.engine.tables.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface TableToolsShowControl {
    int getWidth();
}
