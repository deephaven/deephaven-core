package io.deephaven.engine.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface TableToolsShowControl {
    int getWidth();
}
