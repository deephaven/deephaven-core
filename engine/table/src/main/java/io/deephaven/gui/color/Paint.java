/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

/**
 * Color abstraction.
 */
public interface Paint {

    /**
     * Gets the Java object representative of this Paint.
     *
     * @return Java object representative of this Paint
     */
    java.awt.Paint javaColor();

}
