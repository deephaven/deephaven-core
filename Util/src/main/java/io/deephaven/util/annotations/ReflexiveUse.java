//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.annotations;

/**
 * Tagging attribute to indicate a method or class is used reflexively and should not be deleted.
 */
public @interface ReflexiveUse {
    String[] referrers();
}
