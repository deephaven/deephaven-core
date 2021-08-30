package io.deephaven.util.annotations;

/**
 * Tagging attribute to indicate a method or class is used reflexively and should not be deleted.
 */
public @interface ReflexiveUse {
    String[] referrers();
}
