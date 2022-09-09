/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.io;

import io.deephaven.base.LowGarbageArrayIntegerMap;
import io.deephaven.base.LowGarbageArrayList;
import io.deephaven.base.LowGarbageArraySet;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;

import java.lang.reflect.Field;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelector;
import java.util.List;
import java.util.Set;

// --------------------------------------------------------------------
/**
 * General utilities for NIO
 */
public class NioUtil {

    private static final String JAVA_8_SPEC_VERSION = "1.8";

    // ----------------------------------------------------------------
    /**
     * Use reflection to change the collection implementations so iteration operations used in the selector
     * implementation will not produce garbage.
     *
     * <p>
     * This is only applied when the system property {@code java.specification.version} is equal to "1.8".
     *
     * <P>
     * We can do this because, by looking at the source code, we can tell that there are no simultaneous iterations so
     * reusing one iterator is OK. Because of concurrent modification issues and thread safety issues, this is generally
     * likely to be the case anyway. The implementation of selector is not likely to change between minor JDK revisions.
     * A major JDK release might produce a rewrite, but in that case we can check the JDK version and apply the
     * appropriate set of patches.
     */
    public static Selector reduceSelectorGarbage(Selector selector) {
        final String javaSpecificationVersion = System.getProperty("java.specification.version");
        if (JAVA_8_SPEC_VERSION.equals(javaSpecificationVersion)) {
            return reduceSelectorGarbageImpl(selector);
        }
        return selector;
    }

    private static Selector reduceSelectorGarbageImpl(Selector selector) {
        try {
            Class<?> selectorImplClass = Class.forName("sun.nio.ch.SelectorImpl");
            Require.instanceOf(selector, "selector", selectorImplClass);

            Field cancelledKeysField = AbstractSelector.class.getDeclaredField("cancelledKeys");
            cancelledKeysField.setAccessible(true);
            Set newCancelledKeys = new LowGarbageArraySet();
            cancelledKeysField.set(selector, newCancelledKeys);

            Field keysField = selectorImplClass.getDeclaredField("keys");
            keysField.setAccessible(true);
            Field publicKeysField = selectorImplClass.getDeclaredField("publicKeys");
            publicKeysField.setAccessible(true);
            Set newKeys = new LowGarbageArraySet();
            keysField.set(selector, newKeys);
            publicKeysField.set(selector, newKeys);

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            selectedKeysField.setAccessible(true);
            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
            publicSelectedKeysField.setAccessible(true);
            Set newSelectedKeys = new LowGarbageArraySet();
            selectedKeysField.set(selector, newSelectedKeys);
            publicSelectedKeysField.set(selector, newSelectedKeys);

            if (System.getProperty("os.name").startsWith("Windows")
                    && System.getProperty("java.vendor").startsWith("Oracle")) {
                Class<?> windowsSelectorImplClass = Class.forName("sun.nio.ch.WindowsSelectorImpl");
                Require.instanceOf(selector, "selector", windowsSelectorImplClass);

                Field threadsField = windowsSelectorImplClass.getDeclaredField("threads");
                threadsField.setAccessible(true);
                List newThreads = new LowGarbageArrayList();
                threadsField.set(selector, newThreads);

            } else if (System.getProperty("os.name").startsWith("Linux")) {
                Class<?> ePollSelectorImplClass = Class.forName("sun.nio.ch.EPollSelectorImpl");
                Require.instanceOf(selector, "selector", ePollSelectorImplClass);

                Field fdToKeyField = ePollSelectorImplClass.getDeclaredField("fdToKey");
                fdToKeyField.setAccessible(true);
                LowGarbageArrayIntegerMap newFdToKey = new LowGarbageArrayIntegerMap();
                fdToKeyField.set(selector, newFdToKey);

            } else if (System.getProperty("os.name").startsWith("SunOS")) {
                Class<?> devPollSelectorImplClass = Class.forName("sun.nio.ch.DevPollSelectorImpl");
                Require.instanceOf(selector, "selector", devPollSelectorImplClass);

                Field fdToKeyField = devPollSelectorImplClass.getDeclaredField("fdToKey");
                fdToKeyField.setAccessible(true);
                LowGarbageArrayIntegerMap newFdToKey = new LowGarbageArrayIntegerMap();
                fdToKeyField.set(selector, newFdToKey);
            }

            return selector;
        } catch (final NoSuchFieldException | IllegalAccessException | ClassNotFoundException e) {
            throw Assert.exceptionNeverCaught(e);
        }
    }
}
