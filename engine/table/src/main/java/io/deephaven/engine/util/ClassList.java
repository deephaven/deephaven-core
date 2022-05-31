/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Read a list of classes from a classpath resource.
 */
public class ClassList {
    @NotNull
    public static Class[] readClassList(String resourceName) throws IOException, ClassNotFoundException {
        final ArrayList<String> classString = getClassStrings(resourceName);

        final Class[] classList = new Class[classString.size()];
        for (int i = 0; i < classString.size(); i++) {
            classList[i] = Class.forName(classString.get(i));
        }
        return classList;
    }

    @NotNull
    public static Collection<Class<?>> readClassListAsCollection(String resourceName)
            throws IOException, ClassNotFoundException {
        final ArrayList<String> classString = getClassStrings(resourceName);

        ArrayList<Class<?>> result = new ArrayList<>(classString.size());

        for (final String c : classString) {
            result.add(Class.forName(c));
        }

        return result;
    }

    @NotNull
    private static ArrayList<String> getClassStrings(String resourceName) throws IOException {
        final String[] resourceNameAry = resourceName.split(";");

        final ArrayList<String> classString = new ArrayList<>();

        for (String resourceNameLocal : resourceNameAry) {

            final InputStream pushListStream = ClassList.class.getResourceAsStream("/" + resourceNameLocal);
            if (pushListStream == null) {
                throw new IOException("Could not open class list: " + resourceNameLocal);
            }
            final BufferedReader file = new BufferedReader(new InputStreamReader(pushListStream));

            String c;

            while ((c = file.readLine()) != null) {
                c = c.trim();
                if (c.length() > 0 && c.charAt(0) != '#') {
                    // No idea why this was here, pretty sure it's unnecessary: c = c.replace(" ", "");
                    classString.add(c);
                }
            }
            file.close();
        }

        return classString;
    }

    @NotNull
    public static Collection<Package> readPackageList(String resourceName) throws IOException {
        final ArrayList<String> classString = getClassStrings(resourceName);

        ArrayList<Package> result = new ArrayList<>(classString.size());

        for (final String c : classString) {
            result.add(Package.getPackage(c));
        }

        return result;
    }
}
