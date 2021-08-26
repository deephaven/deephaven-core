/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.Predicate;

@SuppressWarnings("WeakerAccess")
public class OSUtil {

    public enum OSFamily {

        LINUX(name -> name.startsWith("Linux")), WINDOWS(name -> name.contains("Windows")), MAC_OS(
                name -> name.startsWith("Mac OS")), SOLARIS(name -> name.startsWith("SunOs"));

        private final Predicate<String> nameMatcher;

        OSFamily(@NotNull final Predicate<String> nameMatcher) {
            this.nameMatcher = nameMatcher;
        }

        private boolean matchesName(@NotNull final String osName) {
            return nameMatcher.test(osName);
        }
    }

    public static OSFamily getOSFamily() {
        final String name = getOSName();
        final OSFamily[] matchingFamilies =
                Arrays.stream(OSFamily.values()).filter(family -> family.matchesName(name)).toArray(OSFamily[]::new);
        if (matchingFamilies.length == 0) {
            throw new IllegalArgumentException("Unknown OS family for OS name " + name);
        }
        if (matchingFamilies.length > 1) {
            throw new IllegalArgumentException(
                    "Ambiguous OS family for OS name " + name + ", matches: " + Arrays.toString(matchingFamilies));
        }
        return matchingFamilies[0];
    }

    public static boolean runningLinux() {
        return OSFamily.LINUX.matchesName(getOSName());
    }

    public static boolean runningWindows() {
        return OSFamily.WINDOWS.matchesName(getOSName());
    }

    public static boolean runningMacOS() {
        return OSFamily.MAC_OS.matchesName(getOSName());
    }

    public static boolean runningSolaris() {
        return OSFamily.SOLARIS.matchesName(getOSName());
    }

    private static String getOSName() {
        return System.getProperty("os.name");
    }
}
