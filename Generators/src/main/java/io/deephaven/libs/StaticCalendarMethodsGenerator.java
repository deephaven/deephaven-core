//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.libs;

import io.deephaven.gen.AbstractBasicJavaGenerator;
import io.deephaven.gen.GenUtils;
import io.deephaven.gen.JavaFunction;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Generate a static library containing methods that use the default business calendar.
 */
public class StaticCalendarMethodsGenerator extends AbstractBasicJavaGenerator {

    final Function<String, String> renamer;

    public StaticCalendarMethodsGenerator(String gradleTask, String packageName, String className, String[] imports,
            Predicate<Method> includeMethod, Collection<Predicate<JavaFunction>> skipsGen,
            Function<String, String> renamer, Level logLevel) throws ClassNotFoundException {
        super(gradleTask, packageName, className, imports, includeMethod, skipsGen, logLevel);
        this.renamer = renamer;
    }

    @Override
    public String generateClassJavadoc() {
        String code = "";
        code += "/**\n";
        code += " * Static versions of business calendar methods that use the default business calendar.\n";
        code += " *\n";
        code += " * @see io.deephaven.time.calendar\n";
        code += " * @see io.deephaven.time.calendar.Calendar\n";
        code += " * @see io.deephaven.time.calendar.BusinessCalendar\n";
        code += " * @see io.deephaven.time.calendar.Calendars\n";
        code += " */\n";
        return code;
    }

    @Override
    public String generateFunction(JavaFunction f) {

        final String rename = renamer.apply(f.getMethodName());
        final JavaFunction f2 = f.transform(null, null, rename, null);

        final String javadoc = "    /** @see " + f.getClassName() + "#" + f.getMethodName() + "(" +
                Arrays.stream(f.getParameterTypes()).map(GenUtils::javadocLinkParamTypeString)
                        .collect(Collectors.joining(","))
                +
                ") */";
        final String sigPrefix = "public static";
        final String callArgs = GenUtils.javaArgString(f2, false);
        final String funcBody = " {return Calendars.calendar()." + f.getMethodName() + "(" + callArgs + " );}\n";
        return GenUtils.javaFunction(f2, sigPrefix, javadoc, funcBody);
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        final String gradleTask = ":Generators:generateStaticCalendarMethods";
        final String packageName = "io.deephaven.time.calendar";
        final String className = "StaticCalendarMethods";

        final String relativeFilePath =
                "/engine/time/src/main/java/io/deephaven/time/calendar/StaticCalendarMethods.java";

        final String[] imports = {
                "io.deephaven.time.calendar.BusinessCalendar",
        };

        final Set<String> excludes = new HashSet<>();
        excludes.add("toString");
        excludes.add("name");
        excludes.add("description");
        excludes.add("firstValidDate");
        excludes.add("lastValidDate");
        excludes.add("clearCache");

        StaticCalendarMethodsGenerator gen =
                new StaticCalendarMethodsGenerator(gradleTask, packageName, className, imports,
                        (m) -> Modifier.isPublic(m.getModifiers()) && !m.getDeclaringClass().equals(Object.class),
                        Collections.singletonList((f) -> excludes.contains(f.getMethodName())),
                        (s) -> {
                            if (s.equals("dayOfWeek")) {
                                return "calendarDayOfWeek";
                            } else if (s.equals("dayOfWeekValue")) {
                                return "calendarDayOfWeekValue";
                            } else if (s.equals("timeZone")) {
                                return "calendarTimeZone";
                            } else {
                                return s;
                            }
                        },
                        Level.WARNING);

        runCommandLine(gen, relativeFilePath, args);
    }

}
