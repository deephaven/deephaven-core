package io.deephaven.engine.util;

import io.deephaven.plot.Figure;
import io.deephaven.engine.table.Table;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PlotReflectionPlaypen {
    public static void main(String[] args) {
        final Method[] methods = Figure.class.getMethods();
        System.out.println(methods.length);

        final List<Method> plotMethods =
                Arrays.stream(methods).filter(m -> m.getName().equals("plot")).collect(Collectors.toList());
        System.out.println(plotMethods.size());

        final List<Method> fourArg =
                plotMethods.stream().filter(m -> m.getParameterCount() == 4).collect(Collectors.toList());
        System.out.println(fourArg.size());

        // fourArg.forEach(System.out::println);

        final List<Method> table2nd =
                fourArg.stream().filter(m -> m.getParameterTypes()[1] == Table.class).collect(Collectors.toList());
        System.out.println(table2nd.size());

        for (Method method : table2nd) {
            System.out.println(method);
            System.out.println("Bridge: " + method.isBridge());
            System.out.println("Synthetic: " + method.isSynthetic());
        }

        System.exit(1);
    }
}
