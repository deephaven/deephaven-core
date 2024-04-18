//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;

/**
 * A request to compile a java class.
 */
@Immutable
@BuildableStyle
public abstract class QueryCompilerRequest {
    public static Builder builder() {
        return ImmutableQueryCompilerRequest.builder();
    }

    /**
     * @return the description to add to the query performance recorder nugget for this request
     */
    public abstract String description();

    /**
     * @return the class name to use for the generated class
     */
    public abstract String className();

    /**
     * @return the class body, before update with "$CLASS_NAME$" replacement and package name prefixing
     */
    public abstract String classBody();

    /**
     * @return the package name prefix
     */
    public abstract String packageNameRoot();

    /** Optional "log" for final class code. */
    public abstract Optional<StringBuilder> codeLog();

    /**
     * @return the generic parameters, empty if none required
     */
    public abstract Map<String, Class<?>> parameterClasses();

    String getPackageName(final String packageNameSuffix) {
        final String root = packageNameRoot();
        return root.isEmpty()
                ? packageNameSuffix
                : root + (root.endsWith(".") ? "" : ".") + packageNameSuffix;
    }

    public interface Builder {
        Builder description(String description);

        Builder className(String className);

        Builder classBody(String classBody);

        Builder packageNameRoot(String packageNameRoot);

        Builder codeLog(StringBuilder codeLog);

        Builder putAllParameterClasses(Map<String, ? extends Class<?>> parameterClasses);

        QueryCompilerRequest build();
    }
}
